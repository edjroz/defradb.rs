#!/usr/bin/env python3
"""H3 reproduction: does a receiving Rust node's RSS grow per applied write?

Starts a writer + receiver pair (or one standalone node with --mode local),
drives writes, and samples the receiver's RSS every 10 s during load and for
the settle window. Reports MB per applied write, so runs at different rates
and under different machine load stay comparable.

Everything lands under --out: nodes.log, rss.jsonl, summary.json.
"""
import argparse, json, os, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.request, urllib.error

SCHEMA = "type Users { name: String age: Int score: Float blob: String }"
DOC_BYTES = 1200  # soak p0-crud profile (backbone crates/soak/src/generator.rs:56)


def gql(url, query, timeout=30):
    body = json.dumps({"query": query}).encode()
    req = urllib.request.Request(f"{url}/api/v0/graphql", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out = json.loads(r.read())
    if out.get("errors"):
        raise RuntimeError(out["errors"])
    return out["data"]


def wait_up(url, secs=60):
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{url}/health-check", timeout=2).read()
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} never came up")


def rss_kb(pid):
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout.strip() or 0)


def load_avg():
    return os.getloadavg()[0]


def other_defra(mypids):
    out = subprocess.run(["ps", "-Ao", "pid,rss,comm"], capture_output=True, text=True).stdout
    n = 0
    for line in out.splitlines():
        f = line.split(None, 2)
        if len(f) == 3 and "defra" in f[2] and int(f[0]) not in mypids:
            n += 1
    return n


class Node:
    def __init__(self, binary, name, http_port, p2p_port, outdir, p2p=True, signing=False):
        self.name = name
        self.url = f"http://127.0.0.1:{http_port}"
        self.root = tempfile.mkdtemp(prefix=f"h3-{name}-")
        args = [binary, "--rootdir", self.root, "--url", f"127.0.0.1:{http_port}",
                "--no-log-color", "--log-output", "stdout", "--no-keyring",
                "start", "--store", "regolith", "--no-telemetry",
                "--no-encryption", "--no-searchable-encryption"]
        if not signing:
            args.append("--no-signing")
        args += (["--p2paddr", f"/ip4/127.0.0.1/tcp/{p2p_port}"] if p2p else ["--no-p2p"])
        self.log = open(os.path.join(outdir, f"{name}.log"), "w")
        env = dict(os.environ)
        if os.environ.get("H3_STACK_LOGGING"):
            env["MallocStackLogging"] = "1"
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT, env=env)
        self.args = args

    def cli(self, binary, *rest):
        return subprocess.run([binary, "--url", self.url.replace("http://", ""), "--no-keyring", *rest],
                              capture_output=True, text=True, timeout=60)

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.log.close()
        shutil.rmtree(self.root, ignore_errors=True)


def applied_docs(url):
    try:
        d = gql(url, "query { Users { _docID } }", timeout=120)
        return len(d["Users"])
    except Exception:
        return None


class MergeCounter:
    """Applied composite merges, read forward from the receiver log so a
    10-minute run does not re-scan a 40 MB file on every sample."""

    NEEDLE = b"Processing Composite delta"

    def __init__(self, path):
        self.path, self.pos, self.n = path, 0, 0

    def count(self):
        try:
            with open(self.path, "rb") as f:
                f.seek(self.pos)
                data = f.read()
        except FileNotFoundError:
            return self.n
        cut = data.rfind(b"\n") + 1  # keep a partial trailing line for next time
        self.n += data[:cut].count(self.NEEDLE)
        self.pos += cut
        return self.n


def store_files(root):
    """(*.sst count, total KB, wal KB) under a node root. regolith flushes a
    memtable into `sst/` and recycles `wal/` (engine/mod.rs:471-472), so an
    SST appearing is the rotation signal and the WAL is the live memtable's
    on-disk twin."""
    ssts = total = wal = 0
    for dirpath, _, names in os.walk(root):
        in_wal = os.path.basename(dirpath) == "wal"
        for name in names:
            try:
                size = os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                continue
            total += size
            wal += size if in_wal else 0
            ssts += name.endswith(".sst")
    return ssts, total // 1024, wal // 1024


def snapshot(pid, outdir, tag):
    """vmmap + heap for the receiver; `heap` reports live malloc'd bytes, so a
    growing RSS with a flat heap total means retention outside malloc."""
    for tool in ("vmmap", "heap"):
        args = ["vmmap", "-summary", str(pid)] if tool == "vmmap" else ["heap", str(pid)]
        with open(os.path.join(outdir, f"{tool}-{tag}.txt"), "w") as f:
            subprocess.run(args, stdout=f, stderr=subprocess.STDOUT, timeout=600)
    if os.environ.get("H3_STACK_LOGGING"):
        with open(os.path.join(outdir, f"malloc-calltree-{tag}.txt"), "w") as f:
            subprocess.run(["malloc_history", str(pid), "-callTree", "-consolidateAllBySymbol"],
                           stdout=f, stderr=subprocess.STDOUT, timeout=900)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--ops", type=int, default=4000)
    ap.add_argument("--rate", type=float, default=15.0)
    ap.add_argument("--docs", type=int, default=0, help="0 = a new doc per write; N = update N docs round-robin")
    ap.add_argument("--settle", type=int, default=240)
    ap.add_argument("--mode", choices=["replicate", "local"], default="replicate")
    ap.add_argument("--workers", type=int, default=4, help="load threads per writer node")
    ap.add_argument("--writer-nodes", type=int, default=1, help="writer nodes replicating into one receiver")
    ap.add_argument("--interval", type=float, default=10.0, help="seconds between RSS samples")
    ap.add_argument("--doc-bytes", type=int, default=DOC_BYTES)
    ap.add_argument("--random-blob", action="store_true",
                    help="fresh random hex payload per write, like the soak generator's alnum blob "
                         "(backbone crates/soak/src/generator.rs:336); the default 'xxxx' blob is "
                         "compressed away by regolith's LZ4 and never reaches the store")
    ap.add_argument("--out", required=True)
    ap.add_argument("--port-base", type=int, default=19180)
    ap.add_argument("--stack-logging", action="store_true", help="MallocStackLogging on the nodes, for malloc_history")
    ap.add_argument("--snapshot", action="store_true", help="vmmap+heap at load end and settle end")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.stack_logging:
        os.environ["H3_STACK_LOGGING"] = "1"

    writers = [Node(a.binary, "writer" if a.writer_nodes == 1 else f"writer{w}",
                    a.port_base + 2 * w, a.port_base + 2 * w + 1, a.out, p2p=(a.mode == "replicate"))
               for w in range(a.writer_nodes)]
    writer = writers[0]
    receiver = (Node(a.binary, "receiver", a.port_base + 2 * a.writer_nodes,
                     a.port_base + 2 * a.writer_nodes + 1, a.out)
                if a.mode == "replicate" else None)
    target = receiver or writer
    nodes = writers + ([receiver] if receiver else [])
    mypids = {n.proc.pid for n in nodes}
    try:
        for n in nodes:
            wait_up(n.url)
            n.cli(a.binary, "client", "collection", "add", SCHEMA)
        if receiver:
            info = json.loads(receiver.cli(a.binary, "client", "p2p", "info").stdout)
            addr = info[0] if isinstance(info, list) else info
            for w in writers:
                r = w.cli(a.binary, "client", "p2p", "replicator", "add", "-c", "Users", addr)
                if r.returncode != 0:
                    raise RuntimeError(f"replicator add failed on {w.name}: {r.stderr}")
            time.sleep(3)

        samples, stop = [], threading.Event()
        t0 = time.time()
        counter = {"sent": 0, "failed": 0}
        merges = MergeCounter(os.path.join(a.out, f"{target.name}.log"))

        def sample():
            while not stop.is_set():
                applied = applied_docs(target.url)
                ssts, store_kb, wal_kb = store_files(target.root)
                samples.append({"t": round(time.time() - t0, 1),
                                "rss_target_kb": rss_kb(target.proc.pid),
                                "rss_writer_kb": rss_kb(writer.proc.pid),
                                "applied_docs": applied, "merges": merges.count(),
                                "sst_files": ssts, "store_kb": store_kb, "wal_kb": wal_kb,
                                "sent": counter["sent"],
                                "load1": load_avg(), "other_defra": other_defra(mypids)})
                stop.wait(a.interval)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()

        blob = "x" * a.doc_bytes
        payload = (lambda: os.urandom(a.doc_bytes // 2).hex()) if a.random_blob else (lambda: blob)
        lock = threading.Lock()
        docids = {w.name: [] for w in writers}
        per_writer = {w.name: 0 for w in writers}

        def work(node):
            my_docids = docids[node.name]
            interval = a.workers / a.rate
            nxt = time.time()
            while True:
                with lock:
                    i = per_writer[node.name]
                    if i >= a.ops:
                        return
                    per_writer[node.name] += 1
                    counter["sent"] += 1
                nxt += interval
                delay = nxt - time.time()
                if delay > 0:
                    time.sleep(delay)
                try:
                    if a.docs and len(my_docids) >= a.docs:
                        did = my_docids[i % a.docs]
                        gql(node.url, f'mutation {{ update_Users(docID: "{did}", '
                                      f'input: {{age: {i % 90}, blob: "{payload()}"}}) {{ _docID }} }}')
                    else:
                        d = gql(node.url, f'mutation {{ create_Users(input: {{name: "{node.name}u{i}", '
                                          f'age: {i % 90}, score: 1.5, blob: "{payload()}"}}) {{ _docID }} }}')
                        rows = next(iter(d.values()))  # the node answers under add_Users
                        with lock:
                            my_docids.append(rows[0]["_docID"])
                except Exception as e:
                    with lock:
                        counter["failed"] += 1
                        counter.setdefault("first_error", f"{type(e).__name__}: {e}")

        threads = [threading.Thread(target=work, args=(n,)) for n in writers for _ in range(a.workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        load_end = time.time() - t0
        if a.snapshot:
            snapshot(target.proc.pid, a.out, "load-end")
        time.sleep(a.settle)
        if a.snapshot:
            snapshot(target.proc.pid, a.out, "settle-end")
        stop.set()
        sampler.join(timeout=30)

        with open(os.path.join(a.out, "du.txt"), "w") as f:
            for n in nodes:
                f.write(subprocess.run(["du", "-sk", n.root], capture_output=True, text=True).stdout)

        with open(os.path.join(a.out, "rss.jsonl"), "w") as f:
            for s in samples:
                f.write(json.dumps(s) + "\n")

        def slope(pairs):
            """Least squares slope of RSS (KB) on applied writes."""
            n = len(pairs)
            if n < 3:
                return None
            mx = sum(p[0] for p in pairs) / n
            my = sum(p[1] for p in pairs) / n
            den = sum((p[0] - mx) ** 2 for p in pairs)
            return None if den == 0 else round(sum((p[0] - mx) * (p[1] - my) for p in pairs) / den, 1)

        load_s = [s for s in samples if s["t"] <= load_end and s["applied_docs"] is not None]
        first, last = load_s[0], load_s[-1]
        d_rss = (last["rss_target_kb"] - first["rss_target_kb"]) / 1024.0
        d_applied = (last["applied_docs"] - first["applied_docs"]) if a.docs == 0 else None
        settle_s = [s for s in samples if s["t"] > load_end]
        summary = {
            "argv": vars(a), "node_args": {n.name: n.args for n in nodes},
            "load_end_s": round(load_end, 1), "sent": counter["sent"], "failed": counter["failed"],
            "first_error": counter.get("first_error"),
            "rss_start_mb": round(first["rss_target_kb"] / 1024, 1),
            "rss_load_end_mb": round(last["rss_target_kb"] / 1024, 1),
            "rss_peak_mb": round(max(s["rss_target_kb"] for s in samples) / 1024, 1),
            "rss_settle_end_mb": round(settle_s[-1]["rss_target_kb"] / 1024, 1) if settle_s else None,
            "applied_docs_start": first["applied_docs"], "applied_docs_load_end": last["applied_docs"],
            "applied_docs_settle_end": settle_s[-1]["applied_docs"] if settle_s else None,
            "merges_load_end": last["merges"], "merges_settle_end": settle_s[-1]["merges"] if settle_s else None,
            "sst_files_load_end": last["sst_files"], "sst_files_settle_end": settle_s[-1]["sst_files"] if settle_s else None,
            "kb_per_merge_settle_end": (round((settle_s[-1]["rss_target_kb"] - first["rss_target_kb"])
                                              / settle_s[-1]["merges"], 1)
                                        if settle_s and settle_s[-1]["merges"] else None),
            "sent_per_writer": dict(per_writer),
            "mb_per_min_load": round(d_rss / max(1e-9, last["t"] - first["t"]) * 60, 2),
            "kb_per_applied_write": round(d_rss * 1024 / d_applied, 1) if d_applied else None,
            "kb_per_sent_write": round(d_rss * 1024 / max(1, last["sent"] - first["sent"]), 1),
            "kb_per_applied_regression": slope([(s["applied_docs"], s["rss_target_kb"])
                                                for s in samples if s["applied_docs"] is not None]),
            "applied_peak": max((s["applied_docs"] for s in samples if s["applied_docs"] is not None), default=None),
            "rss_after_applied_plateau_mb": None,
            "load1_min_max": [min(s["load1"] for s in samples), max(s["load1"] for s in samples)],
            "other_defra_max": max(s["other_defra"] for s in samples),
        }
        with open(os.path.join(a.out, "summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        print(json.dumps(summary, indent=2))
    finally:
        for n in nodes:
            n.stop()


if __name__ == "__main__":
    main()
