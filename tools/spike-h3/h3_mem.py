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
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT)
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--ops", type=int, default=4000)
    ap.add_argument("--rate", type=float, default=15.0)
    ap.add_argument("--docs", type=int, default=0, help="0 = a new doc per write; N = update N docs round-robin")
    ap.add_argument("--settle", type=int, default=240)
    ap.add_argument("--mode", choices=["replicate", "local"], default="replicate")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", required=True)
    ap.add_argument("--port-base", type=int, default=19180)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    writer = Node(a.binary, "writer", a.port_base, a.port_base + 1, a.out, p2p=(a.mode == "replicate"))
    receiver = (Node(a.binary, "receiver", a.port_base + 2, a.port_base + 3, a.out)
                if a.mode == "replicate" else None)
    target = receiver or writer
    nodes = [n for n in (writer, receiver) if n]
    mypids = {n.proc.pid for n in nodes}
    try:
        for n in nodes:
            wait_up(n.url)
            n.cli(a.binary, "client", "collection", "add", SCHEMA)
        if receiver:
            info = json.loads(receiver.cli(a.binary, "client", "p2p", "info").stdout)
            addr = info[0] if isinstance(info, list) else info
            r = writer.cli(a.binary, "client", "p2p", "replicator", "add", "-c", "Users", addr)
            if r.returncode != 0:
                raise RuntimeError(f"replicator add failed: {r.stderr}")
            time.sleep(3)

        samples, stop = [], threading.Event()
        t0 = time.time()
        counter = {"sent": 0, "failed": 0}

        def sample():
            while not stop.is_set():
                applied = applied_docs(target.url)
                samples.append({"t": round(time.time() - t0, 1),
                                "rss_target_kb": rss_kb(target.proc.pid),
                                "rss_writer_kb": rss_kb(writer.proc.pid),
                                "applied_docs": applied, "sent": counter["sent"],
                                "load1": load_avg(), "other_defra": other_defra(mypids)})
                stop.wait(10)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()

        blob = "x" * DOC_BYTES
        lock = threading.Lock()
        docids = []

        def work(worker_id):
            interval = a.workers / a.rate
            nxt = time.time()
            while True:
                with lock:
                    i = counter["sent"]
                    if i >= a.ops:
                        return
                    counter["sent"] += 1
                nxt += interval
                delay = nxt - time.time()
                if delay > 0:
                    time.sleep(delay)
                try:
                    if a.docs and len(docids) >= a.docs:
                        did = docids[i % a.docs]
                        gql(writer.url, f'mutation {{ update_Users(docID: "{did}", '
                                        f'input: {{age: {i % 90}, blob: "{blob}"}}) {{ _docID }} }}')
                    else:
                        d = gql(writer.url, f'mutation {{ create_Users(input: {{name: "u{i}", age: {i % 90}, '
                                            f'score: 1.5, blob: "{blob}"}}) {{ _docID }} }}')
                        rows = next(iter(d.values()))  # the node answers under add_Users
                        with lock:
                            docids.append(rows[0]["_docID"])
                except Exception as e:
                    with lock:
                        counter["failed"] += 1
                        counter.setdefault("first_error", f"{type(e).__name__}: {e}")

        threads = [threading.Thread(target=work, args=(w,)) for w in range(a.workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        load_end = time.time() - t0
        time.sleep(a.settle)
        stop.set()
        sampler.join(timeout=30)

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
