//! Stopping a peer, in one order for every node.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use n0_future::task::JoinHandle;
use p2p::iroh::IrohTransport;
use p2p::P2PTransport;

const TASK_STOP_TIMEOUT: Duration = Duration::from_secs(1);
const ENDPOINT_STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Stops the peer once; later calls, from any clone, return immediately.
#[derive(Clone)]
pub struct IrohPeerShutdown {
    parts: Arc<Mutex<Option<Parts>>>,
}

struct Parts {
    transport: IrohTransport,
    coordinator: p2p::sync::SyncShutdownHandle,
    endpoint_task: JoinHandle<()>,
    retry_loop_task: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
}

impl IrohPeerShutdown {
    pub(super) fn new(
        transport: IrohTransport,
        coordinator: p2p::sync::SyncShutdownHandle,
        endpoint_task: JoinHandle<()>,
        retry_loop_task: JoinHandle<()>,
        tasks: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            parts: Arc::new(Mutex::new(Some(Parts {
                transport,
                coordinator,
                endpoint_task,
                retry_loop_task,
                tasks,
            }))),
        }
    }

    /// No new retries, then the coordinator's own work, then the endpoint,
    /// then the tasks that only consumed what those produced.
    pub async fn shutdown(&self) {
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(Parts {
            transport,
            coordinator,
            mut endpoint_task,
            retry_loop_task,
            tasks,
        }) = parts
        else {
            return;
        };

        stop_task(retry_loop_task).await;
        coordinator.shutdown().await;
        if let Err(error) = transport.shutdown().await {
            tracing::debug!(%error, "iroh transport shutdown returned an error");
        }
        drop(transport);

        for task in tasks {
            stop_task(task).await;
        }

        if n0_future::time::timeout(ENDPOINT_STOP_TIMEOUT, &mut endpoint_task)
            .await
            .is_err()
        {
            tracing::warn!("iroh endpoint did not stop after graceful shutdown; aborting");
            stop_task(endpoint_task).await;
        }
    }
}

async fn stop_task(task: JoinHandle<()>) {
    task.abort();
    let _ = n0_future::time::timeout(TASK_STOP_TIMEOUT, task).await;
}
