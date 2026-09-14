//! `JoinSet` operations that exist on tokio's set but not on the browser one.
//!
//! Both sets expose `poll_join_next` and `abort_all`, so these are written
//! against those two alone and behave identically on either target.

use std::future::poll_fn;

use futures::FutureExt;
use n0_future::task::{JoinError, JoinSet};

/// The next finished task, without waiting for one.
pub(super) fn try_join_next<T: 'static>(tasks: &mut JoinSet<T>) -> Option<Result<T, JoinError>> {
    poll_fn(|cx| tasks.poll_join_next(cx))
        .now_or_never()
        .flatten()
}

/// The next task to finish, or `None` once the set is empty.
pub(super) async fn join_next<T: 'static>(tasks: &mut JoinSet<T>) -> Option<Result<T, JoinError>> {
    poll_fn(|cx| tasks.poll_join_next(cx)).await
}

/// Abort every task and wait for each to observe it.
pub(super) async fn shutdown<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while join_next(tasks).await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn try_join_next_reports_only_finished_tasks() {
        let mut tasks = JoinSet::new();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        tasks.spawn(async move {
            let _ = released.await;
            1
        });

        assert!(try_join_next(&mut tasks).is_none());
        release.send(()).unwrap();
        assert_eq!(join_next(&mut tasks).await.unwrap().unwrap(), 1);
        assert!(try_join_next(&mut tasks).is_none());
    }

    #[tokio::test]
    async fn shutdown_empties_the_set() {
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<()>());
        tasks.spawn(std::future::pending::<()>());

        shutdown(&mut tasks).await;
        assert!(tasks.is_empty());
    }
}
