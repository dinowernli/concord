use crate::harness::Harness;
use crate::raft::FailureOptions;
use std::time::Duration;
use tokio::time::sleep;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

const NAMES: [&str; 3] = ["A", "B", "C"];

#[tokio::test]
async fn test_start_and_elect_leader() {
    let (harness, serving) = Harness::builder(NAMES.to_vec())
        .await
        .expect("builder")
        .build(FailureOptions::no_failures())
        .await
        .expect("harness");

    harness.start().await;

    tokio::spawn(async { serving.await });

    let diag = harness.diagnostics();
    wait_for(DEFAULT_TIMEOUT, || async {
        let mut d = diag.lock().await;

        // Currently needed because this causes the diagnostics to collect leaders
        d.validate().await.expect("validate");

        // TODO(dino): technically racy if the first term yields no leader. Ideally
        // we'd wait for the cluster to have some term with a leader.
        let leader = d.get_leader(1);

        leader.is_some() && NAMES.contains(&leader.unwrap().name.as_str())
    })
    .await
    .expect("wait");

    // TODO(dino): Need to implement clean shutdown.
}

/// Waits for a condition to become true, up to the given `timeout_duration`.
/// Returns `Ok(())` if the condition is met in time, or `Err(())` on timeout.
async fn wait_for<F, Fut>(timeout_duration: Duration, mut condition: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if condition().await {
            return Ok(());
        }
        sleep(Duration::from_millis(300)).await;
    }
    Err(())
}
