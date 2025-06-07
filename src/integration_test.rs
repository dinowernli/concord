use std::time::Duration;
use tokio::time::sleep;
use crate::DEFAULT_FAILURE_OPTIONS;
use crate::harness::Harness;
use crate::raft::FailureOptions;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn test_something() {
    let (harness, serving) = Harness::builder(vec!["A", "B", "C"])
        .await
        .expect("builder")
        .build(FailureOptions::no_failures())
        .await
        .expect("harness");

    harness.start().await;

    tokio::spawn(async {
        serving.await
    });

    let start = tokio::time::Instant::now();
    let mut found = false;
    let diag = harness.diagnostics();
    while !found && start.elapsed() < DEFAULT_TIMEOUT {
        let d = diag.lock().await;
        if let Some(leader) = d.get_leader(1) {
            if leader.name == "A" || leader.name == "B" || leader.name == "C" {
                found = true;
                break;
            }
        }
        sleep(Duration::from_millis(300)).await;
    }

    assert!(found)


    // wait_for(DEFAULT_TIMEOUT, async move {
    //     let diag = harness.diagnostics().lock().await;
    //     if let Some(leader) = diag.get_leader(1) {
    //         if leader.name == "A" || leader.name == "B" || leader.name == "C" {
    //             return true
    //         }
    //     }
    //     false
    // }).await.expect("wait");
}

/// Waits for a condition to become true, up to the given `timeout_duration`.
/// Returns `Ok(())` if the condition is met in time, or `Err(())` on timeout.
async fn wait_for<F, Fut>(
    timeout_duration: Duration,
    mut condition: F,
) -> Result<(), ()>
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