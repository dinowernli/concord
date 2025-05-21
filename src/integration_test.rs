use crate::harness::Harness;
use crate::raft::FailureOptions;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(3);
const NAMES: [&str; 3] = ["A", "B", "C"];
const CLUSTER_NAME: &str = "test-cluster";

#[tokio::test]
async fn test_start_and_elect_leader() {
    let harness = make_harness().await;

    // Match on any term >0, just looking for an established leader
    harness
        .wait_for_leader(TIMEOUT, |(term, _)| *term > 0)
        .await;

    harness.stop().await;
}

async fn make_harness() -> Harness {
    let wipe_persistence = true;
    let (harness, serving) = Harness::builder(NAMES.to_vec())
        .await
        .expect("builder")
        .build(
            CLUSTER_NAME,
            FailureOptions::no_failures(),
            wipe_persistence,
        )
        .await
        .expect("harness");
    harness.start().await;
    tokio::spawn(async { serving.await });
    harness
}
