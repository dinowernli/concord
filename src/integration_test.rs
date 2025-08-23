use crate::harness::Harness;
use crate::raft::raft_common_proto::Server;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(3);
const NAMES: [&str; 3] = ["A", "B", "C"];

#[tokio::test]
async fn test_start_and_elect_leader() {
    let harness = make_harness(NAMES.to_vec()).await;

    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_start_and_elect_leader_many_nodes() {
    let n = 17;
    let owned: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    let names: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    let harness = make_harness(names).await;

    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_disconnect_leader() {
    let harness = make_harness(NAMES.to_vec()).await;

    // Wait for the initial leader and capture its term and server.
    let (initial_term, first_leader) = harness.wait_for_leader(TIMEOUT, term_greater(0)).await;

    harness.failures().lock().await.disconnect(&first_leader);

    // Wait for a new leader (i.e, for a higher term).
    let (second_term, second_leader) = harness
        .wait_for_leader(TIMEOUT, term_greater(initial_term))
        .await;

    // Verify that it's not the disconnected node.
    assert_ne!(second_leader.name, first_leader.name);

    // Now reconnect the original leader, and disconnect the second one.
    harness.failures().lock().await.reconnect(&first_leader);
    harness.failures().lock().await.disconnect(&second_leader);

    // Wait for another new leader (i.e, for a higher term).
    let (_, third_leader) = harness
        .wait_for_leader(TIMEOUT, term_greater(second_term))
        .await;

    // Verify that it's not the disconnected node.
    assert_ne!(third_leader.name, second_leader.name);

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_commit() {
    let harness = make_harness(NAMES.to_vec()).await;
    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;
    let client = harness.make_client();

    let payload: &[u8] = "some-payload".as_bytes();
    let result = client.commit(payload).await;
    assert!(result.is_ok());

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_reconfigure_cluster() {
    let names = vec!["A", "B", "C", "D", "E"];
    let harness = make_harness(names.clone()).await;

    let (t1, leader1) = harness.wait_for_leader(TIMEOUT, term_greater(0)).await;
    let without_leader: Vec<&str> = names
        .iter()
        .copied()
        .filter(|s| *s != leader1.name)
        .collect();
    assert_eq!(without_leader.len(), 4);

    // Change cluster to contain only 3 members, and not including the current leader.
    let new_members: Vec<&str> = without_leader.iter().copied().take(3).collect();
    let result = harness.update_members(new_members.clone()).await;
    assert!(result.is_ok());

    // Wait for a new leader and verify.
    let (_, leader2) = harness.wait_for_leader(TIMEOUT, term_greater(t1)).await;
    assert_ne!(&leader2.name, &leader1.name);
    assert!(new_members.contains(&leader2.name.as_str()));

    harness.validate().await;
    harness.stop().await;
}

// Convenience method that returns a matcher for terms greater than a value.
fn term_greater(n: i64) -> Box<dyn Fn(&(i64, Server)) -> bool> {
    Box::new(move |(term, _)| *term > n)
}

async fn make_harness(nodes: Vec<&str>) -> Harness {
    let (harness, serving) = Harness::builder(nodes)
        .await
        .expect("builder")
        .build()
        .await
        .expect("harness");
    harness.start().await;
    tokio::spawn(async { serving.await });
    harness
}
