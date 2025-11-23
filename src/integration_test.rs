use crate::harness::Harness;
use crate::keyvalue::keyvalue_proto::PutRequest;
use crate::raft::Options;
use crate::raft::raft_common_proto::Server;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(3);
const NAMES: [&str; 3] = ["A", "B", "C"];

#[tokio::test]
async fn test_start_and_elect_leader() {
    let harness = make_harness(&NAMES).await;

    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_start_and_elect_leader_many_nodes() {
    let n = 17;
    let owned: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
    let names: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    let harness = make_harness(&names).await;

    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_disconnect_leader() {
    let harness = make_harness(&NAMES).await;

    // Wait for the initial leader and capture its term and server.
    let (term1, leader1) = harness.wait_for_leader(TIMEOUT, term_greater(0)).await;
    let name1 = leader1.name.clone();
    harness.failures().lock().await.disconnect(name1.as_str());

    // Wait for a new leader (i.e, for a higher term).
    let (term2, leader2) = harness.wait_for_leader(TIMEOUT, term_greater(term1)).await;
    let name2 = leader2.name.clone();

    // Verify that it's not the disconnected node.
    assert_ne!(&name2, &name1);

    // Now reconnect the original leader, and disconnect the second one.
    harness.failures().lock().await.reconnect(name1.as_str());
    harness.failures().lock().await.disconnect(name2.as_str());

    // Wait for another new leader (i.e, for a higher term).
    let (_, leader3) = harness.wait_for_leader(TIMEOUT, term_greater(term2)).await;

    // Verify that it's not the disconnected node.
    assert_ne!(leader3.name, leader2.name);

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_commit() {
    let harness = make_harness(&NAMES).await;
    harness.wait_for_leader(TIMEOUT, term_greater(0)).await;
    let client = harness.make_raft_client();

    let payload: &[u8] = "some-payload".as_bytes();
    let result = client.commit(payload).await;
    assert!(result.is_ok());

    harness.validate().await;
    harness.stop().await;
}

#[tokio::test]
async fn test_reconfigure_cluster() {
    let names = vec!["A", "B", "C", "D", "E"];
    let harness = make_harness(&names).await;

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

#[tokio::test]
async fn test_keyvalue() {
    let harness = make_harness(&NAMES).await;
    let mut kv = harness.make_kv_client().await;

    let k1 = "k1".as_bytes().to_vec();
    let v1 = "v1".as_bytes().to_vec();
    kv.put(PutRequest {
        key: k1.clone(),
        value: v1.clone(),
    })
    .await
    .expect("put");

    let get_result = harness.wait_for_key(k1.as_slice(), TIMEOUT).await;
    let entry = get_result.entry.expect("entry");

    assert_eq!(&entry.key, &k1);
    assert_eq!(&entry.value, &v1);
}

#[tokio::test]
async fn test_snapshotting() {
    let raft_options = Options::default().with_compaction(5 * 1024 * 1024, 1000);
    let harness = make_harness_with_options(&NAMES, Some(raft_options)).await;

    // Disconnect a node that will later have to catch up.
    harness.failures().lock().await.disconnect("B");

    let mut client = harness.make_kv_client().await;
    let key = "snapshot-key".as_bytes().to_vec();

    for i in 0..20 {
        let large_payload: Vec<u8> = vec![i as u8; 3 * 1024 * 1024]; // 3MB payload
        client
            .put(PutRequest {
                key: key.clone(),
                value: large_payload,
            })
            .await
            .expect("commit");
    }

    // Reconnect the node.
    harness.failures().lock().await.reconnect("B");

    // Wait for node "B" to install a snapshot.
    let snapshot_info = harness.wait_for_snapshot("B", TIMEOUT).await;

    // Verify that the snapshot is small.
    assert!(snapshot_info.size_bytes > 0);
    assert!(snapshot_info.size_bytes < 10 * 1024 * 1024);

    harness.validate().await;
    harness.stop().await;
}

// Convenience method that returns a matcher for terms greater than a value.
fn term_greater(n: i64) -> Box<dyn Fn(&(i64, Server)) -> bool> {
    Box::new(move |(term, _)| *term > n)
}

async fn make_harness(nodes: &[&str]) -> Harness {
    make_harness_with_options(nodes, None).await
}

async fn make_harness_with_options(nodes: &[&str], options: Option<Options>) -> Harness {
    let mut builder = Harness::builder("test-cluster", nodes).await.expect("builder");

    if let Some(opts) = options {
        builder = builder.with_options(opts)
    }

    let (harness, serving) = builder.build().await.expect("harness");
    harness.start().await;
    tokio::spawn(async { serving.await });
    harness
}
