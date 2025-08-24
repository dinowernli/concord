use async_std::sync::{Arc, Mutex};
use std::cmp::PartialEq;
use std::collections::btree_map::Entry::{Occupied, Vacant};
use std::collections::{BTreeMap, HashMap};

use crate::raft::raft_common_proto::Server;

// Holds information about the execution of a cluster over time. Can be used
// to perform various integrity checks based on the recorded data. For
// instance, there should be no term in the cluster's history where members
// disagree on who is the leader.
pub struct Diagnostics {
    servers: HashMap<String, Arc<Mutex<ServerDiagnostics>>>,

    // Maps a term number to information about the leader for that term.
    leaders: BTreeMap<i64, LeaderInfo>,

    // Historical record of validated commits, keyed by entry index.
    applied: BTreeMap<i64, CommitInfo>,

    // Maps a server name to a map of term to (index, size) for snapshot installs.
    snapshot_installs: BTreeMap<String, BTreeMap<i64, SnapshotInfo>>,

    // Holds the index of the first conflict in leader, if any.
    first_leader_conflict_index: Option<i64>,

    // Holds the index of the first conflict in applied entries, if any.
    first_applied_conflict_term: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum LeaderInfo {
    // Indicates that there is agreement on the leader.
    Leader(Server),

    // Indicates that at least two different servers were believed to be leaders
    // in the same term. Contains the two alleged leaders.
    Conflict(Vec<ConflictEntry>),
}

// The prost generated messages implement Debug, but via a macro rather than explicitly
// annotating "derive(Debug)" on the generated type. This means the "derive" macro above
// isn't able to figure out that Server actually implements Debug. This intermediate
// wrapper struct works around this.
//
// Apparently prost 0.14+ explicitly adds Debug, so we may be able to remove this once
// we upgrade.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ConflictEntry(Server);

#[derive(Clone, Debug, PartialEq)]
enum CommitInfo {
    Entry(AppliedCommit),
    Conflict(),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotInfo {
    pub size_bytes: usize,
    pub index: i64,
}

impl Diagnostics {
    // Returns a new instance which, initially, know about no servers.
    pub fn new() -> Self {
        Diagnostics {
            servers: HashMap::new(),
            leaders: BTreeMap::new(),
            applied: BTreeMap::new(),
            snapshot_installs: BTreeMap::new(),

            first_leader_conflict_index: None,
            first_applied_conflict_term: None,
        }
    }

    // Returns the ServerDiagnostics object for the supplied server, creating
    // one if necessary.
    pub fn get_server(&mut self, server: &Server) -> Arc<Mutex<ServerDiagnostics>> {
        let key = address_key(server);
        if !self.servers.contains_key(key.as_str()) {
            self.servers
                .insert(key.clone(), Arc::new(Mutex::new(ServerDiagnostics::new())));
        }
        self.servers.get(key.as_str()).unwrap().clone()
    }

    // Returns the latest leader of a term that has been recognized as leader by some
    // participant. Returns None if there is no such term and leader.
    pub fn latest_leader(&self) -> Option<(i64, Server)> {
        for (term, info) in self.leaders.iter().rev() {
            if let LeaderInfo::Leader(leader) = info {
                return Some((*term, leader.clone()));
            }
        }
        None
    }

    pub fn get_snapshot_installs(&self, server_name: &str) -> Option<&BTreeMap<i64, SnapshotInfo>> {
        self.snapshot_installs.get(server_name)
    }

    // Processes new information collected by the individual servers and runs sanity checks
    // on the cluster consensus, e.g., making sure that no servers disagree on the leader
    // of terms, on the contents of log entries, etc.
    pub async fn validate(&mut self) -> Result<(), String> {
        self.collect().await;

        self.validate_leaders().await?;
        self.validate_applied().await?;
        Ok(())
    }

    // Collects information from the individual diagnostics objects of the participants.
    pub async fn collect(&mut self) {
        self.collect_leaders().await;
        self.collect_applied().await;
        self.collect_snapshot_installs().await;
    }

    async fn collect_snapshot_installs(&mut self) {
        for (name, server_arc) in &self.servers {
            let mut s = server_arc.lock().await;
            if !s.snapshot_installs.is_empty() {
                let server_installs = self.snapshot_installs.entry(name.clone()).or_default();
                for (term, info) in s.snapshot_installs.iter() {
                    server_installs.insert(*term, info.clone());
                }
                s.snapshot_installs.clear();
            }
        }
    }

    async fn collect_leaders(&mut self) {
        for (_, server_arc) in &self.servers {
            let mut s = server_arc.lock().await;
            while let Some((term, leader)) = s.leaders.pop_first() {
                match self.leaders.entry(term) {
                    // We've seen this term before. Check for conflicts.
                    Occupied(mut occupied) => {
                        let existing = occupied.get().clone();
                        let updated = match existing {
                            LeaderInfo::Leader(other) => {
                                if &other == &leader {
                                    LeaderInfo::Leader(leader.clone())
                                } else {
                                    LeaderInfo::Conflict(vec![
                                        ConflictEntry(leader.clone()),
                                        ConflictEntry(other.clone()),
                                    ])
                                }
                            }
                            LeaderInfo::Conflict(c) => {
                                LeaderInfo::Conflict(append(c, ConflictEntry(leader.clone())))
                            }
                        };

                        if let LeaderInfo::Conflict(_) = &updated {
                            self.first_leader_conflict_index.get_or_insert(term);
                        }
                        occupied.insert(updated);
                    }

                    // First time seeing this term.
                    Vacant(vacant) => {
                        vacant.insert(LeaderInfo::Leader(leader));
                    }
                }
            }
        }
    }

    async fn collect_applied(&mut self) {
        for (_, server) in &self.servers {
            let mut s = server.lock().await;
            while let Some((index, commit)) = s.applied.pop_first() {
                let new_entry = match self.applied.get(&index) {
                    None => CommitInfo::Entry(commit.clone()),
                    Some(CommitInfo::Conflict()) => CommitInfo::Conflict(),
                    Some(CommitInfo::Entry(c)) => {
                        if c.digest == commit.digest {
                            CommitInfo::Entry(commit.clone())
                        } else {
                            CommitInfo::Conflict()
                        }
                    }
                };

                if &new_entry == &CommitInfo::Conflict() {
                    self.first_applied_conflict_term.get_or_insert(index);
                }

                self.applied.insert(index, new_entry);
            }
        }
    }

    // Returns whether any conflicts have been found for applied entries.
    async fn validate_leaders(&mut self) -> Result<(), String> {
        match self.first_leader_conflict_index {
            Some(term) => Err(format!("Found multiple leaders for term: {}", term)),
            None => Ok(()),
        }
    }

    // Returns whether any conflicts have been found for leaders.
    async fn validate_applied(&mut self) -> Result<(), String> {
        match self.first_applied_conflict_term {
            Some(index) => Err(format!("Found conflict for index: {}", index)),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct AppliedCommit {
    term: i64,
    index: i64,
    digest: u64,
}

// Holds information about a single server's execution as part of a raft
// cluster over time.
pub struct ServerDiagnostics {
    // Keeps track of the leader for each term.
    leaders: BTreeMap<i64, Server>,

    // Keeps track of all applied commits, in order.
    applied: BTreeMap<i64, AppliedCommit>,

    // Keeps track of all snapshot installs, mapping term to SnapshotInfo.
    snapshot_installs: BTreeMap<i64, SnapshotInfo>,
}

impl ServerDiagnostics {
    fn new() -> Self {
        ServerDiagnostics {
            leaders: BTreeMap::new(),
            applied: BTreeMap::new(),
            snapshot_installs: BTreeMap::new(),
        }
    }

    // Called when the server acknowledges a leader for the supplied term.
    pub fn report_leader(&mut self, term: i64, leader: &Server) {
        let existing = self.leaders.get(&term);
        assert!(existing.is_none() || existing.unwrap() == leader);
        self.leaders.insert(term, leader.clone());
    }

    // Called when the server installs a snapshot. The term and index refer to the last
    // entry included in the snapshot.
    pub fn report_snapshot_install(&mut self, term: i64, index: i64, size_bytes: i64) {
        self.snapshot_installs.insert(
            term,
            SnapshotInfo {
                index,
                size_bytes: size_bytes as usize,
            },
        );
    }

    pub fn report_apply(&mut self, term: i64, index: i64, digest: u64) {
        self.applied.insert(
            index,
            AppliedCommit {
                term,
                index,
                digest,
            },
        );
    }
}

fn address_key(address: &Server) -> String {
    format!("{}", &address.name)
}

fn append<T>(mut vec: Vec<T>, entry: T) -> Vec<T> {
    vec.push(entry);
    vec
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        s1: Server,
        s2: Server,
        s3: Server,
        diag: Diagnostics,
    }

    impl Fixture {
        fn new() -> Self {
            Fixture {
                s1: make_server("foo", 123),
                s2: make_server("bar", 123),
                s3: make_server("baz", 123),
                diag: Diagnostics::new(),
            }
        }
    }

    #[tokio::test]
    async fn test_validate_empty() {
        let mut f = Fixture::new();
        f.diag.validate().await.expect("validation should succeed");
    }

    #[tokio::test]
    #[should_panic]
    async fn test_catch_inconsistent_single_server() {
        let mut f = Fixture::new();

        let d1 = f.diag.get_server(&f.s1);
        d1.lock().await.report_leader(1, &f.s1);
        d1.lock().await.report_leader(1, &f.s2);
    }

    #[tokio::test]
    async fn test_validate_happy() {
        let mut f = Fixture::new();

        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);

        d1.lock().await.report_leader(2, &f.s1);
        d2.lock().await.report_leader(2, &f.s1);

        f.diag.validate().await.expect("validation should succeed");
    }

    #[tokio::test]
    async fn test_validate_failure() {
        let mut f = Fixture::new();

        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);
        let d3 = f.diag.get_server(&f.s3);

        d1.lock().await.report_leader(1, &f.s1);
        d2.lock().await.report_leader(1, &f.s1);
        d3.lock().await.report_leader(1, &f.s1);

        d1.lock().await.report_leader(2, &f.s2);
        d2.lock().await.report_leader(2, &f.s2);
        d3.lock().await.report_leader(2, &f.s3);

        assert!(f.diag.validate().await.is_err());
    }

    #[tokio::test]
    async fn test_validate_skips_gaps() {
        let mut f = Fixture::new();

        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);
        let d3 = f.diag.get_server(&f.s3);

        d1.lock().await.report_leader(1, &f.s1);
        d2.lock().await.report_leader(1, &f.s1);
        d3.lock().await.report_leader(1, &f.s1);

        // Bunch of missing terms, then a conflict.

        d1.lock().await.report_leader(6, &f.s2);
        d2.lock().await.report_leader(6, &f.s2);
        d3.lock().await.report_leader(6, &f.s3);

        assert!(f.diag.validate().await.is_err());
    }

    #[tokio::test]
    async fn test_validate_applied_happy_path() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);

        // Report some applied commits with matching digests
        d1.lock().await.report_apply(1, 1, 100);
        d2.lock().await.report_apply(1, 1, 100);

        d1.lock().await.report_apply(1, 2, 200);
        d2.lock().await.report_apply(1, 2, 200);

        // Validation should succeed
        f.diag.validate().await.expect("validation should succeed");
    }

    #[tokio::test]
    async fn test_validate_applied_detects_conflict() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);
        let d3 = f.diag.get_server(&f.s3);

        // First commit is good
        d1.lock().await.report_apply(1, 1, 100);
        d2.lock().await.report_apply(1, 1, 100);
        d3.lock().await.report_apply(1, 1, 100);

        // Second commit has a conflicting digest
        d1.lock().await.report_apply(1, 2, 200);
        d2.lock().await.report_apply(1, 2, 300); // Conflict here

        // Validation should fail
        let result = f.diag.validate().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Found conflict for index: 2");
    }

    #[tokio::test]
    async fn test_validate_applied_with_missing_entry() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);

        // First commit is good
        d1.lock().await.report_apply(1, 1, 100);
        d2.lock().await.report_apply(1, 1, 100);

        // One server is missing the next entry
        d1.lock().await.report_apply(1, 2, 200);

        f.diag.validate().await.expect("validation should succeed");
    }

    #[tokio::test]
    async fn test_validate_applied_multiple_commits() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);
        let d3 = f.diag.get_server(&f.s3);

        // All servers agree on 3 commits
        d1.lock().await.report_apply(1, 1, 10);
        d2.lock().await.report_apply(1, 1, 10);
        d3.lock().await.report_apply(1, 1, 10);

        d1.lock().await.report_apply(1, 2, 20);
        d2.lock().await.report_apply(1, 2, 20);
        d3.lock().await.report_apply(1, 2, 20);

        d1.lock().await.report_apply(2, 3, 30);
        d2.lock().await.report_apply(2, 3, 30);
        d3.lock().await.report_apply(2, 3, 30);

        // Validation should be successful
        f.diag.validate().await.expect("validation should succeed");
    }

    #[tokio::test]
    async fn test_validate_applied_leader_and_commit_checks_together() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);
        let d2 = f.diag.get_server(&f.s2);
        let d3 = f.diag.get_server(&f.s3);

        // Valid leader and valid commits for term 1.
        d1.lock().await.report_leader(1, &f.s1);
        d2.lock().await.report_leader(1, &f.s1);
        d3.lock().await.report_leader(1, &f.s1);
        d1.lock().await.report_apply(1, 1, 100);
        d2.lock().await.report_apply(1, 1, 100);
        d3.lock().await.report_apply(1, 1, 100);

        // Validation should succeed.
        f.diag.validate().await.expect("validation should succeed");

        // Check publicly observable state: The latest validated leader should be from term 1.
        assert_eq!(f.diag.latest_leader(), Some((1, f.s1.clone())));

        // Now, introduce a leader conflict in term 2.
        d1.lock().await.report_leader(2, &f.s2);
        d2.lock().await.report_leader(2, &f.s3); // Conflict here
        d3.lock().await.report_leader(2, &f.s2);

        // Also add a valid commit at index 2.
        d1.lock().await.report_apply(2, 2, 200);
        d2.lock().await.report_apply(2, 2, 200);
        d3.lock().await.report_apply(2, 2, 200);

        // The validation should now fail due to the leader conflict.
        let result = f.diag.validate().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Found multiple leaders for term: 2");

        // The latest validated leader should still be the one from term 1, as the conflict in term 2
        // prevented it from being validated.
        assert_eq!(f.diag.latest_leader(), Some((1, f.s1.clone())));
    }

    #[tokio::test]
    async fn test_report_and_collect_snapshot_install() {
        let mut f = Fixture::new();
        let d1 = f.diag.get_server(&f.s1);

        // Report a snapshot install.
        d1.lock().await.report_snapshot_install(1, 10, 1024);

        // Collect diagnostics.
        f.diag.collect().await;

        // Verify that the snapshot install is recorded.
        let installs = f.diag.get_snapshot_installs("foo").expect("installs");
        assert_eq!(installs.len(), 1);
        assert_eq!(
            installs.get(&1),
            Some(&SnapshotInfo {
                index: 10,
                size_bytes: 1024
            })
        );
    }

    fn make_server(host: &str, port: i16) -> Server {
        Server {
            host: host.to_string(),
            port: port as i32,
            name: host.to_string(),
        }
    }
}
