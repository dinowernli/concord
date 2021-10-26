use std::collections::{BTreeMap, HashMap};

use async_std::sync::{Arc, Mutex};
use log::info;

use raft_proto::Server;

use crate::raft::raft_proto;

// Holds information about the execution of a cluster over time. Can be used
// to perform various integrity checks based on the recorded data. For
// instance, there should be no term in the cluster's history where members
// disagree on who is the leader.
pub struct Diagnostics {
    servers: HashMap<String, Arc<Mutex<ServerDiagnostics>>>,
    leaders: BTreeMap<i64, Server>,
}

impl Diagnostics {
    // Returns a new instance which, initially, know about no servers.
    pub fn new() -> Self {
        Diagnostics {
            servers: HashMap::new(),
            leaders: BTreeMap::new(),
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

    // Performs a set of a sequence of checks on the data recorded by the
    // individual servers. Returns an error if any of the checks fail.
    pub async fn validate(&mut self) -> Result<(), String> {
        self.validate_leaders().await?;

        // TODO(dino): Also collect (term, index, fprint) triples and validate.

        Ok(())
    }

    // Validates that across the execution history, all servers have a
    // compatible view of who was the leader for every term. Specifically,
    // there should be no term for which two servers recognize different peers
    // as the leader of the cluster.
    async fn validate_leaders(&mut self) -> Result<(), String> {
        let latest = match self.leaders.last_entry() {
            Some(e) => *e.key(),
            None => -1,
        };

        let mut term = latest;
        loop {
            term = term + 1;

            let mut candidate: Option<Server> = None;
            for (_, server) in &self.servers {
                let s = server.lock().await;
                if s.latest_term().unwrap_or(-1) < term {
                    // This server hasn't seen a leader for this term yet. Stop.
                    return Ok(());
                }

                let leader_opt = s.leaders.get(&term).clone();
                if leader_opt.is_none() {
                    // This server has seen a leader for a later term, but not this one.
                    continue;
                }

                let leader = leader_opt.unwrap();
                match candidate.clone() {
                    None => candidate = Some(leader.clone()),
                    Some(c) => {
                        if &c != leader {
                            return Err(format!("Incompatible leader for term: {}", term));
                        }
                    }
                }
            }

            // At this point, all servers have moved on to seeing leaders for a future
            // term. Note that we may or may not have an actual leader.
            info!(
                "Validated agreed leader for term {} to be {:?}",
                term, &candidate
            );
            candidate.map(|c| self.leaders.insert(term, c.clone()));
        }
    }
}

// Holds information about a single server's execution as part of a raft
// cluster over time.
pub struct ServerDiagnostics {
    // Keeps track of the leader for each term.
    leaders: BTreeMap<i64, Server>,
}

impl ServerDiagnostics {
    fn new() -> Self {
        ServerDiagnostics {
            leaders: BTreeMap::new(),
        }
    }

    // Called when the server acknowledges a leader for the supplied term.
    pub fn report_leader(&mut self, term: i64, leader: &Server) {
        let existing = self.leaders.get(&term);
        assert!(existing.is_none() || existing.unwrap() == leader);
        self.leaders.insert(term, leader.clone());
    }

    fn latest_term(&self) -> Option<i64> {
        self.leaders.last_key_value().map(|(k, _)| k.clone())
    }
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.host, address.port)
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

        // Whole bunch of missing terms, then a conflict.

        d1.lock().await.report_leader(6, &f.s2);
        d2.lock().await.report_leader(6, &f.s2);
        d3.lock().await.report_leader(6, &f.s3);

        assert!(f.diag.validate().await.is_err());
    }

    fn make_server(host: &str, port: i16) -> Server {
        Server {
            host: host.to_string(),
            port: port as i32,
        }
    }
}
