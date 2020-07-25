use crate::cluster::raft;

use log::info;
use raft::Server;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    pub fn validate(&mut self) -> Result<(), String> {
        self.validate_leaders()?;

        // TODO(dino): Also collect (term, index, fprint) triples and validate.

        Ok(())
    }

    // Validates that across the execution history, all servers have a
    // compatible view of who was the leader for every term. Specifically,
    // there should be no term for which two servers recognize different peers
    // as the leader of the cluster.
    fn validate_leaders(&mut self) -> Result<(), String> {
        // TODO(dino): Start at -1 instead of 0 and add proper support for the case
        // where terms may have no leader at all (like term 0).
        let latest = match self.leaders.last_entry() {
            Some(e) => *e.key(),
            None => 0,
        };

        let mut term = latest;
        loop {
            term = term + 1;

            let mut candidate_opt: Option<Server> = None;
            for (_, server) in &self.servers {
                let s = server.lock().unwrap();

                let leader_opt = s.leaders.get(&term).clone();
                if leader_opt.is_none() {
                    // This server hasn't found a leader for this term yet. Stop.
                    return Ok(());
                }

                let leader = leader_opt.unwrap();
                match candidate_opt.clone() {
                    None => candidate_opt = Some(leader.clone()),
                    Some(candidate) => {
                        if &candidate != leader {
                            return Err(format!("Incompatible leader for term: {}", term));
                        }
                    }
                }
            }

            if candidate_opt.is_none() {
                return Ok(());
            }

            // All servers agree on the leader for this term, store it.
            let candidate = candidate_opt.unwrap().clone();
            self.leaders.insert(term, candidate.clone());
            info!(
                "Validated agreed leader for term {} to be {:?}",
                term, &candidate
            );
        }
    }
}

// Holds information about a single server's execution as part of a raft
// cluster over time.
pub struct ServerDiagnostics {
    // Keeps track of the leader for each term.
    leaders: HashMap<i64, Server>,
}

impl ServerDiagnostics {
    fn new() -> Self {
        ServerDiagnostics {
            leaders: HashMap::new(),
        }
    }

    // Called when the server acknowledges a leader for the supplied term.
    pub fn report_leader(&mut self, term: i64, leader: &Server) {
        let existing = self.leaders.get(&term);
        assert!(existing.is_none() || existing.unwrap() == leader);
        self.leaders.insert(term, leader.clone());
    }
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.get_host(), address.get_port())
}
