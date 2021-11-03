use crate::raft::raft_proto::Server;
use std::collections::HashMap;
use tonic::transport::Channel;

// Holds information about a Raft cluster.
pub struct Cluster {
    me: Server,
    others: Vec<Server>,
    channels: HashMap<String, Channel>,
    last_known_leader: Option<Server>,
}

impl Cluster {
    // Returns the last known leader of this cluster, if any. This information could
    // be stale.
    pub fn leader(&self) -> Option<Server> {
        self.last_known_leader.clone()
    }

    pub fn record_leader(&mut self, leader: &Server) {
        self.last_known_leader = Some(leader.clone());
    }
}
