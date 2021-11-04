use crate::raft::raft_proto::raft_client::RaftClient;
use crate::raft::raft_proto::Server;
use std::collections::HashMap;
use tonic::transport::{Channel, Endpoint, Error};

// Holds information about a Raft cluster.
pub struct Cluster {
    me: Server,
    others: Vec<Server>,
    channels: HashMap<String, Channel>,
    last_known_leader: Option<Server>,
}

impl Cluster {
    // Creates a new cluster object from the supplied servers.
    pub fn new(me: Server, all: &[Server]) -> Self {
        let mut map = HashMap::new();
        for server in all {
            let k = key(&server);
            if k == key(&me) {
                continue;
            }
            map.insert(k, server.clone());
        }
        Cluster {
            me,
            others: map.into_values().collect(),
            channels: HashMap::new(),
            last_known_leader: None,
        }
    }

    // Returns the last known leader of this cluster, if any. This information could
    // be stale.
    pub fn leader(&self) -> Option<Server> {
        self.last_known_leader.clone()
    }

    // Stores the fact that we have observed a new leader.
    pub fn record_leader(&mut self, leader: &Server) {
        self.last_known_leader = Some(leader.clone());
    }

    // Returns the address we are running on.
    pub fn me(&self) -> Server {
        self.me.clone()
    }

    // Returns the addresses of all other members in the cluster.
    pub fn others(&self) -> Vec<Server> {
        self.others.to_vec()
    }

    // Returns the number of participants in the cluster (including us).
    pub fn size(&self) -> usize {
        self.others.len() + 1
    }

    // Returns an rpc client which can be used to contact the supplied peer.
    pub async fn new_client(&mut self, address: &Server) -> Result<RaftClient<Channel>, Error> {
        let k = key(address);
        let cached = self.channels.get_mut(&k);
        if let Some(channel) = cached {
            // The "clone()" operation on channels is advertized as cheap and is the
            // recommended way to reuse channels.
            return Ok(RaftClient::new(channel.clone()));
        }

        // Cache miss, create a new channel.
        let dst = format!("http://[::1]:{}", address.port);
        let channel = Endpoint::new(dst)?.connect().await?;
        self.channels.insert(k, channel.clone());
        Ok(RaftClient::new(channel))
    }
}

fn key(server: &Server) -> String {
    format!("{}:{}", server.host, server.port).to_string()
}
