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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_cluster() -> Cluster {
        let s1 = Server {
            host: "foo".to_string(),
            port: 1234,
            name: "a".to_string(),
        };
        let s2 = Server {
            host: "bar".to_string(),
            port: 1234,
            name: "a".to_string(),
        };
        let s3 = Server {
            host: "baz".to_string(),
            port: 1234,
            name: "a".to_string(),
        };
        Cluster::new(s2.clone(), vec![s1, s3].as_slice())
    }

    #[test]
    fn test_initial() {
        let cluster = create_cluster();
        assert_eq!(cluster.size(), 3);
        assert_eq!(cluster.others.len(), 2);
        assert!(cluster.leader().is_none());
    }

    #[test]
    fn test_leader() {
        let mut cluster = create_cluster();
        assert!(cluster.leader().is_none());

        let other = cluster.others[0].clone();
        cluster.record_leader(&other);

        let leader = cluster.leader();
        assert!(leader.is_some());
        let leader = leader.unwrap();
        assert_eq!(other.host, leader.host);
        assert_eq!(other.port, leader.port);
    }

    #[test]
    fn test_singleton_cluster() {
        let me = Server {
            host: "foo".to_string(),
            port: 1234,
            name: "a".to_string(),
        };
        let cluster = Cluster::new(me.clone(), vec![me.clone()].as_slice());
        assert_eq!(cluster.size(), 1);
        assert!(cluster.others.is_empty());
    }
}
