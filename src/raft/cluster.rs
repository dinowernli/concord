use crate::raft::raft_proto::raft_client::RaftClient;
use crate::raft::raft_proto::{ClusterConfig, Server};
use std::time::Duration;
use std::collections::{HashMap, HashSet};
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

    // Returns whether or not the supplied votes constitute a quorum, given the
    // current cluster configuration.
    pub fn is_quorum(&self, votes: &Vec<Server>) -> bool {
        let mut uniques = HashSet::new();

        // Always assume we vote for our outcome.
        uniques.insert(key(&self.me));

        for server in votes {
            uniques.insert(key(&server));
        }
        2 * uniques.len() > self.size()
    }

    // Returns the highest index which has been replicated to a sufficient quorum of
    // voting peer, and thus is safe to be committed.
    //
    // The supplied "matches" represents, for each known peer, the index up to which
    // its log is identical to ours.
    pub fn highest_replicated_index(&self, matches: HashMap<String, i64>) -> i64 {
        let mut indexes: Vec<i64> = matches.values().cloned().collect();
        indexes.sort();

        // Note that we've implicitly appended ourselves to the end of the list because
        // we assume no follower will be ahead of us (as leader) by construction.
        //
        // Examples:
        // * [1, 3],    leader (3) ==> (len = 2) => (mid = 1)
        // * [1, 1, 3], leader (3) ==> (len = 3) => (mid = 1)
        let mid = indexes.len() / 2;

        indexes[mid]
    }

    // Updates the cluster according to the supplied configuration.
    pub fn update(&mut self, config: ClusterConfig) {
        self.others = config
            .members
            .into_iter()
            .filter(|s| key(&s) != key(&self.me))
            .collect();
        self.channels.drain();
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
        let timeout = Duration::from_secs(1);
        let channel = Endpoint::new(dst)?
            .connect_timeout(timeout.clone())
            .timeout(timeout.clone())
            .connect()
            .await?;

        self.channels.insert(k, channel.clone());
        Ok(RaftClient::new(channel))
    }

    pub fn create_joint(&self, new_members: Vec<Server>) -> ClusterConfig {

        // TODO - DONOTLAND
        // This doesn't actually work because we need quorum among both the old
        // members and the new members, which is different from quorum among the
        // union. We probably want a special "joint" mode and to have the
        // consensus module ask the cluster whether there is sufficient quorum
        // based on a set of votes from different servers.

        let mut keys = HashSet::<String>::new();
        let mut union = Vec::<Server>::new();

        union.push(self.me.clone());
        keys.insert(key(&self.me));

        for server in self
            .others
            .iter()
            .cloned()
            .chain(new_members.iter().cloned())
        {
            let k = key(&server);
            if keys.contains(&k) {
                continue;
            }
            keys.insert(k);
            union.push(server);
        }

        ClusterConfig {
            members: union,
            members_next: new_members,
        }
    }
}

fn key(server: &Server) -> String {
    format!("{}:{}", server.host, server.port).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_cluster() -> Cluster {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        Cluster::new(s2.clone(), vec![s1, s2, s3].as_slice())
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
        let me = server("foo", 1234, "some-name");
        let cluster = Cluster::new(me.clone(), vec![me.clone()].as_slice());
        assert_eq!(cluster.size(), 1);
        assert!(cluster.others.is_empty());
    }

    #[test]
    fn test_dedup() {
        let s1 = server("foo", 1234, "name");
        let s2 = server("bar", 1234, "name");
        let s3 = server("baz", 1234, "name");

        // Create a cluster with a bunch of duplicates.
        let cluster = Cluster::new(
            s1.clone(),
            vec![
                s1.clone(),
                s1.clone(),
                s2.clone(),
                s2.clone(),
                s2.clone(),
                s3.clone(),
            ]
            .as_slice(),
        );

        assert_eq!(cluster.size(), 3);
    }

    #[test]
    fn test_quorum() {
        let cluster = create_cluster();
        assert_eq!(cluster.size(), 3);

        // No votes other than ourself, no quorum.
        assert!(!cluster.is_quorum(&Vec::new()));

        // One other vote, this is quorum.
        assert!(cluster.is_quorum(&vec![server("foo", 1234, "name1")]));

        // One other vote, but that's just also us. No quorum.
        let me = cluster.me.clone();
        assert!(!cluster.is_quorum(&vec![me]));
    }

    #[test]
    fn test_highest_replicated_index() {
        let cluster = create_cluster();
        assert_eq!(cluster.size(), 3);

        let data = HashMap::from([("server1".to_string(), 4), ("server2".to_string(), 3)]);
        assert_eq!(4, cluster.highest_replicated_index(data));

        let data = HashMap::from([
            ("server1".to_string(), 2),
            ("server2".to_string(), 2),
            ("server3".to_string(), 4),
            ("server4".to_string(), 5),
        ]);
        assert_eq!(4, cluster.highest_replicated_index(data));

        let data = HashMap::from([
            ("server1".to_string(), 2),
            ("server2".to_string(), 2),
            ("server3".to_string(), 3),
        ]);
        assert_eq!(2, cluster.highest_replicated_index(data));
    }

    #[test]
    fn test_update() {
        let mut cluster = create_cluster();
        assert_eq!(cluster.size(), 3);

        let new_config = ClusterConfig {
            members: vec![
                server("foo", 1234, "name"),
                server("bar", 1234, "name"),
                server("baz", 1234, "name"),
                server("wiggle", 1234, "name"),
                server("ziggle", 1234, "name"),
            ],
            members_next: vec![],
        };
        cluster.update(new_config);
        assert_eq!(cluster.size(), 5);
    }

    fn server(host: &str, port: i32, name: &str) -> Server {
        Server {
            host: host.to_string(),
            port,
            name: name.to_string(),
        }
    }
}
