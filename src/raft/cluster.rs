use crate::raft::raft_proto::entry::Data::Config;
use crate::raft::raft_proto::raft_client::RaftClient;
use crate::raft::raft_proto::{ClusterConfig, Server};
use crate::raft::store::ConfigInfo;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Error};
use tracing::info;

// Holds information about a Raft cluster.
pub struct Cluster {
    me: Server,
    voters: HashMap<String, Server>,
    voters_next: HashMap<String, Server>,
    channels: HashMap<String, Channel>,
    last_known_leader: Option<Server>,
    config_info: Option<ConfigInfo>,
}

impl Cluster {
    // Creates a new cluster object from the supplied servers.
    pub fn new(me: Server, all: &[Server]) -> Self {
        Cluster {
            me,
            voters: server_map(all.to_vec()),
            voters_next: HashMap::new(),
            channels: HashMap::new(),
            last_known_leader: None,
            config_info: None,
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

    // Returns whether "me" is a voting member of the cluster.
    pub fn am_voting_member(&self) -> bool {
        let k = key(&self.me);
        self.voters.contains_key(k.as_str()) || self.voters_next.contains_key(k.as_str())
    }

    // Returns the addresses of all other members in the cluster.
    pub fn others(&self) -> Vec<Server> {
        let mut all = self.all();
        all.remove(key(&self.me).as_str());
        all.into_values().collect()
    }

    // Returns the number of participants in the cluster (including us).
    pub fn size(&self) -> usize {
        self.others().len() + 1
    }

    // Returns whether there is an ongoing cluster configuration transition.
    pub fn has_ongoing_mutation(&self) -> bool {
        !self.voters_next.is_empty()
    }

    // Returns whether or not the supplied votes constitute a quorum, given the
    // current cluster configuration.
    pub fn is_quorum(&self, votes: &Vec<Server>) -> bool {
        // First, get all the unique keys from the votes.
        let mut uniques = HashSet::new();
        uniques.insert(key(&self.me));
        for server in votes {
            uniques.insert(key(&server));
        }

        // Joint consensus means we must have quorum individually among both
        // sets of members if present (trivially true if voters_next is empty).
        if !is_quorum_among(&uniques, &self.voters) {
            return false;
        }
        if !is_quorum_among(&uniques, &self.voters_next) {
            return false;
        }
        true
    }

    // Returns the highest index which has been replicated to a sufficient quorum of
    // voting peer, and thus is safe to be committed.
    //
    // The supplied "matches" represents, for each known peer, the index up to which
    // its log is identical to ours.
    pub fn highest_replicated_index(&self, matches: HashMap<String, i64>) -> i64 {
        assert!(!matches.contains_key(key(&self.me).as_str()));
        let mut result = highest_replicated_index_among(&matches, &self.voters);
        if !self.voters_next.is_empty() {
            let other = highest_replicated_index_among(&matches, &self.voters_next);
            if other < result {
                result = other;
            }
        }
        result
    }

    // Updates the cluster's members based on the supplied latest cluster information.
    // Returns whether an update took place.
    pub fn update(&mut self, info: ConfigInfo) -> bool {
        if let Some(inner) = &self.config_info {
            if inner == &info {
                // Nothing to do.
                return false;
            }
        }

        // Only keep going if there's an actual config inside.
        let config;
        let index;
        match &info.latest {
            None => return false,
            Some(entry) => match &entry.data {
                Some(Config(c)) => {
                    index = entry.id.as_ref().expect("id").index;
                    config = c.clone();
                }
                _ => return false,
            },
        }

        self.config_info = Some(info.clone());
        if info.committed {
            // Apply the "next voters" part of the latest config.
            self.voters = server_map(config.voters_next);
            self.voters_next = HashMap::new();
        } else {
            // Apply the "joint consensus" version of the config.
            self.voters = server_map(config.voters);
            self.voters_next = server_map(config.voters_next);
        }
        // We could probably reuse some of these. Clear them all for now.
        self.channels.drain();

        info!(committed = info.committed, index, "new cluster config");
        true
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

    // Returns a cluster configuration that represents joint consensus between the
    // current voters and the supplied incoming voters. Must not be called if there
    // is already an ongoing cluster transition.
    pub fn create_joint(&self, new_voters: Vec<Server>) -> ClusterConfig {
        // For now, we only allow one transition at a time.
        assert!(!self.has_ongoing_mutation());
        ClusterConfig {
            voters: self.voters.values().cloned().collect(),
            voters_next: new_voters,
        }
    }

    // Returns all known members of the cluster.
    fn all(&self) -> HashMap<String, Server> {
        let mut result = HashMap::new();
        result.insert(key(&self.me), self.me.clone());
        result.extend(self.voters.clone());
        result.extend(self.voters_next.clone());
        result
    }
}

fn key(server: &Server) -> String {
    format!("{}:{}", server.host, server.port).to_string()
}

fn server_map(servers: Vec<Server>) -> HashMap<String, Server> {
    servers.into_iter().map(|s| (key(&s), s.clone())).collect()
}

fn is_quorum_among(votes: &HashSet<String>, members: &HashMap<String, Server>) -> bool {
    if members.is_empty() {
        return true;
    }

    // Count the number present in the members.
    let mut count = 0;
    for key in votes {
        if members.contains_key(key.as_str()) {
            count = count + 1;
        }
    }
    2 * count > members.len()
}

fn highest_replicated_index_among(
    matches: &HashMap<String, i64>,
    members: &HashMap<String, Server>,
) -> i64 {
    let mut indexes: Vec<i64> = Vec::new();
    for (key, _) in members {
        let match_index = matches.get(key.as_str()).cloned().unwrap_or(-1);
        indexes.push(match_index);
    }

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
        assert_eq!(cluster.others().len(), 2);
        assert!(cluster.leader().is_none());
    }

    #[test]
    fn test_leader() {
        let mut cluster = create_cluster();
        assert!(cluster.leader().is_none());

        let other = cluster.others()[0].clone();
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
        assert!(cluster.others().is_empty());
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

        let data = HashMap::from([("key1".to_string(), 4), ("key2".to_string(), 3)]);
        assert_eq!(4, cluster.highest_replicated_index(data));

        let data = HashMap::from([
            ("key1".to_string(), 2),
            ("key2".to_string(), 2),
            ("key3".to_string(), 4),
            ("key4".to_string(), 5),
        ]);
        assert_eq!(4, cluster.highest_replicated_index(data));

        let data = HashMap::from([
            ("key1".to_string(), 2),
            ("key2".to_string(), 2),
            ("key3".to_string(), 3),
        ]);
        assert_eq!(2, cluster.highest_replicated_index(data));
    }

    fn server(host: &str, port: i32, name: &str) -> Server {
        Server {
            host: host.to_string(),
            port,
            name: name.to_string(),
        }
    }
}
