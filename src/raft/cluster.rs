use crate::raft::failure_injection::{ChannelInfo, FailureInjectionMiddleware, FailureOptions};
use crate::raft::raft_common_proto::entry::Data::Config;
use crate::raft::raft_common_proto::{ClusterConfig, Server};
use crate::raft::raft_service_proto::raft_client::RaftClient;
use crate::raft::store::ConfigInfo;

use crate::raft::error::{RaftError, RaftResult};
use async_std::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::debug;

pub type RaftClientType = RaftClient<FailureInjectionMiddleware<Channel>>;

// The outcome of an assessment of whether we have quorum based on a set of "aye" votes
// and a set of "nay" votes.
#[derive(PartialEq, PartialOrd, Clone, Debug)]
pub enum QuorumResult {
    // Indicates that a set of votes has achieved quorum.
    Yes,

    // Indicates that, based on votes, we cannot achieve quorum.
    No,

    // Indicates that we need more votes in order to decide whether we have quorum.
    Unknown,
}

// Returns a string key for the supplied server. Two server structs
// map to the same key if they are reachable at the same address. For
// instance, two servers differing only in name will share a key.
pub fn key(server: &Server) -> String {
    format!("{}:{}", server.host, server.port).to_string()
}

// Holds information about a Raft cluster.
#[derive(Clone)]
pub struct Cluster {
    me: Server,
    voters: HashMap<String, Server>,
    voters_next: HashMap<String, Server>,
    channels: HashMap<String, Channel>,
    last_known_leader: Option<Server>,
    config_info: Option<ConfigInfo>,
    failure: Arc<Mutex<FailureOptions>>,
}

impl Cluster {
    // Creates a new cluster object from the supplied servers.
    pub fn new(me: Server, all: &[Server]) -> Self {
        let failures = Arc::new(Mutex::new(FailureOptions::no_failures()));
        Self::new_with_failures(me, all, failures)
    }

    pub fn new_with_failures(
        me: Server,
        all: &[Server],
        failure: Arc<Mutex<FailureOptions>>,
    ) -> Self {
        Cluster {
            me,
            voters: server_map(all.to_vec()),
            voters_next: HashMap::new(),
            channels: HashMap::new(),
            last_known_leader: None,
            config_info: None,
            failure,
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
        self.is_voting_member(&self.me)
    }

    // Returns whether the supplied server is a voting member of the cluster.
    pub fn is_voting_member(&self, server: &Server) -> bool {
        let k = key(server);
        self.voters.contains_key(k.as_str()) || self.voters_next.contains_key(k.as_str())
    }

    // Returns whether "me" is eligible for candidacy as a leader. This is true if we're a
    // voting member and if there isn't a known next membership state where we're absent.
    pub fn am_eligible_candidate(&self) -> bool {
        let k = key(&self.me);
        if !self.voters_next.is_empty() && !self.voters_next.contains_key(k.as_str()) {
            // Known next membership state where "me" is absent.
            false
        } else {
            // Otherwise, we're happy as long as "me" is a voting member.
            self.am_voting_member()
        }
    }

    // Returns the addresses of all other members in the cluster.
    pub fn others(&self) -> Vec<Server> {
        let mut all = self.all();
        all.remove(key(&self.me).as_str());
        all.into_values().collect()
    }

    // Returns whether there is an ongoing cluster configuration transition.
    pub fn has_ongoing_mutation(&self) -> bool {
        !self.voters_next.is_empty()
    }

    // Returns whether the current votes constitute quorum for the current cluster config.
    pub fn has_quorum(&self, aye: &Vec<Server>, nay: &Vec<Server>) -> QuorumResult {
        let ayes: HashSet<_> = aye.iter().map(|s| key(s)).collect();
        let nays: HashSet<_> = nay.iter().map(|s| key(s)).collect();

        let voters_current = &self.voters.keys().cloned().collect();
        let voters_next = &self.voters_next.keys().cloned().collect();

        let quorum_current = has_quorum_among(&voters_current, &ayes, &nays);
        let quorum_next = has_quorum_among(&voters_next, &ayes, &nays);

        match (quorum_current, quorum_next) {
            (QuorumResult::Yes, QuorumResult::Yes) => QuorumResult::Yes,
            (QuorumResult::No, _) => QuorumResult::No,
            (_, QuorumResult::No) => QuorumResult::No,
            _ => QuorumResult::Unknown,
        }
    }

    // Returns the highest index which has been replicated to a sufficient quorum of
    // voting peer, and thus is safe to be committed.
    //
    // The supplied "matches" represents, for each known peer, the index up to which
    // its log is identical to ours.
    pub fn highest_replicated_index(&self, matches: HashMap<String, i64>) -> i64 {
        assert!(!matches.contains_key(key(&self.me).as_str()));
        let mut result = highest_replicated_index_among(&self.me, &matches, &self.voters);
        if !self.voters_next.is_empty() {
            let other = highest_replicated_index_among(&self.me, &matches, &self.voters_next);
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

        debug!(committed = info.committed, index, "new cluster config");
        true
    }

    // Returns an RPC client which can be used to contact the supplied peer.
    pub async fn new_client(&mut self, address: &Server) -> RaftResult<RaftClientType> {
        let channel_info = ChannelInfo::from_server(self.me.clone(), address.clone());
        let other = address.name.clone();

        let k = key(address);
        let cached = self.channels.get_mut(&k);
        let failure_options = self.failure.clone();
        if let Some(channel) = cached {
            // The "clone()" operation on channels is advertized as cheap and is the
            // recommended way to reuse channels.
            return Ok(RaftClient::new(wrap_channel(
                channel.clone(),
                failure_options,
                channel_info,
            )));
        }

        // Cache miss, create a new channel.
        let dst = format!("http://[{}]:{}", address.host, address.port);
        let timeout = Duration::from_secs(1);

        let endpoint = Endpoint::new(dst).map_err(|e| RaftError::ConnectionFailed {
            peer: other.clone(),
            source: e.into(),
        })?;
        let channel = endpoint
            .connect_timeout(timeout.clone())
            .timeout(timeout.clone())
            .connect()
            .await
            .map_err(|e| RaftError::ConnectionFailed {
                peer: other.clone(),
                source: e.into(),
            })?;
        self.channels.insert(k, channel.clone());

        Ok(RaftClient::new(wrap_channel(
            channel,
            failure_options.clone(),
            channel_info,
        )))
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

    // Returns all voting members of the cluster.
    fn all(&self) -> HashMap<String, Server> {
        let mut result = HashMap::new();
        result.extend(self.voters.clone());
        result.extend(self.voters_next.clone());
        result
    }

    // Convenience method for testing, indicates whether a set of votes is
    // sufficient for a quorum given the current configuration.
    #[cfg(test)]
    fn is_quorum(&self, votes: &Vec<Server>) -> bool {
        let nay = Vec::new();
        self.has_quorum(votes, &nay) == QuorumResult::Yes
    }
}

fn wrap_channel(
    channel: Channel,
    failure: Arc<Mutex<FailureOptions>>,
    channel_info: ChannelInfo,
) -> FailureInjectionMiddleware<Channel> {
    FailureInjectionMiddleware::new(channel, failure, channel_info)
}

fn server_map(servers: Vec<Server>) -> HashMap<String, Server> {
    servers.into_iter().map(|s| (key(&s), s.clone())).collect()
}

// Returns whether the supplied votes constitute a quorum (i.e. majority) among the supplied
// members. Callers mustn't pass the same string in both "aye" and "nay".
fn has_quorum_among(
    members: &HashSet<String>,
    aye: &HashSet<String>,
    nay: &HashSet<String>,
) -> QuorumResult {
    if members.is_empty() {
        return QuorumResult::Yes;
    }

    let member_count = members.len();
    let aye_count = aye.iter().filter(|&s| members.contains(s.as_str())).count();
    let nay_count = nay.iter().filter(|&s| members.contains(s.as_str())).count();
    let remaining = member_count - (aye_count + nay_count);

    // Check for majority.
    if 2 * aye_count > member_count {
        return QuorumResult::Yes;
    }

    // Even if all remaining votes are "aye", we don't get a majority. So a quorum
    // is no longer possible.
    if 2 * (aye_count + remaining) <= member_count {
        return QuorumResult::No;
    }

    QuorumResult::Unknown
}

fn highest_replicated_index_among(
    me: &Server,
    matches: &HashMap<String, i64>,
    members: &HashMap<String, Server>,
) -> i64 {
    let mut indexes: Vec<i64> = Vec::new();
    for (k, _) in members {
        if k == &key(me) {
            continue;
        }
        let match_index = matches.get(k.as_str()).cloned().unwrap_or(-1);
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
pub mod testing {
    use super::*;

    pub async fn create_local_client_for_testing(server: Server) -> RaftClientType {
        let dst = format!("http://[{}]:{}", server.host, server.port);
        let timeout = Duration::from_secs(1);
        let channel = Endpoint::new(dst.clone())
            .expect("endpoint")
            .connect_timeout(timeout.clone())
            .timeout(timeout.clone())
            .connect()
            .await
            .expect("channel");

        let name = "client-for-testing";
        let channel_info = ChannelInfo::from_client(name.to_string(), server.clone());
        RaftClient::new(wrap_channel(
            channel,
            Arc::new(Mutex::new(FailureOptions::no_failures())),
            channel_info,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::raft_common_proto::{Entry, EntryId};

    fn create_cluster() -> Cluster {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        Cluster::new(s2.clone(), vec![s1, s2, s3].as_slice())
    }

    #[test]
    fn test_key() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("foo", 1234, "name2");
        let s3 = server("bar", 1234, "name3");

        assert_eq!(key(&s1), key(&s1));
        assert_eq!(key(&s1), key(&s2));
        assert_ne!(key(&s1), key(&s3));
        assert_ne!(key(&s2), key(&s3));
    }

    #[test]
    fn test_all() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        let cluster = Cluster::new(
            s2.clone(),
            vec![s1.clone(), s2.clone(), s3.clone()].as_slice(),
        );

        assert!(cluster.all().contains_key(key(&s1).as_str()));
        assert!(cluster.all().contains_key(key(&s2).as_str()));
        assert!(cluster.all().contains_key(key(&s3).as_str()));
        assert_eq!(cluster.all().len(), 3);
    }

    #[test]
    fn test_initial() {
        let cluster = create_cluster();
        assert_eq!(cluster.voters.len(), 3);
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
        assert!(cluster.others().is_empty());
    }

    #[test]
    fn test_update() {
        let s1 = server("foo", 1234, "name");
        let s2 = server("bar", 1234, "name");
        let s3 = server("baz", 1234, "name");
        let s4 = server("wiggle", 1234, "name");
        let s5 = server("ziggle", 1234, "name");

        let mut c = create_cluster();

        let config_joint = entry_with_config(
            vec![s1.clone(), s2.clone(), s3.clone()],
            vec![s2.clone(), s4.clone(), s5.clone()],
        );
        let info_joint = ConfigInfo {
            latest: Some(config_joint),
            committed: true,
        };

        c.update(info_joint);

        assert!(!c.is_quorum(&vec![s1.clone(), s2.clone()]));
        assert!(c.is_quorum(&vec![s1.clone(), s2.clone(), s4.clone()]));

        // Because we've set commit:true, the cluster swaps straight to the new config.
        assert!(c.is_quorum(&vec![s2.clone(), s4.clone()]));
    }

    #[test]
    fn test_joint_consensus() {
        let s1 = server("foo", 1234, "name");
        let s2 = server("bar", 1234, "name");
        let s3 = server("baz", 1234, "name");
        let s4 = server("wiggle", 1234, "name");
        let s5 = server("ziggle", 1234, "name");

        let mut c = create_cluster();

        let config_joint = entry_with_config(
            vec![s1.clone(), s2.clone(), s3.clone()],
            vec![s2.clone(), s4.clone(), s5.clone()],
        );
        let info_joint = ConfigInfo {
            latest: Some(config_joint),
            committed: false,
        };

        c.update(info_joint);

        assert!(!c.is_quorum(&vec![s1.clone(), s2.clone()]));
        assert!(c.is_quorum(&vec![s1.clone(), s2.clone(), s4.clone()]));

        // Joint consensus, this is not enough for quorum.
        assert!(!c.is_quorum(&vec![s2.clone(), s4.clone()]));
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

        assert_eq!(cluster.voters.len(), 3);
    }

    #[test]
    fn test_quorum() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        let s4 = server("wiggle", 1234, "name2");
        let s5 = server("ziggle", 1234, "name2");

        let s6 = server("biggle", 1234, "not part of the cluster");

        let cluster = Cluster::new(
            s2.clone(),
            vec![s1.clone(), s2.clone(), s3.clone(), s4.clone(), s5.clone()].as_slice(),
        );

        assert!(!cluster.is_quorum(&Vec::new()));
        assert!(!cluster.is_quorum(&vec![s1.clone(), s2.clone()]));
        assert!(!cluster.is_quorum(&vec![s1.clone(), s3.clone()]));

        assert!(cluster.is_quorum(&vec![s1.clone(), s2.clone(), s3.clone()]));
        assert!(!cluster.is_quorum(&vec![s1.clone(), s2.clone(), s6.clone()]));
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_empty_membership_always_yes() {
        let members = set(&[]);
        assert_eq!(
            has_quorum_among(&members, &set(&["a"]), &set(&["b"])),
            QuorumResult::Yes
        );
    }

    #[test]
    fn test_single_member_aye_wins() {
        let members = set(&["a"]);
        let aye = &set(&["a"]);
        assert_eq!(
            has_quorum_among(&members, &aye, &set(&[])),
            QuorumResult::Yes
        );
    }

    #[test]
    fn test_single_member_nay_wins() {
        let members = set(&["a"]);
        assert_eq!(
            has_quorum_among(&members, &set(&[]), &set(&["a"])),
            QuorumResult::No
        );
    }

    #[test]
    fn test_majority_aye() {
        let members = set(&["a", "b", "c"]);
        let aye = set(&["a", "b"]);
        let nay = set(&[]);
        assert_eq!(has_quorum_among(&members, &aye, &nay), QuorumResult::Yes);
    }

    #[test]
    fn test_majority_nay() {
        let members = set(&["a", "b", "c"]);
        let aye = set(&[]);
        let nay = set(&["a", "b"]);
        assert_eq!(has_quorum_among(&members, &aye, &nay), QuorumResult::No);
    }

    #[test]
    fn test_not_enough_votes_yet_unknown() {
        let members = set(&["a", "b", "c"]);
        let aye = set(&["a"]);
        let nay = set(&[]);
        // With 1 aye, 0 nay, and 2 undecided, either side could still win.
        assert_eq!(
            has_quorum_among(&members, &aye, &nay),
            QuorumResult::Unknown
        );
    }

    #[test]
    fn test_invalid_votes_are_ignored() {
        let members = set(&["a", "b"]);
        let aye = set(&["c"]); // not in members
        let nay = set(&["b"]);
        // "c" is ignored, so aye=0, nay=1, remaining=1.
        // Nay is guaranteed to win → No.
        assert_eq!(has_quorum_among(&members, &aye, &nay), QuorumResult::No);
    }

    #[test]
    fn test_highest_replicated_index_odd() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        let cluster = Cluster::new(
            s2.clone(),
            vec![s1.clone(), s2.clone(), s3.clone()].as_slice(),
        );

        let data = HashMap::from([(key(&s1).to_string(), 4), (key(&s3).to_string(), 3)]);
        assert_eq!(4, cluster.highest_replicated_index(data));
    }

    #[test]
    fn test_highest_replicated_index_even() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        let s4 = server("wiggle", 1234, "name2");
        let cluster = Cluster::new(
            s2.clone(),
            vec![s1.clone(), s2.clone(), s3.clone(), s4.clone()].as_slice(),
        );

        let data = HashMap::from([
            (key(&s1).to_string(), 4),
            (key(&s3).to_string(), 3),
            (key(&s4).to_string(), 3),
        ]);
        assert_eq!(3, cluster.highest_replicated_index(data));
    }

    #[test]
    fn test_create_joint() {
        let s1 = server("foo", 1234, "name1");
        let s2 = server("bar", 1234, "name1");
        let s3 = server("baz", 1234, "name1");
        let voters = vec![s1.clone(), s2.clone(), s3.clone()];
        let cluster = Cluster::new(s2.clone(), voters.clone().as_slice());

        let s4 = server("wiggle", 1234, "name1");
        let s5 = server("ziggle", 1234, "name1");
        let new_voters = vec![s1.clone(), s4.clone(), s5.clone()];

        let config = cluster.create_joint(new_voters.clone());
        assert_eq!(sorted(config.voters), sorted(voters));
        assert_eq!(sorted(config.voters_next), sorted(new_voters));
    }

    fn sorted(input: Vec<Server>) -> Vec<Server> {
        let mut result = input;
        result.sort_by(|l, r| (&l.host, l.port).cmp(&(&r.host, r.port)));
        result
    }

    fn server(host: &str, port: i32, name: &str) -> Server {
        Server {
            host: host.to_string(),
            port,
            name: name.to_string(),
        }
    }

    fn entry_with_config(voters: Vec<Server>, voters_next: Vec<Server>) -> Entry {
        Entry {
            id: Some(EntryId { term: 1, index: 3 }),
            data: Some(Config(ClusterConfig {
                voters,
                voters_next,
            })),
        }
    }
}
