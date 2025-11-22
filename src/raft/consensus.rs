extern crate rand;

use async_std::stream::StreamExt;
use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::{Either, join_all};
use futures::stream::FuturesUnordered;
use rand::Rng;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::{Request, Response, Status};
use tracing::{Instrument, debug, error, info, info_span, instrument, warn};

use diagnostics::ServerDiagnostics;
use raft::StateMachine;
use raft::log::ContainsResult;
use raft::raft_common_proto::EntryId;
use raft::raft_service_proto::{AppendRequest, AppendResponse, VoteRequest, VoteResponse};
use raft::raft_service_proto::{CommitRequest, CommitResponse, StepDownRequest, StepDownResponse};
use raft::raft_service_proto::{InstallSnapshotRequest, InstallSnapshotResponse, RaftSnapshot};

use crate::raft;
use crate::raft::cluster::{Cluster, QuorumResult};
use crate::raft::cluster::{RaftClientType, key};
use crate::raft::consensus::CommitError::{Internal, NotLeader};
use crate::raft::consensus::RaftRole::Leader;
use crate::raft::diagnostics;
use crate::raft::error::{RaftError, RaftResult};
use crate::raft::failure_injection::FailureOptions;
use crate::raft::raft_common_proto::entry::Data;
use crate::raft::raft_common_proto::entry::Data::{Config, Payload};
use crate::raft::raft_common_proto::{ClusterConfig, Server};
use crate::raft::raft_service_proto;
use crate::raft::raft_service_proto::raft_server::Raft;
use crate::raft::raft_service_proto::{ChangeConfigRequest, ChangeConfigResponse};
use crate::raft::store::{LogSnapshot, Store};

const RPC_TIMEOUT_MS: u64 = 100;

// Parameters used to configure the behavior of a cluster participant.
#[derive(Debug, Clone)]
pub struct Options {
    // Timeout after which a server in follower state starts a new election.
    follower_timeout_ms: i64,

    // Timeout after which a server in candidate state declares its candidacy a
    // failure and starts a new election.
    candidate_timeouts_ms: i64,

    // How frequently a leader will wake up and replicate entries to followers.
    // Note that this also serves as the leader's heartbeat, so this should be
    // lower than the follower timeout.
    leader_replicate_ms: i64,

    // A total number of bytes to accumulate in payloads before triggering a
    // compaction, i.e., snapshotting the state machine and clearing the log
    // entries stored locally.
    compaction_threshold_bytes: i64,

    // How frequently to check whether or not a compaction is necessary.
    compaction_check_periods_ms: i64,
}

impl Options {
    pub fn default() -> Self {
        Options {
            follower_timeout_ms: 200,
            candidate_timeouts_ms: 300,
            leader_replicate_ms: 50,
            compaction_threshold_bytes: 10 * 1000 * 1000,
            compaction_check_periods_ms: 5000,
        }
    }

    pub fn with_compaction(self: Self, threshold_bytes: i64, check_period_ms: i64) -> Self {
        Self {
            follower_timeout_ms: self.follower_timeout_ms,
            candidate_timeouts_ms: self.candidate_timeouts_ms,
            leader_replicate_ms: self.leader_replicate_ms,
            compaction_threshold_bytes: threshold_bytes,
            compaction_check_periods_ms: check_period_ms,
        }
    }
}

// Canonical implementation of the raft service. Acts as one server among peers
// which form a cluster.
pub struct RaftImpl {
    address: Server,
    state: Arc<Mutex<RaftState>>,
}

impl RaftImpl {
    pub async fn new(
        server: &Server,
        all: &Vec<Server>,
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
        options: Options,
        failures: Arc<Mutex<FailureOptions>>,
    ) -> RaftResult<RaftImpl> {
        let snapshot_bytes = state_machine.lock().await.create_snapshot();

        // A config which gives us (once applied) all the initial members as voters.
        let config = ClusterConfig {
            voters: all.clone().to_vec(),
            voters_next: all.clone().to_vec(),
        };

        // TODO(dino) - Consider pre-populating the store / log with an initial
        // sentinel entry just containing the cluster config.
        let snapshot = LogSnapshot {
            data: snapshot_bytes,
            last: EntryId {
                term: -1,
                index: -1,
            },
            config,
        };

        let store = Store::new(
            state_machine,
            snapshot,
            diagnostics.clone(),
            options.compaction_threshold_bytes,
            server.name.as_str(),
        )
        .map_err(|e| RaftError::Initialization(format!("Failed to create store: {}", e)))?;

        let mut cluster =
            Cluster::new_with_failures(server.clone(), all.as_slice(), failures.clone());

        // Make sure we propagate the initial cluster info to the cluster.
        // TODO(dino): We probably want to just only support initializing with a cluster info.
        cluster.update(store.get_config_info());

        Ok(RaftImpl {
            address: server.clone(),
            state: Arc::new(Mutex::new(RaftState {
                options,
                store,
                cluster,
                diagnostics,

                role: RaftRole::Follower,
                followers: HashMap::new(),
                timer_guard: None,
            })),
        })
    }

    pub async fn start(&self) {
        let arc_state = self.state.clone();

        let mut state = self.state.lock().await;
        let term = state.term();
        debug!(term, "starting");
        state.become_follower(arc_state.clone(), term).await;

        tokio::spawn(async move {
            RaftImpl::compaction_loop(arc_state.clone()).await;
        });
    }

    async fn compaction_loop(arc_state: Arc<Mutex<RaftState>>) {
        loop {
            {
                let mut state = arc_state.lock().await;
                if state.role == RaftRole::Stopping {
                    return;
                }
                state.store.try_compact().await;
            }
            let period_ms = arc_state.lock().await.options.compaction_check_periods_ms;
            sleep(Duration::from_millis(add_jitter(period_ms))).await;
        }
    }

    // Keeps running elections until either the term changes, a leader has emerged,
    // or an own election has been won.
    async fn election_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let timeout_ms = arc_state.lock().await.options.candidate_timeouts_ms.clone();
        let mut term = term;

        loop {
            let eligible = arc_state.lock().await.cluster.am_eligible_candidate();

            if !eligible {
                // Wait for longer than the regular election timeout, mostly because servers
                // will remain in this state for as long as they are not part of the cluster.
                sleep(Duration::from_millis(add_jitter(2000))).await;
                continue;
            }

            // Try and actually get elected, stop the process on completion.
            if RaftImpl::run_election(arc_state.clone(), term).await {
                break;
            }

            // Not successful, try again in the next iteration with a new term.
            term = term + 1;
            sleep(Duration::from_millis(add_jitter(timeout_ms))).await;
        }
    }

    // Returns whether the election process has completed, either because we've
    // won the election or because we've detected a higher term. In particular,
    // if we return true, no further elections are required for this term.
    async fn run_election(arc_state: Arc<Mutex<RaftState>>, term: i64) -> bool {
        let (request, cluster) = {
            let mut state = arc_state.lock().await;

            if state.term() > term {
                return true;
            }
            if !state.cluster.am_voting_member() {
                debug!("Not a voting member, aborted election");
                return true;
            }

            // Prepare the election. Note that we don't reset the timer because this
            // would lead to cancelling our own ongoing execution.
            debug!(term, "starting election");
            let voted_for = state.cluster.me(); // We vote for ourselves.
            state.role = RaftRole::Candidate;
            state.store.update_term_info(term, &Some(voted_for)).await;

            // Take a snapshot of the cluster so we can release the state lock.
            let request = state.create_vote_request();
            let cluster = state.cluster.clone();
            (request, cluster)
        };

        // Request votes from all peers.
        let futs: Vec<_> = cluster
            .others()
            .iter()
            .map(|o| {
                let other = o.clone();
                let request = request.clone();
                let cluster = cluster.clone();

                async move {
                    let client = match cluster.new_client(&other).await {
                        Ok(client) => client,
                        Err(error) => return (other, Err(error)),
                    };
                    RaftState::request_vote(client, other.clone(), request).await
                }
            })
            .collect();

        // Process the results one-by-one as they arrive.
        let mut unordered: FuturesUnordered<_> = futs.into_iter().map(Box::pin).collect();
        let mut aye = vec![cluster.me().clone()];
        let mut nay = Vec::new();

        let mut won = false;
        while let Some((server, result)) = unordered.next().await {
            match result {
                Ok(response) => {
                    let other_term = response.term;
                    if other_term > term {
                        let mut state = arc_state.lock().await;
                        if state.term() > other_term {
                            // The world has moved on. Stop.
                            return true;
                        }

                        info!(term=term, other_term=other_term, role=?state.role, "detected higher term");
                        state.become_follower(arc_state.clone(), other_term).await;
                        return true;
                    }
                    if response.granted {
                        aye.push(server)
                    } else {
                        nay.push(server)
                    }
                }
                Err(error) => {
                    // TODO(dino): we could wrap the future in something that retries a few times
                    // before giving up. Right now, if the RPC fails, we assume no vote.
                    warn!("vote request failed: {}", error);
                    nay.push(server)
                }
            }

            match cluster.has_quorum(&aye, &nay) {
                QuorumResult::Unknown => continue,
                QuorumResult::No => {
                    debug!(term, yes=?aye, no=?nay, "lost election");
                    return false;
                }
                QuorumResult::Yes => {
                    debug!(term, yes=?aye, no=?nay, "won election");
                    won = true;
                    break;
                }
            }
        }

        let arc_state_copy = arc_state.clone();
        if won {
            let mut state = arc_state.lock().await;
            if state.term() > term {
                // The world has moved on. Stop.
                return true;
            }

            let me = state.cluster.me();

            state.role = Leader;
            state.create_follower_positions(true /* clear_existing */);
            state.timer_guard = None;
            state.cluster.record_leader(&me, term);

            tokio::spawn(async move {
                let span = info_span!("replicate", server=%me.name);
                RaftImpl::replicate_loop(arc_state_copy, term)
                    .instrument(span)
                    .await;
            });
            return true;
        }

        // We should never get here. If we lost the election, we return above.
        error!(term, yes=?aye, no=?nay, "bug in election logic, should not reach here");
        false
    }

    // Starts the main leader replication loop. The loop stops once the term has
    // moved on (or we otherwise detect we are no longer leader).
    async fn replicate_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let timeouts_ms = arc_state.lock().await.options.leader_replicate_ms.clone();
        let mut first_heartbeat_done = false;
        loop {
            {
                let arc_state_copy = arc_state.clone();
                let mut state = arc_state.lock().await;
                if state.term() > term {
                    debug!(term=state.term(), role=?state.role, "detected higher term");
                    return;
                }
                if state.role != Leader {
                    debug!(term=state.term(), role=?state.role, "no longer leader");
                    return;
                }
                if !state.cluster.am_voting_member() {
                    info!("no longer voting member, stepping down");
                    let t = state.term();
                    state.become_follower(arc_state_copy, t).await;
                    return;
                }
                match &state.diagnostics {
                    Some(d) => d.lock().await.report_leader(term, &state.cluster.me()),
                    _ => (),
                }
            }

            RaftImpl::replicate_entries(arc_state.clone(), term).await;
            if !first_heartbeat_done {
                debug!(term, role=?Leader, "established");
                first_heartbeat_done = true;
            }

            sleep(Duration::from_millis(add_jitter(timeouts_ms))).await;
        }
    }

    // Makes a single request to all followers, heartbeating them and replicating
    // any entries they don't have.
    async fn replicate_entries(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let futures: Vec<_> = {
            let state = arc_state.lock().await;
            if state.term() > term || state.role != Leader {
                return;
            }

            state
                .cluster
                .others()
                .into_iter()
                .map(|follower| {
                    let state_clone = arc_state.clone();
                    async move {
                        let result =
                            Self::replicate_to_follower(state_clone, follower.clone()).await;
                        if let Err(e) = result {
                            // Log errors for individual followers without stopping the leader.
                            debug!(peer = %follower.name, "Failed to replicate to follower: {}", e);
                        }
                    }
                })
                .collect()
        };

        join_all(futures).await;

        // After attempts, lock again to update the commit index.
        let mut state = arc_state.lock().await;
        if state.term() > term || state.role != Leader {
            return;
        }
        state.update_committed(arc_state.clone()).await
    }

    // Makes a single request to a follower, heartbeating them and replicating
    // any entries they don't have.
    async fn replicate_to_follower(
        arc_state: Arc<Mutex<RaftState>>,
        follower: Server,
    ) -> RaftResult<()> {
        let (cluster, request) = {
            let state = arc_state.lock().await;
            if state.role != Leader {
                return Err(RaftError::StaleState);
            }

            let next_index = state
                .followers
                .get(key(&follower).as_str())
                .ok_or(RaftError::Internal(format!(
                    "Follower {} not in state map",
                    follower.name
                )))?
                .next_index;

            // Snapshot the cluster so that we can drop the state lock.
            let cluster = state.cluster.clone();

            // Decide whether to send entries or a snapshot.
            if state.store.is_index_compacted(next_index) {
                let snapshot_request = state.create_snapshot_request();
                (cluster, Either::Left(snapshot_request))
            } else {
                let append_request = state.create_append_request(next_index);
                (cluster, Either::Right(append_request))
            }
        };

        // Perform the RPC outside the main state lock.
        let client = cluster.new_client(&follower).await?;
        match request {
            Either::Left(req) => Self::replicate_snapshot(client, arc_state, follower, req).await?,
            Either::Right(req) => Self::replicate_append(client, arc_state, follower, req).await?,
        }

        Ok(())
    }

    // Send a request to the follower (baked into "client") to send the supplied request
    // to install a snapshot.
    async fn replicate_snapshot(
        mut client: RaftClientType,
        arc_state: Arc<Mutex<RaftState>>,
        follower: Server,
        install_request: InstallSnapshotRequest,
    ) -> RaftResult<()> {
        let last = install_request
            .snapshot
            .as_ref()
            .ok_or(RaftError::missing("snapshot"))?
            .last_included_entry
            .as_ref()
            .ok_or(RaftError::missing("last"))?
            .clone();

        let mut request = Request::new(install_request);
        request.set_timeout(Duration::from_millis(RPC_TIMEOUT_MS));
        let name = follower.name.clone();
        let result = client
            .install_snapshot(request)
            .await
            .map_err(|status| RaftError::Rpc { peer: name, status })?;

        let mut state = arc_state.lock().await;
        let other_term = result.into_inner().term;
        if other_term > state.term() {
            info!(other_term, peer=%follower.name, role=?state.role, "detected higher term");
            state.become_follower(arc_state.clone(), other_term).await;
            return Ok(());
        }

        state
            .record_follower_matches(&follower, last.index)
            .map_err(|e| RaftError::Internal(format!("Failed to record follower matches: {}", e)))
    }

    // Send a request to the follower (baked into "client") to send the supplied request
    // to append entries we have but the follower might not.
    async fn replicate_append(
        mut client: RaftClientType,
        arc_state: Arc<Mutex<RaftState>>,
        follower: Server,
        append_request: AppendRequest,
    ) -> RaftResult<()> {
        let mut request = Request::new(append_request.clone());
        request.set_timeout(Duration::from_millis(RPC_TIMEOUT_MS));
        let peer = follower.clone().name;
        let result = client
            .append(request)
            .await
            .map_err(|status| RaftError::Rpc { peer, status })?;

        let mut state = arc_state.lock().await;
        if state.term() > append_request.term || state.role != Leader {
            debug!(term=state.term(), role=?state.role, "stale term");
            return Ok(());
        }

        let message = result.into_inner();
        let other_term = message.term;
        if other_term > state.term() {
            debug!(other_term, term=state.term(), role=?state.role, "detected higher term");
            state.become_follower(arc_state.clone(), other_term).await;
            return Ok(());
        }

        state.handle_append_response(&follower, &message, &append_request)
    }

    // Adds the supplied data to the store and waits for the commit to go through.
    // Returns the resulting entry id if the commit was successful, or a failure
    // status if the commit was unsuccessful (e.g., because we are no longer the
    // leader).
    async fn commit_internal(
        arc_state: Arc<Mutex<RaftState>>,
        data: Data,
    ) -> Result<EntryId, CommitError> {
        let term;
        let entry_id;
        let receiver;
        {
            let mut state = arc_state.lock().await;
            if state.role != Leader {
                return Err(NotLeader);
            }

            term = state.term();
            entry_id = state.store.append(term, data);
            receiver = state.store.add_listener(entry_id.index);

            // Latest appended/committed configs may have changed, update the cluster.
            let config_info = state.store.get_config_info();
            let updated = state.cluster.update(config_info);
            if updated {
                state.create_follower_positions(false /* clear_existing */);
            }
        }

        // Make an attempt to replicate the newly appended entry to followers. This is
        // optional an intended to cut the happy-path latency by avoiding having to wait
        // for the next organic heartbeat to happen.
        //
        // TODO(dino): should probably rate limit this to make sure lots of commit operations
        // don't cause lots of stray RPCs.
        tokio::spawn(async move {
            Self::replicate_entries(arc_state.clone(), term).await;
        });

        let committed = receiver.await;
        match committed {
            Ok(committed_id) => {
                if entry_id == committed_id {
                    Ok(entry_id)
                } else {
                    // A different entry got committed to this index. This means
                    // the leader must have changed, let the caller know.
                    Err(NotLeader)
                }
            }
            Err(_) => {
                // The sender went out of scope without ever being resolved. This can
                // happen in rare cases where the index we're interested in got compacted.
                // In this case we don't know whether the entry was committed.
                Err(Internal)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum CommitError {
    NotLeader,
    Internal,
}

// Holds the state a cluster leader tracks about its followers. Used to decide
// which entries to replicate to the follower.
#[derive(Debug, Clone, PartialEq)]
struct FollowerPosition {
    // Next log entry to send to the follower.
    next_index: i64,

    // Highest index known to be replicated on the follower.
    match_index: i64,

    // The actual server, for convenience.
    server: Server,
}

impl Display for FollowerPosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(next={},match={})", self.next_index, self.match_index)
    }
}

// Holds on to the handle of a timed operation and cancels it upon destruction.
// This allows callers to just replace the guard in order to refresh a timer,
// ensuring that there is only one timer active at any given time.
struct TimerGuard {
    name: String,
    handle: JoinHandle<()>,
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        self.handle.abort();
        debug!(name=%self.name, "aborted handle");
    }
}

#[derive(Debug, PartialEq, Clone)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
    Stopping,
}

struct RaftState {
    options: Options,
    store: Store,
    cluster: Cluster,

    // Volatile term state. The persistent term state is kept in "store"
    role: RaftRole,
    followers: HashMap<String, FollowerPosition>,
    timer_guard: Option<TimerGuard>,

    // If present, this instance will inform the diagnostics object of relevant
    // updates as they happen during execution.
    diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
}

impl RaftState {
    fn name(&self) -> String {
        self.cluster.me().name.to_string()
    }

    // Returns the canonical record of the current term for this instance.
    fn term(&self) -> i64 {
        self.store.term()
    }

    // Returns which candidate this instance voted for in the current term, if any.
    fn voted_for(&self) -> Option<Server> {
        self.store.voted_for()
    }

    // Returns a suitable initial map of follower positions. Meant to be called
    // by a new leader initializing itself.
    fn create_follower_positions(&mut self, clear_existing: bool) {
        if clear_existing {
            self.followers.clear();
        }

        let others = self.cluster.others();
        for server in others {
            let k = key(&server);
            if self.followers.contains_key(k.as_str()) {
                continue;
            }
            self.followers.insert(
                k,
                FollowerPosition {
                    // Optimistically start assuming next is the same as our own next.
                    next_index: self.store.next_index(),
                    match_index: -1,
                    server: server.clone(),
                },
            );
        }
    }

    // Returns an append request from a leader to a follower, appending entries starting
    // with the supplied index. Must only be called as leader, and the supplied index must
    // exist in our current log.
    fn create_append_request(&self, next_index: i64) -> AppendRequest {
        // This method should only get called if we know the index is present.
        assert!(!self.store.is_index_compacted(next_index));
        let previous = self.store.entry_id_at_index(next_index - 1);

        AppendRequest {
            term: self.term(),
            leader: Some(self.cluster.me()),
            previous: Some(previous.clone()),
            entries: self.store.get_entries_after(&previous),
            committed: self.store.committed_index(),
        }
    }

    // Returns a request which the leader can send to a follower in order to install the
    // same snapshot currently held on the leader.
    fn create_snapshot_request(&self) -> InstallSnapshotRequest {
        let snap = self.store.get_latest_snapshot();
        let data = snap.data.to_vec();
        let cluster_config = Some(snap.config);
        let last_included_entry = Some(snap.last.clone());

        let snapshot = RaftSnapshot {
            last_included_entry,
            data,
            cluster_config,
        };

        InstallSnapshotRequest {
            term: self.term(),
            leader: Some(self.cluster.me()),
            snapshot: Some(snapshot),
        }
    }

    async fn become_follower(&mut self, arc_state: Arc<Mutex<RaftState>>, term: i64) {
        debug!(term, "becoming follower");
        assert!(term >= self.term(), "Term should never decrease");

        self.store
            .update_term_info(term, &None /* voted_for */)
            .await;
        self.role = RaftRole::Follower;
        self.reset_follower_timer(arc_state.clone(), term + 1);
    }

    fn reset_follower_timer(&mut self, arc_state: Arc<Mutex<RaftState>>, next_term: i64) {
        let timeout_ms = self.options.follower_timeout_ms;
        let me = self.name();
        let term = self.term();
        let span = info_span!(parent: None, "election", server = %me);
        let task = sleep(Duration::from_millis(add_jitter(timeout_ms))).then(async move |_| {
            debug!(term, "follower timeout");
            RaftImpl::election_loop(arc_state.clone(), next_term).await;
        });
        self.timer_guard = Some(TimerGuard {
            name: self.name(),
            handle: tokio::spawn(task.instrument(span)),
        });
    }

    // Incorporates the provided response corresponding to the supplied request.
    // Must only be called for responses with a valid term.
    fn handle_append_response(
        &mut self,
        peer: &Server,
        response: &AppendResponse,
        request: &AppendRequest,
    ) -> RaftResult<()> {
        let follower = self.followers.get_mut(key(&peer).as_str());
        if follower.is_none() {
            info!(peer=%peer.name, "skipped response from unknown peer");
            return Ok(());
        }

        let follower = follower.unwrap();
        if !response.success {
            // The follower has rejected our entries, presumably because they could
            // not find the entry we sent as "previous". We repeatedly reduce the
            // "next" index until we hit a "previous" entry present on the follower.
            let old_next = follower.next_index;
            if response.next < follower.next_index {
                follower.next_index = response.next;
            } else {
                follower.next_index = follower.next_index - 1;
            }

            debug!(follower=%peer.name, state=%follower, old_next, "decremented");
            return Ok(());
        }

        // The follower has appended our entries, record the updated follower state.
        match request.entries.last() {
            Some(e) => self.record_follower_matches(&peer, e.id.as_ref().expect("id").index),
            None => Ok(()),
        }
    }

    // Called when, as a leader, we know that a follower's entries up to (and including)
    // match_index match our entries.
    fn record_follower_matches(&mut self, peer: &Server, match_index: i64) -> RaftResult<()> {
        let follower = self
            .followers
            .get_mut(key(&peer).as_str())
            .ok_or(RaftError::Internal(format!("Unknown peer {}", &peer.name)))?;

        let old_f = follower.clone();
        follower.match_index = match_index;
        follower.next_index = match_index + 1;

        if follower != &old_f {
            debug!(follower = %peer.name, state = %follower, "updated");
        }

        Ok(())
    }

    // Returns the highest index I such that each index at most I is replicated
    // to a majority of followers. In practice, this means that it is safe to
    // commit up to (and including) the result.
    fn compute_highest_majority_match(&self) -> i64 {
        let matches = self
            .followers
            .iter()
            .map(|(k, v)| (k.to_string(), v.match_index))
            .collect();
        self.cluster.highest_replicated_index(matches)
    }

    // Scans the state of our followers in the hope of finding a new index which
    // has been replicated to a majority. If such an index is found, this updates
    // the index this leader considers committed.
    async fn update_committed(&mut self, arc_state: Arc<Mutex<RaftState>>) {
        assert_eq!(self.role, RaftRole::Leader);
        let new_commit_index = self.compute_highest_majority_match();
        self.store.commit_to(new_commit_index).await;

        // The committing may have changed the latest configs. Update the cluster.
        self.update_cluster(arc_state).await;
    }

    // Feeds the latest state of stored config info into the cluster, giving the
    // cluster a chance to update itself.
    async fn update_cluster(&mut self, arc_state: Arc<Mutex<RaftState>>) {
        let config_info = self.store.get_config_info();
        self.cluster.update(config_info);

        if self.role == Leader && !self.cluster.am_voting_member() {
            debug!(role=?self.role, me=?self.cluster.me().name, "not voting member, stepping down");
            let term = self.term();
            self.become_follower(arc_state, term).await;
        }
    }

    // Returns which server is currently believed to be the leader. Returns absent if we don't
    // know who the leader is. Returns a pointer to this instance if we are leader.
    fn current_leader(&self) -> Option<Server> {
        let role = self.role.clone();
        let me = self.cluster.me();
        let member = self.cluster.am_voting_member();
        if role == Leader {
            return Some(me);
        }

        let record = self.cluster.last_known_leader();
        let term = self.term();

        record
            .map(|r| {
                if r.term != term {
                    None
                } else if r.leader == me && (role != Leader || !member) {
                    // Don't return ourselves as leader if we've just stepped down, or are not
                    // a voting member of the cluster.
                    None
                } else {
                    Some(r.leader)
                }
            })
            .flatten()
    }

    // Returns a request which a candidate can send in order to request the vote
    // of peer servers in an election.
    fn create_vote_request(&self) -> VoteRequest {
        VoteRequest {
            term: self.term(),
            candidate: Some(self.cluster.me()),
            last: Some(self.store.last_known_log_entry_id().clone()),
        }
    }

    // Requests a vote from a follower. Used to run leader elections.
    async fn request_vote(
        mut client: RaftClientType,
        other: Server,
        req: VoteRequest,
    ) -> (Server, RaftResult<VoteResponse>) {
        let mut request = Request::new(req.clone());
        request.set_timeout(Duration::from_millis(RPC_TIMEOUT_MS));
        let name = other.name.clone();
        let result = client
            .vote(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| RaftError::Rpc { peer: name, status });
        (other, result)
    }
}

#[tonic::async_trait]
impl Raft for RaftImpl {
    #[instrument(fields(server=%self.address.name),skip(self,request))]
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let mut state = self.state.lock().await;

        // Reject anything from an outdated term.
        if state.term() > request.term {
            return Ok(Response::new(VoteResponse {
                term: state.term(),
                granted: false,
            }));
        }

        // There could be former members of the cluster requesting votes even though they
        // are no longer voting members. Reject their votes.
        if let Some(server) = request.candidate.clone() {
            if !state.cluster.is_voting_member(&server) {
                return Ok(Response::new(VoteResponse {
                    term: state.term(),
                    granted: false,
                }));
            }
        }

        // If we're in an outdated term, we revert to follower in the new later
        // term and may still grant the requesting candidate our vote.
        if request.term > state.term() {
            state
                .become_follower(self.state.clone(), request.term)
                .await;
        }

        let candidate = request
            .candidate
            .clone()
            .ok_or(RaftError::missing("candidate"))?;
        let last_log_id = request.last.clone().ok_or(RaftError::missing("last"))?;

        let granted;
        let term = state.term();
        if state.voted_for().is_none() || &Some(candidate.clone()) == &state.voted_for() {
            let candidate_name = candidate.name.clone();
            if state.store.log_entry_is_up_to_date(&last_log_id) {
                state.store.update_voted_for(&Some(candidate.clone())).await;
                granted = true;
                debug!(term, candidate=?candidate_name, "granted vote");
            } else {
                debug!(term, candidate=?candidate_name, "denied vote because candidate is not up-to-date");
                granted = false;
            }
        } else {
            debug!(term, voted_for = ?state.voted_for().clone().map(|x| x.name), "denied vote, already voted");
            granted = false;
        }

        Ok(Response::new(VoteResponse {
            term: state.term(),
            granted,
        }))
    }

    #[instrument(fields(server=%self.address.name),skip(self,request))]
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let mut state = self.state.lock().await;

        // Handle the case where we are ahead of the leader. We inform the
        // leader of our (greater) term and fail the append.
        if state.term() > request.term {
            return Ok(Response::new(AppendResponse {
                term: state.term(),
                success: false,
                next: state.store.next_index(),
            }));
        }

        // If the leader's term is greater than ours, we update ours to match
        // by resetting to a "clean" follower state for the leader's (greater)
        // term. Note that we then handle the leader's append afterwards.
        if request.term > state.term() {
            state
                .become_follower(self.state.clone(), request.term)
                .await;
        }
        let term = state.term();

        // Record the latest leader.
        let leader = request.leader.clone().ok_or(RaftError::missing("leader"))?;
        state.cluster.record_leader(&leader, term);
        match &state.diagnostics {
            Some(d) => d.lock().await.report_leader(term, &leader),
            _ => (),
        }

        // Reset the election timer
        let next_term = term + 1;
        state.reset_follower_timer(self.state.clone(), next_term);

        // Make sure we have the previous log index sent. Note that COMPACTED
        // can happen whenever we have no entries (e.g.,initially or just after
        // a snapshot install).
        let previous = &request.previous.ok_or(RaftError::missing("previous"))?;
        let next_index = state.store.next_index();
        if state.store.log_contains(previous) == ContainsResult::ABSENT {
            // Let the leader know that this entry is too far in the future, so
            // it can try again from with earlier index.
            return Ok(Response::new(AppendResponse {
                term,
                success: false,
                next: next_index,
            }));
        }

        // Store all the entries received.
        if !request.entries.is_empty() {
            state.store.append_all(request.entries.as_slice()).await;
        }

        // If the leader considers an entry committed, it is guaranteed that
        // all members of the cluster agree on the log up to that index, so it
        // is safe to apply the entries to the state machine.
        let leader_commit_index = request.committed;
        state.store.commit_to(leader_commit_index).await;

        // The appending and committing may have changed the latest configs. Update the cluster.
        state.update_cluster(self.state.clone()).await;

        debug!("handled request");
        Ok(Response::new(AppendResponse {
            term,
            success: true,
            next: state.store.next_index(),
        }))
    }

    #[instrument(fields(server=%self.address.name),skip(self,request))]
    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let result = RaftImpl::commit_internal(self.state.clone(), Payload(request.payload)).await;
        let leader = self.state.lock().await.current_leader();
        match result {
            Ok(entry_id) => Ok(Response::new(CommitResponse {
                entry_id: Some(entry_id),
                status: raft_service_proto::Status::Success as i32,
                leader,
            })),
            Err(NotLeader) => Ok(Response::new(CommitResponse {
                entry_id: None,
                status: raft_service_proto::Status::NotLeader as i32,
                leader,
            })),
            Err(Internal) => Err(Status::internal("internal error while committing")),
        }
    }

    #[instrument(fields(server=%self.address.name),skip(self,request))]
    async fn step_down(
        &self,
        request: Request<StepDownRequest>,
    ) -> Result<Response<StepDownResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let mut state = self.state.lock().await;
        let leader = state.current_leader();
        if state.role != RaftRole::Leader {
            return Ok(Response::new(StepDownResponse {
                status: raft_service_proto::Status::NotLeader as i32,
                leader,
            }));
        }

        let term = state.term();
        state.become_follower(self.state.clone(), term).await;

        Ok(Response::new(StepDownResponse {
            status: raft_service_proto::Status::Success as i32,
            leader: Some(state.cluster.me()),
        }))
    }

    #[instrument(fields(server=?self.address.name),skip(self,request))]
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let snapshot = request.snapshot.ok_or(RaftError::missing("snapshot"))?;
        let last = snapshot
            .last_included_entry
            .clone()
            .ok_or(RaftError::missing("last"))?;
        let config = snapshot
            .cluster_config
            .clone()
            .ok_or(RaftError::missing("config"))?;
        let image = Bytes::from(snapshot.data.to_vec());
        let image_len_bytes = image.len() as i64;

        let store_snapshot = LogSnapshot {
            last,
            data: image,
            config,
        };

        let mut state = self.state.lock().await;
        if request.term >= state.term() && !snapshot.data.is_empty() {
            // First make sure the store has caught up to the snapshot.
            let status = state.store.install_snapshot(store_snapshot.clone()).await;
            if status.code() != tonic::Code::Ok {
                return Err(status);
            }

            // The config_info is expected to be exactly what's contained in
            // snapshot.config_info, but we always only update the cluster with config
            // info instances produced by the store, so let's do that here as well.
            let config_info = state.store.get_config_info();
            state.cluster.update(config_info);

            if let Some(diagnostics) = &state.diagnostics {
                diagnostics.lock().await.report_snapshot_install(
                    request.term,
                    last.index,
                    image_len_bytes,
                );
            }
        }
        Ok(Response::new(InstallSnapshotResponse {
            term: state.term(),
        }))
    }

    #[instrument(fields(server=?self.address.name),skip(self,request))]
    async fn change_config(
        &self,
        request: Request<ChangeConfigRequest>,
    ) -> Result<Response<ChangeConfigResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

        let joint_config;
        {
            let state = self.state.lock().await;
            let leader = state.current_leader();
            if state.role != RaftRole::Leader {
                return Ok(Response::new(ChangeConfigResponse {
                    status: raft_service_proto::Status::NotLeader as i32,
                    leader,
                }));
            }
            if state.cluster.has_ongoing_mutation() {
                return Err(Status::already_exists(
                    "Can't clobber ongoing cluster mutation",
                ));
            }
            joint_config = state.cluster.create_joint(request.members.to_vec());
        }

        let joint = joint_config.clone();
        let result = RaftImpl::commit_internal(self.state.clone(), Config(joint_config)).await;

        debug!(?joint, "committed config");

        let leader = self.state.lock().await.current_leader();
        match result {
            Ok(_) => Ok(Response::new(ChangeConfigResponse {
                status: raft_service_proto::Status::Success as i32,
                leader,
            })),
            Err(NotLeader) => Ok(Response::new(ChangeConfigResponse {
                status: raft_service_proto::Status::NotLeader as i32,
                leader,
            })),
            Err(Internal) => Err(Status::internal("failed to commit config")),
        }
    }
}

// Returns a value no lower than the supplied bound, with some additive jitter.
fn add_jitter(lower: i64) -> u64 {
    let mut rng = rand::thread_rng();
    let upper = (lower as f64 * 1.3) as i64;
    rng.gen_range(lower..upper) as u64
}

#[cfg(test)]
mod tests {
    use crate::raft::cluster::testing::create_local_client_for_testing;
    use crate::raft::raft_common_proto::Entry;
    use crate::raft::raft_common_proto::entry::Data;
    use crate::raft::raft_service_proto::raft_server::RaftServer;
    use crate::raft::testing::FakeStateMachine;
    use crate::testing::TestRpcServer;

    use super::*;

    fn make_cluster_config() -> ClusterConfig {
        ClusterConfig {
            voters: vec![create_server(1), create_server(2), create_server(3)],
            voters_next: vec![],
        }
    }

    #[tokio::test]
    async fn test_initial_state() {
        let raft = create_raft().await;
        let state = raft.state.lock().await;
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.term(), 0);
    }

    #[tokio::test]
    async fn test_initial_membership() {
        let raft = create_raft().await;
        let state = raft.state.lock().await;

        // Part of initial member list
        assert!(state.cluster.is_voting_member(&create_server(1)));
        assert!(state.cluster.is_voting_member(&create_server(2)));
        assert!(state.cluster.is_voting_member(&create_server(3)));

        // Not part of initial member list
        assert!(!state.cluster.is_voting_member(&create_server(4)));
    }

    // This test verifies that initially, a follower fails to accept entries
    // too far in the future. Then, after installing an appropriate snapshot,
    // sending those same entries succeeds.
    #[tokio::test]
    async fn test_load_snapshot_and_append() {
        let raft = create_raft().await;
        let raft_state = raft.state.clone();
        let server = TestRpcServer::run(RaftServer::new(raft)).await;

        // Make an append request coming from a supposed leader, for a bunch of
        // entries far in the future.
        let leader = create_fake_server_list()[1].clone();

        let append_request = AppendRequest {
            term: 12,
            leader: Some(leader.clone()),
            previous: Some(entry_id(10, 75)),
            entries: vec![
                entry(entry_id(10, 76), Vec::new()),
                entry(entry_id(10, 77), Vec::new()),
            ],
            committed: 0,
        };

        let dst = server.address().expect("server");
        let mut client = create_local_client_for_testing(dst).await;
        let append_response_1 = client
            .append(Request::new(append_request.clone()))
            .await
            .expect("request")
            .into_inner();

        // Make sure the handler has updated its term.
        assert_eq!(append_response_1.term, 12);

        // The entries were too far in the future, the append should fail.
        assert!(!append_response_1.success);

        // The raft server should acknowledge the new leader.
        {
            let state = raft_state.lock().await;
            assert_eq!(state.current_leader(), Some(leader.clone()));
        }

        // Now install a snapshot which should make the original append request
        // valid.
        let snapshot = RaftSnapshot {
            last_included_entry: Some(entry_id(10, 75)),
            data: vec![1, 2],
            cluster_config: Some(make_cluster_config()),
        };
        let snapshot_request = InstallSnapshotRequest {
            leader: Some(leader.clone()),
            term: 12,
            snapshot: Some(snapshot),
        };
        let install_response_1 = client
            .install_snapshot(snapshot_request.clone())
            .await
            .expect("snap")
            .into_inner();
        assert_eq!(install_response_1.term, 12);

        // Then, try the same append request again. This time, it should work.
        let append_response_2 = client
            .append(append_request.clone())
            .await
            .expect("append")
            .into_inner();
        assert!(append_response_2.success);
    }

    #[tokio::test]
    async fn test_install_snapshot_updates_cluster_config() {
        let raft = create_raft().await;
        let raft_state = raft.state.clone();
        let server = TestRpcServer::run(RaftServer::new(raft)).await;
        let dst = server.address().expect("server");
        let mut client = create_local_client_for_testing(dst).await;

        let s1 = create_server(1);
        let s2 = create_server(2);
        let s3 = create_server(3);
        let s4 = create_server(4);
        let s5 = create_server(5);

        {
            let state = raft_state.lock().await;

            assert!(state.cluster.is_voting_member(&s1));
            assert!(state.cluster.is_voting_member(&s2));
            assert!(state.cluster.is_voting_member(&s3));

            assert!(!state.cluster.is_voting_member(&s4));
            assert!(!state.cluster.is_voting_member(&s5));
        }

        let leader = create_server(1);
        let new_config = ClusterConfig {
            voters: vec![leader.clone(), s4.clone(), s5.clone()],
            voters_next: vec![leader.clone(), s4.clone(), s5.clone()],
        };

        let new_index = 77;
        let snapshot = RaftSnapshot {
            last_included_entry: Some(entry_id(10, new_index)),
            data: vec![1, 2],
            cluster_config: Some(new_config.clone()),
        };
        let snapshot_request = InstallSnapshotRequest {
            leader: Some(leader.clone()),
            term: 12,
            snapshot: Some(snapshot),
        };

        client
            .install_snapshot(snapshot_request)
            .await
            .expect("install");

        {
            let state = raft_state.lock().await;
            assert_eq!(new_index, state.store.last_known_log_entry_id().index);
            assert_eq!(new_config, state.store.get_config_info().latest().0);

            assert!(state.cluster.is_voting_member(&s1));
            assert!(!state.cluster.is_voting_member(&s2));
            assert!(!state.cluster.is_voting_member(&s3));

            assert!(state.cluster.is_voting_member(&s4));
            assert!(state.cluster.is_voting_member(&s5));
        }
    }

    // This test verifies that we can append entries to a follower and that once the
    // follower's log grows too large, it will correctly compact.
    #[tokio::test]
    async fn test_append_and_compact() {
        let raft = create_raft().await;
        let raft_state = raft.state.clone();
        let server = TestRpcServer::run(RaftServer::new(raft)).await;

        // Make an append request coming from a leader, appending one record.
        let leader = create_fake_server_list()[1].clone();
        let append_request = AppendRequest {
            term: 12,
            leader: Some(leader.clone()),
            previous: Some(entry_id(-1, -1)),
            entries: vec![
                entry(entry_id(8, 0), Vec::new()),
                entry(entry_id(8, 1), Vec::new()),
                entry(entry_id(8, 2), Vec::new()),
            ],
            committed: 0,
        };

        let dst = server.address().expect("server");
        let mut client = create_local_client_for_testing(dst).await;
        let append_response_1 = client
            .append(append_request)
            .await
            .expect("append")
            .into_inner();

        // Make sure the handler has processed the rpc successfully.
        assert_eq!(append_response_1.term, 12);
        assert!(append_response_1.success);
        {
            let state = raft_state.lock().await;
            assert_eq!(state.current_leader(), Some(leader.clone()));
            assert_eq!(state.term(), 12);
            assert!(!state.store.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.next_index(), 3);
        }

        // Run a compaction, should have no effect
        {
            let mut state = raft_state.lock().await;
            state.store.try_compact().await;
            assert!(!state.store.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.next_index(), 3);
        }

        // Now send an append request with a payload large enough to trigger compaction.
        let compaction_bytes = raft_state.lock().await.options.compaction_threshold_bytes;

        let append_request_2 = AppendRequest {
            term: 12,
            leader: Some(leader.clone()),
            previous: Some(entry_id(8, 2)),
            entries: vec![entry(
                entry_id(8, 3),
                vec![0; 2 * compaction_bytes as usize],
            )],
            // This tells the follower that the entries are committed (only committed
            // entries are eligible for compaction).
            committed: 3,
        };

        let append_response_2 = client
            .append(append_request_2)
            .await
            .expect("append")
            .into_inner();

        // Make sure the handler has processed the rpc successfully.
        assert_eq!(append_response_2.term, 12);
        assert!(append_response_2.success);
        {
            let state = raft_state.lock().await;
            assert!(!state.store.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.next_index(), 4); // New entry incorporated
        }

        // Run a compaction, this one should actually compact things now.
        {
            let mut state = raft_state.lock().await;
            state.store.try_compact().await;
            assert!(state.store.is_index_compacted(0)); // Compacted
            assert_eq!(state.store.get_latest_snapshot().last.index, 3)
        }
    }

    fn entry(id: EntryId, payload: Vec<u8>) -> Entry {
        Entry {
            id: Some(id),
            data: Some(Data::Payload(payload)),
        }
    }

    fn entry_id(term: i64, index: i64) -> EntryId {
        EntryId { term, index }
    }

    async fn create_raft() -> RaftImpl {
        let servers = create_fake_server_list();
        RaftImpl::new(
            &servers[0].clone(),
            &servers.clone(),
            Arc::new(Mutex::new(FakeStateMachine::new())),
            None, /* diagnostics */
            create_config_for_testing(),
            Arc::new(Mutex::new(FailureOptions::no_failures())),
        )
        .await
        .unwrap()
    }

    fn create_config_for_testing() -> Options {
        // Configs with very high timeouts to make sure none of them ever
        // trigger during a unit test.
        Options {
            follower_timeout_ms: 100000000,
            candidate_timeouts_ms: 100000000,
            leader_replicate_ms: 100000000,
            compaction_threshold_bytes: 1000,
            compaction_check_periods_ms: 10000000000,
        }
    }

    fn create_fake_server_list() -> Vec<Server> {
        vec![create_server(1), create_server(2), create_server(3)]
    }

    fn create_server(port: i32) -> Server {
        Server {
            host: "::1".to_string(),
            port,
            name: port.to_string(),
        }
    }
}
