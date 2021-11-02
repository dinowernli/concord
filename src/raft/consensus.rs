extern crate chrono;
extern crate math;
extern crate rand;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use async_std::sync::{Arc, Mutex};
use bytes::Bytes;
use futures::future::{err, join_all};
use futures::FutureExt;
use log::{debug, info};
use rand::Rng;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Error};
use tonic::{Request, Response, Status};

use diagnostics::ServerDiagnostics;
use raft::log::ContainsResult;
use raft::raft_proto;
use raft::StateMachine;
use raft_proto::{AppendRequest, AppendResponse, EntryId, Server, VoteRequest, VoteResponse};
use raft_proto::{CommitRequest, CommitResponse, StepDownRequest, StepDownResponse};
use raft_proto::{InstallSnapshotRequest, InstallSnapshotResponse};

use crate::raft;
use crate::raft::diagnostics;
use crate::raft::store::Store;
use crate::raft_proto::raft_client::RaftClient;
use crate::raft_proto::raft_server::Raft;

// Parameters used to configure the behavior of a cluster participant.
pub struct Config {
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

impl Config {
    pub fn default() -> Self {
        Config {
            follower_timeout_ms: 100,
            candidate_timeouts_ms: 300,
            leader_replicate_ms: 50,
            compaction_threshold_bytes: 10 * 1000 * 1000,
            compaction_check_periods_ms: 5000,
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
    pub fn new(
        server: &Server,
        all: &Vec<Server>,
        state_machine: Arc<Mutex<dyn StateMachine + Send>>,
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
        config: Config,
    ) -> RaftImpl {
        let compaction_threshold_bytes = config.compaction_threshold_bytes;
        RaftImpl {
            address: server.clone(),
            state: Arc::new(Mutex::new(RaftState {
                config,

                term: 0,
                voted_for: None,
                store: Store::new(
                    state_machine,
                    compaction_threshold_bytes,
                    server.name.as_str(),
                ),

                role: RaftRole::Follower,
                followers: HashMap::new(),

                timer_guard: None,

                address: server.clone(),
                cluster: all.clone(),
                channels: HashMap::new(),
                last_known_leader: None,

                diagnostics,
            })),
        }
    }

    pub async fn start(&self) {
        let arc_state = self.state.clone();

        let mut state = self.state.lock().await;
        let term = state.term;
        info!("[{}] Starting", state.address.name);
        RaftImpl::become_follower(&mut state, arc_state.clone(), term);

        tokio::spawn(async move {
            RaftImpl::compaction_loop(arc_state.clone()).await;
        });
    }

    fn become_follower(state: &mut RaftState, arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let me = state.address.clone();
        info!("[{}] Becoming follower for term {}", &me.name, term);
        assert!(term >= state.term, "Term should never decrease");

        state.term = term;
        state.role = RaftRole::Follower;
        state.voted_for = None;
        let timeout_ms = state.config.follower_timeout_ms;

        let task = sleep(Duration::from_millis(add_jitter(timeout_ms))).then(async move |_| {
            tokio::spawn(async move {
                info!("[{}] Follower timeout in term {}", me.clone().name, term);
                RaftImpl::election_loop(arc_state.clone(), term + 1).await;
            });
        });
        state.timer_guard = Some(TimerGuard {
            handle: tokio::spawn(task),
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
            let period_ms = arc_state.lock().await.config.compaction_check_periods_ms;
            sleep(Duration::from_millis(add_jitter(period_ms))).await;
        }
    }

    // Keeps running elections until either the term changes, a leader has emerged,
    // or an own election has been won.
    async fn election_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let timeout_ms = arc_state.lock().await.config.candidate_timeouts_ms.clone();
        let mut term = term;
        while !RaftImpl::run_election(arc_state.clone(), term).await {
            term = term + 1;
            sleep(Duration::from_millis(add_jitter(timeout_ms))).await;
        }
    }

    // Returns whether or not the election process is deemed complete. If complete,
    // there is no need to run any further elections.
    async fn run_election(arc_state: Arc<Mutex<RaftState>>, term: i64) -> bool {
        type Res = Result<Response<VoteResponse>, Status>;
        type Fut = Pin<Box<dyn Future<Output = Res> + Send>>;
        let mut futures = Vec::<Fut>::new();
        {
            let mut state = arc_state.lock().await;

            // The world has moved on.
            if state.term > term {
                return true;
            }

            // Prepare the election.
            info!(
                "[{}] Starting election for term {}",
                state.address.name, term
            );
            state.role = RaftRole::Candidate;
            state.term = term;
            state.timer_guard = None;
            state.voted_for = Some(state.address.clone());

            // Request votes from all peer.
            let request = state.create_vote_request();
            let others = state.get_others();
            for server in others {
                match state.get_client(&server).await {
                    Ok(client) => {
                        futures.push(Box::pin(RaftState::request_vote(client, request.clone())));
                    }
                    Err(msg) => {
                        futures.push(Box::pin(err(Status::unavailable(format!(
                            "Unable to connect to {} : {}",
                            &server.name, msg
                        )))));
                    }
                }
            }
        }

        let results = join_all(futures).await;

        {
            let mut state = arc_state.lock().await;
            let me = state.address.name.to_string();
            debug!("[{}] Done waiting for vote requests", &me);

            // The world has moved on or someone else has won in this term.
            if state.term > term || state.role != RaftRole::Candidate {
                return true;
            }

            let mut votes = 1; // Here we count our own vote for ourselves.
            for response in results {
                match response {
                    Ok(result) => {
                        let message = result.into_inner();
                        if message.term > term {
                            info!("[{}] Detected higher term {}", &me, message.term);
                            RaftImpl::become_follower(&mut state, arc_state.clone(), message.term);
                            return true;
                        }
                        if message.granted {
                            votes = votes + 1;
                        }
                    }
                    Err(message) => info!("[{}] Vote request error: {}", &me, message),
                }
            }

            let arc_state_copy = arc_state.clone();
            return if 2 * votes > state.cluster.len() {
                info!(
                    "[{}] Won election with {} votes, becoming leader for term {}",
                    &me, votes, term
                );
                state.role = RaftRole::Leader;
                state.followers = state.create_follower_positions();
                state.timer_guard = None;
                tokio::spawn(async move {
                    RaftImpl::replicate_loop(arc_state_copy.clone(), term).await;
                });
                true
            } else {
                info!("[{}] Lost election with {} votes", &me, votes);
                false
            };
        }
    }

    // Starts the main leader replication loop. The loop stops once the term has
    // moved on (or we otherwise detect we are no longer leader).
    async fn replicate_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let me = arc_state.lock().await.address.clone().name;
        let timeouts_ms = arc_state.lock().await.config.leader_replicate_ms.clone();
        loop {
            {
                let locked_state = arc_state.lock().await;
                if locked_state.term > term {
                    info!("[{}] Detected higher term {}", me, locked_state.term);
                    return;
                }
                if locked_state.role != RaftRole::Leader {
                    return;
                }
                match &locked_state.diagnostics {
                    Some(d) => d.lock().await.report_leader(term, &locked_state.address),
                    _ => (),
                }
            }

            RaftImpl::replicate_entries(arc_state.clone(), term).await;
            sleep(Duration::from_millis(add_jitter(timeouts_ms))).await;
        }
    }

    // Makes a single request to all followers, heartbeating them and replicating
    // any entries they don't have.
    async fn replicate_entries(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        // This needs to be marked as "Send" because it's being sent across await points.
        let mut results = Vec::<Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>>::new();
        {
            let mut state = arc_state.lock().await;
            debug!("[{}] Replicating entries", &state.address.name);
            let others = state.get_others();
            for follower in others {
                // Figure out which entry the follower is expecting next and decide whether to
                // send an append request (if we have the entries) or to fast-forward the follower
                // by installing a snapshot (if that entry has been compacted away in our log).
                let next_index = state
                    .followers
                    .get(address_key(&follower).as_str())
                    .unwrap()
                    .next_index;

                let client = state.get_client(&follower).await;
                if let Err(msg) = &client {
                    results.push(Box::pin(err(Status::unavailable(format!(
                        "Unable to connect to {} : {}",
                        &follower.name, msg
                    )))));
                    continue;
                }

                let client = client.unwrap();
                if state.store.log.is_index_compacted(next_index) {
                    let request = state.create_snapshot_request();
                    let fut = RaftImpl::replicate_snapshot(
                        client,
                        arc_state.clone(),
                        follower.clone(),
                        request.clone(),
                    );
                    results.push(Box::pin(fut.map(|_| Ok(()))));
                } else {
                    let request = state.create_append_request(next_index);
                    let fut = RaftImpl::replicate_append(
                        client,
                        arc_state.clone(),
                        follower.clone(),
                        request.clone(),
                    );
                    results.push(Box::pin(fut.map(|_| Ok(()))));
                }
            }
        }

        // Wait for these async replication rpcs to finish.
        join_all(results).await;

        {
            let mut state = arc_state.lock().await;
            let me = state.address.clone().name;
            if state.term > term {
                info!("[{}] Detected higher term {}", &me, state.term);
                return;
            }
            if state.role != RaftRole::Leader {
                info!("[{}] No longer leader", &me);
                return;
            }
            state.update_committed().await;
            debug!("[{}] Done replicating entries", &me);
        }
    }

    // Send a request to the follower (baked into "client") to send the supplied request
    // to install a snapshot.
    async fn replicate_snapshot(
        mut client: RaftClient<Channel>,
        arc_state: Arc<Mutex<RaftState>>,
        follower: Server,
        install_request: InstallSnapshotRequest,
    ) {
        let mut request = Request::new(install_request.clone());
        request.set_timeout(Duration::from_millis(100));
        let result = client.install_snapshot(request).await;

        let mut state = arc_state.lock().await;
        let me = state.address.clone().name;
        match result {
            Ok(result) => {
                let response = result.into_inner();
                let other_term = response.term;
                if other_term > state.term {
                    info!(
                        "[{}] Detected higher term {} from peer {}",
                        &me, other_term, &follower.name,
                    );
                    RaftImpl::become_follower(&mut state, arc_state.clone(), other_term);
                    return;
                }
                state.record_follower_matches(&follower, install_request.last.expect("last").index);
            }
            Err(message) => info!(
                "[{}] InstallSnapshot request failed, error: {}",
                &me, message
            ),
        }
    }

    // Send a request to the follower (baked into "client") to send the supplied request
    // to append entries we have but the follower might not.
    async fn replicate_append(
        mut client: RaftClient<Channel>,
        arc_state: Arc<Mutex<RaftState>>,
        follower: Server,
        append_request: AppendRequest,
    ) {
        let mut request = Request::new(append_request.clone());
        request.set_timeout(Duration::from_millis(100));
        let result = client.append(request).await;

        let mut state = arc_state.lock().await;
        let me = state.address.clone().name;
        if state.term > append_request.term {
            info!("[{}] Detected higher term {}", &me, state.term);
            return;
        }
        if state.role != RaftRole::Leader {
            info!("[{}] No longer leader", &me);
            return;
        }

        match result {
            Err(message) => info!("[{}] Append request failed, error: {}", &me, message),
            Ok(response) => {
                let message = response.into_inner();
                let other_term = message.term;
                if other_term > state.term {
                    info!(
                        "[{}] Detected higher term {} from peer {}",
                        &me, other_term, &follower.name,
                    );
                    RaftImpl::become_follower(&mut state, arc_state.clone(), other_term);
                    return;
                }
                state.handle_append_response(&follower, &message, &append_request);
            }
        }
    }
}

// Holds the state a cluster leader tracks about its followers. Used to decide
// which entries to replicate to the follower.
#[derive(Debug, Clone, PartialEq)]
struct FollowerPosition {
    // Next log entry to send to the follower.
    next_index: i64,

    // Highest index known to be replicated on the follower.
    match_index: i64,
}

// Holds on to the handle of a timed operation and cancels it upon destruction.
// This allows callers to just replace the guard in order to refresh a timer,
// ensuring that there is only one timer active at any given time.
struct TimerGuard {
    handle: JoinHandle<()>,
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Debug, PartialEq)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
    Stopping,
}

struct RaftState {
    // Constant state.
    config: Config,

    // Persistent raft state.
    term: i64,
    voted_for: Option<Server>,
    store: Store, // RaftState gets sent between threads.

    // Volatile raft state.
    role: RaftRole,
    followers: HashMap<String, FollowerPosition>,

    timer_guard: Option<TimerGuard>,

    // Cluster membership.
    address: Server,
    cluster: Vec<Server>,
    channels: HashMap<String, Channel>,
    last_known_leader: Option<Server>,

    // If present, this instance will inform the diagnostics object of relevant
    // updates as they happen during execution.
    diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
}

impl RaftState {
    // Returns an rpc client which can be used to contact the supplied peer.
    async fn get_client(&mut self, address: &Server) -> Result<RaftClient<Channel>, Error> {
        let key = address_key(address);
        let cached = self.channels.get_mut(&key);
        if let Some(channel) = cached {
            // The "clone()" operation on channels is advertized as cheap and is the
            // recommended way to reuse channels.
            return Ok(RaftClient::new(channel.clone()));
        }

        // Cache miss, create a new channel.
        let dst = format!("http://[::1]:{}", address.port);
        let channel = Endpoint::new(dst)?.connect().await?;
        self.channels.insert(key, channel.clone());
        Ok(RaftClient::new(channel))
    }

    // Returns the peers in the cluster (ourself excluded).
    fn get_others(&self) -> Vec<Server> {
        self.cluster
            .clone()
            .into_iter()
            .filter(|server| server != &self.address)
            .collect()
    }

    // Returns a suitable initial map of follower positions. Meant to be called
    // by a new leader initializing itself.
    fn create_follower_positions(&self) -> HashMap<String, FollowerPosition> {
        let mut result = HashMap::new();
        for server in self.get_others() {
            result.insert(
                address_key(&server),
                FollowerPosition {
                    // Optimistically start assuming next is the same as our own next.
                    next_index: self.store.log.next_index(),
                    match_index: -1,
                },
            );
        }
        result
    }

    // Returns an append request from a leader to a follower, appending entries starting
    // with the supplied index. Must only be called as leader, and the supplied index must
    // exist in our current log.
    fn create_append_request(&self, next_index: i64) -> AppendRequest {
        // This method should only get called if we know the index is present.
        assert!(!self.store.log.is_index_compacted(next_index));
        let previous = self.store.log.id_at(next_index - 1);

        AppendRequest {
            term: self.term,
            leader: Some(self.address.clone()),
            previous: Some(previous.clone()),
            entries: self.store.log.get_entries_after(&previous),
            committed: self.store.committed,
        }
    }

    // Returns a request which the leader can send to a follower in order to install the
    // same snapshot currently held on the leader.
    fn create_snapshot_request(&self) -> InstallSnapshotRequest {
        let mut snapshot: Vec<u8> = vec![];
        let mut last: Option<EntryId> = None;
        match self.store.get_latest_snapshot() {
            None => (),
            Some(snap) => {
                snapshot = snap.snapshot.to_vec();
                last = Some(snap.last.clone());
            }
        }
        InstallSnapshotRequest {
            term: self.term,
            leader: Some(self.address.clone()),
            snapshot,
            last,
        }
    }

    // Incorporates the provided response corresponding to the supplied request.
    // Must only be called for responses with a valid term.
    fn handle_append_response(
        &mut self,
        peer: &Server,
        response: &AppendResponse,
        request: &AppendRequest,
    ) {
        let follower = self.followers.get_mut(address_key(&peer).as_str());
        let me = self.address.clone().name;
        if follower.is_none() {
            info!(
                "[{}] Ignoring append response for unknown peer {}",
                &me, &peer.name
            );
            return;
        }

        let f = follower.unwrap();
        if !response.success {
            // The follower has rejected our entries, presumably because they could
            // not find the entry we sent as "previous". We repeatedly reduce the
            // "next" index until we hit a "previous" entry present on the follower.
            f.next_index = f.next_index - 1;
            info!(
                "[{}] Decremented follower next_index for peer {} to {}",
                &me, &peer.name, f.next_index
            );
            return;
        }

        // The follower has appended our entries, record the updated follower state.
        match request.entries.last() {
            Some(e) => self.record_follower_matches(&peer, e.id.as_ref().expect("id").index),
            None => (),
        }
    }

    // Called when, as a leader, we know that a follower's entries up to (and including)
    // match_index match our entries.
    fn record_follower_matches(&mut self, peer: &Server, match_index: i64) {
        let follower = self
            .followers
            .get_mut(address_key(&peer).as_str())
            .expect(format!("Unknown peer {}", &peer.name).as_str());
        let old_f = follower.clone();
        follower.match_index = match_index;
        follower.next_index = match_index + 1;
        if follower != &old_f {
            info!(
                "[{}] Follower state for peer {} is now (next={},match={})",
                &self.address.name, &peer.name, follower.next_index, follower.match_index
            );
        }
    }

    // Returns the highest index I such that each index at most I is replicated
    // to a majority of followers. In practice, this means that it is safe to
    // commit up to (and including) the result.
    fn compute_highest_majority_match(&self) -> i64 {
        let mut matches: Vec<i64> = self
            .followers
            .values()
            .clone()
            .into_iter()
            .map(|f| f.match_index)
            .collect();
        matches.sort();
        let mid = matches.len() / 2;

        // The second half of the array has match_index above the return value. For even,
        // we round down so that [1, 2, 4, 7] ends up as "2" (with the latter 3 followers
        // making up the majority).
        matches[mid]
    }

    // Scans the state of our followers in the hope of finding a new index which
    // has been replicated to a majority. If such an index is found, this updates
    // the index this leader considers committed.
    async fn update_committed(&mut self) {
        let new_commit_index = self.compute_highest_majority_match();
        self.store.commit_to(new_commit_index).await;
    }

    // Returns a request which a candidate can send in order to request the vote
    // of peer servers in an election.
    fn create_vote_request(&self) -> VoteRequest {
        VoteRequest {
            term: self.term,
            candidate: Some(self.address.clone()),
            last: Some(self.store.log.last_known_id().clone()),
        }
    }

    // Requests a vote from a follower. Used to run leader elections.
    async fn request_vote(
        mut client: RaftClient<Channel>,
        req: VoteRequest,
    ) -> Result<Response<VoteResponse>, Status> {
        let mut request = Request::new(req);
        request.set_timeout(Duration::from_millis(100));
        client.vote(request).await
    }
}

#[tonic::async_trait]
impl Raft for RaftImpl {
    async fn vote(&self, request: Request<VoteRequest>) -> Result<Response<VoteResponse>, Status> {
        let request = request.into_inner();
        debug!(
            "[{}] Handling vote request: [{:?}]",
            self.address.name, request
        );

        let mut state = self.state.lock().await;

        // Reject anything from an outdated term.
        if state.term > request.term {
            return Ok(Response::new(VoteResponse {
                term: state.term,
                granted: false,
            }));
        }

        // If we're in an outdated term, we revert to follower in the new later
        // term and may still grant the requesting candidate our vote.
        if request.term > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone(), request.term);
        }

        let candidate = request.candidate;

        let granted;
        let me = self.address.clone().name;
        if state.voted_for.is_none() || &candidate == &state.voted_for {
            if state.store.log.is_up_to_date(&request.last.expect("last")) {
                state.voted_for = candidate.clone();
                info!(
                    "[{}] Granted vote to {:?}",
                    &me,
                    state.voted_for.clone().map(|x| x.name)
                );
                granted = true;
            } else {
                info!(
                    "[{}] Denied vote to {:?}",
                    &me,
                    state.voted_for.clone().map(|x| x.name)
                );
                granted = false;
            }
        } else {
            info!(
                "[{}] Rejecting, already voted for candidate [{:?}]",
                &me,
                state.voted_for.clone().map(|x| x.name)
            );
            granted = false;
        }

        Ok(Response::new(VoteResponse {
            term: state.term,
            granted,
        }))
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let request = request.into_inner();
        debug!(
            "[{}] Handling append request: [{:?}]",
            self.address.name, request
        );

        let mut state = self.state.lock().await;

        // Handle the case where we are ahead of the leader. We inform the
        // leader of our (greater) term and fail the append.
        if state.term > request.term {
            return Ok(Response::new(AppendResponse {
                term: state.term,
                success: false,
            }));
        }

        // If the leader's term is greater than ours, we update ours to match
        // by resetting to a "clean" follower state for the leader's (greater)
        // term. Note that we then handle the leader's append afterwards.
        if request.term > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone(), request.term);
        }

        // Record the latest leader.
        let leader = request.leader.expect("leader").clone();
        state.last_known_leader = Some(leader.clone());
        match &state.diagnostics {
            Some(d) => d.lock().await.report_leader(state.term, &leader),
            _ => (),
        }

        // Reset the election timer
        let term = state.term;
        let arc_state = self.state.clone();
        let me = state.address.clone().name;
        let timeout_ms = state.config.follower_timeout_ms;

        let task = sleep(Duration::from_millis(add_jitter(timeout_ms))).then(async move |_| {
            tokio::spawn(async move {
                info!("[{}] Timed out waiting for leader heartbeat", me.clone());
                RaftImpl::election_loop(arc_state.clone(), term + 1).await;
            });
        });
        state.timer_guard = Some(TimerGuard {
            handle: tokio::spawn(task),
        });

        let term = state.term;

        // Make sure we have the previous log index sent. Note that COMPACTED
        // can happen whenever we have no entries (e.g.,initially or just after
        // a snapshot install).
        if state
            .store
            .log
            .contains(&request.previous.expect("previous"))
            == ContainsResult::ABSENT
        {
            // Let the leader know that this entry is too far in the future, so
            // it can try again from with earlier index.
            return Ok(Response::new(AppendResponse {
                term,
                success: false,
            }));
        }

        if !request.entries.is_empty() {
            state.store.log.append_all(request.entries.as_slice());
        }

        // If the leader considers an entry committed, it is guaranteed that
        // all members of the cluster agree on the log up to that index, so it
        // is safe to apply the entries to the state machine.
        let leader_commit_index = request.committed;
        state.store.commit_to(leader_commit_index).await;

        debug!("[{}] Successfully processed heartbeat", &self.address.name);
        Ok(Response::new(AppendResponse {
            term,
            success: true,
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        debug!("[{}] Handling commit request", self.address.name);

        let term;
        let entry_id;
        let receiver;

        {
            let mut state = self.state.lock().await;
            if state.role != RaftRole::Leader {
                return Ok(Response::new(CommitResponse {
                    status: raft_proto::Status::NotLeader as i32,
                    leader: state.last_known_leader.clone(),
                    entry_id: None,
                }));
            }

            term = state.term;
            entry_id = state.store.log.append(term, request.payload);
            receiver = state.store.add_listener(entry_id.index);
        }

        let committed = receiver.await;

        let state = self.state.lock().await;
        let mut result = CommitResponse {
            leader: state.last_known_leader.clone(),

            // Replaced below
            status: 0,
            entry_id: None,
        };

        match committed {
            Ok(committed_id) => {
                if entry_id == committed_id {
                    result.entry_id = Some(entry_id.clone());
                    result.status = raft_proto::Status::Success as i32;
                } else {
                    // A different entry got committed to this index. This means
                    // the leader must have changed, let the caller know.
                    result.status = raft_proto::Status::NotLeader as i32;
                }
            }
            Err(_) => {
                // The sender went out of scope without ever being resolved. This can
                // happen in rare cases where the index we're interested in got compacted.
                // In this case we don't know whether the entry was committed.
                result.status = raft_proto::Status::NotLeader as i32;
            }
        };
        Ok(Response::new(result))
    }

    async fn step_down(
        &self,
        _: Request<StepDownRequest>,
    ) -> Result<Response<StepDownResponse>, Status> {
        debug!("[{}] Handling step down request", self.address.name);

        let mut state = self.state.lock().await;
        if state.role != RaftRole::Leader {
            return Ok(Response::new(StepDownResponse {
                status: raft_proto::Status::NotLeader as i32,
                leader: state.last_known_leader.clone(),
            }));
        }

        let term = state.term;
        RaftImpl::become_follower(&mut state, self.state.clone(), term);

        Ok(Response::new(StepDownResponse {
            status: raft_proto::Status::Success as i32,
            leader: Some(state.address.clone()),
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let request = request.into_inner();
        debug!("[{}] Handling install snapshot request", self.address.name);

        let mut state = self.state.lock().await;
        if request.term >= state.term && !request.snapshot.is_empty() {
            let last = request.last.expect("last");
            let snapshot = Bytes::from(request.snapshot.to_vec());
            let status = state.store.install_snapshot(snapshot, last).await;
            if status.code() != tonic::Code::Ok {
                return Err(status);
            }
        }
        Ok(Response::new(InstallSnapshotResponse { term: state.term }))
    }
}

// Returns a value no lower than the supplied bound, with some additive jitter.
fn add_jitter(lower: i64) -> u64 {
    let mut rng = rand::thread_rng();
    let upper = (lower as f64 * 1.3) as i64;
    rng.gen_range(lower, upper) as u64
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.host, address.port)
}

#[cfg(test)]
mod tests {
    use crate::raft::raft_proto::raft_server::RaftServer;
    use crate::raft::testing::FakeStateMachine;
    use crate::raft_proto::Entry;
    use crate::testing::TestServer;

    use super::*;

    #[tokio::test]
    async fn test_initial_state() {
        let raft = create_raft();
        let state = raft.state.lock().await;
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.term, 0);
    }

    // This test verifies that initially, a follower fails to accept entries
    // too far in the future. Then, after installing an appropriate snapshot,
    // sending those same entries succeeds.
    #[tokio::test]
    async fn test_load_snapshot_and_append() {
        let raft = create_raft();
        let raft_state = raft.state.clone();
        let server = TestServer::run(RaftServer::new(raft)).await;

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

        let mut client = create_grpc_client(server.port().unwrap() as i32).await;
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
        let state = raft_state.lock().await;
        assert_eq!(state.last_known_leader, Some(leader.clone()));
        drop(state);

        // Now install a snapshot which should make the original append request
        // valid.
        let snapshot_request = InstallSnapshotRequest {
            leader: Some(leader.clone()),
            term: 12,
            last: Some(entry_id(10, 75)),
            snapshot: vec![1, 2],
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

    // This test verifies that we can append entries to a follower and that once the
    // follower's log grows too large, it will correctly compact.
    #[tokio::test]
    async fn test_append_and_compact() {
        let raft = create_raft();
        let raft_state = raft.state.clone();
        let server = TestServer::run(RaftServer::new(raft)).await;

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

        let mut client = create_grpc_client(server.port().unwrap() as i32).await;
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
            assert_eq!(state.last_known_leader, Some(leader.clone()));
            assert_eq!(state.term, 12);
            assert!(!state.store.log.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.log.next_index(), 3);
        }

        // Run a compaction, should have no effect
        {
            let mut state = raft_state.lock().await;
            state.store.try_compact().await;
            assert!(!state.store.log.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.log.next_index(), 3);
        }

        // Now send an append request with a payload large enough to trigger compaction.
        let compaction_bytes = raft_state.lock().await.config.compaction_threshold_bytes;

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
            assert!(!state.store.log.is_index_compacted(0)); // Not compacted
            assert_eq!(state.store.log.next_index(), 4); // New entry incorporated
        }

        // Run a compaction, this one should actually compact things now.
        {
            let mut state = raft_state.lock().await;
            state.store.try_compact().await;
            assert!(state.store.log.is_index_compacted(0)); // Compacted
            assert_eq!(
                state
                    .store
                    .get_latest_snapshot()
                    .expect("snapshot")
                    .last
                    .index,
                3
            )
        }
    }

    fn entry(id: EntryId, payload: Vec<u8>) -> Entry {
        Entry {
            id: Some(id),
            payload,
        }
    }

    fn entry_id(term: i64, index: i64) -> EntryId {
        EntryId { term, index }
    }

    fn create_raft() -> RaftImpl {
        let servers = create_fake_server_list();
        RaftImpl::new(
            &servers[0].clone(),
            &servers.clone(),
            Arc::new(Mutex::new(FakeStateMachine::new())),
            None, /* diagnostics */
            create_config_for_testing(),
        )
    }

    fn create_config_for_testing() -> Config {
        // Configs with very high timeouts to make sure none of them ever
        // trigger during a unit test.
        Config {
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

    async fn create_grpc_client(port: i32) -> RaftClient<Channel> {
        RaftClient::connect(format!("http://[::1]:{}", port))
            .await
            .expect("client")
    }
}
