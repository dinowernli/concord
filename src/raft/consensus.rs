use crate::raft;
use crate::raft::diagnostics;
use raft::raft_proto;
use raft::raft_proto_grpc;
use raft::StateMachine;

extern crate chrono;
extern crate math;
extern crate rand;
extern crate timer;

use async_std::task;
use futures::future::join_all;
use futures::TryFutureExt;
use grpc::{
    ClientStubExt, GrpcFuture, ServerHandlerContext, ServerRequestSingle, ServerResponseUnarySink,
};
use log::{debug, error, info, warn};
use protobuf::RepeatedField;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::Duration;
use timer::Guard;
use timer::Timer;

use bytes::{Buf, Bytes};
use diagnostics::ServerDiagnostics;
use raft::log::{ContainsResult, LogSlice};
use raft_proto::{AppendRequest, AppendResponse, EntryId, Server, VoteRequest, VoteResponse};
use raft_proto::{CommitRequest, CommitResponse, Status, StepDownRequest, StepDownResponse};
use raft_proto::{InstallSnapshotRequest, InstallSnapshotResponse};
use raft_proto_grpc::{Raft, RaftClient};

const DEFAULT_FOLLOWER_TIMEOUTS_MS: i64 = 2000;
const DEFAULT_CANDIDATE_TIMEOUT_MS: i64 = 3000;
const DEFAULT_LEADER_REPLICATE_MS: i64 = 500;

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
}

impl Config {
    fn default() -> Self {
        Config {
            follower_timeout_ms: DEFAULT_FOLLOWER_TIMEOUTS_MS,
            candidate_timeouts_ms: DEFAULT_CANDIDATE_TIMEOUT_MS,
            leader_replicate_ms: DEFAULT_LEADER_REPLICATE_MS,
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
        state_machine: Box<dyn StateMachine + Send>,
        diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
    ) -> RaftImpl {
        RaftImpl {
            address: server.clone(),
            state: Arc::new(Mutex::new(RaftState {
                config: Config::default(),

                term: 0,
                voted_for: None,
                log: LogSlice::initial(),
                state_machine: state_machine,

                committed: -1,
                applied: -1,
                role: RaftRole::Follower,
                followers: HashMap::new(),
                timer: Timer::new(),
                timer_guard: None,

                address: server.clone(),
                cluster: all.clone(),
                clients: HashMap::new(),
                last_known_leader: None,

                diagnostics,
            })),
        }
    }

    pub fn start(&self) {
        let mut s = self.state.lock().unwrap();
        let term = s.term;
        info!("[{:?}] Starting", s.address);
        RaftImpl::become_follower(&mut s, self.state.clone(), term);
    }

    fn become_follower(state: &mut RaftState, arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let me = state.address.clone();
        info!("[{:?}] Becoming follower for term {}", &me, term);
        assert!(term >= state.term, "Term should never decrease");

        state.term = term;
        state.role = RaftRole::Follower;
        state.voted_for = None;
        let timeout_ms = state.config.follower_timeout_ms;
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(add_jitter(timeout_ms)),
            move || {
                let arc_state = arc_state.clone();
                let me = me.clone();
                task::spawn(async move {
                    info!("[{:?}] Follower timeout in term {}", me, term);
                    RaftImpl::election_loop(arc_state.clone(), term + 1).await;
                });
            },
        ));
    }

    // Keeps running elections until either the term changes, a leader has emerged,
    // or an own election has been won.
    async fn election_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let timeout_ms = arc_state
            .lock()
            .unwrap()
            .config
            .candidate_timeouts_ms
            .clone();
        let mut term = term;
        while !RaftImpl::run_election(arc_state.clone(), term).await {
            term = term + 1;
            let sleep_ms = add_jitter(timeout_ms) as u64;
            task::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    // Returns whether or not the election process is deemed complete. If complete,
    // there is no need to run any further elections.
    async fn run_election(arc_state: Arc<Mutex<RaftState>>, term: i64) -> bool {
        let mut results = Vec::<GrpcFuture<VoteResponse>>::new();
        {
            let mut state = arc_state.lock().unwrap();

            // The world has moved on.
            if state.term > term {
                return true;
            }

            // Prepare the election.
            info!("[{:?}] Starting election for term {}", state.address, term);
            state.role = RaftRole::Candidate;
            state.term = term;
            state.timer_guard = None;
            state.voted_for = Some(state.address.clone());

            // Request votes from all peer.
            let me = state.address.clone();
            let request = state.create_vote_request();
            for server in state.get_others() {
                let client = state.get_client(&server);
                debug!("[{:?}] Making vote rpc to [{:?}]", &me, &server);
                results.push(
                    client
                        .vote(grpc::RequestOptions::new(), request.clone())
                        .drop_metadata(),
                );
            }
        }

        let results = join_all(results).await;

        {
            let mut state = arc_state.lock().unwrap();
            let me = state.address.clone();
            debug!("[{:?}] Done waiting for vote requests", &me);

            // The world has moved on or someone else has won in this term.
            if state.term > term || state.role != RaftRole::Candidate {
                return true;
            }

            let mut votes = 1; // Here we count our own vote for ourselves.
            for response in &results {
                match response {
                    Ok(message) => {
                        if message.get_term() > term {
                            info!("[{:?}] Detected higher term {}", &me, message.get_term());
                            RaftImpl::become_follower(
                                &mut state,
                                arc_state.clone(),
                                message.get_term(),
                            );
                            return true;
                        }
                        if message.get_granted() {
                            votes = votes + 1;
                        }
                    }
                    Err(message) => info!("[{:?}] Vote request error: {:?}", &me, message),
                }
            }

            let arc_state_copy = arc_state.clone();
            return if 2 * votes > state.cluster.len() {
                info!(
                    "[{:?}] Won election with {} votes, becoming leader for term {}",
                    &me, votes, term
                );
                state.role = RaftRole::Leader;
                state.followers = state.create_follower_positions();
                state.timer_guard = None;
                task::spawn(async move {
                    RaftImpl::replicate_loop(arc_state_copy.clone(), term).await;
                });
                true
            } else {
                info!("[{:?}] Lost election with {} votes", &me, votes);
                false
            };
        }
    }

    // Starts the main leader replication loop. The loop stops once the term has
    // moved on (or we otherwise detect we are no longer leader).
    async fn replicate_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let me = arc_state.lock().unwrap().address.clone();
        let timeouts_ms = arc_state.lock().unwrap().config.leader_replicate_ms.clone();
        loop {
            {
                let locked_state = arc_state.lock().unwrap();
                if locked_state.term > term {
                    info!("[{:?}] Detected higher term {}", me, locked_state.term);
                    return;
                }
                if locked_state.role != RaftRole::Leader {
                    return;
                }
                match &locked_state.diagnostics {
                    Some(d) => d.lock().unwrap().report_leader(term, &locked_state.address),
                    _ => (),
                }
            }

            RaftImpl::replicate_entries(arc_state.clone(), term).await;
            let sleep_ms = add_jitter(timeouts_ms) as u64;
            task::sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    // Makes a single request to all followers, heartbeating them and replicating
    // any entries they don't have.
    async fn replicate_entries(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let mut results = Vec::<GrpcFuture<(Server, AppendRequest, AppendResponse)>>::new();
        {
            let mut state = arc_state.lock().unwrap();
            debug!("[{:?}] Replicating entries", &state.address);
            for server in state.get_others() {
                let request = state.create_append_request(&server);
                let client = state.get_client(&server);
                let s = server.clone();
                let fut = client
                    .append(grpc::RequestOptions::new(), request.clone())
                    .drop_metadata()
                    .map_ok(move |result| (s.clone(), request.clone(), result));
                results.push(Box::pin(fut));
            }
        }

        // TODO(dino): Add timeouts to these rpcs.
        let results = join_all(results).await;

        {
            let mut state = arc_state.lock().unwrap();
            if state.term > term {
                info!("[{:?}] Detected higher term {}", state.address, state.term);
                return;
            }
            if state.role != RaftRole::Leader {
                info!("[{:?}] No longer leader", state.address);
                return;
            }

            let me = state.address.clone();
            for result in results {
                match result {
                    Err(message) => info!("[{:?}] Append request failed, error: {}", &me, message),
                    Ok((peer, request, response)) => {
                        let rterm = response.get_term();
                        if rterm > state.term {
                            info!(
                                "[{:?}] Detected higher term {} from peer {:?}",
                                &me, rterm, &peer,
                            );
                            RaftImpl::become_follower(&mut state, arc_state.clone(), rterm);
                            return;
                        }
                        state.handle_append_response(&peer, &response, &request);
                    }
                }
            }
            state.update_committed();
            debug!("[{:?}] Done replicating entries", &state.address);
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

#[derive(PartialEq)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

struct RaftState {
    // Constant state.
    config: Config,

    // Persistent raft state.
    term: i64,
    voted_for: Option<Server>,
    log: LogSlice,
    state_machine: Box<dyn StateMachine + Send>, // RaftState gets sent between threads.

    // Volatile raft state.
    committed: i64,
    applied: i64,
    role: RaftRole,
    followers: HashMap<String, FollowerPosition>,

    timer: Timer,
    timer_guard: Option<Guard>,

    // Cluster membership.
    address: Server,
    cluster: Vec<Server>,
    clients: HashMap<String, RaftClient>,
    last_known_leader: Option<Server>,

    // If present, this instance will inform the diagnostics object of relevant
    // updates as they happen during execution.
    diagnostics: Option<Arc<Mutex<ServerDiagnostics>>>,
}

impl RaftState {
    // Returns an rpc client which can be used to contact the supplied peer.
    fn get_client(&mut self, address: &Server) -> &mut RaftClient {
        let key = address_key(address);
        self.clients.entry(key).or_insert_with(|| {
            let client_conf = Default::default();
            let port = address.get_port() as u16;
            RaftClient::new_plain(address.get_host(), port, client_conf).unwrap()
        })
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
                    next_index: self.log.next_index(),
                    match_index: -1,
                },
            );
        }
        result
    }

    // Returns the next append request to send to this follower. Must only be called
    // while assuming the role of leader for the cluster.
    fn create_append_request(&self, follower: &Server) -> AppendRequest {
        // Construct the id of the last known index to be replicated on the follower.
        let position = self.followers.get(address_key(&follower).as_str()).unwrap();

        // TODO(dino): Before enabling snapshots, handle the case where the index
        // the follower wants has already been compacted locally.
        let previous = self.log.id_at(position.next_index - 1);

        let mut request = AppendRequest::new();
        request.set_term(self.term);
        request.set_leader(self.address.clone());
        request.set_previous(previous.clone());
        request.set_entries(RepeatedField::from(self.log.get_entries_after(&previous)));
        request.set_committed(self.committed);
        request
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
        if follower.is_none() {
            info!(
                "[{:?}] Ignoring append response for unknown peer {:?}",
                &self.address, &peer
            );
            return;
        }

        let f = follower.unwrap();
        if !response.get_success() {
            // The follower has rejected our entries, presumably because they could
            // not find the entry we sent as "previous". We repeatedly reduce the
            // "next" index until we hit a "previous" entry present on the follower.
            f.next_index = f.next_index - 1;
            info!(
                "[{:?}] Decremented follower next_index for peer {:?} to {}",
                &self.address, &peer, f.next_index
            );
            return;
        }

        // The follower has appended our entries. The next index we wish to
        // send them is the one immediately after the last one we sent.
        let old_f = f.clone();
        match request.get_entries().last() {
            Some(e) => {
                f.next_index = e.get_id().get_index() + 1;
                f.match_index = e.get_id().get_index();
                if f != &old_f {
                    info!(
                        "[{:?}] Follower state for peer {:?} is now (next={},match={})",
                        &self.address, &peer, f.next_index, f.match_index
                    );
                }
            }
            None => (),
        }
    }

    // Scans the state of our followers in the hope of finding a new index which
    // has been replicated to a majority. If such an index is found, this updates
    // the index this leader considers committed.
    fn update_committed(&mut self) {
        let saved_committed = self.committed;
        for index in self.committed + 1..self.log.next_index() {
            let mut matches = 1; // We match.
            for (_, follower) in &self.followers {
                if follower.match_index >= index {
                    matches = matches + 1;
                }
            }

            if 2 * matches > self.cluster.len() {
                self.committed = index;
            }
        }
        if self.committed != saved_committed {
            debug!(
                "[{:?}] Updated committed index from {} to {}",
                &self.address, saved_committed, self.committed
            );
        }

        self.apply_committed();
    }

    // Called to apply any committed values that haven't been applied to the
    // state machine. This method is always safe to call, on leaders and followers.
    fn apply_committed(&mut self) {
        while self.applied < self.committed {
            self.applied = self.applied + 1;
            let entry = self.log.entry_at(self.applied);
            let entry_id = entry.get_id().clone();

            let result = self.state_machine.apply(&entry.get_payload().to_bytes());
            match result {
                Ok(()) => {
                    info!(
                        "[{:?}] Applied entry: {}",
                        self.address,
                        entry_id_key(&entry_id)
                    );
                }
                Err(msg) => {
                    warn!(
                        "[{:?}] Failed to apply {}: {}",
                        self.address,
                        entry_id_key(&entry_id),
                        msg,
                    );
                }
            }
        }
    }

    // Returns a request which a candidate can send in order to request the vote
    // of peer servers in an election.
    fn create_vote_request(&self) -> VoteRequest {
        let mut request = VoteRequest::new();
        request.set_term(self.term);
        request.set_candidate(self.address.clone());
        request.set_last(self.log.last_known_id().clone());
        request
    }
}

impl Raft for RaftImpl {
    fn vote(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<VoteRequest>,
        sink: ServerResponseUnarySink<VoteResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        info!(
            "[{:?}] Handling vote request: [{:?}]",
            self.address, request
        );

        let mut state = self.state.lock().unwrap();

        // Reject anything from an outdated term.
        if state.term > request.get_term() {
            let mut result = VoteResponse::new();
            result.set_term(state.term);
            result.set_granted(false);
            return sink.finish(result);
        }

        // If we're in an outdated term, we revert to follower in the new later
        // term and may still grant the requesting candidate our vote.
        if request.get_term() > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone(), request.get_term());
        }

        let mut result = VoteResponse::new();
        result.set_term(state.term);

        let candidate = request.get_candidate();
        if candidate == state.voted_for.as_ref().unwrap_or(candidate) {
            if state.log.is_up_to_date(request.get_last()) {
                state.voted_for = Some(candidate.clone());
                info!("[{:?}] Granted vote", self.address);
                result.set_granted(true);
            } else {
                info!("[{:?}] Denied vote", self.address);
                result.set_granted(false);
            }
            return sink.finish(result);
        } else {
            info!(
                "[{:?}] Rejecting, already voted for candidate [{:?}]",
                self.address,
                state.voted_for.as_ref()
            );
            result.set_granted(false);
            sink.finish(result)
        }
    }

    fn append(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<AppendRequest>,
        sink: ServerResponseUnarySink<AppendResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        debug!(
            "[{:?}] Handling append request: [{:?}]",
            self.address, request
        );

        let mut state = self.state.lock().unwrap();

        // Handle the case where we are ahead of the leader. We inform the
        // leader of our (greater) term and fail the append.
        if state.term > request.get_term() {
            let mut result = AppendResponse::new();
            result.set_term(state.term);
            result.set_success(false);
            return sink.finish(result);
        }

        // If the leader's term is greater than ours, we update ours to match
        // by resetting to a "clean" follower state for the leader's (greater)
        // term. Note that we then handle the leader's append afterwards.
        if request.get_term() > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone(), request.get_term());
        }

        // Record the latest leader.
        let leader = request.get_leader().clone();
        state.last_known_leader = Some(leader.clone());
        match &state.diagnostics {
            Some(d) => d.lock().unwrap().report_leader(state.term, &leader),
            _ => (),
        }

        // Reset the election timer
        let term = state.term;
        let arc_state = self.state.clone();
        let me = state.address.clone();
        let timeout_ms = state.config.follower_timeout_ms;
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(add_jitter(timeout_ms)),
            move || {
                let arc_state = arc_state.clone();
                let me = me.clone();
                task::spawn(async move {
                    info!("[{:?}] Timed out waiting for leader heartbeat", &me);
                    RaftImpl::election_loop(arc_state.clone(), term + 1).await;
                });
            },
        ));

        let mut result = AppendResponse::new();
        result.set_term(state.term);

        // Make sure we have the previous log index sent. Note that COMPACTED
        // can happen whenever we have no entries (e.g.,initially or just after
        // a snapshot install).
        if state.log.contains(request.get_previous()) == ContainsResult::ABSENT {
            // Let the leader know that this entry is too far in the future, so
            // it can try again from with earlier index.
            result.set_success(false);
            return sink.finish(result);
        }

        if !request.get_entries().is_empty() {
            state.log.append_all(request.get_entries());
        }

        // If the leader considers an entry committed, it is guaranteed that
        // all members of the cluster agree on the log up to that index, so it
        // is safe to apply the entries to the state machine.
        let leader_commit = request.get_committed();
        if leader_commit > state.committed {
            state.committed = leader_commit;
            state.apply_committed();
        }

        debug!("[{:?}] Successfully processed heartbeat", &state.address);
        result.set_success(true);
        return sink.finish(result);
    }

    fn commit(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<CommitRequest>,
        sink: ServerResponseUnarySink<CommitResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        debug!("[{:?}] Handling commit request", self.address);

        let mut state = self.state.lock().unwrap();
        if state.role != RaftRole::Leader {
            let mut result = CommitResponse::new();
            result.set_status(Status::NOT_LEADER);
            match &state.last_known_leader {
                None => (),
                Some(l) => result.set_leader(l.clone()),
            }
            return sink.finish(result);
        }

        let term = state.term;
        let entry_id = state.log.append(term, request.payload);

        // Make sure the regular operations can continue while we wait.
        drop(state);

        // TODO(dino): Turn this into a more efficient future-based wait (or,
        // even better, something entirely async).
        loop {
            let state = self.state.lock().unwrap();

            // Even if we're no longer the leader, we may have managed to
            // get the entry committed while we were. Let the state of the
            // replicated log and commit state be the source of truth.
            if state.committed >= entry_id.index {
                let mut result = CommitResponse::new();
                state
                    .last_known_leader
                    .as_ref()
                    .map(|l| result.set_leader(l.clone()));

                // TODO(dino): Handle the case where the state of the world has
                // moved on so much that the entry we committed got compacted.
                // This would mean "contains" below returns COMPACTED.

                if state.log.contains(&entry_id) == ContainsResult::PRESENT {
                    result.set_entry_id(entry_id.clone());
                    result.set_status(Status::SUCCESS);
                } else {
                    result.set_status(Status::NOT_LEADER);
                }
                return sink.finish(result);
            }

            // We know the log hasn't caught up. If the term has changed,
            // chances are we the entry we appended earlier has been replaced
            // by the new leader.
            if state.term > entry_id.term {
                let mut result = CommitResponse::new();
                result.set_status(Status::NOT_LEADER);
                state
                    .last_known_leader
                    .as_ref()
                    .map(|l| result.set_leader(l.clone()));
                return sink.finish(result);
            }

            // Now we're still in the same term and we just haven't managed to
            // commit the entry yet. Check again in the next iteration.
            std::thread::sleep(time::Duration::from_millis(10));
        }
    }

    fn step_down(
        &self,
        _: ServerHandlerContext,
        _req: ServerRequestSingle<StepDownRequest>,
        sink: ServerResponseUnarySink<StepDownResponse>,
    ) -> grpc::Result<()> {
        debug!("[{:?}] Handling step down request", self.address);

        let mut state = self.state.lock().unwrap();
        if state.role != RaftRole::Leader {
            let mut result = StepDownResponse::new();
            result.set_status(Status::NOT_LEADER);
            match &state.last_known_leader {
                None => (),
                Some(l) => result.set_leader(l.clone()),
            }
            return sink.finish(result);
        }

        let term = state.term;
        RaftImpl::become_follower(&mut state, self.state.clone(), term);

        let mut result = StepDownResponse::new();
        result.set_status(Status::SUCCESS);
        result.set_leader(state.address.clone());
        return sink.finish(result);
    }

    fn install_snapshot(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<InstallSnapshotRequest>,
        sink: ServerResponseUnarySink<InstallSnapshotResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        debug!("[{:?}] Handling install snapshot request", self.address);

        let mut state = self.state.lock().unwrap();
        if request.term >= state.term {
            let contains = state.log.contains(request.get_last());

            // If we have a copy of the latest entry in our log, there is
            // nothing to do and we should retain any log entries that are
            // newer than the latest entry in the snapshot.
            if contains != ContainsResult::PRESENT {
                let snapshot = Bytes::from(request.snapshot.clone());
                match state.state_machine.load_snapshot(&snapshot) {
                    Ok(_) => (),
                    Err(message) => error!(
                        "[{:?}] Failed to load snapshot: [{:?}]",
                        self.address, message
                    ),
                }
                state.log = LogSlice::new(request.get_last().clone());
            }
        }

        // TODO(dino): Need to make sure that after installing a snaphot, the
        // leader's request to append the next entry succeeds.

        let mut result = InstallSnapshotResponse::new();
        result.set_term(state.term);
        return sink.finish(result);
    }
}

// Returns a value no lower than the supplied bound, with some additive jitter.
fn add_jitter(lower: i64) -> i64 {
    let mut rng = rand::thread_rng();
    let upper = (lower as f64 * 1.3) as i64;
    rng.gen_range(lower, upper)
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.get_host(), address.get_port())
}

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}
