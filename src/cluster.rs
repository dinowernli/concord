extern crate chrono;
extern crate math;
extern crate rand;
extern crate timer;

#[path = "generated/raft.rs"]
pub mod raft;

#[path = "generated/raft_grpc.rs"]
pub mod raft_grpc;

use async_std::task;
use futures::future::join_all;
use futures::{executor, TryFutureExt};
use grpc::ClientStubExt;
use grpc::GrpcFuture;
use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;
use log::debug;
use log::info;
use protobuf::RepeatedField;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use timer::Guard;
use timer::Timer;

use raft::AppendRequest;
use raft::AppendResponse;
use raft::Entry;
use raft::EntryId;
use raft::Server;
use raft::VoteRequest;
use raft::VoteResponse;
use raft::{CommitRequest, CommitResponse, Status};
use raft_grpc::Raft;
use raft_grpc::RaftClient;
use std::time;

// Timeout after which a server in follower state starts a new election.
const FOLLOWER_TIMEOUTS_MS: i64 = 2000;

// Timeout after which a server in candidate state declares its candidacy a
// failure and starts a new election.
const CANDIDATE_TIMEOUT_MS: i64 = 3000;

// How frequently a leader will wake up and replicate entries to followers.
// Note that this also serves as the leader's heartbeat, so this should be
// lower than the follower timeout.
const LEADER_REPLICATE_MS: i64 = 500;

// Returns a value no lower than the supplied bound, with some additive jitter.
fn add_jitter(lower: i64) -> i64 {
    let mut rng = rand::thread_rng();
    let upper = (lower as f64 * 1.3) as i64;
    rng.gen_range(lower, upper)
}

// Returns true if the supplied latest entry id is at least as
// up-to-date as the supplied log.
fn is_up_to_date(last: &EntryId, log: &Vec<Entry>) -> bool {
    if log.is_empty() {
        return true;
    }

    let log_last = log.last().unwrap().get_id();
    if log_last.get_term() != last.get_term() {
        return last.get_term() > last.get_term();
    }

    // Terms are equal, last index decides.
    return last.get_index() >= log_last.get_index();
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.get_host(), address.get_port())
}

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}

fn sentinel_entry_id() -> EntryId {
    let mut result = EntryId::new();
    result.set_term(-1);
    result.set_index(-1);
    return result;
}

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
    // Persistent raft state.
    term: i64,
    voted_for: Option<Server>,
    entries: Vec<Entry>,

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
                    next_index: self.entries.len() as i64,
                    match_index: -1,
                },
            );
        }
        result
    }

    // Returns whether or not the supplied entry id exists in the log.
    fn has_entry(&self, entry_id: &EntryId) -> bool {
        if entry_id == &sentinel_entry_id() {
            return true;
        }
        match self.entries.get(entry_id.index as usize) {
            None => false,
            Some(entry) => entry.get_id().get_term() == entry_id.term,
        }
    }

    // Returns all entries strictly after the supplied id. Must only be called
    // if the supplied entry id is present in the log.
    fn get_entries_after(&self, entry_id: &EntryId) -> Vec<Entry> {
        if entry_id == &sentinel_entry_id() {
            return self.entries.clone();
        }
        let mut result = Vec::new();
        let idx = entry_id.index as usize;
        for value in self.entries[idx..].iter() {
            result.push(value.clone());
        }
        result
    }

    // Returns the next append request to send to this follower. Must only be called
    // while assuming the role of leader for the cluster.
    fn create_append_request(&self, follower: &Server) -> AppendRequest {
        // Construct the id of the last known index to be replicated on the follower.
        let position = self.followers.get(address_key(&follower).as_str()).unwrap();
        let previous = if position.next_index > 0 {
            self.entries
                .get(position.next_index as usize - 1)
                .unwrap()
                .get_id()
                .clone()
        } else {
            sentinel_entry_id()
        };

        let mut request = AppendRequest::new();
        request.set_term(self.term);
        request.set_leader(self.address.clone());
        request.set_previous(previous.clone());
        request.set_entries(RepeatedField::from(self.get_entries_after(&previous)));
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
        for index in self.committed + 1..self.entries.len() as i64 {
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
            let entry_id = self
                .entries
                .get(self.applied as usize)
                .expect("invalid applied")
                .get_id();
            info!(
                "[{:?}] Applied entry: {}",
                self.address,
                entry_id_key(entry_id)
            );
        }
    }

    // Returns a request which a candidate can send in order to request the vote
    // of peer servers in an election.
    fn create_vote_request(&self) -> VoteRequest {
        let mut request = VoteRequest::new();
        request.set_term(self.term);
        request.set_candidate(self.address.clone());
        request.set_last(match self.entries.last() {
            Some(e) => e.get_id().clone(),
            None => sentinel_entry_id(),
        });
        request
    }

    // Adds a new entry to the end of the log. Meant to be called by leaders
    // when processing requests to commit new payloads.
    fn append_entry(&mut self, payload: Vec<u8>) -> EntryId {
        let mut entry_id = EntryId::new();
        entry_id.set_term(self.term);
        entry_id.set_index(self.entries.len() as i64);

        let mut entry = Entry::new();
        entry.set_id(entry_id.clone());
        entry.set_payload(payload);

        self.entries.push(entry);
        entry_id
    }
}

pub struct RaftImpl {
    address: Server,
    state: Arc<Mutex<RaftState>>,
}

impl RaftImpl {
    pub fn new(server: &Server, all: &Vec<Server>) -> RaftImpl {
        RaftImpl {
            address: server.clone(),
            state: Arc::new(Mutex::new(RaftState {
                term: 0,
                voted_for: None,
                entries: Vec::new(),
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
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(add_jitter(FOLLOWER_TIMEOUTS_MS)),
            move || {
                if arc_state.lock().unwrap().term > term {
                    // We've moved on, ignore this callback.
                    return;
                }
                info!("[{:?}] Follower timeout in term {}", me, term);
                RaftImpl::become_candidate(arc_state.clone());
            },
        ));
    }

    fn become_candidate(arc_state: Arc<Mutex<RaftState>>) {
        let mut state = arc_state.lock().unwrap();

        state.role = RaftRole::Candidate;
        state.term = state.term + 1;
        state.voted_for = Some(state.address.clone());
        info!(
            "[{:?}] Becoming candidate for term {}",
            state.address, state.term
        );

        let s = arc_state.clone();
        let a = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(add_jitter(CANDIDATE_TIMEOUT_MS)),
            move || {
                info!("[{:?}] Election timed out, trying again", &a);
                RaftImpl::become_candidate(s.clone());
            },
        ));

        let me = state.address.clone();

        // Start an election and request votes from all servers.
        let request = state.create_vote_request();
        let term = request.term;
        let mut results = Vec::<GrpcFuture<VoteResponse>>::new();
        for server in state.get_others() {
            let client = state.get_client(&server);
            info!("[{:?}] Making vote rpc to [{:?}]", &me, &server);
            results.push(
                client
                    .vote(grpc::RequestOptions::new(), request.clone())
                    .drop_metadata(),
            );
        }

        // TODO(dino): This blocking call is problematic, and can cause everything
        // to lock up. Probably want to declare this whole method async and make
        // sure nothing anywhere needs to block.
        let results = executor::block_on(join_all(results));
        info!("[{:?}] Done waiting for vote requests", &me);

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
                        return;
                    }
                    if message.get_granted() {
                        votes = votes + 1;
                    }
                }
                Err(message) => info!("[{:?}] Vote request error: {:?}", &me, message),
            }
        }

        let arc_state_copy = arc_state.clone();
        if 2 * votes > state.cluster.len() {
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
        } else {
            info!("[{:?}] Lost election with {} votes", &me, votes);
        }
    }

    // Starts the main leader replication loop. The loop stops once the term has
    // moved on (or we otherwise detect we are no longer leader).
    async fn replicate_loop(arc_state: Arc<Mutex<RaftState>>, term: i64) {
        let me = arc_state.lock().unwrap().address.clone();
        loop {
            let new_term = arc_state.lock().unwrap().term;
            if new_term > term {
                info!("[{:?}] Detected higher term {}", me, new_term);
                return;
            }
            RaftImpl::replicate_entries(arc_state.clone(), term).await;
            let sleep_ms = add_jitter(LEADER_REPLICATE_MS) as u64;
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
            assert!(
                state.role == RaftRole::Leader,
                "Leader change should change term"
            );

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
            info!(
                "[{:?}] Our term {} is higher than incoming request term {}",
                self.address,
                state.term,
                request.get_term(),
            );
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
            if is_up_to_date(request.get_last(), &state.entries) {
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
        let mut result = AppendResponse::new();
        result.set_term(state.term);

        if request.get_term() > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone(), request.get_term());
            result.set_success(false);
            result.set_term(state.term);
            return sink.finish(result);
        }

        if state.term > request.get_term() {
            result.set_success(false);
            return sink.finish(result);
        }

        state.last_known_leader = Some(request.get_leader().clone());

        // Make sure we have the previous log index sent.
        if !state.has_entry(request.get_previous()) {
            result.set_success(false);
            return sink.finish(result);
        }

        for entry in request.get_entries() {
            let index = entry.get_id().get_index() as usize;
            if index == state.entries.len() {
                state.entries.push(entry.clone());
            } else {
                state.entries[index] = entry.clone();
            }
        }
        match request.get_entries().last() {
            Some(entry) => state.entries.truncate(entry.get_id().index as usize + 1),
            None => (),
        }

        let leader_commit = request.get_committed();
        if leader_commit > state.committed {
            state.committed = leader_commit;
            state.apply_committed();
        }

        // Reset the election timer
        let s = self.state.clone();
        let a = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::seconds(3),
            move || {
                info!("[{:?}] Timed out waiting for leader heartbeat", &a);
                RaftImpl::become_candidate(s.clone());
            },
        ));

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

        let entry_id = state.append_entry(request.payload);

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
                if state.has_entry(&entry_id) {
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
}
