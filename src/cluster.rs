extern crate chrono;
extern crate math;
extern crate rand;
extern crate timer;

#[path = "generated/raft.rs"]
pub mod raft;

#[path = "generated/raft_grpc.rs"]
pub mod raft_grpc;

use futures::executor;
use futures::future::join_all;
use grpc::ClientStubExt;
use grpc::GrpcFuture;
use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;
use log::info;
use math::round::floor;
use protobuf::RepeatedField;
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use timer::Guard;
use timer::Timer;

use raft::AppendRequest;
use raft::AppendResponse;
use raft::Entry;
use raft::EntryId;
use raft::Server;
use raft::VoteRequest;
use raft::VoteResponse;
use raft_grpc::Raft;
use raft_grpc::RaftClient;

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

fn sentinel_entry_id() -> EntryId {
    let mut result = EntryId::new();
    result.set_term(-1);
    result.set_index(-1);
    return result;
}

struct FollowerPosition {
    next_index: i64,
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
}

impl RaftState {
    fn get_client(&mut self, address: &Server) -> &mut RaftClient {
        let key = address_key(address);
        self.clients.entry(key).or_insert_with(|| {
            let client_conf = Default::default();
            let port = address.get_port() as u16;
            RaftClient::new_plain(address.get_host(), port, client_conf).unwrap()
        })
    }

    fn get_others(&self) -> Vec<Server> {
        self.cluster
            .clone()
            .into_iter()
            .filter(|server| server != &self.address)
            .collect()
    }

    fn create_follower_positions(&self) -> HashMap<String, FollowerPosition> {
        let mut result = HashMap::new();
        for server in self.get_others() {
            result.insert(
                address_key(&server),
                FollowerPosition {
                    // Next log entry to send to the follower.
                    next_index: self.entries.len() as i64,

                    // Highest index known to be replicated on the follower.
                    match_index: -1,
                },
            );
        }
        result
    }

    fn check_present(&self, entry_id: &EntryId) -> bool {
        if entry_id == &sentinel_entry_id() {
            return true;
        }
        match self.entries.get(entry_id.index as usize) {
            None => false,
            Some(entry) => entry.get_id().get_term() == entry_id.term,
        }
    }

    // Returns all entries strictly after the supplied id.
    fn get_entries_after(&self, entry_id: &EntryId) -> Vec<Entry> {
        if entry_id == &sentinel_entry_id() {
            return self.entries.clone();
        }
        let mut result = Vec::new();
        let idx = entry_id.index as usize;
        result.clone_from_slice(self.entries[idx..].as_ref());
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
                committed: 0,
                applied: 0,
                role: RaftRole::Follower,
                followers: HashMap::new(),

                timer: Timer::new(),
                timer_guard: None,

                address: server.clone(),
                cluster: all.clone(),
                clients: HashMap::new(),
            })),
        }
    }

    pub fn start(&self) {
        let mut s = self.state.lock().unwrap();
        info!("[{:?}] Starting", s.address.clone());
        RaftImpl::become_follower(&mut s, self.state.clone());
    }

    fn become_follower(state: &mut RaftState, arc_state: Arc<Mutex<RaftState>>) {
        info!("[{:?}] Becoming follower", state.address);
        state.role = RaftRole::Follower;
        state.voted_for = None;

        let mut rng = rand::thread_rng();
        let delay_ms = 2000 + rng.gen_range(1000, 2000);

        let me = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(delay_ms),
            move || {
                info!("[{:?}] Follower timeout", me);
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
            chrono::Duration::seconds(3),
            move || {
                info!("[{:?}] Election timed out, trying again", &a);
                RaftImpl::become_candidate(s.clone());
            },
        ));

        let source = state.address.clone();

        // Start an election and request votes from all servers.
        let mut results = Vec::<GrpcFuture<VoteResponse>>::new();

        let mut request = VoteRequest::new();
        request.set_term(state.term);
        request.set_candidate(source.clone());
        state
            .entries
            .last()
            .map(|entry| request.set_last(entry.id.clone().unwrap()));

        for server in state.get_others() {
            let client = state.get_client(&server);
            info!("[{:?}] Making vote rpc to [{:?}]", &source, &server);
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
        info!("[{:?}] Done waiting for vote requests", &source);

        let mut votes = 0;
        let me = state.address.clone();
        for response in &results {
            match response {
                Ok(message) => {
                    if message.get_term() > state.term {
                        info!("[{:?}] Detected higher term {}", &me, message.get_term());
                        state.term = message.get_term();
                        RaftImpl::become_follower(&mut state, arc_state.clone());
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
        if votes > floor(results.len() as f64 / 2.0, 0) as i32 {
            info!(
                "[{:?}] Won election with {} votes, becoming leader for term {}",
                &me, votes, state.term
            );
            state.role = RaftRole::Leader;
            state.followers = state.create_follower_positions();
            state.timer_guard = Some(state.timer.schedule_with_delay(
                chrono::Duration::milliseconds(500),
                move || {
                    RaftImpl::replicate_entries(arc_state_copy.clone());
                },
            ));
        } else {
            info!("[{:?}] Lost election with {} votes", &me, votes);
        }
    }

    fn replicate_entries(arc_state: Arc<Mutex<RaftState>>) {
        let mut state = arc_state.lock().unwrap();

        if state.role != RaftRole::Leader {
            info!("[{:?}] No longer leader", state.address);
            RaftImpl::become_follower(&mut state, arc_state.clone());
            return;
        }

        info!("[{:?}] Replicating entries", &state.address);
        let mut results = Vec::<GrpcFuture<AppendResponse>>::new();
        for server in state.get_others() {
            let request = state.create_append_request(&server);
            let client = state.get_client(&server);
            results.push(
                client
                    .append(grpc::RequestOptions::new(), request)
                    .drop_metadata(),
            );
        }

        // TODO(dino): Probably want to declare this async instead.
        for result in executor::block_on(join_all(results)) {
            match result {
                Err(message) => info!("[{:?}] Append request failed: {}", &state.address, message),
                Ok(message) => {
                    if message.get_term() > state.term {
                        info!(
                            "[{:?}] Detected higher term {}",
                            &state.address,
                            message.get_term()
                        );
                        state.term = message.get_term();
                        RaftImpl::become_follower(&mut state, arc_state.clone());
                        return;
                    }
                    if message.get_success() {
                        // TODO(dinow): Update next index and match index for follower
                    } else {
                        // TODO(dinow): Decrement next index for follower and try again
                    }
                }
            }
        }
        info!("[{:?}] Done replicating entries", &state.address);

        // Schedule the next run.
        let arc_state_copy = arc_state.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(500),
            move || {
                RaftImpl::replicate_entries(arc_state_copy.clone());
            },
        ));
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
        let mut result = VoteResponse::new();
        result.set_term(state.term);

        if state.term > request.get_term() {
            info!(
                "[{:?}] Our term higher than incoming request term",
                self.address
            );
            result.set_granted(false);
            return sink.finish(result);
        }

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
        info!(
            "[{:?}] Handling append request: [{:?}]",
            self.address, request
        );

        let mut state = self.state.lock().unwrap();
        let mut result = AppendResponse::new();
        result.set_term(state.term);

        if request.get_term() > state.term {
            RaftImpl::become_follower(&mut state, self.state.clone());
            result.set_success(false);
            return sink.finish(result);
        }

        if state.term > request.get_term() {
            result.set_success(false);
            return sink.finish(result);
        }

        // Make sure we have the previous log index sent.
        if !state.check_present(request.get_previous()) {
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
            // TODO(dinow): Apply here?
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

        info!("[{:?}] Successfully processed heartbeat", &state.address);

        result.set_success(true);
        return sink.finish(result);
    }
}
