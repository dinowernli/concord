extern crate chrono;
extern crate math;
extern crate rand;
extern crate timer;

#[path = "generated/raft.rs"]
pub mod raft;

#[path = "generated/raft_grpc.rs"]
pub mod raft_grpc;

use futures::future::join_all;
use futures::{executor, TryFutureExt};
use grpc::ClientStubExt;
use grpc::GrpcFuture;
use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;
use log::info;
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
use raft::{CommitRequest, CommitResponse, Status};
use raft_grpc::Raft;
use raft_grpc::RaftClient;
use std::time;

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

#[derive(Debug)]
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
                last_known_leader: None,
            })),
        }
    }

    pub fn start(&self) {
        let mut s = self.state.lock().unwrap();
        info!("[{:?}] Starting", s.address.clone());
        RaftImpl::become_follower(&mut s, self.state.clone());
    }

    fn become_follower(state: &mut RaftState, arc_state: Arc<Mutex<RaftState>>) {
        info!(
            "[{:?}] Becoming follower for term {}",
            state.address, state.term
        );
        state.role = RaftRole::Follower;
        state.voted_for = None;

        let mut rng = rand::thread_rng();
        let delay_ms = 2000 + rng.gen_range(1000, 2000);

        let me = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::milliseconds(delay_ms),
            move || {
                // TODO(dino): Only do this if we're still in the same term.
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

        let mut votes = 1; // Here we count our own vote for ourselves.
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
        if 2 * votes > state.cluster.len() {
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
        let mut results = Vec::<GrpcFuture<(Server, AppendRequest, AppendResponse)>>::new();
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

        // TODO(dino): Probably want to declare this async instead.
        let results = executor::block_on(join_all(results));

        for result in results {
            match result {
                Err(message) => info!(
                    "[{:?}] Append request failed, error: {}",
                    &state.address, message
                ),
                Ok((peer, request, response)) => {
                    if response.get_term() > state.term {
                        info!(
                            "[{:?}] Detected higher term {} from peer {:?}",
                            &state.address,
                            response.get_term(),
                            &peer,
                        );
                        state.term = response.get_term();
                        RaftImpl::become_follower(&mut state, arc_state.clone());
                        return;
                    }

                    let me = state.address.clone();
                    let follower = state.followers.get_mut(address_key(&peer).as_str());
                    if follower.is_some() {
                        let f = follower.unwrap();
                        if response.get_success() {
                            // The follower has appended our entries. The next index we wish to
                            // send them is the one immediately after the last one we sent.
                            match request.get_entries().last() {
                                Some(e) => {
                                    f.next_index = e.get_id().get_index() + 1;
                                    f.match_index = e.get_id().get_index();
                                    info!(
                                        "[{:?}] Updated follower state for peer {:?} to {:?}",
                                        &me, &peer, f
                                    );
                                }
                                None => (),
                            }
                        } else {
                            // The follower has rejected our entries, presumably because they could
                            // not find the entry we sent as "previous". We repeatedly reduce the
                            // "next" index until we hit a "previous" entry present on the follower.
                            f.next_index = f.next_index - 1;
                            info!(
                                "[{:?}] Decremented follower next_index for peer {:?} to {}",
                                &me, &peer, f.next_index
                            );
                        }
                    }
                }
            }
        }

        // Next, we check if we can increment our committed index.
        let saved_committed = state.committed;
        for index in state.committed + 1..state.entries.len() as i64 {
            let mut matches = 1; // We match.
            for (_, follower) in &state.followers {
                if follower.match_index >= index {
                    matches = matches + 1;
                }
            }

            if 2 * matches > state.cluster.len() {
                state.committed = index;
            }
        }
        if state.committed != saved_committed {
            info!(
                "[{:?}] Updated committed index from {} to {}",
                &state.address, saved_committed, state.committed
            );
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
            state.term = request.get_term();
            RaftImpl::become_follower(&mut state, self.state.clone());
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
        info!(
            "[{:?}] Handling append request: [{:?}]",
            self.address, request
        );

        let mut state = self.state.lock().unwrap();
        let mut result = AppendResponse::new();
        result.set_term(state.term);

        if request.get_term() > state.term {
            state.term = request.get_term();
            RaftImpl::become_follower(&mut state, self.state.clone());

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

    fn commit(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<CommitRequest>,
        sink: ServerResponseUnarySink<CommitResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        info!("[{:?}] Handling commit request", self.address);

        let mut state = self.state.lock().unwrap();
        if state.role != RaftRole::Leader {
            let mut result = CommitResponse::new();
            result.set_status(Status::NOT_LEADER);
            state
                .last_known_leader
                .as_ref()
                .map(|l| result.set_leader(l.clone()));
            return sink.finish(result);
        }

        let mut entry_id = EntryId::new();
        entry_id.set_term(state.term);
        entry_id.set_index(state.entries.len() as i64);

        let mut entry = Entry::new();
        entry.set_id(entry_id.clone());
        entry.set_payload(request.payload);

        state.entries.push(entry);

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
