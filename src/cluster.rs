extern crate chrono;
extern crate timer;

#[path = "generated/raft.rs"]
pub mod raft;

#[path = "generated/raft_grpc.rs"]
pub mod raft_grpc;

use log::info;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use timer::Guard;
use timer::Timer;

use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;

use raft::AppendRequest;
use raft::AppendResponse;
use raft::Entry;
use raft::EntryId;
use raft::Server;
use raft::VoteRequest;
use raft::VoteResponse;

use raft_grpc::Raft;

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

    timer: Timer,
    timer_guard: Option<Guard>,
}

pub struct RaftImpl {
    state: Arc<Mutex<RaftState>>,

    // Cluster membership.
    address: Server,
    cluster: Vec<Server>,
}

impl RaftImpl {
    pub fn new(server: &Server, all: &Vec<Server>) -> RaftImpl {
        RaftImpl {
            address: server.clone(),
            cluster: all.clone(),
            state: Arc::new(Mutex::new(RaftState {
                term: 0,
                voted_for: None,
                entries: Vec::new(),
                committed: 0,
                applied: 0,
                role: RaftRole::Follower,

                timer: Timer::new(),
                timer_guard: None,
            })),
        }
    }

    fn become_follower(&self, state: &mut RaftState) {
        state.role = RaftRole::Follower;
        info!("[{:?}] becoming follower", self.address);

        let address = self.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::seconds(3),
            move || {
                info!("[{:?}] follower timeout", address);
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
        info!(
            "[{:?}] handling vote request: [{:?}]",
            self.address, req.message
        );
        let request = req.message;
        let mut state = self.state.lock().unwrap();
        let mut result = VoteResponse::new();
        result.set_term(state.term);

        if request.get_term() > state.term {
            self.become_follower(&mut state);
            result.set_granted(false);
            return sink.finish(result);
        }

        if state.term > request.get_term() {
            result.set_granted(false);
            return sink.finish(result);
        }

        let candidate = request.get_candidate();
        if candidate == state.voted_for.as_ref().unwrap_or(candidate) {
            if is_up_to_date(request.get_last(), &state.entries) {
                state.voted_for = Some(candidate.clone());
                result.set_granted(true);
            } else {
                result.set_granted(false);
            }
            return sink.finish(result);
        }

        result.set_granted(false);
        sink.finish(result)
    }

    fn append(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<AppendRequest>,
        sink: ServerResponseUnarySink<AppendResponse>,
    ) -> grpc::Result<()> {
        info!(
            "[{:?}] handling vote request: [{:?}]",
            self.address, req.message
        );
        let request = req.message;
        let mut state = self.state.lock().unwrap();
        let mut result = AppendResponse::new();
        result.set_term(state.term);

        if request.get_term() > state.term {
            self.become_follower(&mut state);
            result.set_success(false);
            return sink.finish(result);
        }

        if state.term > request.get_term() {
            result.set_success(false);
            return sink.finish(result);
        }

        // Make sure we have the previous log index sent.
        let pindex = request.get_previous().get_index() as usize;
        let pterm = request.get_previous().get_term();
        if pindex >= state.entries.len() || state.entries[pindex].get_id().get_term() != pterm {
            result.set_success(false);
            return sink.finish(result);
        }

        let mut last_written = state.entries.len() - 1;
        for entry in request.get_entries() {
            let index = entry.get_id().get_index() as usize;
            if index == state.entries.len() {
                state.entries.push(entry.clone());
            } else {
                state.entries[index] = entry.clone();
            }
            last_written = index;
        }
        state.entries.truncate(last_written + 1);

        let leader_commit = request.get_committed();
        if leader_commit > state.committed {
            state.committed = min(leader_commit, last_written as i64);
        }

        result.set_success(true);
        return sink.finish(result);
    }
}
