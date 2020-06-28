extern crate chrono;
extern crate timer;

#[path = "generated/raft.rs"]
pub mod raft;

#[path = "generated/raft_grpc.rs"]
pub mod raft_grpc;

use futures::executor;
use futures::future::join_all;
use log::info;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use timer::Guard;
use timer::Timer;

use grpc::ClientStubExt;
use grpc::GrpcFuture;
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

fn rpc_request_vote_sync(source: &Server, address: &Server, request: &VoteRequest) {
    let client_conf = Default::default();
    let port = address.get_port() as u16;
    info!(
        ">>>>>>> Making sync vote rpc from [{:?}] to [{:?}]",
        &source, &address
    );
    let client = RaftClient::new_plain(address.get_host(), port, client_conf).unwrap();
    let response = client
        .vote(grpc::RequestOptions::new(), request.clone())
        .join_metadata_result();
    info!(
        ">>>>>>> Finished sync vote rpc, got response {:?}",
        executor::block_on(response)
    );
}

fn address_key(address: &Server) -> String {
    format!("{}:{}", address.get_host(), address.get_port())
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
        info!("[{:?}] starting", s.address.clone());
        self.become_follower(&mut s);
    }

    fn become_follower(&self, state: &mut RaftState) {
        info!("[{:?}] becoming follower", state.address);
        state.role = RaftRole::Follower;
        state.voted_for = None;

        let s = self.state.clone();
        let a = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::seconds(3),
            move || {
                info!("[{:?}] follower timeout", a);
                RaftImpl::become_candidate(s.clone());
            },
        ));
    }

    fn become_candidate(arc_state: Arc<Mutex<RaftState>>) {
        let mut state = arc_state.lock().unwrap();

        info!("[{:?}] becoming candidate", state.address);
        state.role = RaftRole::Candidate;
        state.term = state.term + 1;
        state.voted_for = Some(state.address.clone());

        let s = arc_state.clone();
        let a = state.address.clone();
        state.timer_guard = Some(state.timer.schedule_with_delay(
            chrono::Duration::seconds(3),
            move || {
                info!("[{:?}] election timed out, trying agin", &a);
                RaftImpl::become_candidate(s.clone());
            },
        ));

        // Start an election and request votes from all servers.
        let mut results = Vec::<GrpcFuture<VoteResponse>>::new();

        let mut request = VoteRequest::new();
        let source = state.address.clone();

        request.set_term(state.term);
        request.set_candidate(source.clone());

        for server in state.cluster.clone() {
            if server == state.address {
                continue;
            }

            let client = state.get_client(&server);

            // TODO: For debugging, remove.
            // rpc_request_vote_sync(&source, &server, &request);

            info!("[{:?}] Making vote rpc to [{:?}]", &source, &server);
            results.push(client
                .vote(grpc::RequestOptions::new(), request.clone())
                .drop_metadata());
        }

        /*
        let result = executor::block_on(join_all(results));
        let mut votes = 0;
        let b = state.address.clone();
        for r in result {
            match r {
                Ok(message) => {
                    if message.get_granted() {
                        info!("[{:?}] got vote granted", &b);
                        votes = votes + 1;
                    } else {
                        info!("[{:?}] got vote denied", &b);
                    }
                }
                Err(message) => {
                    info!("[{:?}] rpc to request votes failed: {:?}", &b, message);
                }
            }
        }
        info!("[{:?}] get {} votes", &b, votes);
        */
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
            "[{:?}] handling vote request: [{:?}]",
            self.address, request
        );

        let mut state = self.state.lock().unwrap();
        info!("[{:?}] acquired lock in vote request", self.address);

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
        let x = sink.finish(result);

        info!("[{:?}] done processing vote request", self.address);
        x
    }

    fn append(
        &self,
        _: ServerHandlerContext,
        req: ServerRequestSingle<AppendRequest>,
        sink: ServerResponseUnarySink<AppendResponse>,
    ) -> grpc::Result<()> {
        let request = req.message;
        info!(
            "[{:?}] handling append request: [{:?}]",
            self.address, request
        );

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
