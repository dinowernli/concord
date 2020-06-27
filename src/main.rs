mod raft;
mod raft_grpc;

use log::info;

use env_logger::Env;
use futures::executor;
use std::cmp::min;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::thread;

use grpc::ClientStubExt;
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
use raft_grpc::RaftServer;

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
}

struct RaftImpl {
    state: Arc<Mutex<RaftState>>,

    // Cluster membership.
    address: Server,
    cluster: Vec<Server>,
}

impl RaftImpl {
    fn new(server: &Server, all: &Vec<Server>) -> RaftImpl {
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
            })),
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
        info!(
            "[{:?}] handling vote request: [{:?}]",
            self.address, req.message
        );
        let request = req.message;
        let mut state = self.state.lock().unwrap();

        let mut result = VoteResponse::new();
        result.set_term(state.term);

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

fn server(host: &str, port: i32) -> Server {
    let mut result = Server::new();
    result.set_host(host.to_string());
    result.set_port(port);
    return result;
}

fn start_node(address: &Server, all: &Vec<Server>) -> grpc::Server {
    let mut server_builder = grpc::ServerBuilder::new_plain();
    server_builder.add_service(RaftServer::new_service_def(RaftImpl::new(address, all)));
    server_builder
        .http
        .set_port(address.get_port().try_into().unwrap());
    server_builder.build().expect("server")
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];

    let mut servers = Vec::<grpc::Server>::new();
    for address in &addresses {
        servers.push(start_node(&address, &addresses));
        info!("Started server on port {}", address.get_port());
    }

    let client_conf = Default::default();
    let client = RaftClient::new_plain(
        "::1",
        addresses[0].get_port().try_into().unwrap(),
        client_conf,
    )
    .unwrap();
    let mut request = AppendRequest::new();
    request.set_term(1);
    let response = client
        .append(grpc::RequestOptions::new(), request)
        .join_metadata_result();
    info!("Made rpc to server");
    info!("{:?}", executor::block_on(response));

    loop {
        thread::park();
    }
}
