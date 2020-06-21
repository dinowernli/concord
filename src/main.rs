mod raft;
mod raft_grpc;

use log::info;

use env_logger::Env;
use futures::executor;
use std::thread;

use grpc::ClientStubExt;
use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;

use raft::AppendRequest;
use raft::AppendResponse;
use raft::VoteRequest;
use raft::VoteResponse;

use raft_grpc::Raft;
use raft_grpc::RaftClient;
use raft_grpc::RaftServer;

struct RaftImpl;

impl Raft for RaftImpl {
    fn vote(
        &self,
        _: ServerHandlerContext,
        _request: ServerRequestSingle<VoteRequest>,
        response: ServerResponseUnarySink<VoteResponse>,
    ) -> grpc::Result<()> {
        info!("Processed vote rpc");
        response.finish(VoteResponse::new())
    }

    fn append(
        &self,
        _: ServerHandlerContext,
        _request: ServerRequestSingle<AppendRequest>,
        response: ServerResponseUnarySink<AppendResponse>,
    ) -> grpc::Result<()> {
        info!("Processed append rpc");
        response.finish(AppendResponse::new())
    }
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let port = 12345;
    let mut server_builder = grpc::ServerBuilder::new_plain();
    server_builder.add_service(RaftServer::new_service_def(RaftImpl));
    server_builder.http.set_port(port);

    // Give this a name so it doesn't get immediately destroyed.
    let _server = server_builder.build().expect("server");
    info!("Started server on port {}", port);

    let client_conf = Default::default();
    let client = RaftClient::new_plain("::1", port, client_conf).unwrap();
    let response = client
        .append(grpc::RequestOptions::new(), AppendRequest::new())
        .join_metadata_result();
    info!("Made rpc to server");
    info!("{:?}", executor::block_on(response));

    loop {
        thread::park();
    }
}
