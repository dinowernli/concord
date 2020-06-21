mod raft;
mod raft_grpc;

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
        println!("Vote request not implemented");
        response.finish(VoteResponse::new())
    }

    fn append(
        &self,
        _: ServerHandlerContext,
        _request: ServerRequestSingle<AppendRequest>,
        response: ServerResponseUnarySink<AppendResponse>,
    ) -> grpc::Result<()> {
        println!("Append request not implemented");
        response.finish(AppendResponse::new())
    }
}

fn main() {
    let port = 12345;
    let mut server_builder = grpc::ServerBuilder::new_plain();
    server_builder.add_service(RaftServer::new_service_def(RaftImpl));
    server_builder.http.set_port(port);

    // Give this a name so it doesn't get immediately destroyed.
    let _server = server_builder.build().expect("server");
    println!("Started server on port {}", port);

    let client_conf = Default::default();
    let client = RaftClient::new_plain("::1", port, client_conf).unwrap();
    let response = client
        .append(grpc::RequestOptions::new(), AppendRequest::new())
        .join_metadata_result();
    println!("Made rpc to server");
    println!("{:?}", executor::block_on(response));

    loop {
        thread::park();
    }
}
