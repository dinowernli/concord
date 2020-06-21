mod raft;
mod raft_grpc;

use grpc::ServerHandlerContext;
use grpc::ServerRequestSingle;
use grpc::ServerResponseUnarySink;

use raft::VoteRequest;
use raft::VoteResponse;
use raft::AppendRequest;
use raft::AppendResponse;

struct RaftImpl;

impl raft_grpc::Raft for RaftImpl {
  fn vote(
      &self,
      _: ServerHandlerContext,
      _request: ServerRequestSingle<VoteRequest>,
      response: ServerResponseUnarySink<VoteResponse>) -> grpc::Result<()> {
    response.finish(VoteResponse::new())
  }

  fn append(
      &self,
      _: ServerHandlerContext,
      _request: ServerRequestSingle<AppendRequest>,
      response: ServerResponseUnarySink<AppendResponse>) -> grpc::Result<()> {
    response.finish(AppendResponse::new())
  }
}

fn main() {
  println!("Hello, world!");
}
