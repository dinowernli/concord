pub mod diagnostics;

#[path = "generated/raft_proto.rs"]
pub mod raft_proto;

#[path = "generated/raft_proto_grpc.rs"]
pub mod raft_proto_grpc;

mod client;
pub use client::Client;
