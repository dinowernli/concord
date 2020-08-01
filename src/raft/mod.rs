#[path = "generated/raft_proto.rs"]
pub mod raft_proto;

#[path = "generated/raft_proto_grpc.rs"]
pub mod raft_proto_grpc;

mod log;

mod client;
pub use client::Client;

mod consensus;
pub use consensus::RaftImpl;

mod diagnostics;
pub use diagnostics::Diagnostics;

mod state_machine;
pub use state_machine::StateMachine;
