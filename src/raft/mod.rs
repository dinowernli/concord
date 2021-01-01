// This module implements consensus based on Raft, capable of adding and
// removing servers from the Raft cluster, committing (opaque) payloads, etc.
//
// Users of this module must supply an implementation of the StateMachine
// trait. This module then guarantees that for all servers in the Raft cluster,
// the respective StateMachine objects will receive the same payloads.
//
// The implementation in this module is based on the paper at:
// https://raft.github.io/raft.pdf

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
pub use state_machine::{StateMachine, StateMachineResult};
