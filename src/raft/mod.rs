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

mod log;

mod client;
pub use client::{new_client, Client};

mod consensus;
pub use consensus::{Config, RaftImpl};

mod diagnostics;
pub use diagnostics::Diagnostics;

mod state_machine;
pub use state_machine::{StateMachine, StateMachineResult};
