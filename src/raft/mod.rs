// This module implements consensus based on Raft, capable of adding and
// removing servers from the Raft cluster, committing (opaque) payloads, etc.
//
// Users of this module must supply an implementation of the StateMachine
// trait. This module then guarantees that for all servers in the Raft cluster,
// the respective StateMachine objects will receive the same payloads.
//
// The implementation in this module is based on the paper at:
// https://raft.github.io/raft.pdf

pub use client::{new_client, Client};
pub use consensus::{Config, RaftImpl};
pub use diagnostics::Diagnostics;
pub use state_machine::{StateMachine, StateMachineResult};

#[path = "generated/raft_proto.rs"]
pub mod raft_proto;

mod client;
mod cluster;
mod consensus;
mod diagnostics;
mod log;
mod state_machine;
mod store;

#[cfg(test)]
mod testing;
