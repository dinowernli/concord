// This module implements consensus based on Raft, capable of adding and
// removing servers from the Raft cluster, committing (opaque) payloads, etc.
//
// Users of this module must supply an implementation of the StateMachine
// trait. This module then guarantees that for all servers in the Raft cluster,
// the respective StateMachine objects will receive the same payloads.
//
// The implementation in this module is based on the paper at:
// https://raft.github.io/raft.pdf

pub use client::{Client, new_client};
pub use consensus::{Options, RaftImpl};
pub use diagnostics::Diagnostics;
pub use failure_injection::FailureOptions;
pub use state_machine::{StateMachine, StateMachineResult};

#[path = "generated/raft_common_proto.rs"]
pub mod raft_common_proto;

#[path = "generated/raft_persistence_proto.rs"]
pub mod raft_persistence_proto;

#[path = "generated/raft_service_proto.rs"]
pub mod raft_service_proto;

mod client;
mod cluster;
mod consensus;
mod diagnostics;
mod failure_injection;
mod log;
mod persistence;
mod state_machine;
mod store;

#[cfg(test)]
mod testing;
