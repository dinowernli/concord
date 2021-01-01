// This module provides an implementation of a Raft StateMachine whose payloads
// are updates to a simple versioned map from bytes to bytes (called a Store).
//
// This module contains a pure data structure and has no concept of service,
// networking, cluster, etc.
//
// Users of this module are expected to combine this implementation of
// StateMachine with a Raft consensus cluster (as provided by the raft module)
// and expose the result as a service.

#[path = "generated/keyvalue_proto.rs"]
pub mod keyvalue_proto;

mod store;
pub use store::MapStore;
pub use store::Store;
