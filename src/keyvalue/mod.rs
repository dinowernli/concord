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
// TODO(dinow): switch "main.rs" from using the (internal) "Operation" directly
// and only expose the Request/Response types here rather than the entire module.
// This amounts to removing "pub" here and adding more targeted "pub use" directives.

// TODO(dinow): stop exposing these and make the "store" types an implementation detail.
mod store;
pub use store::MapStore;
pub use store::Store;

mod service;
pub use service::KeyValueService;
