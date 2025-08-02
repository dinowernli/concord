// This module provides a keyvalue store grpc service which is backed by a raft
// cluster. Users of this module are expected to run the service in their grpc
// server and/or use the generated grpc client code to make requests.

pub use crate::keyvalue::store::MapStore;
pub use http::HttpHandler;
pub use service::KeyValueService;

pub mod grpc {
    pub use crate::keyvalue::keyvalue_proto::PutRequest;
    pub use crate::keyvalue::keyvalue_proto::key_value_client::KeyValueClient;
    pub use crate::keyvalue::keyvalue_proto::key_value_server::KeyValueServer;
}

pub(in crate::keyvalue) mod http;
#[path = "generated/keyvalue_proto.rs"]
pub(crate) mod keyvalue_proto;
pub(in crate::keyvalue) mod service;
pub(in crate::keyvalue) mod store;
