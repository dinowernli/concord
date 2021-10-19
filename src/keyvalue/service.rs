use std::sync::{Arc, Mutex};

use bytes::Buf;
use bytes::Bytes;
use futures::executor;
use grpc::{GrpcStatus, ServerHandlerContext, ServerRequestSingle, ServerResponseUnarySink};
use log::{info, warn};
use protobuf::Message;

use crate::keyvalue::keyvalue_proto::{
    Entry, GetRequest, GetResponse, Operation, PutRequest, PutResponse,
};
use crate::keyvalue::keyvalue_proto_grpc::KeyValue;
use crate::keyvalue::{keyvalue_proto, MapStore, Store};
use crate::raft::raft_proto::Server;
use crate::raft::{Client, StateMachine};

pub struct KeyValueService {
    address: Server,
    store: Arc<Mutex<MapStore>>,
    raft: Client,
}

impl KeyValueService {
    // Creates a new instance of the service which will use the cluster of the
    // supplied member for its Raft consensus.
    pub fn new(address: &Server) -> KeyValueService {
        KeyValueService {
            address: address.clone(),
            store: Arc::new(Mutex::new(MapStore::new())),

            // We assume that every node running this service also runs a Raft service
            // underneath, so we pass the same address twice to the Raft client.
            raft: Client::new(address, address),
        }
    }

    pub fn raft_state_machine(&self) -> Arc<Mutex<dyn StateMachine + Send>> {
        self.store.clone()
    }

    fn make_set_operation(key: &[u8], value: &[u8]) -> Operation {
        let mut entry = keyvalue_proto::Entry::new();
        entry.set_key(key.to_vec());
        entry.set_value(value.to_vec());

        let mut op = keyvalue_proto::SetOperation::new();
        op.set_entry(entry);

        let mut result = Operation::new();
        result.set_set(op);
        result
    }
}

impl KeyValue for KeyValueService {
    fn get(
        &self,
        _: ServerHandlerContext,
        request: ServerRequestSingle<GetRequest>,
        response: ServerResponseUnarySink<GetResponse>,
    ) -> grpc::Result<()> {
        if request.message.get_key().is_empty() {
            return response.send_grpc_error(GrpcStatus::Argument, "Empty key".to_string());
        }
        let key = request.message.get_key().to_bytes();

        let locked = self.store.lock().unwrap();
        let version = if request.message.version <= 0 {
            locked.latest_version()
        } else {
            request.message.version
        };

        let lookup = locked.get_at(&key, version);
        if lookup.is_err() {
            return response.send_grpc_error(GrpcStatus::OutOfRange, "Compacted".to_string());
        }

        let mut result = GetResponse::new();
        result.set_version(version);
        match lookup.unwrap() {
            None => (),
            Some(value) => {
                let mut entry = Entry::new();
                entry.set_key(key.to_vec());
                entry.set_value(value.to_vec());
                result.set_entry(entry);
            }
        }
        response.finish(result)
    }

    fn put(
        &self,
        _: ServerHandlerContext,
        request_single: ServerRequestSingle<PutRequest>,
        response: ServerResponseUnarySink<PutResponse>,
    ) -> grpc::Result<()> {
        let request = request_single.message;
        if request.get_key().is_empty() {
            return response.send_grpc_error(GrpcStatus::Argument, "Empty key".to_string());
        }
        if request.get_value().is_empty() {
            return response.send_grpc_error(GrpcStatus::Argument, "Empty value".to_string());
        }

        let op = KeyValueService::make_set_operation(&request.get_key(), &request.get_value());
        let serialized = Bytes::from(op.write_to_bytes().expect("serialization"));

        // TODO(dino): Use await once this GRPC implementation can become async.
        let commit = executor::block_on(self.raft.commit(&serialized));
        return match commit {
            Ok(id) => {
                let key_str = String::from_utf8_lossy(request.get_key());
                info!(
                    "Committed put operation with raft index {} for key {}",
                    id.get_index(),
                    key_str
                );
                response.finish(PutResponse::new())
            }
            Err(message) => {
                let key_str = String::from_utf8_lossy(request.get_key());
                warn!(
                    "Failed to commit put operation for key: {}, message: {}",
                    key_str, message
                );
                response
                    .send_grpc_error(GrpcStatus::Internal, "Failed to commit to Raft".to_string())
            }
        };
    }
}
