use std::sync::{Arc, Mutex};

use bytes::Buf;
use grpc::{GrpcStatus, ServerHandlerContext, ServerRequestSingle, ServerResponseUnarySink};

use crate::keyvalue::keyvalue_proto::{Entry, GetRequest, GetResponse, PutRequest, PutResponse};
use crate::keyvalue::keyvalue_proto_grpc::KeyValue;
use crate::keyvalue::Store;
use crate::raft::Client;

struct KeyValueService {
    store: Arc<Mutex<dyn Store>>,
    raft: Client,
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
        self.store
            .lock()
            .unwrap()
            .set(request.get_key().to_bytes(), request.get_value().to_bytes());
        response.finish(PutResponse::new())
    }
}
