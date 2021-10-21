use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use futures::executor;
use grpc::{GrpcStatus, ServerHandlerContext, ServerRequestSingle, ServerResponseUnarySink};
use log::{debug, info, warn};
use protobuf::Message;

use crate::keyvalue::keyvalue_proto::{
    Entry, GetRequest, GetResponse, Operation, PutRequest, PutResponse,
};

use crate::keyvalue::{keyvalue_proto, MapStore, Store};
use crate::raft::{new_client, Client, StateMachine};
use tonic::{Request, Response, Status};

use crate::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue_proto::key_value_server::KeyValueServer;
use crate::raft::raft_proto::Server;

// This allows us to combine two non-auto traits into one.
trait StoreStateMachine: Store + StateMachine {}
impl<T: Store + StateMachine> StoreStateMachine for T {}

pub struct KeyValueService {
    address: Server,
    store: Arc<Mutex<dyn StoreStateMachine + Send>>,
    raft: Box<dyn Client + Sync + Send>,
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
            raft: new_client(address, address),
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

// &self,
//         request: Request<HelloRequest>,

#[tonic::async_trait]
impl KeyValue for KeyValueService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        debug!("[{:?}] Handling GET request", &self.address);
        let request = request.into_inner();
        if request.key.is_empty() {
            return Err(Status::invalid_argument("Empty key"));
        }
        let key = request.key.to_bytes();

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

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        debug!("[{:?}] Handling PUT request", &self.address);
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

#[cfg(test)]
mod tests {
    use grpc::{ClientStubExt, Error};

    use crate::keyvalue::keyvalue_proto_grpc::{KeyValueClient, KeyValueServer};
    use crate::raft::raft_proto::EntryId;

    use super::*;

    // A fake client which just takes any commits and writes them straight to the
    // supplied store (rather than going through remote consensus).
    struct FakeRaftClient {
        store: Arc<Mutex<MapStore>>,
    }

    #[async_trait]
    impl Client for FakeRaftClient {
        async fn commit(&self, payload: &[u8]) -> Result<EntryId, Error> {
            let copy = payload.clone().to_bytes();
            self.store
                .lock()
                .unwrap()
                .apply(&copy)
                .expect("bad payload");
            Ok(EntryId::new())
        }

        async fn preempt_leader(&self) -> Result<Server, Error> {
            unimplemented!();
        }
    }

    #[async_std::test]
    async fn test_get() {
        let service = create_service();
        let store = service.store.clone();
        let server = create_grpc_server(service);

        store
            .lock()
            .unwrap()
            .set(Bytes::from("foo"), Bytes::from("bar"));

        let mut request = GetRequest::new();
        request.set_key(String::from("foo").into_bytes());

        let response = create_grpc_client(&server)
            .get(grpc::RequestOptions::new(), request.clone())
            .drop_metadata()
            .await
            .expect("response");

        assert!(response.has_entry());
        let entry = response.get_entry().clone();
        assert_eq!(Bytes::from("foo").as_ref(), entry.get_key());
        assert_eq!(Bytes::from("bar").as_ref(), entry.get_value());
    }

    #[async_std::test]
    async fn test_put() {
        let service = create_service();
        let store = service.store.clone();
        let server = create_grpc_server(service);

        let mut request = PutRequest::new();
        request.set_key(String::from("foo").into_bytes());
        request.set_value(String::from("bar").into_bytes());

        create_grpc_client(&server)
            .put(grpc::RequestOptions::new(), request.clone())
            .drop_metadata()
            .await
            .expect("response");

        let value = store
            .lock()
            .unwrap()
            .get(&Bytes::from("foo"))
            .expect("present");
        assert_eq!(Bytes::from("bar"), value);
    }

    // Returns an instance of the service struct we're testing
    fn create_service() -> KeyValueService {
        let store = Arc::new(Mutex::new(MapStore::new()));
        KeyValueService {
            address: make_server("test", 1234),
            store: store.clone(),
            raft: Box::new(FakeRaftClient {
                store: store.clone(),
            }),
        }
    }

    fn create_grpc_server(service: KeyValueService) -> grpc::Server {
        let mut server_builder = grpc::ServerBuilder::new_plain();
        server_builder.add_service(KeyValueServer::new_service_def(service));
        server_builder.http.set_addr(("0.0.0.0", 0)).unwrap();
        server_builder.build().expect("server")
    }

    fn create_grpc_client(server: &grpc::Server) -> KeyValueClient {
        let client_conf = Default::default();
        let port = server.local_addr().port().expect("port");
        KeyValueClient::new_plain("0.0.0.0", port, client_conf).expect("client")
    }

    fn make_server(host: &str, port: i32) -> Server {
        let mut result = Server::new();
        result.set_host(host.to_string());
        result.set_port(port);
        result
    }
}
