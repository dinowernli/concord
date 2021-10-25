use async_std::sync::{Arc, Mutex};

use bytes::Bytes;
use log::{debug, info, warn};
use tonic::{Request, Response, Status};

use crate::keyvalue::keyvalue_proto::operation::Op;
use crate::keyvalue::keyvalue_proto::{
    Entry, GetRequest, GetResponse, Operation, PutRequest, PutResponse, SetOperation,
};
use crate::keyvalue::{keyvalue_proto, MapStore, Store};
use crate::keyvalue_proto::key_value_server::KeyValue;
use crate::raft::raft_proto::Server;
use crate::raft::{new_client, Client, StateMachine};
use prost::Message;

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
        Operation {
            op: Some(Op::Set(SetOperation {
                entry: Some(keyvalue_proto::Entry {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }),
            })),
        }
    }
}

#[tonic::async_trait]
impl KeyValue for KeyValueService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        debug!("[{:?}] Handling GET request", &self.address);
        let request = request.into_inner();
        if request.key.is_empty() {
            return Err(Status::invalid_argument("Empty key"));
        }
        let key = request.key.to_vec();

        let locked = self.store.lock().await;
        let version = if request.version <= 0 {
            locked.latest_version()
        } else {
            request.version
        };

        let lookup = locked.get_at(&Bytes::from(key.to_vec()), version);
        if lookup.is_err() {
            return Err(Status::out_of_range("Compacted"));
        }

        Ok(Response::new(GetResponse {
            version,
            entry: match lookup.unwrap() {
                None => None,
                Some(value) => Some(Entry {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }),
            },
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        debug!("[{:?}] Handling PUT request", &self.address);
        let request = request.into_inner();
        if request.key.is_empty() {
            return Err(Status::invalid_argument("Empty key"));
        }
        if request.value.is_empty() {
            return Err(Status::invalid_argument("Empty value"));
        }

        let op =
            KeyValueService::make_set_operation(&request.key.to_vec(), &request.value.to_vec());
        let serialized = op.encode_to_vec();

        let commit = self.raft.commit(&serialized).await;
        let key_str = String::from_utf8_lossy(request.key.as_slice());
        match commit {
            Ok(id) => {
                info!(
                    "Committed put operation with raft index {} for key {}",
                    id.index, key_str
                );
                Ok(Response::new(PutResponse {}))
            }
            Err(message) => {
                warn!(
                    "Failed to commit put operation for key: {}, message: {}",
                    key_str, message
                );
                Err(Status::internal(format!("Failed to commit: {}", message)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::keyvalue::keyvalue_proto::key_value_client::KeyValueClient;
    use crate::keyvalue::keyvalue_proto::key_value_server::KeyValueServer;
    use crate::raft::raft_proto::EntryId;
    use tokio::time::Duration;
    use tonic::transport::Channel;

    use super::*;

    // A fake client which just takes any commits and writes them straight to the
    // supplied store (rather than going through remote consensus).
    struct FakeRaftClient {
        store: Arc<Mutex<MapStore>>,
    }

    #[tonic::async_trait]
    impl Client for FakeRaftClient {
        async fn commit(&self, payload: &[u8]) -> Result<EntryId, Status> {
            let copy = payload.to_vec();
            self.store
                .lock()
                .await
                .apply(&Bytes::from(copy))
                .expect("bad payload");
            Ok(EntryId { term: 0, index: 0 })
        }

        async fn preempt_leader(&self) -> Result<Server, Status> {
            unimplemented!();
        }
    }

    #[tokio::test]
    async fn test_get() {
        let service = create_service();
        let store = service.store.clone();
        tokio::spawn(create_grpc_server(service, 12344));

        // TODO when is the server ready?!
        tokio::time::sleep(Duration::from_millis(3000)).await;

        store
            .lock()
            .await
            .set(Bytes::from("foo"), Bytes::from("bar"));

        let request = GetRequest {
            key: "foo".as_bytes().to_vec(),
            version: -1,
        };
        let response = create_grpc_client(12344)
            .await
            .get(Request::new(request.clone()))
            .await
            .expect("response")
            .into_inner();

        assert!(response.entry.is_some());
        let entry = response.entry.clone().expect("entry");
        assert_eq!(Bytes::from("foo").as_ref(), entry.key);
        assert_eq!(Bytes::from("bar").as_ref(), entry.value);
    }

    #[tokio::test]
    async fn test_put() {
        let service = create_service();
        let store = service.store.clone();
        tokio::spawn(create_grpc_server(service, 12340));

        // TODO when is the server ready?!
        tokio::time::sleep(Duration::from_millis(3000)).await;
        // HealthClient.watch?

        let request = PutRequest {
            key: "foo".as_bytes().to_vec(),
            value: "bar".as_bytes().to_vec(),
        };

        create_grpc_client(12340)
            .await
            .put(Request::new(request.clone()))
            .await
            .expect("response");

        let value = store
            .lock()
            .await
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

    async fn create_grpc_server(service: KeyValueService, port: i32) {
        // TODO(dino): bind to arbitrary port (0) and figure out how to retrieve it
        let serve = tonic::transport::Server::builder()
            .add_service(KeyValueServer::new(service))
            .serve(format!("[{}]:{}", "::1", port).parse().unwrap())
            .await;
    }

    async fn create_grpc_client(port: i32) -> KeyValueClient<Channel> {
        // TODO get port from server (?)
        KeyValueClient::connect(format!("http://[::1]:{}", port))
            .await
            .expect("server")
    }

    fn make_server(host: &str, port: i32) -> Server {
        Server {
            host: host.to_string(),
            port,
        }
    }
}
