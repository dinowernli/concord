use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};

use crate::keyvalue::keyvalue_proto;
use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue::keyvalue_proto::operation::Op;
use crate::keyvalue::keyvalue_proto::{
    Entry, GetRequest, GetResponse, Operation, PutRequest, PutResponse, SetOperation,
};
use crate::keyvalue::store::Store;
use crate::raft::raft_common_proto::Server;
use crate::raft::{Client, new_client};

pub struct KeyValueService {
    // A name for this service, used for debugging, log messages, etc.
    name: String,

    // Holds the actual key-value data. We only ever read - writes happen via
    // the raft client, and we let someone else (the raft server) apply changes
    // to the store via raft state machine updates.
    store: Arc<Mutex<dyn Store + Send>>,

    // A client talking to the underlying Raft cluster.
    raft: Box<dyn Client + Sync + Send>,
}

impl KeyValueService {
    // Creates a new instance of the service.
    //
    // The instance will only ever read from the supplied store, and will connect to the
    // supplied raft cluster (represented by an arbitrary member) in order to commit writes.
    pub fn new(
        name: &str,
        raft_member: &Server,
        store: Arc<Mutex<dyn Store + Send>>,
    ) -> KeyValueService {
        KeyValueService {
            name: name.into(),
            store,
            raft: new_client(name.into(), raft_member),
        }
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

#[async_trait]
impl KeyValue for KeyValueService {
    #[instrument(fields(server=%self.name),skip(self,request))]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

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
            return Err(Status::out_of_range(format!(
                "Version {} compacted",
                version
            )));
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

    #[instrument(fields(server=%self.name),skip(self,request))]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        debug!(?request, "handling request");

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
                debug!(
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
    use tonic::transport::Channel;

    use crate::keyvalue::keyvalue_proto::key_value_client::KeyValueClient;
    use crate::keyvalue::keyvalue_proto::key_value_server::KeyValueServer;
    use crate::keyvalue::store::MapStore;
    use crate::raft::StateMachine;
    use crate::raft::raft_common_proto::{EntryId, Server};
    use crate::testing::TestRpcServer;

    use super::*;

    // A fake client which just takes any commits and writes them straight to the
    // supplied store (rather than going through remote consensus).
    struct FakeRaftClient {
        store: Arc<Mutex<MapStore>>,
    }

    #[async_trait]
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

        async fn change_config(&self, _members: Vec<Server>) -> Result<(), Status> {
            unimplemented!();
        }
    }

    #[tokio::test]
    async fn test_get() {
        let service = create_service();
        let store = service.store.clone();
        let server = TestRpcServer::run(KeyValueServer::new(service)).await;

        store
            .lock()
            .await
            .set(Bytes::from("foo"), Bytes::from("bar"));

        let request = GetRequest {
            key: "foo".as_bytes().to_vec(),
            version: -1,
        };
        let response = create_grpc_client(server.port().unwrap() as i32)
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
        let server = TestRpcServer::run(KeyValueServer::new(service)).await;

        let request = PutRequest {
            key: "foo".as_bytes().to_vec(),
            value: "bar".as_bytes().to_vec(),
        };

        create_grpc_client(server.port().unwrap() as i32)
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
            name: "test".into(),
            store: store.clone(),
            raft: Box::new(FakeRaftClient {
                store: store.clone(),
            }),
        }
    }

    async fn create_grpc_client(port: i32) -> KeyValueClient<Channel> {
        KeyValueClient::connect(format!("http://[::1]:{}", port))
            .await
            .expect("client")
    }
}
