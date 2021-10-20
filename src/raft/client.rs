use std::sync::Mutex;
use std::time::Duration;

use async_std::task;
use async_trait::async_trait;
use grpc::ClientStubExt;
use grpc::Error;
use log::debug;

use raft_proto::{CommitRequest, EntryId, Server, Status, StepDownRequest};

use crate::raft_proto::raft_server::RaftServer;
use crate::raft_proto::raft_client::RaftClient;
use crate::raft::raft_proto;

// Returns a new client instance talking to a Raft cluster.
// - address: The address this client is running on. Mostly used for logging.
// - member: The address of a (any) member of the Raft cluster.
//
// Note that "address" and "member" can be equal.
pub fn new_client(address: &Server, member: &Server) -> Box<dyn Client + Sync + Send> {
    Box::new(ClientImpl {
        address: address.clone(),
        leader: Mutex::new(member.clone()),
    })
}

// A client object which can be used to interact with a Raft cluster.
#[async_trait]
pub trait Client {
    // Adds the supplied payload as the next entry in the cluster's shared log.
    // Returns once the payload has been added (or the operation has failed).
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, Error>;

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    async fn preempt_leader(&self) -> Result<Server, Error>;
}

struct ClientImpl {
    // The address of the server this is running on.
    address: Server,
    // Our current best guess as to who is the leader.
    leader: Mutex<Server>,
}

impl ClientImpl {
    // Encapsulates updating the leader in a thread-safe way.
    fn update_leader(&self, leader: &Server) {
        let mut locked = self.leader.lock().unwrap();
        *locked = leader.clone();
        debug!(
            "[{:?}] updated to new leader: [{:?}]",
            &self.address, &leader
        );
    }

    // Returns a client with an open connection to the server currently
    // believed to be the leader of the cluster.
    fn new_leader_client(&self) -> RaftClient {
        let client_conf = Default::default();
        let address = self.leader.lock().unwrap().clone();
        let port = address.get_port() as u16;
        RaftClient::new_plain(address.get_host(), port, client_conf).expect("Creating client")
    }
}

#[async_trait]
impl Client for ClientImpl {
    // Adds the supplied payload as the next entry in the cluster's shared log.
    // Returns once the payload has been added (or the operation has failed).
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, Error> {
        let mut request = CommitRequest::new();
        request.set_payload(Vec::from(payload));

        loop {
            let result = self
                .new_leader_client()
                .commit(grpc::RequestOptions::new(), request.clone())
                .drop_metadata()
                .await?;

            match result.get_status() {
                Status::SUCCESS => return Ok(result.get_entry_id().clone()),
                Status::NOT_LEADER => {
                    if result.has_leader() {
                        self.update_leader(result.get_leader());
                    }
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    async fn preempt_leader(&self) -> Result<Server, Error> {
        loop {
            let result = self
                .new_leader_client()
                .step_down(grpc::RequestOptions::new(), StepDownRequest::new())
                .drop_metadata()
                .await?;

            match result.get_status() {
                Status::SUCCESS => return Ok(result.get_leader().clone()),
                Status::NOT_LEADER => {
                    if result.has_leader() {
                        self.update_leader(result.get_leader());
                    }
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }
}
