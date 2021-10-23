use std::sync::Mutex;
use std::time::Duration;

use async_std::task;
use async_trait::async_trait;
use log::debug;
use tonic::transport::Channel;

use raft_proto::{CommitRequest, EntryId, Server, Status, StepDownRequest};

use crate::raft::raft_proto;
use crate::raft_proto::raft_client::RaftClient;

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
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, tonic::Status>;

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    async fn preempt_leader(&self) -> Result<Server, tonic::Status>;
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
            &self.address, leader
        );
    }

    // Returns a client with an open connection to the server currently
    // believed to be the leader of the cluster.
    async fn new_leader_client(&self) -> RaftClient<Channel> {
        let address = self.leader.lock().unwrap().clone();
        RaftClient::connect(format!("http://[::1]:{}", address.port)).await.expect("connect")
    }
}

#[async_trait]
impl Client for ClientImpl {
    // Adds the supplied payload as the next entry in the cluster's shared log.
    // Returns once the payload has been added (or the operation has failed).
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, tonic::Status> {
        let request = CommitRequest {
            payload: payload.to_vec(),
        };

        loop {
            let result = self
                .new_leader_client()
                .await
                .commit(request.clone())
                .await?
                .into_inner();

            match Status::from_i32(result.status) {
                Some(Status::Success) => return Ok(result.entry_id.expect("entryid").clone()),
                Some(Status::NotLeader) => {
                    result.leader.into_iter().for_each(|l| self.update_leader(&l));
                }
                _ => panic!("Unknown enum value {}", result.status)
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    async fn preempt_leader(&self) -> Result<Server, tonic::Status> {
        loop {
            let result = self
                .new_leader_client()
                .await
                .step_down(StepDownRequest {})
                .await?
                .into_inner();

            match Status::from_i32(result.status) {
                Some(Status::Success) => return Ok(result.leader.expect("leader").clone()),
                Some(Status::NotLeader) => {
                    result.leader.into_iter().for_each(|l| self.update_leader(&l));
                }
                _ => panic!("Unknown enum value {}", result.status)
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }
}
