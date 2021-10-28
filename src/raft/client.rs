use std::time::Duration;

use async_std::sync::Mutex;
use async_trait::async_trait;
use log::debug;
use tokio::time::sleep;
use tonic::transport::{Channel, Error};
use tonic::Request;

use raft_proto::{CommitRequest, EntryId, Server, Status, StepDownRequest};

use crate::raft::raft_proto;
use crate::raft::raft_proto::{CommitResponse, StepDownResponse};
use crate::raft_proto::raft_client::RaftClient;

// Returns a new client instance talking to a Raft cluster.
// - address: The address this client is running on. Mostly used for logging.
// - member: The address of a (any) member of the Raft cluster.
//
// Note that "address" and "member" can be equal.
pub fn new_client(name: &str, member: &Server) -> Box<dyn Client + Sync + Send> {
    // Arbitrary, this member will redirect us if necessary.
    let initial_leader = member.clone();
    Box::new(ClientImpl {
        name: name.into(),
        leader: Mutex::new(initial_leader),
        max_leader_follow_attempts: 10,
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
    name: String,

    // Our current best guess as to who is the leader.
    leader: Mutex<Server>,

    // The number of times to try and redirect the request to a new leader
    // before failing.
    max_leader_follow_attempts: i32,
}

impl ClientImpl {
    // Encapsulates updating the leader in a thread-safe way.
    async fn update_leader(&self, leader: &Server) {
        let mut locked = self.leader.lock().await;
        *locked = leader.clone();
        debug!("[{:?}] updated to new leader: [{:?}]", &self.name, leader);
    }

    // Returns a client with an open connection to the server currently
    // believed to be the leader of the cluster.
    async fn new_leader_client(&self) -> Result<RaftClient<Channel>, Error> {
        let address = self.leader.lock().await.clone();
        RaftClient::connect(format!("http://[::1]:{}", address.port)).await
    }

    // TODO(dino): Add a helper which accepts a request body and does the loop
    // and leader resolution.
}

#[async_trait]
impl Client for ClientImpl {
    // Adds the supplied payload as the next entry in the cluster's shared log.
    // Returns once the payload has been added (or the operation has failed).
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, tonic::Status> {
        for _ in 0..self.max_leader_follow_attempts {
            let mut request = Request::new(CommitRequest {
                payload: payload.to_vec(),
            });
            request.set_timeout(Duration::from_secs(3));

            let result: CommitResponse = self
                .new_leader_client()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .commit(request)
                .await?
                .into_inner();

            match Status::from_i32(result.status) {
                Some(Status::NotLeader) => {
                    if result.leader.is_some() {
                        self.update_leader(&result.leader.unwrap()).await;
                    }
                    // Try again in the next iteration.
                }
                Some(Status::Success) => return Ok(result.entry_id.expect("entryid").clone()),
                _ => {
                    return Err(tonic::Status::internal(format!(
                        "Unrecognized status: {}",
                        result.status
                    )))
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
        Err(tonic::Status::internal(format!(
            "Failed to contact leader after {} attempts",
            self.max_leader_follow_attempts
        )))
    }

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    async fn preempt_leader(&self) -> Result<Server, tonic::Status> {
        for _ in 0..self.max_leader_follow_attempts {
            let mut request = Request::new(StepDownRequest {});
            request.set_timeout(Duration::from_millis(100));

            let result: StepDownResponse = self
                .new_leader_client()
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .step_down(request)
                .await?
                .into_inner();

            match Status::from_i32(result.status) {
                Some(Status::NotLeader) => {
                    if result.leader.is_some() {
                        self.update_leader(&result.leader.unwrap()).await;
                    }
                    // Try again in the next iteration.
                }
                Some(Status::Success) => return Ok(result.leader.expect("leader").clone()),
                _ => {
                    return Err(tonic::Status::internal(format!(
                        "Unrecognized status: {}",
                        result.status
                    )))
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
        Err(tonic::Status::internal(format!(
            "Failed to contact leader after {} attempts",
            self.max_leader_follow_attempts
        )))
    }
}
