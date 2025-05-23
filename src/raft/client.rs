use std::time::Duration;

use async_std::sync::Mutex;
use async_trait::async_trait;
use futures::Future;
use tokio::time::sleep;
use tonic::Request;
use tonic::transport::{Channel, Error};
use tracing::debug;

use crate::raft::client::Outcome::{Failure, NewLeader, Success};
use crate::raft::raft_common_proto::{EntryId, Server};
use crate::raft::raft_service_proto::raft_client::RaftClient;
use crate::raft::raft_service_proto::{
    ChangeConfigRequest, ChangeConfigResponse, CommitRequest, CommitResponse, Status,
    StepDownRequest, StepDownResponse,
};

// How long to wait if the server points us to a new leader.
const NEW_LEADER_WAIT_MS: u64 = 5;
// How long to wait if the server tells us there is no leader. This is a bit
// longer to allow the cluster to elect a new leader.
const NO_LEADER_WAIT_MS: u64 = 80;

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

    // Asks the cluster leader to step down. Returns the address of
    // the leader which stepped down if successful.
    async fn preempt_leader(&self) -> Result<Server, tonic::Status>;

    // Modifies the cluster configuration with a new set of (voting) members.
    async fn change_config(&self, members: Vec<Server>) -> Result<(), tonic::Status>;
}

// The outcome of an individual operation sent to one server in the Raft cluster.
// Used to facilitate retries which follow the leader around.
enum Outcome<T> {
    // The operation has completed successfully and yielded a result.
    Success(T),

    // The operation failed (permanently) and should not be retried.
    Failure(tonic::Status),

    // The operation failed because it got redirected to a new leader. This
    // can happen when the cluster elects a new leader.
    NewLeader(Option<Server>),
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
        debug!(name=%self.name, leader=%leader.name, "updated leader");
    }

    // Helper used to connect to a remote server.
    async fn connect(server: &Server) -> Result<RaftClient<Channel>, Error> {
        RaftClient::connect(format!("http://[{}]:{}", server.host, server.port)).await
    }

    // Some operations on Raft clusters need to talk to the leader. This helper
    // takes care of retrying a supplied operation a fixed number of times, following
    // leader as it changes.
    //
    // The supplied "operation" needs to return Left(T) upon success, Right(leader)
    // if the operation needs to be retried talking to a new leader, or an error if
    // the operation failed entirely.
    async fn retry_helper<T, Fut>(
        &self,
        operation: impl Fn(Server) -> Fut,
    ) -> Result<T, tonic::Status>
    where
        Fut: Future<Output = Outcome<T>>,
    {
        let mut leader = self.leader.lock().await.clone();
        let attempts = self.max_leader_follow_attempts;
        for _ in 0..attempts {
            let retry_wait_ms = match operation(leader.clone()).await {
                Failure(status) => return Err(status),
                Success(result) => return Ok(result),
                NewLeader(Some(new_leader)) => {
                    self.update_leader(&new_leader).await;
                    leader = new_leader;
                    NEW_LEADER_WAIT_MS
                }
                // Right indicates not leader, but if this is empty we don't have
                // an updated leader yet, just retry without doing anything.
                NewLeader(None) => NO_LEADER_WAIT_MS,
            };
            sleep(Duration::from_millis(retry_wait_ms)).await;
        }
        Err(tonic::Status::internal(format!(
            "Failed to contact leader after {} attempts",
            attempts
        )))
    }

    // The body of an individual commit rpc sent to the (presumed) leader.
    async fn commit_impl(leader: Server, payload: &[u8]) -> Outcome<EntryId> {
        let mut request = Request::new(CommitRequest {
            payload: payload.to_vec(),
        });
        request.set_timeout(Duration::from_secs(3));

        let client = ClientImpl::connect(&leader).await;
        if let Err(status) = client {
            return Failure(tonic::Status::internal(status.to_string()));
        }

        let result = client.unwrap().commit(request).await;
        match result {
            Err(status) => return Failure(tonic::Status::internal(status.to_string())),
            Ok(response) => {
                let proto: CommitResponse = response.into_inner();
                match Status::try_from(proto.status) {
                    Ok(Status::Success) => Success(proto.entry_id.expect("entryid")),
                    Ok(Status::NotLeader) => NewLeader(proto.leader),
                    _ => Failure(bad_status(proto.status)),
                }
            }
        }
    }

    // The body an individual step_down rpc to the (presumed) leader.
    async fn preempt_leader_impl(leader: Server) -> Outcome<Server> {
        let mut request = Request::new(StepDownRequest {});
        request.set_timeout(Duration::from_millis(100));

        let client = ClientImpl::connect(&leader).await;
        if let Err(status) = client {
            return Failure(tonic::Status::internal(status.to_string()));
        }

        let result = client.unwrap().step_down(request).await;
        match result {
            Err(status) => return Failure(tonic::Status::internal(status.to_string())),
            Ok(response) => {
                let proto: StepDownResponse = response.into_inner();
                match Status::try_from(proto.status) {
                    Ok(Status::Success) => Success(proto.leader.expect("leader")),
                    Ok(Status::NotLeader) => NewLeader(proto.leader),
                    _ => Failure(bad_status(proto.status)),
                }
            }
        }
    }

    async fn change_config_impl(leader: Server, members: &Vec<Server>) -> Outcome<()> {
        let mut request = Request::new(ChangeConfigRequest {
            members: members.to_vec(),
        });
        request.set_timeout(Duration::from_millis(2000));

        let client = ClientImpl::connect(&leader).await;
        if let Err(status) = client {
            return Failure(tonic::Status::internal(status.to_string()));
        }

        let result = client.unwrap().change_config(request).await;
        match result {
            Err(status) => return Failure(tonic::Status::internal(status.to_string())),
            Ok(response) => {
                let proto: ChangeConfigResponse = response.into_inner();
                match Status::try_from(proto.status) {
                    Ok(Status::Success) => Success(()),
                    Ok(Status::NotLeader) => NewLeader(proto.leader),
                    _ => Failure(bad_status(proto.status)),
                }
            }
        }
    }
}

#[async_trait]
impl Client for ClientImpl {
    async fn commit(&self, payload: &[u8]) -> Result<EntryId, tonic::Status> {
        let op = move |leader| async move { ClientImpl::commit_impl(leader, payload).await };
        self.retry_helper(op).await
    }

    async fn preempt_leader(&self) -> Result<Server, tonic::Status> {
        let op = async move |leader| ClientImpl::preempt_leader_impl(leader).await;
        self.retry_helper(op).await
    }

    async fn change_config(&self, members: Vec<Server>) -> Result<(), tonic::Status> {
        let members = &members;
        let op = move |leader| async move { ClientImpl::change_config_impl(leader, members).await };
        self.retry_helper(op).await
    }
}

fn bad_status(status: i32) -> tonic::Status {
    tonic::Status::internal(format!("Unrecognized status: {}", status))
}
