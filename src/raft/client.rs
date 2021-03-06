use crate::raft::raft_proto;
use crate::raft::raft_proto_grpc;

use async_std::task;
use grpc::ClientStubExt;
use grpc::Error;
use std::time::Duration;

use raft_proto::{CommitRequest, EntryId, Server, Status, StepDownRequest};
use raft_proto_grpc::RaftClient;

pub struct Client {
    // Our current best guess as to who is the leader.
    leader: Server,
}

impl Client {
    pub fn new(servers: &Vec<Server>) -> Client {
        Client {
            leader: servers.first().unwrap().clone(),
        }
    }

    // Adds the supplied payload as the next entry in the cluster's shared log.
    // Returns once the payload has been added (or the operation has failed).
    pub async fn commit(&mut self, payload: &[u8]) -> Result<EntryId, Error> {
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
                        self.leader = result.get_leader().clone();
                    }
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    // Sends an rpc to the cluster leader to step down. Returns the address of
    // the leader which stepped down.
    pub async fn preempt_leader(&mut self) -> Result<Server, Error> {
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
                        self.leader = result.get_leader().clone();
                    }
                }
            }
            task::sleep(Duration::from_secs(1)).await;
        }
    }

    // Returns a client with an open connection to the server currently
    // believed to be the leader of the cluster.
    fn new_leader_client(&self) -> RaftClient {
        let client_conf = Default::default();
        let address = self.leader.clone();
        let port = address.get_port() as u16;
        RaftClient::new_plain(address.get_host(), port, client_conf).expect("Creating client")
    }
}
