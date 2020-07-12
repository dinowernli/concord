use crate::cluster::raft;
use crate::cluster::raft_grpc;

use async_std::task;
use grpc::ClientStubExt;
use grpc::Error;
use raft::CommitRequest;
use raft::EntryId;
use raft::Server;
use raft::Status;
use raft_grpc::RaftClient;
use std::time::Duration;

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
            let client_conf = Default::default();
            let address = self.leader.clone();
            let port = address.get_port() as u16;
            let client = RaftClient::new_plain(address.get_host(), port, client_conf)
                .expect("Creating client");

            let result = client
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
}
