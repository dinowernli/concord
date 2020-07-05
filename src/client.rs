use crate::cluster::raft;
use crate::cluster::raft_grpc;

use raft::CommitRequest;
use raft::CommitResponse;
use raft::Server;

struct Client {
    // Holds all the peers forming the cluster.
    cluster: Vec<Server>,

    // Our current best guess as to who is the leader.
    leader: Server,
}

impl Client {
    fn new(servers: &Vec<Server>) -> Client {
        Client {
            cluster: servers.clone(),
            leader: servers.first().unwrap().clone(),
        }
    }

    pub async fn commit(payload: &[u8]) {
        let mut request = CommitRequest::new();
        request.set_payload(Vec::from(payload));

        // TODO(dino): Send request to current leader guess. If bad leader,
        // update our guess and try again.
    }
}
