#![feature(async_closure)]
#![feature(map_first_last)]

mod cluster;
mod raft;

use raft::diagnostics;
use raft::raft_proto;
use raft::raft_proto_grpc;

use async_std::task;
use diagnostics::Diagnostics;
use env_logger::Env;
use futures::executor;
use futures::future::join3;
use log::info;
use std::time::Duration;

use raft::Client;
use cluster::RaftImpl;
use raft_proto::{EntryId, Server};
use raft_proto_grpc::RaftServer;

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}

fn server(host: &str, port: i32) -> Server {
    let mut result = Server::new();
    result.set_host(host.to_string());
    result.set_port(port);
    return result;
}

fn start_node(address: &Server, all: &Vec<Server>, diagnostics: &mut Diagnostics) -> grpc::Server {
    let server_diagnostics = diagnostics.get_server(&address);

    let mut server_builder = grpc::ServerBuilder::new_plain();
    let raft = RaftImpl::new(address, all, Some(server_diagnostics));
    raft.start();

    server_builder.add_service(RaftServer::new_service_def(raft));
    server_builder.http.set_port(address.get_port() as u16);
    server_builder.build().expect("server")
}

// Starts a loop which provides a steady amount of commit traffic.
async fn run_commit_loop(cluster: &Vec<Server>) {
    let mut client = Client::new(cluster);
    let mut sequence_number = 0;
    loop {
        let payload = format!("Payload number: {}", sequence_number);
        match client.commit(payload.as_bytes()).await {
            Ok(id) => {
                info!(
                    "Committed payload {} with id {}",
                    sequence_number,
                    entry_id_key(&id)
                );
                sequence_number = sequence_number + 1;
            }
            Err(message) => {
                info!("Failed to commit payload: {}", message);
            }
        }
        task::sleep(Duration::from_secs(1)).await;
    }
}

// Starts a loop which periodically preempts the cluster leader, forcing the
// cluster to recover by electing a new one.
async fn run_preempt_loop(cluster: &Vec<Server>) {
    let mut client = Client::new(cluster);
    loop {
        match client.preempt_leader().await {
            Ok(leader) => info!("Preempted cluster leader: {:?}", leader),
            Err(message) => info!("Failed to commit payload: {}", message),
        }
        task::sleep(Duration::from_secs(10)).await;
    }
}

// Starts a loop which periodically asks the diagnostics object to validate the
// execution history of the cluster. If this fails, this indicates a bug in the
// raft implementation.
async fn run_validate_loop(diag: &mut Diagnostics) {
    loop {
        diag.validate().expect("Cluster execution validation");
        task::sleep(Duration::from_secs(5)).await;
    }
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];
    info!("Starting {} servers", addresses.len());

    let mut diag = Diagnostics::new();
    let mut servers = Vec::<grpc::Server>::new();
    for address in &addresses {
        servers.push(start_node(&address, &addresses, &mut diag));
    }

    executor::block_on(join3(
        run_commit_loop(&addresses),
        run_preempt_loop(&addresses),
        run_validate_loop(&mut diag),
    ));
}
