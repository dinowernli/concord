#![feature(async_closure)]

mod client;
mod cluster;

use client::Client;
use cluster::raft;
use cluster::raft_grpc;
use cluster::RaftImpl;
use raft::EntryId;
use raft::Server;
use raft_grpc::RaftServer;

use async_std::task;
use env_logger::Env;
use futures::executor;
use log::info;
use std::time::Duration;

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}

fn server(host: &str, port: i32) -> Server {
    let mut result = Server::new();
    result.set_host(host.to_string());
    result.set_port(port);
    return result;
}

fn start_node(address: &Server, all: &Vec<Server>) -> grpc::Server {
    let mut server_builder = grpc::ServerBuilder::new_plain();
    let raft = RaftImpl::new(address, all);
    raft.start();

    server_builder.add_service(RaftServer::new_service_def(raft));
    server_builder.http.set_port(address.get_port() as u16);
    server_builder.build().expect("server")
}

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

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];
    info!("Starting {} servers", addresses.len());

    let mut servers = Vec::<grpc::Server>::new();
    for address in &addresses {
        servers.push(start_node(&address, &addresses));
    }

    executor::block_on(run_commit_loop(&addresses));
}
