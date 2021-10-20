#![feature(async_closure)]
#![feature(map_first_last)]
#![feature(trait_upcasting)]

use std::time::Duration;

use async_std::task;
use bytes::Bytes;
use env_logger::Env;
use futures::executor;
use futures::future::join3;
use log::info;
use protobuf::Message;

use keyvalue::keyvalue_proto;
use keyvalue_proto::Operation;
use raft::raft_proto;
use raft::raft_proto_grpc;
use raft::{Config, Diagnostics, RaftImpl};
use raft_proto::{EntryId, Server};
use raft_proto_grpc::RaftServer;

use crate::keyvalue::keyvalue_proto_grpc::KeyValueServer;
use crate::keyvalue::KeyValueService;

mod keyvalue;
mod raft;

fn make_set_operation(key: &[u8], value: &[u8]) -> Operation {
    let mut entry = keyvalue_proto::Entry::new();
    entry.set_key(key.to_vec());
    entry.set_value(value.to_vec());

    let mut op = keyvalue_proto::SetOperation::new();
    op.set_entry(entry);

    let mut result = Operation::new();
    result.set_set(op);
    result
}

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
    // A service used to serve the keyvalue store, backed by the
    // underlying Raft cluster.
    let keyvalue = KeyValueService::new(&address);

    // A service used by the Raft cluster.
    let server_diagnostics = diagnostics.get_server(&address);
    let state_machine = keyvalue.raft_state_machine();
    let raft = RaftImpl::new(
        address,
        all,
        state_machine,
        Some(server_diagnostics),
        Config::default(),
    );
    raft.start();

    let mut server_builder = grpc::ServerBuilder::new_plain();
    server_builder.add_service(RaftServer::new_service_def(raft));
    server_builder.add_service(KeyValueServer::new_service_def(keyvalue));
    server_builder.http.set_port(address.get_port() as u16);
    server_builder.build().expect("server")
}

// Starts a loop which provides a steady amount of commit traffic.
async fn run_commit_loop(cluster: &Vec<Server>) {
    let member = cluster.first().unwrap().clone();
    let client = raft::new_client(&server("main-commit", 0), &member);
    let mut sequence_number = 0;
    loop {
        let payload_value = format!("Payload number: {}", sequence_number);
        let op = make_set_operation("payload".as_bytes(), payload_value.as_bytes());
        let serialized = Bytes::from(op.write_to_bytes().expect("serialization"));

        match client.commit(&serialized).await {
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
    let member = cluster.first().unwrap().clone();
    let client = raft::new_client(&server("main-preempt", 0), &member);
    loop {
        match client.preempt_leader().await {
            Ok(leader) => info!("Preempted cluster leader: {:?}", leader),
            Err(message) => info!("Failed to preempt leader: {}", message),
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
