#![feature(async_closure)]
#![feature(map_first_last)]
#![feature(trait_upcasting)]

use std::time::Duration;

use async_std::task;
use env_logger::Env;
use futures::executor;
use futures::future::{join4};
use log::info;

use crate::keyvalue::keyvalue_proto::key_value_server::KeyValueServer;
use keyvalue::keyvalue_proto;
use keyvalue_proto::Operation;
use raft::raft_proto;
use raft::{Config, Diagnostics, RaftImpl};
use raft_proto::{EntryId, Server};
use crate::keyvalue::keyvalue_proto::operation::Op;
use crate::keyvalue::keyvalue_proto::SetOperation;
use prost::Message;
use futures::future::join_all;

use crate::keyvalue::KeyValueService;
use crate::raft_proto::raft_server::RaftServer;

mod keyvalue;
mod raft;

fn make_set_operation(key: &[u8], value: &[u8]) -> Operation {
    Operation {
        op: Some(Op::Set(SetOperation{
            entry: Some(keyvalue_proto::Entry {
                key: key.to_vec(),
                value: value.to_vec(),
            })
        }))
    }
}

fn entry_id_key(entry_id: &EntryId) -> String {
    format!("(term={},id={})", entry_id.term, entry_id.index)
}

fn server(host: &str, port: i32) -> Server {
    Server {
        host: host.into(),
        port
    }
}

async fn run_server(address: &Server, all: &Vec<Server>, diagnostics: &mut Diagnostics) {
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

    tonic::transport::Server::builder()
        .add_service(RaftServer::new(raft))
        .add_service(KeyValueServer::new(keyvalue))
        .serve(format!("{}:{}", address.host, address.port).parse().unwrap())
        .await;
}

// Starts a loop which provides a steady amount of commit traffic.
async fn run_commit_loop(cluster: &Vec<Server>) {
    let member = cluster.first().unwrap().clone();
    let client = raft::new_client(&server("main-commit", 0), &member);
    let mut sequence_number = 0;
    loop {
        let payload_value = format!("Payload number: {}", sequence_number);
        let op = make_set_operation("payload".as_bytes(), payload_value.as_bytes());
        let serialized = op.encode_to_vec();
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
    let mut servers = Vec::new();
    for address in &addresses {
        let running = run_server(&address, &addresses, &mut diag);
        servers.push(running);
    }

    let serving = join_all(servers);
    let all = join4(
        serving,
        run_commit_loop(&addresses),
        run_preempt_loop(&addresses),
        run_validate_loop(&mut diag));

    // TODO: async main
    executor::block_on(all);
}
