#![feature(async_closure)]
#![feature(map_first_last)]
#![feature(trait_upcasting)]

use std::error::Error;
use std::time::Duration;

use async_std::sync::{Arc, Mutex};
use env_logger::Env;
use futures::future::join4;
use futures::future::join_all;
use log::{error, info};
use rand::seq::SliceRandom;
use tokio::time::sleep;

use raft::raft_proto;
use raft::{Config, Diagnostics, RaftImpl};
use raft_proto::Server;

use crate::keyvalue::grpc::KeyValueClient;
use crate::keyvalue::grpc::KeyValueServer;
use crate::keyvalue::grpc::PutRequest;
use crate::keyvalue::KeyValueService;
use crate::raft_proto::raft_server::RaftServer;

mod keyvalue;
mod raft;
mod testing;

fn server(host: &str, port: i32) -> Server {
    Server {
        host: host.into(),
        port,
    }
}

async fn run_server(address: &Server, all: &Vec<Server>, diagnostics: Arc<Mutex<Diagnostics>>) {
    // A service used to serve the keyvalue store, backed by the
    // underlying Raft cluster.
    let keyvalue = KeyValueService::new(&address);
    info!("Created keyvalue service for {:?}", &address);

    // A service used by the Raft cluster.
    let server_diagnostics = diagnostics.lock().await.get_server(&address);
    let state_machine = keyvalue.raft_state_machine();
    let raft = RaftImpl::new(
        address,
        all,
        state_machine,
        Some(server_diagnostics),
        Config::default(),
    );
    raft.start().await;
    info!("Created raft service for {:?}", &address);

    let serve = tonic::transport::Server::builder()
        .add_service(RaftServer::new(raft))
        .add_service(KeyValueServer::new(keyvalue))
        .serve(
            format!("[{}]:{}", address.host, address.port)
                .parse()
                .unwrap(),
        )
        .await;

    match serve {
        Ok(()) => info!("Serving terminated successfully"),
        Err(message) => error!("Serving terminated unsuccessfully: {}", message),
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
            Err(message) => error!("Failed to preempt leader: {}", message),
        }
        sleep(Duration::from_secs(10)).await;
    }
}

// Starts a loop which periodically asks the diagnostics object to validate the
// execution history of the cluster. If this fails, this indicates a bug in the
// raft implementation.
async fn run_validate_loop(diag: Arc<Mutex<Diagnostics>>) {
    loop {
        diag.lock()
            .await
            .validate()
            .await
            .expect("Cluster execution validation");
        sleep(Duration::from_secs(5)).await;
    }
}

// Repeatedly picks a random server in the cluster and sends a put request.
async fn run_put_loop(cluster: &Vec<Server>) {
    let request = PutRequest {
        key: "foo".as_bytes().to_vec(),
        value: "bar".as_bytes().to_vec(),
    };
    let mut i = 0;
    loop {
        let target = cluster.choose(&mut rand::thread_rng()).expect("nonempty");
        let address = format!("http://[{}]:{}", target.host, target.port);
        let mut client = KeyValueClient::connect(address).await.expect("connect");
        match client.put(request.clone()).await {
            Ok(_) => info!("Put {} successful", i),
            Err(msg) => error!("Put {} failed: {}", i, msg),
        }
        i += 1;
        sleep(Duration::from_millis(1000)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];
    info!("Starting {} servers", addresses.len());

    let diag = Arc::new(Mutex::new(Diagnostics::new()));
    let mut servers = Vec::new();
    for address in &addresses {
        let running = run_server(&address, &addresses, diag.clone());
        servers.push(running);
    }

    let serving = join_all(servers);
    let all = join4(
        serving,
        run_put_loop(&addresses),
        run_preempt_loop(&addresses),
        run_validate_loop(diag.clone()),
    );

    all.await;
    Ok(())
}
