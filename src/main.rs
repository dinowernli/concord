#![feature(async_closure)]
#![feature(map_first_last)]
#![feature(trait_upcasting)]

extern crate structopt;
extern crate tracing;

use std::error::Error;
use std::time::Duration;

use async_std::sync::{Arc, Mutex};
use futures::future::join4;
use futures::future::join_all;
use rand::seq::SliceRandom;
use structopt::StructOpt;
use tokio::time::{sleep, Instant};
use tracing::{error, info, info_span, Instrument};
use tracing_subscriber::EnvFilter;

use raft::raft_proto;
use raft::{Diagnostics, Options, RaftImpl};
use raft_proto::Server;

use crate::keyvalue::grpc::KeyValueClient;
use crate::keyvalue::grpc::KeyValueServer;
use crate::keyvalue::grpc::PutRequest;
use crate::keyvalue::KeyValueService;
use crate::raft_proto::raft_server::RaftServer;

mod keyvalue;
mod raft;
mod testing;

#[derive(Debug, StructOpt, Copy, Clone)]
struct Arguments {
    #[structopt(short = "p", long = "disable_preempt")]
    disable_preempt: bool,

    #[structopt(short = "v", long = "disable_validate")]
    disable_validate: bool,

    #[structopt(short = "c", long = "disable_commit")]
    disable_commit: bool,
}

fn server(host: &str, port: i32, name: &str) -> Server {
    Server {
        host: host.into(),
        port,
        name: name.into(),
    }
}

//#[instrument(skip(all,diagnostics))]
async fn run_server(address: &Server, all: &Vec<Server>, diagnostics: Arc<Mutex<Diagnostics>>) {
    let server = address.name.to_string();
    let keyvalue = KeyValueService::new(server.as_str(), &address);

    // A service used to serve the keyvalue store, backed by the
    // underlying Raft cluster.
    info!("created keyvalue service");

    // A service used by the Raft cluster.
    let server_diagnostics = diagnostics.lock().await.get_server(&address);
    let state_machine = keyvalue.raft_state_machine();
    let raft = RaftImpl::new(
        address,
        all,
        state_machine,
        Some(server_diagnostics),
        Options::default(),
    );

    raft.start().await;
    info!("created raft service");

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
async fn run_preempt_loop(args: Arc<Arguments>, cluster: &Vec<Server>) {
    if args.disable_preempt {
        return;
    }

    let member = cluster.first().unwrap().clone();
    let name = "main-preempt";
    let client = raft::new_client(name, &member);
    loop {
        let start = Instant::now();
        match client.preempt_leader().await {
            Ok(leader) => {
                info!(leader = %leader.name, latency_ms = %start.elapsed().as_millis(), "success")
            }
            Err(message) => error!("failed: {}", message),
        }
        sleep(Duration::from_secs(10)).await;
    }
}

// Starts a loop which periodically asks the diagnostics object to validate the
// execution history of the cluster. If this fails, this indicates a bug in the
// raft implementation.
async fn run_validate_loop(args: Arc<Arguments>, diag: Arc<Mutex<Diagnostics>>) {
    if args.disable_validate {
        return;
    }

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
async fn run_put_loop(args: Arc<Arguments>, cluster: &Vec<Server>) {
    if args.disable_commit {
        return;
    }

    let request = PutRequest {
        key: "foo".as_bytes().to_vec(),
        value: "bar".as_bytes().to_vec(),
    };
    let mut i = 0;
    loop {
        let target = cluster.choose(&mut rand::thread_rng()).expect("nonempty");
        let address = format!("http://[{}]:{}", target.host, target.port);
        let mut client = KeyValueClient::connect(address).await.expect("connect");
        let start = Instant::now();
        match client.put(request.clone()).await {
            Ok(_) => info!(i, latency_ms=%start.elapsed().as_millis(), "success"),
            Err(msg) => info!(i, latency_ms=%start.elapsed().as_millis(), "failure: {}", msg),
        }
        i += 1;
        sleep(Duration::from_millis(1000)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // This allows configuring the filters using the RUST_LOG env variable.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::from("concord=info"));

    tracing_subscriber::FmtSubscriber::builder()
        .with_target(false)
        .with_env_filter(env_filter)
        .init();
    let arguments = Arc::new(Arguments::from_args());

    // Note that we use the port as the name because we're running all these servers
    // locally and so the port is sufficient to identify the server.
    let addresses = vec![
        server("::1", 12345, "12345"),
        server("::1", 12346, "12346"),
        server("::1", 12347, "12347"),
    ];
    info!("Starting {} servers", addresses.len());

    let diag = Arc::new(Mutex::new(Diagnostics::new()));
    let mut servers = Vec::new();
    for address in &addresses {
        let span = info_span!("serve", server=%address.name);
        let running = run_server(&address, &addresses, diag.clone());
        servers.push(running.instrument(span));
    }

    let serving = join_all(servers);
    let all = join4(
        serving,
        run_put_loop(arguments.clone(), &addresses).instrument(info_span!("put")),
        run_preempt_loop(arguments.clone(), &addresses).instrument(info_span!("preempt")),
        run_validate_loop(arguments.clone(), diag.clone()).instrument(info_span!("validate")),
    );

    all.await;
    Ok(())
}
