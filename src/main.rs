extern crate structopt;
extern crate tracing;

use async_std::sync::{Arc, Mutex};
use axum::routing::Router;
use axum_tonic::NestTonic;
use axum_tonic::RestGrpcService;
use futures::future::join_all;
use futures::future::join5;
use rand::seq::SliceRandom;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::time::{Instant, sleep};
use tracing::{Instrument, debug, error, info, info_span};
use tracing_subscriber::EnvFilter;

use crate::keyvalue::grpc::KeyValueClient;
use crate::keyvalue::grpc::KeyValueServer;
use crate::keyvalue::grpc::PutRequest;
use crate::keyvalue::{KeyValueService, MapStore};
use crate::raft::raft_common_proto::Server;
use crate::raft::raft_service_proto::raft_server::RaftServer;
use crate::raft::{Diagnostics, FailureOptions, Options, RaftImpl};

mod keyvalue;
mod raft;
mod testing;

// We deliberately inject some RPC failures by default.
const DEFAULT_FAILURE_OPTIONS: FailureOptions = FailureOptions {
    failure_probability: 0.01,
    latency_probability: 0.05,
    latency_ms: 50,
};

#[derive(Debug, StructOpt, Copy, Clone)]
struct Arguments {
    #[structopt(short = "p", long = "disable_preempt")]
    disable_preempt: bool,

    #[structopt(short = "v", long = "disable_validate")]
    disable_validate: bool,

    #[structopt(short = "c", long = "disable_commit")]
    disable_commit: bool,

    #[structopt(short = "r", long = "disable_reconfigure")]
    disable_reconfigure: bool,
}

fn server(host: &str, port: i32, name: &str) -> Server {
    Server {
        host: host.into(),
        port,
        name: name.into(),
    }
}

fn make_address(address: &Server) -> SocketAddr {
    format!("[{}]:{}", address.host, address.port)
        .parse()
        .unwrap()
}

//#[instrument(skip(all,diagnostics))]
async fn run_server(address: &Server, all: &Vec<Server>, diagnostics: Arc<Mutex<Diagnostics>>) {
    let server = address.name.to_string();
    let kv_store = Arc::new(Mutex::new(MapStore::new()));

    // The initial Raft cluster consists of only the first 3 entries, the other 2 remain
    // idle.
    assert!(all.len() >= 3);
    let raft_cluster = vec![all[0].clone(), all[1].clone(), all[2].clone()];

    // A service used by the Raft cluster.
    let server_diagnostics = diagnostics.lock().await.get_server(&address);

    // Set up the grpc service for the key-value store.
    let kv1 = KeyValueService::new(server.as_str(), &address, kv_store.clone());
    let kv_grpc = KeyValueServer::new(kv1);

    // Set up the grpc service for the raft participant.
    let raft = RaftImpl::new(
        address,
        &raft_cluster,
        kv_store.clone(), // The raft cluster participant is the one applying the KV writes.
        Some(server_diagnostics),
        Options::default(),
        Some(DEFAULT_FAILURE_OPTIONS),
    )
    .await;

    raft.start().await;
    let raft_grpc = RaftServer::new(raft);

    // Set up the webservice serving the contents of the kvstore.
    // TODO(dino): See if we can reuse/share the "kv1" instance above.
    let kv2 = KeyValueService::new(server.as_str(), &address, kv_store.clone());
    let kv_http = Arc::new(keyvalue::HttpHandler::new(Arc::new(kv2)));
    let web = Router::new().nest("/keyvalue", kv_http.routes());
    let grpc = Router::new().nest_tonic(raft_grpc).nest_tonic(kv_grpc);

    let rest_grpc = RestGrpcService::new(web, grpc).into_make_service();
    let listener = TcpListener::bind(&make_address(&address))
        .await
        .expect("bind");

    info!("Started server (http, grpc) on port {}", address.port);

    match axum::serve(listener, rest_grpc).await {
        Ok(()) => info!("Serving terminated successfully"),
        Err(message) => error!("Serving terminated unsuccessfully: {}", message),
    }
}

// Starts a loop which periodically preempts the cluster leader, forcing the
// cluster to recover by electing a new one.
async fn run_preempt_loop(args: Arc<Arguments>, all: &Vec<Server>) {
    if args.disable_preempt {
        info!("running without the preempt loop");
        return;
    }

    // First server guaranteed to always be part of the cluster.
    let member = all.first().unwrap().clone();
    let name = "main-preempt";
    let client = raft::new_client(name, &member);
    loop {
        sleep(Duration::from_secs(4)).await;

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
        info!("running without the validate loop");
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
async fn run_put_loop(args: Arc<Arguments>, all: &Vec<Server>) {
    if args.disable_commit {
        info!("running without the put loop");
        return;
    }

    let request = PutRequest {
        key: "foo".as_bytes().to_vec(),
        value: "bar".as_bytes().to_vec(),
    };
    let mut i = 0;
    loop {
        // First server guaranteed to always be part of the cluster.
        let target = all.first().unwrap().clone();
        let address = format!("http://[{}]:{}", target.host, target.port);
        let mut client = KeyValueClient::connect(address).await.expect("connect");
        let start = Instant::now();
        match client.put(request.clone()).await {
            Ok(_) => {
                if i % 10 == 1 {
                    info!(i, latency_ms=%start.elapsed().as_millis(), "success")
                }
            }
            Err(msg) => info!(i, latency_ms=%start.elapsed().as_millis(), "failure: {}", msg),
        }
        i += 1;
        sleep(Duration::from_millis(1000)).await;
    }
}

async fn run_reconfigure_loop(args: Arc<Arguments>, all: &Vec<Server>) {
    if args.disable_reconfigure {
        info!("running without the reconfigure loop");
        return;
    }

    let first = all.first().unwrap().clone();
    loop {
        sleep(Duration::from_secs(15)).await;

        // The new members are the first entry (always) and 2 out of the 4 others.
        let mut new = vec![first.clone()];
        assert!(all.len() >= 5);
        let candidates = vec![
            all[1].clone(),
            all[2].clone(),
            all[3].clone(),
            all[4].clone(),
        ];
        for s in candidates.choose_multiple(&mut rand::thread_rng(), 2) {
            new.push(s.clone());
        }

        let new_members: Vec<String> = new.iter().map(|s| s.name.to_string()).collect();
        debug!(?new_members, "reconfiguring");

        // First server guaranteed to always be part of the cluster.
        let client = raft::new_client("main-reconfigure", &first);
        let start = Instant::now();
        match client.change_config(new.clone()).await {
            Ok(_) => {
                info!(latency_ms = %start.elapsed().as_millis(), ?new_members, "success")
            }
            Err(message) => error!("reconfigure failed: {}", message),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // This allows configuring the filters using the RUST_LOG env variable.
    // Example:
    // > RUST_LOG=info,concord::keyvalue=debug cargo run
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::from("concord=info"));

    tracing_subscriber::FmtSubscriber::builder()
        .with_target(false)
        .with_env_filter(env_filter)
        .init();
    let arguments = Arc::new(Arguments::from_args());

    // Note that we use the port as the name because we're running all these servers
    // locally and so the port is sufficient to identify the server.
    let addresses = vec![
        server("::1", 12345, "12345"), // Stays part of the cluster no matter what.
        server("::1", 12346, "12346"),
        server("::1", 12347, "12347"),
        server("::1", 12348, "12348"),
        server("::1", 12349, "12349"),
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
    let all = join5(
        serving,
        run_put_loop(arguments.clone(), &addresses).instrument(info_span!("put")),
        run_preempt_loop(arguments.clone(), &addresses).instrument(info_span!("preempt")),
        run_validate_loop(arguments.clone(), diag.clone()).instrument(info_span!("validate")),
        run_reconfigure_loop(arguments.clone(), &addresses).instrument(info_span!("reconfigure")),
    );

    all.await;
    Ok(())
}
