extern crate structopt;
extern crate tracing;

use async_std::sync::{Arc, Mutex};
use futures::FutureExt;
use futures::future::{join3, join4};
use rand::seq::SliceRandom;
use std::error::Error;
use std::time::Duration;
use structopt::StructOpt;
use tokio::select;
use tokio::time::{Instant, sleep};
use tracing::{Instrument, debug, error, info, info_span};
use tracing_subscriber::EnvFilter;

use crate::harness::Harness;
use crate::keyvalue::grpc::KeyValueClient;
use crate::keyvalue::grpc::PutRequest;
use crate::raft::raft_common_proto::Server;
use crate::raft::{Diagnostics, FailureOptions};

mod harness;
#[cfg(test)]
mod integration_test;
mod keyvalue;
mod raft;
#[cfg(test)]
mod testing;

// We deliberately inject some RPC failures by default.
fn make_default_failure_options() -> FailureOptions {
    FailureOptions {
        failure_probability: 0.01,
        latency_probability: 0.05,
        latency_ms: 50,
        disconnected: std::collections::hash_set::HashSet::new(),
    }
}

const CLUSTER_NAME: &str = "dev-cluster";

// Simulate large values so that compaction occasionally kicks in.
const VALUE_SIZE_BYES: i64 = 5 * 1000 * 100;

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

    #[structopt(short = "w", long = "wipe_persistence")]
    wipe_persistence: bool,
}

// Starts a loop which periodically preempts the cluster leader, forcing the
// cluster to recover by electing a new one.
async fn run_preempt_loop(
    args: Arc<Arguments>,
    all: &Vec<Server>,
    shutdown: impl Future<Output = ()> + Clone,
) {
    if args.disable_preempt {
        info!("running without the preempt loop");
        return;
    }

    // First server guaranteed to always be part of the cluster.
    let member = all.first().unwrap().clone();
    let name = "main-preempt";
    let client = raft::new_client(name, &member);
    loop {
        let body = async {
            sleep(Duration::from_secs(4)).await;

            let start = Instant::now();
            match client.preempt_leader().await {
                Ok(leader) => {
                    info!(leader = %leader.name, latency_ms = %start.elapsed().as_millis(), "success")
                }
                Err(message) => error!("failed: {}", message),
            }
            sleep(Duration::from_secs(10)).await;
        };

        select! {
          _ = shutdown.clone() => {break;}
          _ = body => {}
        }
    }
    info!("Finished")
}

// Starts a loop which periodically asks the diagnostics object to validate the
// execution history of the cluster. If this fails, this indicates a bug in the
// raft implementation.
async fn run_validate_loop(
    args: Arc<Arguments>,
    diag: Arc<Mutex<Diagnostics>>,
    shutdown: impl Future<Output = ()> + Clone,
) {
    if args.disable_validate {
        info!("running without the validate loop");
        return;
    }

    loop {
        let body = async {
            diag.lock()
                .await
                .validate()
                .await
                .expect("Cluster execution validation");
            sleep(Duration::from_secs(5)).await;
        };

        select! {
          _ = shutdown.clone() => {break;}
          _ = body => {}
        }
    }
    info!("Finished");
}

// Repeatedly picks a random server in the cluster and sends a put request.
async fn run_put_loop(
    args: Arc<Arguments>,
    all: &Vec<Server>,
    shutdown: impl Future<Output = ()> + Clone,
) {
    if args.disable_commit {
        info!("running without the put loop");
        return;
    }

    let value = std::iter::repeat("X")
        .take(VALUE_SIZE_BYES as usize)
        .collect::<String>();
    let request = PutRequest {
        key: "foo".as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    };

    let mut i = 0;
    loop {
        let body = async {
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
        };

        select! {
          _ = shutdown.clone() => {break;}
          _ = body => {}
        }
    }
    info!("Finished")
}

async fn run_reconfigure_loop(
    args: Arc<Arguments>,
    all: &Vec<Server>,
    shutdown: impl Future<Output = ()> + Clone,
) {
    if args.disable_reconfigure {
        info!("running without the reconfigure loop");
        return;
    }

    let first = all.first().unwrap().clone();
    loop {
        let body = async {
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
        };

        select! {
          _ = shutdown.clone() => {break;}
          _ = body => {}
        }
    }
    info!("Finished")
}

fn names() -> Vec<String> {
    vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
        "E".to_string(),
    ]
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

    let (harness, serving) = Harness::builder(names())
        .await
        .expect("builder")
        .with_failure(make_default_failure_options())
        .build(CLUSTER_NAME, arguments.wipe_persistence)
        .await
        .expect("harness");

    let addresses = harness.addresses();
    let diagnostics = harness.diagnostics();

    harness.start().await;
    info!("Started {} servers", addresses.len());

    // Set up a shutdown broadcast by turning the channel receiver into a shared future.
    let (shutdown, rx) = async_std::channel::unbounded::<()>();
    let sx = async { rx.recv().await.expect("shutdown-recv") }.shared();

    // Bundle the clients into a single future that can be awaited.
    let all = addresses.clone();
    let args = arguments.clone();
    let clients = join4(
        run_put_loop(args.clone(), &all, sx.clone()).instrument(info_span!("put")),
        run_preempt_loop(args.clone(), &all, sx.clone()).instrument(info_span!("preempt")),
        run_validate_loop(args.clone(), diagnostics, sx.clone()).instrument(info_span!("validate")),
        run_reconfigure_loop(args.clone(), &all, sx.clone()).instrument(info_span!("reconfigure")),
    )
    .shared();

    // Set up a signal handler that stops the clients and then harness.
    let signal_harness = Arc::new(harness);
    let signal_clients = clients.clone();
    let signal_handler = async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Got SIGINT, shutting down");

        // First tell all the client loops to shut down and wait for them.
        shutdown.send(()).await.expect("shutdown-send");
        signal_clients.await;

        // Now shut down the servers and wait for them to stop serving.
        signal_harness.stop().await;
    };

    join3(serving, signal_handler, clients).await;
    info!("All done, exiting");
    Ok(())
}
