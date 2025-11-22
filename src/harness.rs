use async_std::channel;
use async_std::sync::Mutex;
use axum::Router;
use axum_tonic::NestTonic;
use axum_tonic::RestGrpcService;
use futures::Future;
use futures::future::join_all;
use std::env;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tonic::Status;
use tonic::transport::Channel;
use tracing::{Instrument, error, info, info_span};

use crate::keyvalue;
use crate::keyvalue::grpc::{KeyValueClient, KeyValueServer};
use crate::keyvalue::keyvalue_proto::{GetRequest, GetResponse};
use crate::keyvalue::{KeyValueService, MapStore};
use crate::raft::raft_common_proto::Server;
use crate::raft::raft_service_proto::raft_server::RaftServer;
use crate::raft::{
    Client, Diagnostics, FailureOptions, Options, RaftImpl, SnapshotInfo, new_client,
};

// Represents a collection of participants that interact with each other. In a real
// production deployment, these participants might be on different actual machines,
// but the harness manages all of them in a single process for convenience.
pub struct Harness {
    instances: Vec<Instance>,
    diagnostics: Arc<Mutex<Diagnostics>>,
    failures: Arc<Mutex<FailureOptions>>,
}

// Used to capture the intermediate state while building a harness. This is necessary
// because we first need to bind a port for each instance, then collect all the port
// addresses, and then start the underlying services (which require knowledge of the
// ports for the other participants).
pub struct HarnessBuilder {
    bound: Vec<BoundAddress>,
    failure: FailureOptions,
    options: Options,
}

impl HarnessBuilder {
    // Returns a harness instance, as well a future that callers can wait on for the termination
    // of the serving process for all instances managed by the harness.
    pub async fn build(
        self,
        cluster_name: &str,
        wipe_persistence: bool,
    ) -> Result<(Harness, Pin<Box<dyn Future<Output = ()> + Send>>), Box<dyn Error>> {
        let diag = Arc::new(Mutex::new(Diagnostics::new()));
        let failures = Arc::new(Mutex::new(self.failure.clone()));
        let all = self.addresses();

        let mut serving = Vec::new();
        let mut instances = Vec::new();

        let raft_options = self.options;
        for bound in self.bound {
            let (address, listener) = (bound.server, bound.listener);

            // Use something like /tmp/concord/<cluster>/<server> for persistence
            let persistence_path = env::temp_dir()
                .as_path()
                .join("concord")
                .join(&cluster_name)
                .join(&address.name);

            let options = raft_options.clone().with_persistence(
                persistence_path.to_str().unwrap().as_ref(),
                wipe_persistence,
            );

            let (instance, future) = Instance::new(
                &address,
                listener,
                &all,
                diag.clone(),
                options,
                failures.clone(),
            )
            .await?;
            let span = info_span!("serve", server=%address.name);

            instances.push(instance);
            serving.push(future.instrument(span));
        }

        let future = Box::pin(async {
            join_all(serving).await;
        });
        let harness = Harness {
            instances,
            diagnostics: diag,
            failures,
        };
        Ok((harness, future))
    }

    // Consumes this instance and returns an instance with the failure options set.
    pub fn with_failure(self: Self, failure_options: FailureOptions) -> Self {
        Self {
            bound: self.bound,
            failure: failure_options,
            options: self.options,
        }
    }

    // Consumes this instance and returns an instance with the raft options set.
    pub fn with_options(self: Self, options: Options) -> Self {
        Self {
            bound: self.bound,
            failure: self.failure,
            options,
        }
    }

    // Returns the addresses of all bound ports in this builder.
    pub fn addresses(&self) -> Vec<Server> {
        self.bound.iter().map(|b| b.server.clone()).collect()
    }
}

impl Harness {
    // Creates a harness builder. This will immediately bind an incoming port for
    // each supplied instance name.
    pub async fn builder(names: Vec<String>) -> Result<HarnessBuilder, Box<dyn Error>> {
        let mut bound = Vec::new();
        for name in names {
            let listener = TcpListener::bind("[::1]:0").await?;
            let port = listener.local_addr()?.port();
            let server = server("::1", port as i32, name.as_str());
            bound.push(BoundAddress { listener, server })
        }
        Ok(HarnessBuilder {
            bound,
            failure: FailureOptions::no_failures(),

            // TODO - DONOTMERGE - figure out a better way to only allow in testing and reconcile
            // with the fact that main() needs to support no persistence
            options: Options::new_without_persistence_for_testing(),
        })
    }

    // Returns all the addresses managed by this harness. Note that this can include
    // Raft members that are not currently part of the cluster.
    pub fn addresses(&self) -> Vec<Server> {
        self.instances.iter().map(|i| i.address.clone()).collect()
    }

    // Returns the diagnostics object used for this harness.
    pub fn diagnostics(&self) -> Arc<Mutex<Diagnostics>> {
        self.diagnostics.clone()
    }

    // Returns the failure options object used for this harness.
    pub fn failures(&self) -> Arc<Mutex<FailureOptions>> {
        self.failures.clone()
    }

    // Returns a client that can be used to interact with the cluster.
    pub fn make_raft_client(&self) -> Box<dyn Client + Send + Sync> {
        let server = self.addresses()[0].clone();
        new_client("harness-client", &server)
    }

    // Returns a client that can be used to issue keyvalue operations.
    pub async fn make_kv_client(&self) -> KeyValueClient<Channel> {
        let server = self.addresses()[0].clone();
        let address = format!("http://[{}]:{}", server.host, server.port);
        KeyValueClient::connect(address).await.expect("connect")
    }

    // Update the cluster membership to contain only the supplied members. The supplied
    // members must all be valid servers as defined at harness creation time.
    pub async fn update_members(&self, members: Vec<String>) -> Result<(), Status> {
        let client = self.make_raft_client();

        let addresses = self.addresses().clone();
        let mut new_members: Vec<Server> = Vec::new();
        for m in members {
            let server = addresses.iter().find(|&a| &a.name == &m);
            match server {
                None => return Err(Status::invalid_argument(format!("unknown server: {}", &m))),
                Some(s) => new_members.push(s.clone()),
            }
        }

        client.change_config(new_members).await
    }

    // Validates all available diagnostics and panics on failure.
    pub async fn validate(&self) {
        self.diagnostics
            .lock()
            .await
            .validate()
            .await
            .expect("validate");
    }

    // Starts the logic for this harness.
    pub async fn start(&self) {
        for instance in &self.instances {
            instance.start().await;
        }
    }

    // Stops all the instances of this harness.
    pub async fn stop(&self) {
        for instance in &self.instances {
            instance.stop().await;
        }
    }

    // Repeatedly makes KV requests until the supplied key has a value. Returns the result.
    #[cfg(test)]
    pub async fn wait_for_key(&self, key: &[u8], timeout_duration: Duration) -> GetResponse {
        wait_for(timeout_duration, || async {
            let mut c = self.make_kv_client().await;
            let response = c
                .get(GetRequest {
                    key: key.to_vec(),
                    version: -1, // Request the latest
                })
                .await;

            if let Ok(result) = response {
                let proto = result.into_inner();
                if proto.entry.is_some() {
                    return Some(proto);
                }
            }

            // This currently bunches together different failure modes, e.g., NotFound and others.
            None
        })
        .await
        .expect("wait_for_key")
    }

    #[cfg(test)]
    pub async fn wait_for_leader<M>(&self, timeout_duration: Duration, matcher: M) -> (i64, Server)
    where
        M: Fn(&(i64, Server)) -> bool,
    {
        let diag = self.diagnostics.clone();
        wait_for(timeout_duration, || async {
            let mut locked_diagnostics = diag.lock().await;

            // Make sure we've collected the latest information.
            locked_diagnostics.collect().await;

            let (term, leader) = match locked_diagnostics.latest_leader() {
                Some((term, leader)) => (term, leader),
                None => return None, // Return None if no leader is found
            };

            if !self.valid_server_name(&leader.name) {
                return None;
            }

            if matcher(&(term, leader.clone())) {
                Some((term, leader)) // Return the leader info if matcher passes
            } else {
                None
            }
        })
        .await
        .expect("wait_for_leader")
    }

    #[cfg(test)]
    pub async fn wait_for_snapshot(
        &self,
        server_name: &str,
        timeout_duration: Duration,
    ) -> SnapshotInfo {
        let diag = self.diagnostics.clone();
        let server_name = server_name.to_string();
        wait_for(timeout_duration, || async {
            let mut locked_diagnostics = diag.lock().await;
            locked_diagnostics.collect().await;
            if let Some(installs) = locked_diagnostics.get_snapshot_installs(&server_name) {
                if let Some((_, info)) = installs.iter().next() {
                    return Some(info.clone());
                }
            }
            None
        })
        .await
        .expect("wait_for_snapshot")
    }

    #[cfg(test)]
    fn valid_server_name(&self, name: &String) -> bool {
        self.instances
            .iter()
            .find(|i| i.address.name == *name)
            .is_some()
    }
}

// Holds the state of a single server instance, listening on a port. This includes multiple
// grpc and http services multiplexed on the port.
struct Instance {
    address: Server,
    raft: Arc<RaftImpl>,
    shutdown: channel::Sender<()>,
}

impl Instance {
    // Creates a new server with the supplied listener. This just initializes all the
    // required objects. Callers still need to call start() on this instance once done.
    // The returned future completes once the server finishes serving.
    async fn new(
        address: &Server,
        listener: TcpListener,
        all: &Vec<Server>,
        diagnostics: Arc<Mutex<Diagnostics>>,
        raft_options: Options,
        failure_options: Arc<Mutex<FailureOptions>>,
    ) -> Result<(Self, Pin<Box<dyn Future<Output = ()> + Send>>), Box<dyn Error>> {
        let name = address.name.to_string();
        let port = listener.local_addr()?.port();
        let path = "/keyvalue";
        if port as i32 != address.port {
            return Err("Listener port must match address port".into());
        }
        if all.len() < 3 {
            return Err(format!("Need at least 3 participants, but got {}", all.len()).into());
        }
        let cluster = vec![all[0].clone(), all[1].clone(), all[2].clone()];

        // Set up the keyvalue store, along with the web and grpc serving objects
        let kv_store = Arc::new(Mutex::new(MapStore::new()));
        let kv_service = Arc::new(KeyValueService::new(
            name.as_str(),
            &address,
            kv_store.clone(),
        ));
        let kv_grpc = KeyValueServer::from_arc(kv_service.clone());
        let kv_http = Arc::new(keyvalue::HttpHandler::new(
            path.to_string(),
            kv_service.clone(),
        ));
        let web = Router::new().nest(path, kv_http.routes());

        // Set up the grpc service for the raft participant. The first 3 server entries are active initially.
        let server_diagnostics = diagnostics.lock().await.get_server(&address);
        let raft = Arc::new(
            RaftImpl::new(
                address,
                &cluster,
                kv_store.clone(), // The raft cluster participant is the one applying the KV writes.
                Some(server_diagnostics),
                raft_options,
                failure_options,
            )
            .await
            .map_err(|e| format!("Failed to create Raft for '{}': {}", address.name, e))?,
        );
        let raft_grpc = RaftServer::from_arc(raft.clone());

        // Set up the top-level server
        let grpc = Router::new().nest_tonic(raft_grpc).nest_tonic(kv_grpc);
        let rest_grpc = RestGrpcService::new(web, grpc).into_make_service();

        // Wire up the shutdown
        let (sender, receiver) = channel::unbounded::<()>();
        let signal = async move { receiver.recv().await.unwrap_or(()) };

        // Start serving
        let serving_future = Box::pin(async {
            match axum::serve(listener, rest_grpc)
                .with_graceful_shutdown(signal)
                .await
            {
                Ok(()) => info!("Serving terminated"),
                Err(message) => error!("Serving terminated unsuccessfully: {}", message),
            }
        });

        // Package up the result in a serving instance for the harness
        let result = Instance {
            address: address.clone(),
            raft,
            shutdown: sender,
        };

        info!(
            "Started http and grpc server [name={},port={}]. Open at http://[{}]:{}{}",
            address.name, address.port, address.host, address.port, path
        );
        Ok((result, serving_future))
    }

    // Starts the background logic in service implementations (e.g., raft election loop).
    async fn start(&self) {
        self.raft.start().await;
    }

    // Sends the signal to shut down the server. Must only be called once.
    async fn stop(&self) {
        self.shutdown.send(()).await.expect("shutdown")
    }
}

struct BoundAddress {
    server: Server,
    listener: TcpListener,
}

fn server(host: &str, port: i32, name: &str) -> Server {
    Server {
        host: host.into(),
        port,
        name: name.into(),
    }
}

/// Waits for a condition to become true, up to the given `timeout_duration`.
/// Returns `Ok(T)` if the condition is met in time, or `Err(())` on timeout.
async fn wait_for<F, Fut, T>(timeout_duration: Duration, mut condition: F) -> Result<T, ()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if let Some(result) = condition().await {
            return Ok(result);
        }
        sleep(Duration::from_millis(300)).await;
    }
    Err(())
}
