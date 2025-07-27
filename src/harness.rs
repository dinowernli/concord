use async_std::channel;
use async_std::sync::Mutex;
use axum::Router;
use axum_tonic::NestTonic;
use axum_tonic::RestGrpcService;
use futures::future::join_all;
use std::env;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tracing::{Instrument, error, info, info_span};

use crate::keyvalue;
use crate::keyvalue::grpc::KeyValueServer;
use crate::keyvalue::{KeyValueService, MapStore};
use crate::raft::raft_common_proto::Server;
use crate::raft::raft_service_proto::raft_server::RaftServer;
use crate::raft::{Diagnostics, FailureOptions, Options, RaftImpl};

// Represents a collection of participants that interact with each other. In a real
// production deployment, these participants might be on different actual machines,
// but the harness manages all of them in a single process for convenience.
pub struct Harness {
    instances: Vec<Instance>,
    diagnostics: Arc<Mutex<Diagnostics>>,
}

// Used to capture the intermediate state while building a harness. This is necessary
// because we first need to bind a port for each instance, then collect all the port
// addresses, and then start the underlying services (which require knowledge of the
// ports for the other participants).
pub struct HarnessBuilder {
    bound: Vec<BoundAddress>,
}

impl HarnessBuilder {
    // Returns a harness instance, as well a future that callers can wait on for the termination
    // of the serving process for all instances managed by the harness.
    pub async fn build(
        self,
        cluster_name: &str,
        failure_options: FailureOptions,
        wipe_persistence: bool,
    ) -> Result<(Harness, Pin<Box<dyn Future<Output = ()> + Send>>), Box<dyn Error>> {
        let diag = Arc::new(Mutex::new(Diagnostics::new()));
        let all = self.addresses();

        let mut serving = Vec::new();
        let mut instances = Vec::new();

        for bound in self.bound {
            let (address, listener) = (bound.server, bound.listener);

            // Use something like /tmp/concord/<cluster>/<server> for persistence
            let persistence_path = env::temp_dir()
                .as_path()
                .join("concord")
                .join(cluster_name)
                .join(&address.name);

            let options = Options::new(
                persistence_path.to_str().unwrap().as_ref(),
                wipe_persistence,
            );

            let (instance, future) = Instance::new(
                &address,
                listener,
                &all,
                diag.clone(),
                options,
                failure_options.clone(),
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
        };
        Ok((harness, future))
    }

    // Returns the addresses of all bound ports in this builder.
    pub fn addresses(&self) -> Vec<Server> {
        self.bound.iter().map(|b| b.server.clone()).collect()
    }
}

impl Harness {
    // Creates a harness builder. This will immediately bind an incoming port for
    // each supplied instance name.
    pub async fn builder(names: Vec<&str>) -> Result<HarnessBuilder, Box<dyn Error>> {
        let mut bound = Vec::new();
        for name in names {
            let listener = TcpListener::bind("[::1]:0").await?;
            let port = listener.local_addr()?.port();
            let server = server("::1", port as i32, name);
            bound.push(BoundAddress { listener, server })
        }
        Ok(HarnessBuilder { bound })
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

    #[cfg(test)]
    pub async fn wait_for_leader<M>(&self, timeout_duration: Duration, matcher: M)
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
                None => return false,
            };

            assert!(self.valid_server_name(&leader.name));
            matcher(&(term, leader))
        })
        .await
        .expect("wait_for_leader");
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
        options: Options,
        failure_options: FailureOptions,
    ) -> Result<(Self, Pin<Box<dyn Future<Output = ()> + Send>>), Box<dyn Error>> {
        let name = address.name.to_string();
        let port = listener.local_addr()?.port();
        assert_eq!(port as i32, address.port);

        let kv_store = Arc::new(Mutex::new(MapStore::new()));
        let server_diagnostics = diagnostics.lock().await.get_server(&address);

        // Set up the grpc service for the key-value store.
        let kv1 = KeyValueService::new(name.as_str(), &address, kv_store.clone());
        let kv_grpc = KeyValueServer::new(kv1);

        // Set up the webservice serving the contents of the kvstore.
        // TODO(dino): See if we can reuse/share the "kv1" instance above.
        let kv2 = KeyValueService::new(name.as_str(), &address, kv_store.clone());
        let kv_http = Arc::new(keyvalue::HttpHandler::new(Arc::new(kv2)));
        let web = Router::new().nest("/keyvalue", kv_http.routes());

        // Set up the grpc service for the raft participant. The first 3 server entries are active initially.
        assert!(all.len() >= 3);
        let initial_cluster = vec![all[0].clone(), all[1].clone(), all[2].clone()];
        let raft = Arc::new(
            RaftImpl::new(
                address,
                &initial_cluster,
                kv_store.clone(), // The raft cluster participant is the one applying the KV writes.
                Some(server_diagnostics),
                options,
                Some(failure_options),
            )
            .await,
        );
        let raft_grpc = RaftServer::from_arc(raft.clone());

        // Set up the top-level server
        let grpc = Router::new().nest_tonic(raft_grpc).nest_tonic(kv_grpc);
        let rest_grpc = RestGrpcService::new(web, grpc).into_make_service();

        // Wire up the shutdown
        let (sender, receiver) = channel::unbounded::<()>();
        let signal = async move { receiver.recv().await.unwrap_or(()) };

        // Start serving
        let future = Box::pin(async {
            match axum::serve(listener, rest_grpc)
                .with_graceful_shutdown(signal)
                .await
            {
                Ok(()) => info!("Serving terminated"),
                Err(message) => error!("Serving terminated unsuccessfully: {}", message),
            }
        });
        let result = Instance {
            address: address.clone(),
            raft,
            shutdown: sender,
        };
        info!(
            "Started http and grpc server [name={},port={}] ",
            address.name, address.port
        );

        Ok((result, future))
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
/// Returns `Ok(())` if the condition is met in time, or `Err(())` on timeout.
async fn wait_for<F, Fut>(timeout_duration: Duration, mut condition: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if condition().await {
            return Ok(());
        }
        sleep(Duration::from_millis(300)).await;
    }
    Err(())
}
