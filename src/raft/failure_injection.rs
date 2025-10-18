extern crate async_std;
extern crate chrono;
extern crate futures;
extern crate pin_project;
extern crate rand;
extern crate tonic;
extern crate tower;
extern crate tracing;

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::raft::raft_common_proto::Server;
use async_std::sync::Mutex;
use async_std::task::sleep;
use pin_project::pin_project;
use tonic::client::GrpcService;
use tonic::codegen::http::Request;
use tower::BoxError;
use tracing::debug;

// Options used to control RPC failure injection.
#[derive(Debug, Clone)]
pub struct FailureOptions {
    // Probability with which intercepted RPCs should succeed.
    pub failure_probability: f64,

    // Probability with which to add latency to intercepted calls.
    pub latency_probability: f64,

    // How much latency to add for calls with additional latency.
    pub latency_ms: u32,

    // Holds the names of disconnected participants. All incoming and
    // outgoing traffic to such participants fail.
    pub disconnected: HashSet<String>,
}

impl FailureOptions {
    // Returns failure injection options which don't add any failures.
    pub fn no_failures() -> Self {
        Self {
            failure_probability: 0.0,
            latency_probability: 0.0,
            latency_ms: 0,
            disconnected: HashSet::new(),
        }
    }

    pub fn disconnect(&mut self, name: &str) {
        self.disconnected.insert(name.to_string());
    }

    pub fn reconnect(&mut self, name: &str) {
        self.disconnected.remove(name);
    }
}

#[derive(Debug, Clone)]
pub enum Endpoint {
    Server(Server),
    Client(String),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Endpoint {
    pub fn name(&self) -> String {
        match &self {
            Endpoint::Server(s) => s.name.to_string(),
            Endpoint::Client(c) => c.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    src: Endpoint,
    dst: Server,
}

impl ChannelInfo {
    pub fn from_server(src: Server, dst: Server) -> Self {
        Self {
            src: Endpoint::Server(src),
            dst,
        }
    }

    pub fn from_client(name: String, dst: Server) -> Self {
        Self {
            src: Endpoint::Client(name),
            dst,
        }
    }
}

// Tower middleware intended to wrap GRPC channels, capable of intercepting and injecting failures
// or additional latency into RPC calls.
pub struct FailureInjectionMiddleware<T> {
    inner: T,
    options: Arc<Mutex<FailureOptions>>,
    channel_info: Arc<ChannelInfo>,
}

impl<T> FailureInjectionMiddleware<T> {
    pub fn new(inner: T, options: Arc<Mutex<FailureOptions>>, channel_info: ChannelInfo) -> Self {
        FailureInjectionMiddleware {
            inner,
            options,
            channel_info: Arc::new(channel_info),
        }
    }
}

impl<T, ReqBody> GrpcService<ReqBody> for FailureInjectionMiddleware<T>
where
    T: GrpcService<ReqBody>,
    T::Error: Into<BoxError>,
{
    type ResponseBody = T::ResponseBody;
    type Error = BoxError;
    type Future = FailureInjectionFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        FailureInjectionFuture {
            inner: self.inner.call(request),
            options: self.options.clone(),
            channel_info: self.channel_info.clone(),
            get_options_future: None,
            delay_future: None,
            failed: None,
        }
    }
}

// Special future that implements failure injection.
#[pin_project]
pub struct FailureInjectionFuture<F> {
    #[pin]
    inner: F,

    #[pin]
    get_options_future: Option<Pin<Box<dyn Future<Output = FailureOptions> + Send>>>,

    #[pin]
    delay_future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,

    options: Arc<Mutex<FailureOptions>>,

    failed: Option<bool>,

    channel_info: Arc<ChannelInfo>,
}

impl<F, Response, Error> Future for FailureInjectionFuture<F>
where
    F: Future<Output = Result<Response, Error>>,
    Error: Into<BoxError>,
{
    type Output = Result<Response, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // Stage 1: Get failure options.
        if this.failed.is_none() {
            if this.get_options_future.is_none() {
                let options_arc = this.options.clone();
                let fut = async move {
                    let options_guard = options_arc.lock().await;
                    options_guard.clone()
                };
                this.get_options_future.set(Some(Box::pin(fut)));
            }

            // This is the manual equivalent of the `ready!` macro.
            let options = match this
                .get_options_future
                .as_mut()
                .as_pin_mut()
                .unwrap()
                .poll(cx)
            {
                Poll::Ready(opts) => opts,
                Poll::Pending => return Poll::Pending,
            };

            let channel_info = this.channel_info.clone();
            let (src, dst): (String, String) = (
                channel_info.src.name().clone(),
                channel_info.dst.name.clone(),
            );
            let fail = if options.disconnected.contains(&src) || options.disconnected.contains(&dst)
            {
                true
            } else {
                rand::random::<f64>() < options.failure_probability
            };
            *this.failed = Some(fail);

            let inject_latency = rand::random::<f64>() < options.latency_probability;
            if inject_latency {
                debug!(latency_ms = options.latency_ms, "injected latency");
                let delay = Box::pin(sleep(Duration::from_millis(options.latency_ms as u64)));
                this.delay_future.set(Some(delay));
            }
        }

        // Stage 2: Wait for injected latency.
        if let Some(delay) = this.delay_future.as_mut().as_pin_mut() {
            if delay.poll(cx).is_pending() {
                return Poll::Pending;
            }
        }

        // Stage 3: Return an injected failure.
        if *this.failed.as_ref().unwrap() {
            let (src, dst) = (this.channel_info.src.clone(), this.channel_info.dst.clone());
            let error = tonic::Status::unavailable(format!(
                "error injected in channel {} -> {}",
                src, dst.name
            ));
            return Poll::Ready(Err(error.into()));
        }

        // Stage 4: Poll the inner service.
        this.inner.poll(cx).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyvalue::grpc::{KeyValueClient, KeyValueServer, PutRequest};
    use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
    use crate::keyvalue::keyvalue_proto::{GetRequest, GetResponse, PutResponse};
    use crate::testing::TestRpcServer;
    use std::time::Instant;
    use tonic::transport::Channel;
    use tonic::transport::Endpoint;
    use tonic::{Response, Status};

    const CLIENT_NAME: &str = "client-for-testing";

    struct Fixture {
        failure_options: Arc<Mutex<FailureOptions>>,
        server: TestRpcServer,
    }

    impl Fixture {
        async fn new() -> Self {
            let service = FakeKeyValue {};
            let server = TestRpcServer::run(KeyValueServer::new(service)).await;
            Self {
                failure_options: Arc::new(Mutex::new(FailureOptions::no_failures())),
                server,
            }
        }

        async fn make_client(&self) -> KeyValueClient<FailureInjectionMiddleware<Channel>> {
            let server = self.server.address().unwrap().clone();
            let channel = get_channel(&server).await;
            let channel_info = ChannelInfo::from_client(CLIENT_NAME.to_string(), server.clone());
            let options = self.failure_options.clone();
            let middleware: FailureInjectionMiddleware<Channel> =
                FailureInjectionMiddleware::new(channel, options, channel_info);
            KeyValueClient::new(middleware)
        }
    }

    #[tokio::test]
    async fn test_no_failures() {
        let f = Fixture::new().await;
        let mut client = f.make_client().await;
        let result = client.get(make_request()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_probability_failure() {
        let f = Fixture::new().await;
        let mut client = f.make_client().await;

        f.failure_options.lock().await.failure_probability = 1.0;

        let result = client.get(make_request()).await;
        assert!(result.is_err());

        let message = result.unwrap_err().to_string();
        assert!(message.contains("error injected in channel"));
    }

    #[tokio::test]
    async fn test_disconnected_failure() {
        let f = Fixture::new().await;
        let mut client = f.make_client().await;

        let server_name = f.server.address().unwrap().name;
        f.failure_options
            .lock()
            .await
            .disconnected
            .insert(server_name);

        let result = client.get(make_request()).await;
        assert!(result.is_err());

        let message = result.unwrap_err().to_string();
        assert!(message.contains("error injected in channel"));
    }

    #[tokio::test]
    async fn test_latency_injection() {
        let f = Fixture::new().await;
        let mut client = f.make_client().await;

        {
            let mut opts = f.failure_options.lock().await;
            opts.latency_probability = 1.0;
            opts.latency_ms = 100;
        }

        let start_time = Instant::now();
        let result = client.get(make_request()).await;
        let elapsed = start_time.elapsed();

        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_failure_after_latency() {
        let f = Fixture::new().await;
        let mut client = f.make_client().await;

        {
            let mut opts = f.failure_options.lock().await;
            opts.failure_probability = 1.0;
            opts.latency_probability = 1.0;
            opts.latency_ms = 100;
        }

        let start_time = Instant::now();
        let result = client.get(make_request()).await;
        let elapsed = start_time.elapsed();

        assert!(result.is_err());
        assert!(elapsed.as_millis() >= 100);

        let message = result.unwrap_err().to_string();
        assert!(message.contains("error injected in channel"));
    }

    // Constructs an arbitrary RPC request. Contents are irrelevant.
    fn make_request() -> GetRequest {
        GetRequest {
            key: vec![],
            version: 0,
        }
    }

    // Helper to create a channel to a server managed by the TestRpcServer.
    async fn get_channel(server: &Server) -> Channel {
        Endpoint::from_shared(format!("http://[{}]:{}", server.host, server.port))
            .expect("Endpoint should be valid")
            .connect()
            .await
            .expect("Channel connection should succeed")
    }

    struct FakeKeyValue {}

    #[tonic::async_trait]
    impl KeyValue for FakeKeyValue {
        async fn get(
            &self,
            _: tonic::Request<GetRequest>,
        ) -> Result<Response<GetResponse>, Status> {
            Ok(Response::new(GetResponse {
                entry: None,
                version: 0,
            }))
        }

        async fn put(
            &self,
            _: tonic::Request<PutRequest>,
        ) -> Result<Response<PutResponse>, Status> {
            Ok(Response::new(PutResponse { debug_info: None }))
        }
    }
}
