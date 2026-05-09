extern crate tokio_stream;

use crate::context;
use crate::context::Context;
use crate::raft::raft_common_proto::Server;
use std::convert::Infallible;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::{InterceptedService, Service};
use tonic::server::NamedService;

// A helper struct which can be used to test grpc services. Runs a real server
// which binds to an arbitrary port and provides access to the resulting port.
// Also takes care of tearing down the server when the instance goes out of
// scope.
pub struct TestRpcServer {
    port: Option<u16>,
}

impl TestRpcServer {
    // One-stop-shop for running a single service on an arbitrary port. Returns
    // the instance of TestRpcServer which provides access to the port. Panics if
    // anything goes wrong during setup.
    pub async fn run<S>(ctx: Context, service: S) -> Self
    where
        S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: std::error::Error + Send + Sync,
    {
        let mut server = TestRpcServer { port: None };

        server.start(ctx, service).await;
        server
    }

    async fn start<S>(&mut self, ctx: Context, service: S)
    where
        S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: std::error::Error + Send + Sync,
    {
        // Manually created the TCP listener so we can store the port.
        let listener = TcpListener::bind("[::1]:0").await.expect("bind");
        self.port = Some(listener.local_addr().expect("address").port());

        // Run the server in the background.
        tokio::spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            let shutdown = async {
                ctx.done().await;
            };

            let intercepted_service =
                InterceptedService::new(service, context::interceptor(ctx.clone()));

            tonic::transport::Server::builder()
                .add_service(intercepted_service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .expect("serve");
        });
    }

    // Returns the port the server is listening on.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    // Returns the proto representation of the server address.
    pub fn address(&self) -> Option<Server> {
        match self.port {
            None => None,
            Some(port) => Some(Server {
                host: "::1".to_string(),
                port: port as i32,
                name: "fake-rpc-server".to_string(),
            }),
        }
    }
}
