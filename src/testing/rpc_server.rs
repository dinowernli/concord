extern crate tokio_stream;

use std::convert::Infallible;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Body;

// A helper struct which can be used to test grpc services. Runs a real server
// which binds to an arbitrary port and provides access to the resulting port.
// Also takes care of tearing down the server when the instance goes out of
// scope.
pub struct TestRpcServer {
    port: Option<u16>,
    shutdown: Option<Sender<()>>,
}

impl TestRpcServer {
    // One-stop-shop for running a single service on an arbitrary port. Returns
    // the instance of TestRpcServer which provides access to the port. Panics if
    // anything goes wrong during setup.
    pub async fn run<S>(service: S) -> Self
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: std::error::Error + Send + Sync,
    {
        let mut server = TestRpcServer {
            port: None,
            shutdown: None,
        };
        server.start(service).await;
        server
    }

    async fn start<S>(&mut self, service: S)
    where
        S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
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

        // Create the shutdown channel for the server.
        let (tx, rx) = oneshot::channel();
        self.shutdown = Some(tx);

        // Run the server in the background.
        tokio::spawn(async {
            let incoming = TcpListenerStream::new(listener);
            let shutdown = async {
                rx.await.ok();
            };
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .expect("serve");
        });
    }

    // Returns the port the server is listening on.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    fn stop(&mut self) {
        if self.shutdown.is_some() {
            self.shutdown.take().unwrap().send(()).expect("shutdown");
        }
    }
}

impl Drop for TestRpcServer {
    fn drop(&mut self) {
        self.stop();
    }
}
