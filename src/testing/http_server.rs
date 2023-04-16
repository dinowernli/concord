extern crate tokio_stream;

use axum::routing::IntoMakeService;
use axum::Router;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

// A helper struct which can be used to test http handlers. Runs a real server
// which binds to an arbitrary port and provides access to the resulting port.
// Also takes care of tearing down the server when the instance goes out of
// scope.
pub struct TestHttpServer {
    port: Option<u16>,
    shutdown: Option<Sender<()>>,
}

impl TestHttpServer {
    // One-stop-shop for running a single router on an arbitrary port. Returns
    // the instance of TestHttpServer which provides access to the port. Panics if
    // anything goes wrong during setup.
    pub async fn run(router: IntoMakeService<Router>) -> Self {
        let mut server = TestHttpServer {
            port: None,
            shutdown: None,
        };
        server.start(router).await;
        server
    }

    // Returns the port the server is listening on.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    async fn start(&mut self, router: IntoMakeService<Router>) {
        let (tx, rx) = oneshot::channel();
        self.shutdown = Some(tx);

        // Assign to an arbitrary free port
        let addr = ([127, 0, 0, 1], 0).into();
        let server_builder = hyper::Server::bind(&addr);
        self.port = Some(server_builder.local_addr().port());

        tokio::spawn(async {
            let shutdown = async {
                rx.await.ok();
            };
            server_builder
                .serve(router)
                .with_graceful_shutdown(shutdown)
                .await
                .expect("server");
        });
    }

    fn stop(&mut self) {
        if self.shutdown.is_some() {
            self.shutdown.take().unwrap().send(()).expect("shutdown");
        }
    }
}

impl Drop for TestHttpServer {
    fn drop(&mut self) {
        self.stop();
    }
}
