extern crate tokio_stream;

use crate::context::Context;
use axum::Router;
use axum::routing::IntoMakeService;
use std::net::SocketAddr;
use tokio::net::TcpListener;

// A helper struct which can be used to test http handlers. Runs a real server
// which binds to an arbitrary port and provides access to the resulting port.
// Also takes care of tearing down the server when the instance goes out of
// scope.
pub struct TestHttpServer {
    port: Option<u16>,
}

impl TestHttpServer {
    // One-stop-shop for running a single router on an arbitrary port. Returns
    // the instance of TestHttpServer which provides access to the port. Panics if
    // anything goes wrong during setup.
    pub async fn run(ctx: Context, router: IntoMakeService<Router>) -> Self {
        let mut server = TestHttpServer { port: None };
        server.start(ctx, router).await;
        server
    }

    // Returns the port the server is listening on.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    async fn start(&mut self, ctx: Context, router: IntoMakeService<Router>) {
        // Assign to an arbitrary free port
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let listener = TcpListener::bind(addr).await.expect("Failed to bind");
        self.port = Some(listener.local_addr().unwrap().port());

        tokio::spawn(async move {
            let shutdown = async move {
                ctx.done().await;
            };
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await
                .expect("server");
        });
    }
}
