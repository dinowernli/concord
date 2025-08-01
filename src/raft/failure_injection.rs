extern crate chrono;
extern crate pin_project;
extern crate tokio;
extern crate tower;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project::pin_project;
use tokio::time::sleep;
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
}

impl FailureOptions {
    // Returns failure injection options which don't add any failures.
    pub const fn no_failures() -> Self {
        Self {
            failure_probability: 0.0,
            latency_probability: 0.0,
            latency_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    src: String,
    dst: String,
}

impl ChannelInfo {
    pub fn new(src: String, dst: String) -> Self {
        Self { src, dst }
    }
}

// Tower middleware intended to wrap GRPC channels, capable of intercepting and injecting failures
// or additional latency into RPC calls.
pub struct FailureInjectionMiddleware<T> {
    inner: T,
    options: Arc<FailureOptions>,
    channel_info: Arc<ChannelInfo>,
}

impl<T> FailureInjectionMiddleware<T> {
    pub fn new(inner: T, options: FailureOptions, channel_info: ChannelInfo) -> Self {
        FailureInjectionMiddleware {
            inner,
            options: Arc::new(options),
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
        // Error injection.
        let fail = rand::random::<f64>() < self.options.failure_probability;

        // Latency injection.
        let inject_latency = rand::random::<f64>() < self.options.latency_probability;

        // This line now includes a type hint, allowing the compiler to correctly
        // coerce the concrete `Sleep` future into the `dyn Future` trait object.
        let delay: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = if inject_latency {
            debug!(latency_ms = self.options.latency_ms, "injected latency");
            Some(Box::pin(sleep(Duration::from_millis(
                self.options.latency_ms as u64,
            ))))
        } else {
            None
        };

        FailureInjectionFuture {
            inner: self.inner.call(request),
            failed: fail,
            channel_info: self.channel_info.clone(),
            delay,
        }
    }
}

// Special future that implements failure injection. Does the heavy lifting by preventing the
// underlying RPC future for progressing to inject latency, and returning errors for RPC failure
// cases.
#[pin_project]
pub struct FailureInjectionFuture<F> {
    // The underlying operation.
    #[pin]
    inner: F,

    // Used for latency injection.
    #[pin]
    delay: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,

    // Indicates that this future needs to fail entirely.
    failed: bool,

    // Holds basic information about the channel used for the underlying operation.
    channel_info: Arc<ChannelInfo>,
}

impl<F, Response, Error> Future for FailureInjectionFuture<F>
where
    F: Future<Output = Result<Response, Error>>,
    Error: Into<BoxError>,
{
    type Output = Result<Response, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Poll the delay future first if it exists.
        if let Some(delay) = this.delay.as_pin_mut() {
            if delay.poll(cx).is_pending() {
                // Delay is not complete, return pending.
                return Poll::Pending;
            }
        }

        // Make sure we respect any injected errors.
        if *this.failed {
            let (src, dst) = (this.channel_info.src.clone(), this.channel_info.dst.clone());
            let error =
                tonic::Status::unavailable(format!("error injected in channel {} -> {}", src, dst));
            return Poll::Ready(Err(error.into()));
        }

        // Finally, just forward to the wrapped operation.
        match this.inner.poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            Poll::Pending => Poll::Pending,
        }
    }
}
