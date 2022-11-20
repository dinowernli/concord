extern crate pin_project;
extern crate tower;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use tonic::client::GrpcService;
use tonic::codegen::http::Request;
use tower::BoxError;

// Options used to control RPC failure injection.
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
    #[cfg(test)]
    pub const fn no_failures() -> Self {
        Self {
            failure_probability: 0.0,
            latency_probability: 0.0,
            latency_ms: 0,
        }
    }

    // Returns failure injection options which fail RPCs with the supplied probability.
    pub const fn fail_with_probability(failure_probability: f64) -> Self {
        Self {
            failure_probability,
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
    options: FailureOptions,
    channel_info: ChannelInfo,
}

impl<T> FailureInjectionMiddleware<T> {
    pub fn new(inner: T, options: FailureOptions, channel_info: ChannelInfo) -> Self {
        FailureInjectionMiddleware {
            inner,
            options,
            channel_info,
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
        let fail = rand::random::<f64>() < self.options.failure_probability;
        let channel_info = self.channel_info.clone();
        FailureInjectionFuture {
            inner: self.inner.call(request),
            failed: fail,
            channel_info,
        }
    }
}

// Special future that implements failure injection. Does the heavy lifting by preventing the
// underlying RPC future for progressing to inject latency, and returning errors for RPC failure
// cases.
#[pin_project]
pub struct FailureInjectionFuture<F> {
    #[pin]
    inner: F,
    failed: bool,
    channel_info: ChannelInfo,
}

impl<F, Response, Error> Future for FailureInjectionFuture<F>
where
    F: Future<Output = Result<Response, Error>>,
    Error: Into<BoxError>,
{
    type Output = Result<Response, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO(dino): implement latency injection by doing this:
        // https://rust-lang.github.io/async-book/02_execution/03_wakeups.html

        if self.failed {
            let (src, dst) = (self.channel_info.src.clone(), self.channel_info.dst.clone());
            let error = tonic::Status::unavailable(format!(
                "Failure injection in channel {} -> {}",
                src, dst
            ));
            return Poll::Ready(Err(error.into()));
        }

        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            Poll::Pending => Poll::Pending,
        }
    }
}
