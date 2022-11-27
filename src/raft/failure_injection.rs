extern crate chrono;
extern crate pin_project;
extern crate timer;
extern crate tower;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::task::{Context, Poll};

use self::timer::Guard;
use chrono::Duration;
use pin_project::pin_project;
use timer::Timer;
use tonic::client::GrpcService;
use tonic::codegen::http::Request;
use tower::BoxError;
use tracing::debug;

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
        let channel_info = self.channel_info.clone();

        // Error injection.
        let fail = rand::random::<f64>() < self.options.failure_probability;

        // Latency injection.
        let inject_latency = rand::random::<f64>() < self.options.latency_probability;
        let timer_state = if inject_latency {
            TimerState::ready_after(self.options.latency_ms)
        } else {
            TimerState::ready()
        };

        FailureInjectionFuture {
            inner: self.inner.call(request),
            failed: fail,
            channel_info,
            timer_state,
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

    // Indicates that this future needs to fail entirely.
    failed: bool,

    // Holds basic information about the channel used for the underlying operation.
    channel_info: ChannelInfo,

    // Used for latency injection.
    timer_state: Arc<Mutex<TimerState>>,
}

impl<F, Response, Error> Future for FailureInjectionFuture<F>
where
    F: Future<Output = Result<Response, Error>>,
    Error: Into<BoxError>,
{
    type Output = Result<Response, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Make sure we respect the timing of any injected latency.
        {
            // TODO(dino): based on the debug statements latency injection seems to be working,
            // but not triggering RPC timeouts, presumably because the timeouts only apply to
            // the actual request put on the wire.
            let mut timer_state = self.timer_state.lock().unwrap();
            if !timer_state.done() {
                debug!("timer state not complete, returning pending");
                timer_state.waker = Some(cx.waker().clone());
                return Poll::Pending;
            } else {
                if timer_state.has_ever_waited() {
                    debug!("future with latency injected now complete");
                }
            }
        }

        // Make sure we respect and injected errors.
        if self.failed {
            let (src, dst) = (self.channel_info.src.clone(), self.channel_info.dst.clone());
            let error =
                tonic::Status::unavailable(format!("error injected in channel {} -> {}", src, dst));
            return Poll::Ready(Err(error.into()));
        }

        // Finally, just forward to the wrapped operation.
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Into::into)),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct TimerState {
    // Set to true once the requested amount of time has passed.
    completed: bool,

    // Used to notify the future that we're ready for a poll.
    waker: Option<Waker>,

    // The guard and timer objects that need to be kept alive for the callback to work.
    guard: Option<Guard>,
    timer: Option<Timer>,
}

impl TimerState {
    fn ready() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            completed: true,
            waker: None,
            guard: None,
            timer: None,
        }))
    }

    fn ready_after(duration_ms: u32) -> Arc<Mutex<Self>> {
        let result = Arc::new(Mutex::new(Self {
            completed: false,
            waker: None,
            guard: None,
            timer: None,
        }));

        // Start a background function to allow the future to make progress once the
        // specified time has passed.
        let timer = Timer::new();
        let result_copy = result.clone();

        debug!(latency_ms = duration_ms, "injected latency");
        let guard =
            timer.schedule_with_delay(Duration::milliseconds(duration_ms as i64), move || {
                let mut timer_state = result_copy.lock().unwrap();
                timer_state.completed = true;
                if let Some(waker) = timer_state.waker.take() {
                    waker.wake();
                }
                debug!("woke up after latency injection");
            });

        // Make sure the timer and the guard survive for long enough.
        {
            let mut state = result.lock().unwrap();
            state.guard = Some(guard);
            state.timer = Some(timer);
        }

        result
    }

    fn done(&self) -> bool {
        self.completed
    }

    // Returns whether the timer state ever did any waiting (i.e., did not start its
    // life in the completed state.
    fn has_ever_waited(&self) -> bool {
        self.timer.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initially_done() {
        let s = TimerState::ready();
        assert!(s.lock().unwrap().done());
    }

    #[test]
    fn test_initially_not_done() {
        let s = TimerState::ready_after(50000);
        assert!(!s.lock().unwrap().done());
    }
}
