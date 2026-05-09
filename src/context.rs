use futures_util::future::{BoxFuture, FutureExt};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::watch;
use tokio::time::{Duration, Instant, sleep_until};
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Context provides a thread-safe, hierarchical mechanism for cancellation
/// and deadline management. It is designed to be cheaply clonable by wrapping an Arc.
#[derive(Clone)]
pub struct Context {
    impl_ptr: Arc<ContextImpl>,
}

/// An error type that supports explaining that either the operation stopped and failed
/// because an associated context was done, or forwarding an underlying "inner" error.
#[derive(Debug, Clone)]
pub enum Error<E> {
    // Occurs when the operation was tied to a context that timed out or was canceled.
    ContextDone,

    // Occurs when the underlying operation produces an error.
    Inner(E),
}

// Used to add context support for RPC handler logic. Intended for use with the
// interceptor implementation below.
pub trait RpcContextExt {
    fn ctx(&self) -> Result<Context, Status>;
}

impl<T> RpcContextExt for Request<T> {
    fn ctx(&self) -> Result<Context, Status> {
        self.extensions()
            .get::<Context>()
            .cloned()
            .ok_or_else(|| Status::internal("Lifecycle context missing from request extensions"))
    }
}

/// Returns an interceptor that can be attached to RPC servers. When attached,
/// the interceptor injects the supplied context into every incoming request,
/// making the context available to the request handling logic.
pub fn interceptor(ctx: Context) -> impl Interceptor + Clone {
    move |mut req: Request<()>| {
        // TODO(dino): It would be great to actually insert a child context
        // that can be cancelled, with a fixed upper-bound deadline. In order
        // to do this, we first need to make sure we won't end up with
        // arbitrarily deep context child relationships (e.g. for flows that
        // start in RPC handlers, and keep making children from that ctx).
        req.extensions_mut().insert(ctx.clone());
        Ok(req)
    }
}

impl Context {
    /// Returns a fresh root context with no deadline.
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        let (dtx, drx) = watch::channel(());
        Context {
            impl_ptr: Arc::new(ContextImpl {
                parent: None,
                cancel_tx: tx,
                cancel_rx: rx,
                deadline_tx: dtx,
                deadline_rx: drx,
                deadline: AtomicU64::new(u64::MAX),
            }),
        }
    }

    /// Wraps a future and merges context cancellation into a unified Error type.
    pub async fn wrap<F, T, E>(&self, future: F) -> Result<T, Error<E>>
    where
        F: Future<Output = Result<T, E>>,
    {
        tokio::select! {
            _ = self.done() => Err(Error::ContextDone),
            result = future => result.map_err(Error::Inner),
        }
    }

    /// Creates a child context linked to this parent. The parent context being
    /// done causes the child context to be done as well.
    pub fn create_child(&self) -> Self {
        let (tx, rx) = watch::channel(false);
        let (dtx, drx) = watch::channel(());
        let impl_ptr = Arc::new(ContextImpl {
            parent: Some(self.impl_ptr.clone()),
            cancel_tx: tx,
            cancel_rx: rx,
            deadline_tx: dtx,
            deadline_rx: drx,
            deadline: AtomicU64::new(u64::MAX),
        });
        Context { impl_ptr }
    }

    /// Triggers manual cancellation for this context and its descendants.
    pub fn cancel(&self) {
        let _ = self.impl_ptr.cancel_tx.send(true);
    }

    /// Returns the current deadline if one has been set.
    pub fn deadline(&self) -> Option<Instant> {
        let d = self.impl_ptr.deadline.load(Ordering::Acquire);
        if d == u64::MAX {
            None
        } else {
            Some(u64_to_instant(d))
        }
    }

    /// Shortens the deadline. If the new deadline is later than the existing
    /// one, this call is a no-op.
    pub fn set_deadline(&self, deadline: Instant) {
        let nanos = instant_to_u64(deadline);
        let old = self.impl_ptr.deadline.fetch_min(nanos, Ordering::AcqRel);

        // Notify any active `done()` futures to wake up and reschedule their timers.
        if nanos < old {
            let _ = self.impl_ptr.deadline_tx.send(());
        }
    }

    /// Returns true if this context (or any ancestor) is done.
    ///
    /// Recursively checks the parent for being done. Uses atomic loads and pointer
    /// dereferences (and no locks) for performance.
    pub fn is_done(&self) -> bool {
        if *self.impl_ptr.cancel_rx.borrow() {
            return true;
        }
        if let Some(d) = self.deadline() {
            if Instant::now() >= d {
                return true;
            }
        }
        match &self.impl_ptr.parent {
            Some(p) => Context {
                impl_ptr: p.clone(),
            }
            .is_done(),
            None => false,
        }
    }

    /// Returns a future that completes when the context is cancelled or timed out.
    ///
    /// Needs to return `BoxFuture` because async functions in Rust cannot be
    /// recursive without heap indirection; otherwise, the compiler cannot
    /// calculate the finite size of the generated Future state machine.
    pub fn done(&self) -> BoxFuture<'static, ()> {
        let mut cancel_rx = self.impl_ptr.cancel_rx.clone();
        let mut deadline_rx = self.impl_ptr.deadline_rx.clone();
        let this = self.clone();

        async move {
            loop {
                if this.is_done() {
                    return;
                }

                let current_deadline =
                    u64_to_instant(this.impl_ptr.deadline.load(Ordering::Acquire));

                tokio::select! {
                    // Completes if the manual cancellation is triggered locally.
                    _ = cancel_rx.changed() => {},

                    // Completes if the timer expires.
                    _ = sleep_until(current_deadline) => {},

                    // If the deadline is tightened, we loop to restart the `sleep_until` timer.
                    _ = deadline_rx.changed() => continue,

                    // Recursive check: waits for any ancestor to finish.
                    _ = async {
                        if let Some(p) = &this.impl_ptr.parent {
                            Context { impl_ptr: p.clone() }.done().await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {},
                }
                break;
            }
        }
        .boxed()
    }

    /// Sleeps for the given duration, or until the context is cancelled. Returns
    /// Err(Error::ContextDone) if the context is cancelled before the duration
    /// has passed.
    pub async fn sleep(&self, duration: Duration) -> Result<(), Error<()>> {
        tokio::select! {
            _ = self.done() => Err(Error::ContextDone),
            _ = tokio::time::sleep(duration) => Ok(()),
        }
    }
}

struct ContextImpl {
    /// Upward link to the parent context.
    ///
    /// WHY: Storing a reference to the parent (rather than the parent tracking children)
    /// allows for lock-free cancellation checks via upward tree traversal. This
    /// ensures that children do not "leak" if they aren't explicitly removed from
    /// a parent's list.
    parent: Option<Arc<ContextImpl>>,

    /// Multi-consumer signal for cancellation.
    ///
    /// WHY: `watch::channel` is optimized for "last-value" notifications. Since
    /// cancellation is a binary, one-way state change, it is more memory-efficient
    /// than broadcast channels.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,

    /// Signal to notify listeners that the deadline has been adjusted.
    deadline_tx: watch::Sender<()>,
    deadline_rx: watch::Receiver<()>,

    /// Atomic storage for the deadline timestamp.
    ///
    /// WHY: Deadlines are stored as u64 nanoseconds (relative to ANCHOR) to
    /// enable the use of `fetch_min`. This hardware-level atomic operation
    /// ensures that deadlines can only be tightened (moved earlier), never
    /// extended, without requiring a Mutex.
    deadline: AtomicU64,
}

lazy_static::lazy_static! {
    /// WHY: `Instant` is an opaque, non-serializable type that cannot be
    /// stored in an `AtomicU64`. We establish an `ANCHOR` (the moment the
    /// app starts) to convert `Instant` into nanosecond offsets. This
    /// allows for lock-free atomic math across threads.
    ///
    /// Note that we can't just use Instant's `as_nanos` and `from_nanos`
    /// because Instant goes out of its way to be monotonic whereas just
    /// using raw nanos values would not be monotonic.
    static ref ANCHOR: Instant = Instant::now();
}

fn instant_to_u64(i: Instant) -> u64 {
    i.duration_since(*ANCHOR).as_nanos() as u64
}

fn u64_to_instant(n: u64) -> Instant {
    if n == u64::MAX {
        // Represent "No Deadline" as a time 100 years in the future to keep it monotonic.
        *ANCHOR + Duration::from_secs(86400 * 365 * 100)
    } else {
        *ANCHOR + Duration::from_nanos(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, advance, pause};

    #[tokio::test]
    async fn test_hierarchical_cancellation() {
        let parent = Context::new();
        let child = parent.create_child();

        assert!(!child.is_done());
        parent.cancel();
        assert!(child.is_done());
    }

    #[tokio::test]
    async fn test_deadline_wakes_future() {
        pause();
        let ctx = Context::new();
        let fut = ctx.done();

        ctx.set_deadline(Instant::now() + Duration::from_secs(1));
        advance(Duration::from_secs(2)).await;

        tokio::time::timeout(Duration::from_millis(10), fut)
            .await
            .expect("Done future should have finished on timeout");
    }

    #[tokio::test]
    async fn test_is_cancelled_deadline() {
        pause();
        let ctx = Context::new();
        ctx.set_deadline(Instant::now() + Duration::from_secs(5));

        advance(Duration::from_secs(6)).await;
        assert!(ctx.is_done());
    }
}
