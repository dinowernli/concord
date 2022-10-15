use std::task::{Context, Poll};
use tonic::client::GrpcService;
use tonic::codegen::http::Request;

pub struct InstrumentedGrpcService<T> {
    pub inner: T,
    // TODO add some instrumentation
}

impl<T, ReqBody> GrpcService<ReqBody> for InstrumentedGrpcService<T>
where
    T: GrpcService<ReqBody>,
{
    type ResponseBody = T::ResponseBody;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // TODO fault injection goes here
        self.inner.call(request)
    }
}
