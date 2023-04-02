use crate::keyvalue::grpc::GetRequest;
use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue::keyvalue_proto::GetResponse;
use crate::keyvalue::KeyValueService;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::{empty_body, BoxBody};
use tonic::codegen::{http, Service};
use tonic::transport::NamedService;
use tonic::{Request, Status};

const HEADER_KEY: &str = "x-key";
const HEADER_VALUE: &str = "x-value";
const HEADER_VERSION: &str = "x-version";

#[derive(Clone)]
pub struct DebugHttpService {
    inner: Arc<KeyValueService>,
}

impl DebugHttpService {
    pub fn new(service: KeyValueService) -> Self {
        DebugHttpService {
            inner: Arc::from(service),
        }
    }

    fn extract_key(req: &http::Request<tonic::transport::Body>) -> String {
        let header = req.headers().get(HEADER_KEY);
        if header.is_none() {
            return "".to_string();
        }
        let value = header.unwrap().to_str();
        if value.is_err() {
            return "".to_string();
        }
        value.unwrap().to_string()
    }

    fn http_response(response: &GetResponse, key: &String) -> http::Response<BoxBody> {
        match &response.entry {
            None => Status::not_found(format!("Key not found {}", &key)).to_http(),
            Some(entry) => http::Response::builder()
                .status(200)
                .header("grpc-status", "0")
                .header("content-type", "application/grpc")
                .header(HEADER_VERSION, response.clone().version)
                .header(HEADER_VALUE, entry.clone().value)
                .body(empty_body())
                .unwrap(),
        }
    }
}

impl NamedService for DebugHttpService {
    // > curl "localhost:12345/_debug.kv/Get" -i -H "x-key:foo"
    const NAME: &'static str = "_debug.kv";
}

impl Service<http::Request<tonic::transport::Body>> for DebugHttpService {
    type Response = http::Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::transport::Body>) -> Self::Future {
        match req.uri().path() {
            "/_debug.kv/Get" => {
                let key = DebugHttpService::extract_key(&req);
                let request_proto = GetRequest {
                    key: key.clone().into_bytes(),
                    version: 0, // Latest version.
                };
                let inner = self.inner.clone();
                Box::pin(async move {
                    let result = inner.get(Request::new(request_proto)).await;
                    Ok(match result {
                        Ok(result) => DebugHttpService::http_response(&result.into_inner(), &key),
                        Err(status) => status.to_http(),
                    })
                })
            }
            _ => Box::pin(async move {
                Ok(Status::unimplemented(format!("No method: {}", req.uri().path())).to_http())
            }),
        }
    }
}
