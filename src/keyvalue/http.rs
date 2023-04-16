use axum::routing::get;
use axum::Router;
use hyper::{Body, StatusCode};
use std::str::FromStr;
use std::sync::Arc;
use tonic::Request;
use url::Url;

use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue::keyvalue_proto::GetRequest;
use crate::keyvalue::KeyValueService;

#[derive(Clone)]
pub struct HttpHandler {
    service: Arc<KeyValueService>,
}

impl HttpHandler {
    pub fn new(service: Arc<KeyValueService>) -> Self {
        Self { service }
    }

    pub fn routes(self: Arc<Self>) -> Router {
        let hello = async move |req: hyper::Request<Body>| self.handle_get(req).await;
        Router::new().route("/get", get(hello))
    }

    fn invalid(message: String) -> hyper::Response<Body> {
        hyper::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!("{}\n", message)))
            .unwrap()
    }

    async fn handle_get(&self, request: hyper::Request<Body>) -> hyper::Response<Body> {
        // Bit of a hack to the URL library to parse this.
        let full = format!("https://some.dummy.example.com{}", request.uri());
        let parsed = match Url::from_str(full.as_str()) {
            Ok(u) => u,
            _ => return HttpHandler::invalid("must pass query".to_string()),
        };

        let key = match parsed
            .query_pairs()
            .find(|(k, v)| k == "key")
            .map(|(k, v)| v)
        {
            Some(value) => value,
            _ => return HttpHandler::invalid("must pass key parameter".to_string()),
        };

        let request = Request::new(GetRequest {
            key: key.clone().as_bytes().to_vec(),
            version: -1,
        });

        // TODO - donotmerge - replace these "unwrap"s with better error handling

        let output = match self.service.get(request).await {
            Ok(proto) => {
                let inner = proto.into_inner();
                let value = String::from_utf8(inner.entry.unwrap().value).unwrap();
                format!("{}={}, version={}\n", key, value, inner.version)
            }
            Err(status) => format!("{}\n", status.to_string()),
        };

        hyper::Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(output))
            .unwrap()
    }
}
