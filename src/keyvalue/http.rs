use axum::routing::get;
use axum::Router;
use hyper::{Body, StatusCode};
use std::sync::Arc;
use tonic::Request;

use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue::keyvalue_proto::GetRequest;

// Provides an HTTP interface for the supplied KeyValue instance.
#[derive(Clone)]
pub struct HttpHandler {
    service: Arc<dyn KeyValue>,
}

impl HttpHandler {
    pub fn new(service: Arc<dyn KeyValue>) -> Self {
        Self { service }
    }

    /**
     * Installs routes that allow interacting with the keyvalue store. Example queries
     * include /get?key=foo.
     */
    pub fn routes(self: Arc<Self>) -> Router {
        let hello = move |req: hyper::Request<Body>| async move { self.handle_get(req).await };
        Router::new().route("/get", get(hello))
    }

    fn make_response(code: StatusCode, message: String) -> hyper::Response<Body> {
        hyper::Response::builder()
            .status(code)
            .body(Body::from(format!("{}\n", message)))
            .unwrap()
    }

    fn invalid(message: String) -> hyper::Response<Body> {
        Self::make_response(StatusCode::BAD_REQUEST, message)
    }

    async fn handle_get(&self, request: hyper::Request<Body>) -> hyper::Response<Body> {
        let query = match request.uri().query() {
            Some(q) => q,
            None => return HttpHandler::invalid("must pass query".to_string()),
        };

        let parsed = querystring::querify(query);
        let key = match parsed
            .iter()
            .find(|(a, _)| a.eq(&"key"))
            .map(|(_, b)| b.to_string())
        {
            Some(value) => value,
            _ => return HttpHandler::invalid("must pass key parameter".to_string()),
        };

        let request = Request::new(GetRequest {
            key: key.clone().as_bytes().to_vec(),
            version: -1,
        });

        match self.service.get(request).await {
            Ok(p) => {
                let proto = p.into_inner();
                match proto.entry {
                    Some(e) => match String::from_utf8(e.value) {
                        Ok(value) => HttpHandler::make_response(
                            StatusCode::OK,
                            format!("{}={}, version={}", key, value, proto.version),
                        ),
                        _ => HttpHandler::make_response(
                            StatusCode::BAD_REQUEST,
                            format!("failed to parse value as utf8 for key {}", key),
                        ),
                    },
                    None => HttpHandler::make_response(
                        StatusCode::NOT_FOUND,
                        format!("no value for key {}", key),
                    ),
                }
            }
            Err(status) => HttpHandler::make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query keyvalue store {}", status.to_string()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyvalue::grpc::PutRequest;
    use crate::keyvalue::keyvalue_proto::{Entry, GetResponse, PutResponse};
    use crate::testing::TestHttpServer;
    use async_trait::async_trait;
    use tonic::{Response, Status};

    struct FakeKeyValue {}

    #[async_trait]
    impl KeyValue for FakeKeyValue {
        async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
            let key = request.into_inner().key.clone();
            let key_string = String::from_utf8(key.clone());
            let entry = match key_string.expect("utf8").as_str() {
                "foo" => Some(Entry {
                    key: key.clone(),
                    value: "foo-value".to_string().into_bytes(),
                }),
                "bar" => Some(Entry {
                    key: key.clone(),
                    value: "bar-value".to_string().into_bytes(),
                }),
                _ => None,
            };

            Ok(Response::new(GetResponse { entry, version: 0 }))
        }

        async fn put(&self, _: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
            // We don't need any writes for now.
            panic!("Not implemented");
        }
    }

    async fn make_server() -> TestHttpServer {
        let kv = Arc::new(FakeKeyValue {});
        let http = Arc::new(HttpHandler {
            service: kv.clone(),
        });
        let web_service = Router::new()
            .nest("/keyvalue", http.routes())
            .into_make_service();

        TestHttpServer::run(web_service).await
    }

    async fn send_request(server: &TestHttpServer, path: &str) -> reqwest::Response {
        let port = server.port().expect("port");
        let uri = format!("http://{}:{}{}", "127.0.0.1", port, path);
        reqwest::get(uri.as_str()).await.expect("request")
    }

    #[tokio::test]
    async fn test_returns_value() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue/get?key=foo").await;

        let status = response.status().clone();
        let text = response.text().await.expect("text");

        assert_eq!(reqwest::StatusCode::OK, status);
        assert_eq!("foo=foo-value, version=0", text.trim());
    }

    #[tokio::test]
    async fn test_not_found() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue/get?key=not-a-rea-key").await;
        let status = response.status();
        assert_eq!(reqwest::StatusCode::NOT_FOUND, status);
    }

    #[tokio::test]
    async fn test_bad_path() {
        let server = make_server().await;
        let response = send_request(&server, "/INVALID/get?key=not-a-rea-key").await;
        let status = response.status();
        assert_eq!(reqwest::StatusCode::NOT_FOUND, status);
    }
}
