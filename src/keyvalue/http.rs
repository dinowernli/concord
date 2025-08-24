use axum::Router;
use axum::body::Body;
use axum::http::Request as HttpRequest;
use axum::http::StatusCode;
use axum::routing::get;
use std::sync::Arc;
use tonic::Request;

use crate::keyvalue::KeyValueService;
use crate::keyvalue::keyvalue_proto::GetRequest;
use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;

// Provides an HTTP interface for the supplied keyvalue store.
#[derive(Clone)]
pub struct HttpHandler {
    service: Arc<KeyValueService>,
}

impl HttpHandler {
    pub fn new(service: Arc<KeyValueService>) -> Self {
        Self { service }
    }

    /**
     * Installs routes that allow interacting with the keyvalue store. Example queries
     * include /get?key=foo.
     */
    pub fn routes(self: Arc<Self>) -> Router {
        let get_self = self.clone();
        let get_handler = move |request| async move { get_self.handle_get(request).await };

        let root_self = self.clone();
        let root_handler = move |request| async move { root_self.handle_root(request).await };

        Router::new()
            .route("/", get(root_handler))
            .route("/get", get(get_handler))
    }

    fn invalid(message: String) -> (StatusCode, String) {
        (StatusCode::BAD_REQUEST, message)
    }

    async fn handle_root(self: Arc<Self>, _: HttpRequest<Body>) -> (StatusCode, String) {
        let kv = self.service.clone();
        let latest = kv.store().lock().await.latest_version();
        (
            StatusCode::OK,
            format!("KeyValue store with latest version: {}", latest),
        )
    }

    async fn handle_get(self: Arc<Self>, request: HttpRequest<Body>) -> (StatusCode, String) {
        let kv = self.service.clone();
        let query = match request.uri().query() {
            Some(q) => q,
            None => return Self::invalid("must pass query".to_string()),
        };

        let parsed = querystring::querify(query);
        let key = match parsed
            .iter()
            .find(|(a, _)| a.eq(&"key"))
            .map(|(_, b)| b.to_string())
        {
            Some(value) => value,
            _ => return Self::invalid("must pass key parameter".to_string()),
        };

        let request = Request::new(GetRequest {
            key: key.clone().as_bytes().to_vec(),
            version: -1,
        });

        match kv.get(request).await {
            Ok(p) => {
                let proto = p.into_inner();
                match proto.entry {
                    Some(e) => match String::from_utf8(e.value) {
                        Ok(value) => (
                            StatusCode::OK,
                            format!("{}={}, version={}", key, value, proto.version),
                        ),
                        _ => (
                            StatusCode::BAD_REQUEST,
                            format!("failed to parse value as utf8 for key {}", key),
                        ),
                    },
                    None => (StatusCode::NOT_FOUND, format!("no value for key {}", key)),
                }
            }
            Err(status) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query keyvalue store {}", status.to_string()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyvalue::MapStore;
    use crate::raft::raft_common_proto::Server;
    use crate::testing::TestHttpServer;
    use async_std::sync::Mutex;
    use bytes::Bytes;

    async fn make_server() -> TestHttpServer {
        let store = Arc::new(Mutex::new(MapStore::new()));
        let raft_member = Server {
            name: "kv-raft-for-testing".to_string(),
            host: "[::1]".to_string(),
            port: 12345,
        };
        let kv = Arc::new(KeyValueService::new("kv-for-testing", &raft_member, store));
        kv.store()
            .lock()
            .await
            .set(Bytes::from("foo"), Bytes::from("foo-value"));

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
        assert_eq!("foo=foo-value, version=1", text.trim());
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
