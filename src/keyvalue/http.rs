use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{
    extract::{Form, Query},
    response::Html,
};
use serde::Deserialize;
use std::sync::Arc;
use tonic::{Code, Request, Status};

use crate::keyvalue::KeyValueService;
use crate::keyvalue::keyvalue_proto::key_value_server::KeyValue;
use crate::keyvalue::keyvalue_proto::{GetRequest, PutRequest};

// Provides an HTTP interface for the supplied keyvalue store.
#[derive(Clone)]
pub struct HttpHandler {
    parent_path: String,
    service: Arc<KeyValueService>,
}

#[derive(Deserialize)]
struct GetQuery {
    key: Option<String>,
}

#[derive(Deserialize)]
struct PutForm {
    key: String,
    value: String,
}

enum ResultMsg {
    None,
    Get(String, Option<String>),
    Put(String, String, Status),
}

impl HttpHandler {
    pub fn new(parent_path: String, service: Arc<KeyValueService>) -> Self {
        Self {
            parent_path,
            service,
        }
    }

    /**
     * Installs routes that allow interacting with the keyvalue store. Example queries
     * include /get?key=foo.
     */
    pub fn routes(self: Arc<Self>) -> Router {
        let self_get = self.clone();
        let g = move |query| self_get.handle_root(query);

        let self_post = self.clone();
        let p = move |query| self_post.handle_root_post(query);

        let self_api = self.clone();
        let a = move |query| self_api.handle_api_get(query);

        Router::new()
            .route("/", get(g).post(p))
            .route("/get", get(a))
    }

    async fn handle_root(self: Arc<Self>, Query(query): Query<GetQuery>) -> impl IntoResponse {
        let mut msg = ResultMsg::None;
        if let Some(key) = query.key {
            let req = Request::new(GetRequest {
                key: key.clone().into_bytes(),
                version: -1,
            });
            match self.service.get(req).await {
                Ok(resp) => {
                    let val = resp
                        .into_inner()
                        .entry
                        .map(|v| String::from_utf8_lossy(&v.value).into_owned());
                    msg = ResultMsg::Get(key, val);
                }
                Err(e) => {
                    msg = ResultMsg::Get(key, Some(format!("Failed to fetch value: {:?}", e)))
                }
            }
        }
        self.render_page(msg).await
    }

    async fn handle_root_post(self: Arc<Self>, Form(form): Form<PutForm>) -> impl IntoResponse {
        let key = form.key.clone();
        let value = form.value.clone();
        let req = Request::new(PutRequest {
            key: key.clone().into_bytes(),
            value: value.clone().into_bytes(),
        });
        let msg = match self.service.put(req).await {
            Ok(_) => ResultMsg::Put(key, value, Status::ok("")),
            Err(e) => ResultMsg::Put(key, value, e),
        };
        self.render_page(msg).await
    }

    async fn handle_api_get(self: Arc<Self>, Query(query): Query<GetQuery>) -> impl IntoResponse {
        match query.key {
            Some(key) => {
                let req = Request::new(GetRequest {
                    key: key.clone().into_bytes(),
                    version: -1,
                });
                match self.service.get(req).await {
                    Ok(resp) => {
                        let proto = resp.into_inner();
                        let version = proto.version;
                        match proto.entry {
                            Some(entry) => {
                                let value = String::from_utf8_lossy(&entry.value);
                                (
                                    StatusCode::OK,
                                    format!("{}={}, version={}", key, value, version),
                                )
                            }
                            None => (StatusCode::NOT_FOUND, "not found".to_string()),
                        }
                    }
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
                }
            }
            None => (StatusCode::BAD_REQUEST, "missing key".to_string()),
        }
    }

    async fn render_page(self: Arc<Self>, msg: ResultMsg) -> Html<String> {
        let latest = self.service.store().lock().await.latest_version();

        let result_html = match msg {
            ResultMsg::None => "".to_string(),
            ResultMsg::Get(k, Some(v)) => format!(
                r#"<div class="card success">Got <b>{}</b> = "{}"</div>"#,
                k, v
            ),
            ResultMsg::Get(k, None) => {
                format!(r#"<div class="card error">Key "{}" not found</div>"#, k)
            }
            ResultMsg::Put(k, v, status) if status.code() == Code::Ok => format!(
                r#"<div class="card success">Stored <b>{}</b> = "{}"</div>"#,
                k, v
            ),
            ResultMsg::Put(k, v, status) => format!(
                r#"<div class="card error">Failed to store <b>{}</b> = "{}" - {}</div>"#,
                k, v, status
            ),
        };

        let parent = self.parent_path.clone();
        let html = format!(
            r#"
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>KeyValue Store</title>
                {}
            </head>
            <body>
                <h1>KeyValue Store</h1>
                <div class="card">
                    <p>Latest version: <b>{}</b></p>
                </div>
                {}
                <div class="card">
                    <h2>Get a key</h2>
                    <form action="{}" method="get">
                        <input type="text" name="key" placeholder="key" required />
                        <button type="submit">Get</button>
                    </form>
                </div>
                <div class="card">
                    <h2>Put a key</h2>
                    <form action="{}" method="post">
                        <input type="text" name="key" placeholder="key" required /><br/>
                        <input type="text" name="value" placeholder="value" required />
                        <button type="submit">Put</button>
                    </form>
                </div>
            </body>
            </html>
        "#,
            style(),
            latest,
            result_html,
            parent.clone(),
            parent.clone()
        );

        Html(html)
    }
}

fn style() -> String {
    format!(
        r#"
    <style>
        body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f7f7f7; }}
        h1 {{ color: #2c3e50; }}
        .card {{ background: white; padding: 1rem 2rem; border-radius: 8px;
                 box-shadow: 0 2px 6px rgba(0,0,0,0.1); margin-bottom: 1.5rem; }}
        .success {{ border-left: 5px solid #27ae60; }}
        .error {{ border-left: 5px solid #c0392b; color: #c0392b; }}
        input[type=text] {{ padding: 0.5rem; margin: 0.25rem 0; width: 250px;
                             border: 1px solid #ccc; border-radius: 4px; }}
        button {{ padding: 0.5rem 1rem; background: #3498db; color: white; border: none;
                  border-radius: 4px; cursor: pointer; }}
        button:hover {{ background: #2980b9; }}
    </style>
    "#
    )
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
            parent_path: "/keyvalue".to_string(),
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

        assert_eq!(StatusCode::OK, status);
        assert_eq!("foo=foo-value, version=1", text.trim());
    }

    #[tokio::test]
    async fn test_not_found() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue/get?key=not-a-rea-key").await;
        let status = response.status();
        assert_eq!(StatusCode::NOT_FOUND, status);
    }

    #[tokio::test]
    async fn test_bad_path() {
        let server = make_server().await;
        let response = send_request(&server, "/INVALID/get?key=not-a-rea-key").await;
        let status = response.status();
        assert_eq!(StatusCode::NOT_FOUND, status);
    }

    #[tokio::test]
    async fn test_main_page_loads() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue").await;

        let status = response.status();
        let text = response.text().await.expect("text");

        assert_eq!(StatusCode::OK, status);
        assert!(text.contains("<h1>KeyValue Store</h1>"));
        assert!(text.contains("Latest version:"));
        assert!(text.contains("Get a key"));
        assert!(text.contains("Put a key"));
    }

    #[tokio::test]
    async fn test_main_page_get_existing_key() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue?key=foo").await;

        let status = response.status();
        let text = response.text().await.expect("text");

        assert_eq!(StatusCode::OK, status);
        assert!(text.contains(r#"Got <b>foo</b> = "foo-value""#));
        assert!(text.contains("Latest version:")); // still shows version info
    }

    #[tokio::test]
    async fn test_main_page_get_missing_key() {
        let server = make_server().await;
        let response = send_request(&server, "/keyvalue?key=does-not-exist").await;

        let status = response.status();
        let text = response.text().await.expect("text");

        assert_eq!(StatusCode::OK, status);
        assert!(text.contains(r#"Key "does-not-exist" not found"#));
    }

    #[tokio::test]
    async fn test_main_page_put_key() {
        let server = make_server().await;
        let port = server.port().unwrap();
        let client = reqwest::Client::new();

        // Send a form POST
        let response = client
            .post(&format!("http://127.0.0.1:{}/keyvalue", port))
            .form(&[("key", "bar"), ("value", "bar-value")])
            .send()
            .await
            .expect("request");

        let status = response.status();
        let text = response.text().await.expect("text");

        // The fake setup isn't pointing to a real raft member, so we expect a failure.
        assert_eq!(StatusCode::OK, status);
        assert!(text.contains("Failed to store"));
    }
}
