use std::sync::{Mutex, OnceLock};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;
use zetesis_app::server::build_api_router;
use zetesis_app::services::set_data_dir_override;

fn test_mutex() -> &'static Mutex<()> {
    static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn keyword_invalid_limit_returns_400() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_data_dir_override(None);

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/keyword?index=kio&q=test&limit=0&offset=0&fields=id")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .expect("body must be collected")
        .to_bytes();
    let body_text = std::str::from_utf8(body_bytes.as_ref()).unwrap_or("<non-utf8>");
    let value: Value = serde_json::from_slice(body_bytes.as_ref())
        .unwrap_or_else(|err| panic!("invalid json: {err}; body={body_text}"));
    assert_eq!(value["error"], json!("invalid_parameter"));
    assert_eq!(value["field"], json!("limit"));
}

#[tokio::test]
async fn keyword_missing_index_returns_404() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temp_dir = TempDir::new().expect("tempdir must be created");
    set_data_dir_override(Some(temp_dir.path().to_path_buf()));

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/keyword?index=kio&q=test&limit=10&offset=0&fields=id")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    set_data_dir_override(None);
}

#[tokio::test]
async fn keyword_empty_fields_is_permitted() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temp_dir = TempDir::new().expect("tempdir must be created");
    set_data_dir_override(Some(temp_dir.path().to_path_buf()));

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/keyword?index=kio&q=test&limit=10&offset=0")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    set_data_dir_override(None);
}

#[tokio::test]
async fn vector_missing_embedder_defaults() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temp_dir = TempDir::new().expect("tempdir must be created");
    set_data_dir_override(Some(temp_dir.path().to_path_buf()));

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/vector?index=kio&q=test&k=5&fields=id")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    set_data_dir_override(None);
}

#[tokio::test]
async fn hybrid_limit_zero_returns_400() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_data_dir_override(None);

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/hybrid?index=kio&q=test&limit=0")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    set_data_dir_override(None);
}

#[tokio::test]
async fn hybrid_zero_weights_returns_400() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_data_dir_override(None);

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/hybrid?index=kio&q=test&limit=10&fusion=weighted&keyword_weight=0&vector_weight=0")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    set_data_dir_override(None);
}

#[tokio::test]
async fn typeahead_short_query_returns_400() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_data_dir_override(None);

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/typeahead?index=kio&q=a")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .expect("body must be collected")
        .to_bytes();
    let body_text = std::str::from_utf8(body_bytes.as_ref()).unwrap_or("<non-utf8>");
    let value: Value = serde_json::from_slice(body_bytes.as_ref())
        .unwrap_or_else(|err| panic!("invalid json: {err}; body={body_text}"));
    assert_eq!(value["error"], json!("invalid_parameter"));
    assert_eq!(value["field"], json!("q"));
    set_data_dir_override(None);
}

#[tokio::test]
async fn typeahead_limit_over_10_returns_400() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_data_dir_override(None);

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/typeahead?index=kio&q=abcd&limit=11")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .expect("body must be collected")
        .to_bytes();
    let body_text = std::str::from_utf8(body_bytes.as_ref()).unwrap_or("<non-utf8>");
    let value: Value = serde_json::from_slice(body_bytes.as_ref())
        .unwrap_or_else(|err| panic!("invalid json: {err}; body={body_text}"));
    assert_eq!(value["error"], json!("invalid_parameter"));
    assert_eq!(value["field"], json!("limit"));
    set_data_dir_override(None);
}

#[tokio::test]
async fn typeahead_missing_index_returns_404() {
    let _lock = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temp_dir = TempDir::new().expect("tempdir must be created");
    set_data_dir_override(Some(temp_dir.path().to_path_buf()));

    let app = build_api_router();
    let request = Request::builder()
        .method("GET")
        .uri("/v1/search/typeahead?index=kio&q=abcd&limit=3")
        .body(Body::empty())
        .expect("request builder must not fail");

    let response = app.oneshot(request).await.expect("handler should respond");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    set_data_dir_override(None);
}
