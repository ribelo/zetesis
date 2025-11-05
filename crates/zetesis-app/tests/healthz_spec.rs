use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use zetesis_app::server::build_api_router;

#[tokio::test]
async fn healthz_returns_ok_json() {
    debug_assert!(StatusCode::OK.is_success());
    debug_assert_eq!(StatusCode::OK.as_u16(), 200);

    let app = build_api_router();
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/healthz")
                .body(Body::empty())
                .expect("request builder should not fail"),
        )
        .await
        .expect("healthz handler should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .expect("content-type header present")
        .to_str()
        .expect("content-type must be valid utf-8");
    assert!(
        content_type.starts_with("application/json"),
        "content-type must indicate JSON: {content_type}"
    );

    let body_bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body must be readable")
        .to_bytes();
    let value: Value =
        serde_json::from_slice(body_bytes.as_ref()).expect("healthz response must be valid JSON");
    assert_eq!(value, json!({ "status": "ok" }));
}
