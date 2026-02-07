use axum::{Router, routing::get};
use tower_http::cors::CorsLayer;

use crate::api;
use crate::core::response_middleware;
use crate::core::static_assets::static_handler;

pub fn create_app() -> Router {
    let api_router = api::v1::router();

    Router::new()
        .merge(api_router)
        .route("/static/*path", get(static_handler))
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn(
            response_middleware::log_middleware,
        ))
}
