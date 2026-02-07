use axum::Json;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use serde_json::json;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::model::ModelService;

pub fn router() -> Router {
    Router::new().route("/v1/models", get(list_models))
}

async fn list_models() -> Result<Response, ApiError> {
    let enabled: bool = get_config("downstream.enable_models", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }
    let data = ModelService::list()
        .into_iter()
        .map(|m| json!({"id": m.model_id, "object": "model", "created": 0, "owned_by": "grok2api"}))
        .collect::<Vec<_>>();
    Ok(Json(json!({"object": "list", "data": data})).into_response())
}
