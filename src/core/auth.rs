use axum::http::HeaderMap;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    if let Some(rest) = auth.strip_prefix("Bearer ") {
        return Some(rest.trim().to_string());
    }
    None
}

pub async fn verify_api_key(headers: &HeaderMap) -> Result<(), ApiError> {
    let api_key: String = get_config("app.api_key", String::new()).await;
    if api_key.is_empty() {
        return Ok(());
    }
    let auth = extract_bearer(headers);
    match auth {
        Some(token) if token == api_key => Ok(()),
        Some(_) => Err(ApiError::authentication("Invalid authentication token")),
        None => Err(ApiError::authentication("Missing authentication token")),
    }
}

pub async fn verify_app_key(headers: &HeaderMap) -> Result<(), ApiError> {
    let app_key: String = get_config("app.app_key", String::new()).await;
    if app_key.is_empty() {
        return Err(ApiError::authentication("App key is not configured"));
    }
    let auth = extract_bearer(headers);
    match auth {
        Some(token) if token == app_key => Ok(()),
        Some(_) => Err(ApiError::authentication("Invalid authentication token")),
        None => Err(ApiError::authentication("Missing authentication token")),
    }
}

pub async fn verify_stream_api_key(query_key: Option<String>) -> Result<(), ApiError> {
    let api_key: String = get_config("app.api_key", String::new()).await;
    if api_key.is_empty() {
        return Ok(());
    }
    if let Some(q) = query_key {
        let raw = q
            .strip_prefix("Bearer ")
            .map(|v| v.trim())
            .unwrap_or(q.trim());
        if raw == api_key {
            return Ok(());
        }
    }
    Err(ApiError::authentication("Invalid authentication token"))
}
