use async_stream::stream;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::Event;
use axum::{
    extract::{Path, Query},
    response::{Html, IntoResponse, Response, Sse},
    routing::{get, post},
    Json, Router,
};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::convert::Infallible;

use crate::core::auth::{verify_api_key, verify_app_key, verify_stream_api_key};
use crate::core::batch_tasks::{create_task, expire_task, get_task};
use crate::core::config::{get_all_config, update_config};
use crate::core::exceptions::ApiError;
use crate::core::static_assets;
use crate::core::storage::{get_storage, Storage};
use crate::services::grok::assets::{DeleteService, DownloadService, ListService};
use crate::services::grok::batch::{run_in_batches, OnItem, ShouldCancel};
use crate::services::grok::nsfw::NsfwService;
use crate::services::token::get_token_manager;

pub fn router() -> Router {
    Router::new()
        .route("/admin", get(admin_login_page))
        .route("/admin/config", get(admin_config_page))
        .route("/admin/token", get(admin_token_page))
        .route("/admin/cache", get(admin_cache_page))
        .route("/admin/downstream", get(admin_downstream_page))
        .route("/admin/storage", get(admin_storage_page))
        .route("/api/v1/admin/login", post(admin_login_api))
        .route("/api/v1/admin/config", get(get_config_api).post(update_config_api))
        .route("/api/v1/admin/storage", get(get_storage_api))
        .route("/api/v1/admin/storage/config", get(get_storage_config_api).post(update_storage_config_api))
        .route("/api/v1/admin/storage/test", post(test_storage_api))
        .route("/api/v1/admin/tokens", get(get_tokens_api).post(update_tokens_api))
        .route("/api/v1/admin/tokens/refresh", post(refresh_tokens_api))
        .route("/api/v1/admin/tokens/refresh/async", post(refresh_tokens_api_async))
        .route("/api/v1/admin/tokens/nsfw/enable", post(enable_nsfw_api))
        .route("/api/v1/admin/tokens/nsfw/enable/async", post(enable_nsfw_api_async))
        .route("/api/v1/admin/cache", get(get_cache_stats_api))
        .route("/api/v1/admin/cache/clear", post(clear_local_cache_api))
        .route("/api/v1/admin/cache/list", get(list_local_cache_api))
        .route("/api/v1/admin/cache/item/delete", post(delete_local_cache_item_api))
        .route("/api/v1/admin/cache/online/clear", post(clear_online_cache_api))
        .route("/api/v1/admin/cache/online/clear/async", post(clear_online_cache_api_async))
        .route("/api/v1/admin/cache/online/load/async", post(load_online_cache_api_async))
        .route("/api/v1/admin/batch/:task_id/stream", get(stream_batch))
        .route("/api/v1/admin/batch/:task_id/cancel", post(cancel_batch))
}

async fn render_template(path: &str) -> Response {
    if let Some(content) = static_assets::get_text(path) {
        return Html(content).into_response();
    }
    let file_path = crate::core::config::project_root().join("static").join(path);
    if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
        return Html(content).into_response();
    }
    (StatusCode::NOT_FOUND, format!("Template {path} not found.")).into_response()
}

async fn admin_login_page() -> Response { render_template("login/login.html").await }
async fn admin_config_page() -> Response { render_template("config/config.html").await }
async fn admin_token_page() -> Response { render_template("token/token.html").await }
async fn admin_cache_page() -> Response { render_template("cache/cache.html").await }
async fn admin_downstream_page() -> Response { render_template("downstream/downstream.html").await }
async fn admin_storage_page() -> Response { render_template("storage/storage.html").await }

async fn admin_login_api(headers: HeaderMap) -> Result<Response, ApiError> {
    verify_app_key(&headers).await?;
    let api_key: String = crate::core::config::get_config("app.api_key", String::new()).await;
    // 如果 api_key 为空，使用 app_key 作为替代，确保前端能拿到有效的 token
    let effective_key = if api_key.is_empty() {
        crate::core::config::get_config("app.app_key", String::new()).await
    } else {
        api_key
    };
    Ok(Json(json!({"status": "success", "api_key": effective_key})).into_response())
}

async fn get_config_api(headers: HeaderMap) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let cfg = get_all_config().await;
    Ok(Json(cfg).into_response())
}

async fn update_config_api(headers: HeaderMap, Json(data): Json<JsonValue>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    // 从通用配置保存中移除 storage section，避免覆盖存储管理页面的配置
    let mut filtered = data.clone();
    if let Some(obj) = filtered.as_object_mut() {
        obj.remove("storage");
    }
    update_config(&filtered).await.map_err(|e| ApiError::server(e.to_string()))?;
    Ok(Json(json!({"status": "success", "message": "配置已更新"})).into_response())
}

async fn get_storage_api(headers: HeaderMap) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let storage_type = std::env::var("SERVER_STORAGE_TYPE").unwrap_or_else(|_| "local".to_string());
    Ok(Json(json!({"type": storage_type.to_lowercase()})).into_response())
}

async fn get_tokens_api(headers: HeaderMap) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let storage = get_storage();
    let tokens = storage.load_tokens().await.unwrap_or(JsonValue::Object(Default::default()));
    Ok(Json(tokens).into_response())
}

async fn update_tokens_api(headers: HeaderMap, Json(data): Json<JsonValue>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let storage = get_storage();
    storage
        .with_lock("tokens_save", 10, || async { storage.save_tokens(&data).await })
        .await
        .map_err(|e| ApiError::server(e.to_string()))?;
    let mgr = get_token_manager().await;
    mgr.lock().await.reload().await;
    Ok(Json(json!({"status": "success", "message": "Token 已更新"})).into_response())
}

#[derive(Debug, Deserialize)]
struct TokenRefreshRequest {
    token: Option<String>,
    tokens: Option<Vec<String>>,
}

async fn refresh_tokens_api(headers: HeaderMap, Json(data): Json<TokenRefreshRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let mut mgr = mgr.lock().await;

    let mut tokens = Vec::new();
    if let Some(t) = data.token { tokens.push(t); }
    if let Some(list) = data.tokens { tokens.extend(list); }
    if tokens.is_empty() { return Err(ApiError::invalid_request("No tokens provided")); }
    tokens = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    tokens.dedup();

    let max_tokens: usize = crate::core::config::get_config("performance.usage_max_tokens", 1000usize).await;
    let original_count = tokens.len();
    let truncated = tokens.len() > max_tokens;
    if truncated { tokens.truncate(max_tokens); }

    let mut out = HashMap::new();
    for token in tokens.iter() {
        let ok = mgr
            .sync_usage(token, "grok-3", crate::services::token::models::EffortType::Low, false, false)
            .await;
        out.insert(token.clone(), ok);
    }

    let mut response = json!({"status": "success", "results": out});
    if truncated {
        response["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）"));
    }
    Ok(Json(response).into_response())
}

async fn refresh_tokens_api_async(headers: HeaderMap, Json(data): Json<TokenRefreshRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;

    let mut tokens = Vec::new();
    if let Some(t) = data.token { tokens.push(t); }
    if let Some(list) = data.tokens { tokens.extend(list); }
    if tokens.is_empty() { return Err(ApiError::invalid_request("No tokens provided")); }
    tokens = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    tokens.dedup();

    let max_tokens: usize = crate::core::config::get_config("performance.usage_max_tokens", 1000usize).await;
    let original_count = tokens.len();
    let truncated = tokens.len() > max_tokens;
    if truncated { tokens.truncate(max_tokens); }

    let max_concurrent: usize = crate::core::config::get_config("performance.usage_max_concurrent", 25usize).await;
    let batch_size: usize = crate::core::config::get_config("performance.usage_batch_size", 50usize).await;

    let task = create_task(tokens.len()).await;

    let task_id = task.lock().await.id.clone();
    let task_for_on_item = task.clone();

    let on_item: OnItem = std::sync::Arc::new(move |_token, ok| {
        let task = task_for_on_item.clone();
        Box::pin(async move {
            task.lock().await.record(ok, None, None, None);
        })
    });

    let tokens_for_spawn = tokens.clone();
    let task_for_spawn = task.clone();
    let task_id_for_spawn = task_id.clone();
    let truncated_for_spawn = truncated;
    let max_tokens_for_spawn = max_tokens;
    let original_count_for_spawn = original_count;
    tokio::spawn(async move {
        let results = run_in_batches(
            tokens_for_spawn.clone(),
            move |token| {
                let mgr = mgr.clone();
                async move {
                    let mut mgr = mgr.lock().await;
                    let ok = mgr.sync_usage(&token, "grok-3", crate::services::token::models::EffortType::Low, false, false).await;
                    Ok(ok)
                }
            },
            max_concurrent,
            batch_size,
            Some(on_item),
            None,
        ).await;

        if task_for_spawn.lock().await.cancelled {
            task_for_spawn.lock().await.finish_cancelled();
            return;
        }

        let mut results_out = HashMap::new();
        let mut ok_count = 0;
        let mut fail_count = 0;
        for (token, res) in results {
            match res {
                Ok(true) => { ok_count += 1; results_out.insert(token, true); }
                _ => { fail_count += 1; results_out.insert(token, false); }
            }
        }

        let mut result = json!({
            "status": "success",
            "summary": {"total": tokens_for_spawn.len(), "ok": ok_count, "fail": fail_count},
            "results": results_out,
        });
        if truncated_for_spawn {
            result["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）"));
        }
        task_for_spawn.lock().await.finish(result, if truncated_for_spawn { Some(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）")) } else { None });
        tokio::spawn(expire_task(task_id_for_spawn.clone(), 300));
    });

    Ok(Json(json!({"status": "success", "task_id": task_id, "total": tokens.len()})).into_response())
}

#[derive(Debug, Deserialize)]
struct NsfwRequest {
    token: Option<String>,
    tokens: Option<Vec<String>>,
}

async fn enable_nsfw_api(headers: HeaderMap, Json(data): Json<NsfwRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let service = NsfwService::new().await;

    let mut tokens = Vec::new();
    if let Some(t) = data.token { tokens.push(t); }
    if let Some(list) = data.tokens { tokens.extend(list); }
    if tokens.is_empty() {
        let mgr_guard = mgr.lock().await;
        for pool in mgr_guard.pools.values() {
            for info in pool.list() {
                tokens.push(info.token.clone());
            }
        }
    }
    if tokens.is_empty() { return Err(ApiError::invalid_request("No tokens available")); }
    tokens = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    tokens.dedup();

    let max_tokens: usize = crate::core::config::get_config("performance.nsfw_max_tokens", 1000usize).await;
    let original_count = tokens.len();
    let truncated = tokens.len() > max_tokens;
    if truncated { tokens.truncate(max_tokens); }

    let max_concurrent: usize = crate::core::config::get_config("performance.nsfw_max_concurrent", 10usize).await;
    let batch_size: usize = crate::core::config::get_config("performance.nsfw_batch_size", 50usize).await;

    let results = run_in_batches(
        tokens.clone(),
        move |token| {
            let service = service.clone();
            let mgr = mgr.clone();
            async move {
                let result = service.enable(&token).await;
                if result.success {
                    mgr.lock().await.add_tag(&token, "nsfw").await;
                }
                Ok(json!({
                    "success": result.success,
                    "http_status": result.http_status,
                    "grpc_status": result.grpc_status,
                    "grpc_message": result.grpc_message,
                    "error": result.error,
                }))
            }
        },
        max_concurrent,
        batch_size,
        None,
        None,
    ).await;

    let mut out = HashMap::new();
    let mut ok_count = 0;
    let mut fail_count = 0;
    for (token, res) in results {
        let masked = if token.len() > 20 { format!("{}...{}", &token[..8], &token[token.len()-8..]) } else { token.clone() };
        match res {
            Ok(data) => {
                if data.get("success").and_then(|v| v.as_bool()) == Some(true) {
                    ok_count += 1;
                } else {
                    fail_count += 1;
                }
                out.insert(masked, data);
            }
            Err(err) => {
                fail_count += 1;
                out.insert(masked, json!({"error": err}));
            }
        }
    }

    let mut response = json!({
        "status": "success",
        "summary": {"total": tokens.len(), "ok": ok_count, "fail": fail_count},
        "results": out,
    });
    if truncated {
        response["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）"));
    }
    Ok(Json(response).into_response())
}

async fn enable_nsfw_api_async(headers: HeaderMap, Json(data): Json<NsfwRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let service = NsfwService::new().await;

    let mut tokens = Vec::new();
    if let Some(t) = data.token { tokens.push(t); }
    if let Some(list) = data.tokens { tokens.extend(list); }
    if tokens.is_empty() {
        let mgr_guard = mgr.lock().await;
        for pool in mgr_guard.pools.values() {
            for info in pool.list() {
                tokens.push(info.token.clone());
            }
        }
    }
    if tokens.is_empty() { return Err(ApiError::invalid_request("No tokens available")); }
    tokens = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    tokens.dedup();

    let max_tokens: usize = crate::core::config::get_config("performance.nsfw_max_tokens", 1000usize).await;
    let original_count = tokens.len();
    let truncated = tokens.len() > max_tokens;
    if truncated { tokens.truncate(max_tokens); }

    let max_concurrent: usize = crate::core::config::get_config("performance.nsfw_max_concurrent", 10usize).await;
    let batch_size: usize = crate::core::config::get_config("performance.nsfw_batch_size", 50usize).await;

    let task = create_task(tokens.len()).await;
    let task_id = task.lock().await.id.clone();
    let task_for_on_item = task.clone();

    let on_item: OnItem = std::sync::Arc::new(move |_token, ok| {
        let task = task_for_on_item.clone();
        Box::pin(async move {
            task.lock().await.record(ok, None, None, None);
        })
    });

    let tokens_for_spawn = tokens.clone();
    let task_for_spawn = task.clone();
    let task_id_for_spawn = task_id.clone();
    tokio::spawn(async move {
        let results = run_in_batches(
            tokens_for_spawn.clone(),
            move |token| {
                let service = service.clone();
                let mgr = mgr.clone();
                async move {
                    let result = service.enable(&token).await;
                    if result.success {
                        mgr.lock().await.add_tag(&token, "nsfw").await;
                    }
                    Ok(json!({
                        "success": result.success,
                        "http_status": result.http_status,
                        "grpc_status": result.grpc_status,
                        "grpc_message": result.grpc_message,
                        "error": result.error,
                    }))
                }
            },
            max_concurrent,
            batch_size,
            Some(on_item),
            None,
        ).await;

        if task_for_spawn.lock().await.cancelled {
            task_for_spawn.lock().await.finish_cancelled();
            return;
        }

        let mut out = HashMap::new();
        let mut ok_count = 0;
        let mut fail_count = 0;
        for (token, res) in results {
            let masked = if token.len() > 20 { format!("{}...{}", &token[..8], &token[token.len()-8..]) } else { token.clone() };
            match res {
                Ok(data) => {
                    if data.get("success").and_then(|v| v.as_bool()) == Some(true) {
                        ok_count += 1;
                    } else { fail_count += 1; }
                    out.insert(masked, data);
                }
                Err(err) => {
                    fail_count += 1;
                    out.insert(masked, json!({"error": err}));
                }
            }
        }

        let mut result = json!({
            "status": "success",
            "summary": {"total": tokens_for_spawn.len(), "ok": ok_count, "fail": fail_count},
            "results": out,
        });
        if truncated {
            result["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）"));
        }
        task_for_spawn.lock().await.finish(result.clone(), if truncated { Some(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）")) } else { None });
        tokio::spawn(expire_task(task_id_for_spawn.clone(), 300));
    });

    Ok(Json(json!({"status": "success", "task_id": task_id, "total": tokens.len()})).into_response())
}

#[derive(Debug, Deserialize)]
struct CacheQuery {
    token: Option<String>,
    tokens: Option<String>,
    scope: Option<String>,
}

async fn get_cache_stats_api(headers: HeaderMap, Query(query): Query<CacheQuery>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let dl = DownloadService::new().await;
    let image_stats = dl.get_stats("image");
    let video_stats = dl.get_stats("video");

    // 获取存储类型信息
    let storage_type = if let Some(storage) = crate::core::media_storage::get_media_storage().await {
        storage.storage_type()
    } else {
        "not_initialized"
    };

    let mgr = get_token_manager().await;
    let mgr_guard = mgr.lock().await;
    let mut accounts = Vec::new();
    for (pool_name, pool) in &mgr_guard.pools {
        for info in pool.list() {
            let raw = info.token.clone();
            let masked = if raw.len() > 24 { format!("{}...{}", &raw[..8], &raw[raw.len()-16..]) } else { raw.clone() };
            accounts.push(json!({
                "token": raw,
                "token_masked": masked,
                "pool": pool_name,
                "status": format!("{:?}", info.status).to_lowercase(),
                "last_asset_clear_at": info.last_asset_clear_at,
            }));
        }
    }

    let mut selected_tokens: Vec<String> = Vec::new();
    if let Some(tokens) = &query.tokens {
        selected_tokens = tokens.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    }
    let scope = query.scope.clone();
    let selected_token = query.token.clone();

    let mut online_stats = json!({"count":0,"status":"unknown","token":null,"last_asset_clear_at":null});
    let mut online_details = Vec::new();

    let max_tokens: usize = crate::core::config::get_config("performance.assets_max_tokens", 1000usize).await;
    let mut truncated = false;
    let mut original_count = 0usize;

    if !selected_tokens.is_empty() {
        selected_tokens.dedup();
        original_count = selected_tokens.len();
        if selected_tokens.len() > max_tokens { selected_tokens.truncate(max_tokens); truncated = true; }
        let list_service = ListService::new().await;
        let mut total = 0usize;
        for token in &selected_tokens {
            match list_service.count(token).await {
                Ok(count) => {
                    total += count;
                    online_details.push(json!({"token": token, "token_masked": token, "count": count, "status": "ok"}));
                }
                Err(e) => {
                    online_details.push(json!({"token": token, "token_masked": token, "count": 0, "status": format!("error: {e}")}));
                }
            }
        }
        online_stats = json!({"count": total, "status": "ok", "token": null, "last_asset_clear_at": null});
    } else if scope.as_deref() == Some("all") {
        let tokens: Vec<String> = accounts.iter().filter_map(|a| a.get("token").and_then(|v| v.as_str()).map(|s| s.to_string())).collect();
        original_count = tokens.len();
        let mut tokens = tokens;
        if tokens.len() > max_tokens { tokens.truncate(max_tokens); truncated = true; }
        let list_service = ListService::new().await;
        let mut total = 0usize;
        for token in &tokens {
            match list_service.count(token).await {
                Ok(count) => {
                    total += count;
                    online_details.push(json!({"token": token, "token_masked": token, "count": count, "status": "ok"}));
                }
                Err(e) => {
                    online_details.push(json!({"token": token, "token_masked": token, "count": 0, "status": format!("error: {e}")}));
                }
            }
        }
        online_stats = json!({"count": total, "status": if tokens.is_empty() {"no_token"} else {"ok"}, "token": null, "last_asset_clear_at": null});
    } else if let Some(token) = selected_token {
        let list_service = ListService::new().await;
        match list_service.count(&token).await {
            Ok(count) => {
                online_stats = json!({"count": count, "status": "ok", "token": token});
            }
            Err(e) => {
                online_stats = json!({"count": 0, "status": format!("error: {e}"), "token": token});
            }
        }
    } else {
        online_stats = json!({"count": 0, "status": "not_loaded", "token": null});
    }

    let mut response = json!({
        "storage_type": storage_type,
        "local_cache_image": image_stats,
        "local_cache_video": video_stats,
        "online": online_stats,
        "online_accounts": accounts,
        "online_scope": scope.unwrap_or_else(|| "none".to_string()),
        "online_details": online_details,
    });
    if truncated {
        response["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）"));
    }
    Ok(Json(response).into_response())
}

#[derive(Debug, Deserialize)]
struct CacheClearRequest {
    #[serde(rename = "type")]
    cache_type: Option<String>,
}

async fn clear_local_cache_api(headers: HeaderMap, Json(data): Json<CacheClearRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let cache_type = data.cache_type.unwrap_or_else(|| "image".to_string());
    let dl = DownloadService::new().await;
    let result = dl.clear(&cache_type);
    Ok(Json(json!({"status": "success", "result": result})).into_response())
}

#[derive(Debug, Deserialize)]
struct CacheListQuery {
    #[serde(rename = "type")]
    cache_type: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

async fn list_local_cache_api(headers: HeaderMap, Query(query): Query<CacheListQuery>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let cache_type = query.cache_type.unwrap_or_else(|| "image".to_string());
    let page = query.page.unwrap_or(1);
    let page_size = query.page_size.unwrap_or(1000);
    let dl = DownloadService::new().await;
    let result = dl.list_files(&cache_type, page, page_size);
    Ok(Json(json!({"status": "success", "total": result["total"], "page": result["page"], "page_size": result["page_size"], "items": result["items"]})).into_response())
}

#[derive(Debug, Deserialize)]
struct CacheDeleteRequest {
    #[serde(rename = "type")]
    cache_type: Option<String>,
    name: Option<String>,
}

async fn delete_local_cache_item_api(headers: HeaderMap, Json(data): Json<CacheDeleteRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let cache_type = data.cache_type.unwrap_or_else(|| "image".to_string());
    let name = data.name.ok_or_else(|| ApiError::invalid_request("Missing file name"))?;
    let dl = DownloadService::new().await;
    let result = dl.delete_file(&cache_type, &name);
    Ok(Json(json!({"status": "success", "result": result})).into_response())
}

#[derive(Debug, Deserialize)]
struct OnlineClearRequest {
    token: Option<String>,
    tokens: Option<Vec<String>>,
}

async fn clear_online_cache_api(headers: HeaderMap, Json(data): Json<OnlineClearRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let mut mgr = mgr.lock().await;
    let service = DeleteService::new().await;

    if let Some(tokens) = data.tokens {
        let mut token_list = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect::<Vec<_>>();
        if token_list.is_empty() { return Err(ApiError::invalid_request("No tokens provided")); }
        token_list.dedup();
        let max_tokens: usize = crate::core::config::get_config("performance.assets_max_tokens", 1000usize).await;
        let original_count = token_list.len();
        let truncated = token_list.len() > max_tokens;
        if truncated { token_list.truncate(max_tokens); }

        let mut results = HashMap::new();
        for token in &token_list {
            match service.delete_all(token).await {
                Ok(result) => {
                    mgr.mark_asset_clear(token).await;
                    results.insert(token.clone(), json!({"status": "success", "result": result}));
                }
                Err(e) => {
                    results.insert(token.clone(), json!({"status": "error", "error": e.to_string()}));
                }
            }
        }
        let mut response = json!({"status": "success", "results": results});
        if truncated {
            response["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens} 个（共 {original_count} 个）"));
        }
        return Ok(Json(response).into_response());
    }

    let token = data.token.or_else(|| mgr.get_token("ssoBasic"));
    let token = token.ok_or_else(|| ApiError::invalid_request("No available token to perform cleanup"))?;
    let result = service.delete_all(&token).await.map_err(|e| ApiError::server(e.to_string()))?;
    mgr.mark_asset_clear(&token).await;
    Ok(Json(json!({"status": "success", "result": result})).into_response())
}

async fn clear_online_cache_api_async(headers: HeaderMap, Json(data): Json<OnlineClearRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let service = DeleteService::new().await;

    let tokens = data.tokens.ok_or_else(|| ApiError::invalid_request("No tokens provided"))?;
    let mut token_list = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect::<Vec<_>>();
    if token_list.is_empty() { return Err(ApiError::invalid_request("No tokens provided")); }
    token_list.dedup();
    let max_tokens: usize = crate::core::config::get_config("performance.assets_max_tokens", 1000usize).await;
    let original_count = token_list.len();
    let truncated = token_list.len() > max_tokens;
    if truncated { token_list.truncate(max_tokens); }

    let task = create_task(token_list.len()).await;
    let task_id = task.lock().await.id.clone();
    let task_for_on_item = task.clone();

    let on_item: OnItem = std::sync::Arc::new(move |_token, ok| {
        let task = task_for_on_item.clone();
        Box::pin(async move {
            task.lock().await.record(ok, None, None, None);
        })
    });

    let token_list_for_spawn = token_list.clone();
    let task_for_spawn = task.clone();
    let task_id_for_spawn = task_id.clone();
    let truncated_for_spawn = truncated;
    let max_tokens_for_spawn = max_tokens;
    let original_count_for_spawn = original_count;
    tokio::spawn(async move {
        let results = run_in_batches(
            token_list_for_spawn.clone(),
            move |token| {
                let service = service.clone();
                let mgr = mgr.clone();
                async move {
                    match service.delete_all(&token).await {
                        Ok(result) => {
                            mgr.lock().await.mark_asset_clear(&token).await;
                            Ok(json!({"status": "success", "result": result}))
                        }
                        Err(e) => Ok(json!({"status": "error", "error": e.to_string()})),
                    }
                }
            },
            crate::core::config::get_config("performance.assets_max_concurrent", 25usize).await,
            crate::core::config::get_config("performance.assets_batch_size", 10usize).await,
            Some(on_item),
            None,
        ).await;

        if task_for_spawn.lock().await.cancelled {
            task_for_spawn.lock().await.finish_cancelled();
            return;
        }

        let mut out = HashMap::new();
        let mut ok_count = 0;
        let mut fail_count = 0;
        for (token, res) in results {
            if let Ok(data) = res {
                if data.get("status").and_then(|v| v.as_str()) == Some("success") {
                    ok_count += 1;
                } else { fail_count += 1; }
                out.insert(token, data);
            } else { fail_count += 1; }
        }
        let mut result = json!({"status": "success", "summary": {"total": token_list_for_spawn.len(), "ok": ok_count, "fail": fail_count}, "results": out});
        if truncated_for_spawn {
            result["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）"));
        }
        task_for_spawn.lock().await.finish(result, if truncated_for_spawn { Some(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）")) } else { None });
        tokio::spawn(expire_task(task_id_for_spawn.clone(), 300));
    });

    Ok(Json(json!({"status": "success", "task_id": task_id, "total": token_list.len()})).into_response())
}

#[derive(Debug, Deserialize)]
struct LoadOnlineRequest {
    tokens: Option<Vec<String>>,
    scope: Option<String>,
}

async fn load_online_cache_api_async(headers: HeaderMap, Json(data): Json<LoadOnlineRequest>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let mgr = get_token_manager().await;
    let mgr_guard = mgr.lock().await;
    let mut accounts = Vec::new();
    for (pool_name, pool) in &mgr_guard.pools {
        for info in pool.list() {
            let raw = info.token.clone();
            let masked = if raw.len() > 24 { format!("{}...{}", &raw[..8], &raw[raw.len()-16..]) } else { raw.clone() };
            accounts.push(json!({"token": raw, "token_masked": masked, "pool": pool_name, "status": format!("{:?}", info.status).to_lowercase(), "last_asset_clear_at": info.last_asset_clear_at}));
        }
    }
    drop(mgr_guard);

    let mut tokens = Vec::new();
    let mut scope = data.scope.unwrap_or_else(|| "none".to_string());
    if let Some(list) = data.tokens { tokens = list; }
    if tokens.is_empty() && scope == "all" {
        tokens = accounts.iter().filter_map(|a| a.get("token").and_then(|v| v.as_str()).map(|s| s.to_string())).collect();
        scope = "all".to_string();
    } else if !tokens.is_empty() {
        scope = "selected".to_string();
    } else {
        return Err(ApiError::invalid_request("No tokens provided"));
    }
    tokens = tokens.into_iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    tokens.dedup();

    let max_tokens: usize = crate::core::config::get_config("performance.assets_max_tokens", 1000usize).await;
    let original_count = tokens.len();
    let truncated = tokens.len() > max_tokens;
    if truncated { tokens.truncate(max_tokens); }

    let task = create_task(tokens.len()).await;
    let task_id = task.lock().await.id.clone();
    let task_for_on_item = task.clone();

    let on_item: OnItem = std::sync::Arc::new(move |_token, ok| {
        let task = task_for_on_item.clone();
        Box::pin(async move {
            task.lock().await.record(ok, None, None, None);
        })
    });

    let tokens_for_spawn = tokens.clone();
    let accounts_for_spawn = accounts.clone();
    let scope_for_spawn = scope.clone();
    let task_for_spawn = task.clone();
    let task_id_for_spawn = task_id.clone();
    let truncated_for_spawn = truncated;
    let max_tokens_for_spawn = max_tokens;
    let original_count_for_spawn = original_count;
    tokio::spawn(async move {
        let list_service = ListService::new().await;
        let results = run_in_batches(
            tokens_for_spawn.clone(),
            move |token| {
                let list_service = list_service.clone();
                async move {
                    match list_service.count(&token).await {
                        Ok(count) => Ok(json!({"token": token, "count": count, "status": "ok"})),
                        Err(e) => Ok(json!({"token": token, "count": 0, "status": format!("error: {e}")})),
                    }
                }
            },
            crate::core::config::get_config("performance.assets_max_concurrent", 25usize).await,
            crate::core::config::get_config("performance.assets_batch_size", 10usize).await,
            Some(on_item),
            None,
        ).await;

        let mut online_details = Vec::new();
        let mut total = 0usize;
        for (_token, res) in results {
            if let Ok(detail) = res {
                if let Some(count) = detail.get("count").and_then(|v| v.as_u64()) {
                    total += count as usize;
                }
                online_details.push(detail);
            }
        }
        let dl = DownloadService::new().await;
        let image_stats = dl.get_stats("image");
        let video_stats = dl.get_stats("video");

        // 获取存储类型信息
        let storage_type = if let Some(storage) = crate::core::media_storage::get_media_storage().await {
            storage.storage_type()
        } else {
            "not_initialized"
        };

        let mut result = json!({
            "storage_type": storage_type,
            "local_cache_image": image_stats,
            "local_cache_video": video_stats,
            "online": {"count": total, "status": if tokens_for_spawn.is_empty() {"no_token"} else {"ok"}, "token": null, "last_asset_clear_at": null},
            "online_accounts": accounts_for_spawn,
            "online_scope": scope_for_spawn,
            "online_details": online_details,
        });
        if truncated_for_spawn {
            result["warning"] = JsonValue::String(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）"));
        }
        task_for_spawn.lock().await.finish(result.clone(), if truncated_for_spawn { Some(format!("数量超出限制，仅处理前 {max_tokens_for_spawn} 个（共 {original_count_for_spawn} 个）")) } else { None });
        tokio::spawn(expire_task(task_id_for_spawn.clone(), 300));
    });

    Ok(Json(json!({"status": "success", "task_id": task_id, "total": tokens.len()})).into_response())
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    api_key: Option<String>,
}

async fn stream_batch(Path(task_id): Path<String>, Query(query): Query<StreamQuery>) -> Result<Response, ApiError> {
    verify_stream_api_key(query.api_key).await?;
    let task = get_task(&task_id).await.ok_or_else(|| ApiError::not_found("Task not found"))?;
    let mut rx = task.lock().await.attach();

    let stream = stream! {
        let snapshot = task.lock().await.snapshot();
        yield Ok::<Event, Infallible>(Event::default().data(snapshot.to_string()));

        if let Some(final_event) = task.lock().await.final_event() {
            yield Ok::<Event, Infallible>(Event::default().data(final_event.to_string()));
            return;
        }

        loop {
            let evt = tokio::time::timeout(std::time::Duration::from_secs(15), rx.recv()).await;
            match evt {
                Ok(Some(event)) => {
                    let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    yield Ok::<Event, Infallible>(Event::default().data(event.to_string()));
                    if matches!(event_type, "done" | "error" | "cancelled") {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    yield Ok::<Event, Infallible>(Event::default().comment("ping"));
                    if let Some(final_event) = task.lock().await.final_event() {
                        yield Ok::<Event, Infallible>(Event::default().data(final_event.to_string()));
                        break;
                    }
                }
            }
        }
    };

    Ok(Sse::new(stream).into_response())
}

async fn cancel_batch(headers: HeaderMap, Path(task_id): Path<String>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let task = get_task(&task_id).await.ok_or_else(|| ApiError::not_found("Task not found"))?;
    task.lock().await.cancel();
    Ok(Json(json!({"status": "success"})).into_response())
}

// ========== 存储管理 API ==========

/// 获取存储配置
async fn get_storage_config_api(headers: HeaderMap) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    // 从配置中获取存储配置
    let storage_type: String = crate::core::config::get_config("storage.type", "local".to_string()).await;

    let config = match storage_type.as_str() {
        "s3" => {
            let endpoint: String = crate::core::config::get_config("storage.s3.endpoint", String::new()).await;
            let region: String = crate::core::config::get_config("storage.s3.region", "us-east-1".to_string()).await;
            let access_key: String = crate::core::config::get_config("storage.s3.access_key", String::new()).await;
            let secret_key: String = crate::core::config::get_config("storage.s3.secret_key", String::new()).await;
            let bucket: String = crate::core::config::get_config("storage.s3.bucket", "grok-media".to_string()).await;
            let custom_domain: Option<String> = crate::core::config::get_config("storage.s3.custom_domain", None).await;
            let path_prefix: String = crate::core::config::get_config("storage.s3.path_prefix", "grok/".to_string()).await;
            let upload_timeout: u64 = crate::core::config::get_config("storage.s3.upload_timeout", 300).await;
            let use_direct_url: bool = crate::core::config::get_config("storage.s3.use_direct_url", false).await;

            json!({
                "type": "s3",
                "endpoint": endpoint,
                "region": region,
                "access_key": access_key,
                "secret_key": "***", // 隐藏敏感信息
                "bucket": bucket,
                "custom_domain": custom_domain,
                "path_prefix": path_prefix,
                "upload_timeout": upload_timeout,
                "use_direct_url": use_direct_url
            })
        }
        _ => {
            let base_dir: String = crate::core::config::get_config("storage.local.base_dir", "data/tmp".to_string()).await;
            json!({
                "type": "local",
                "base_dir": base_dir
            })
        }
    };

    Ok(Json(config).into_response())
}

/// 更新存储配置
async fn update_storage_config_api(headers: HeaderMap, Json(data): Json<JsonValue>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    tracing::info!("update_storage_config_api called with data: {}", data);

    let storage_type = data.get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::invalid_request("Missing storage type"))?;

    // 构建要更新的配置
    let mut updates = json!({
        "storage": {
            "type": storage_type
        }
    });

    match storage_type {
        "s3" => {
            // 获取当前的 secret_key，如果提交的是 "***" 或空字符串则保留原值
            let secret_key = data.get("secret_key").and_then(|v| v.as_str()).unwrap_or("");
            let final_secret_key = if secret_key == "***" || secret_key.is_empty() {
                // 保留原有的 secret_key
                crate::core::config::get_config("storage.s3.secret_key", String::new()).await
            } else {
                secret_key.to_string()
            };

            let mut s3_config = serde_json::Map::new();
            s3_config.insert("endpoint".to_string(), json!(data.get("endpoint").and_then(|v| v.as_str()).unwrap_or("")));
            s3_config.insert("region".to_string(), json!(data.get("region").and_then(|v| v.as_str()).unwrap_or("us-east-1")));
            s3_config.insert("access_key".to_string(), json!(data.get("access_key").and_then(|v| v.as_str()).unwrap_or("")));
            s3_config.insert("secret_key".to_string(), json!(final_secret_key));
            s3_config.insert("bucket".to_string(), json!(data.get("bucket").and_then(|v| v.as_str()).unwrap_or("grok-media")));
            s3_config.insert("path_prefix".to_string(), json!(data.get("path_prefix").and_then(|v| v.as_str()).unwrap_or("grok/")));
            s3_config.insert("upload_timeout".to_string(), json!(data.get("upload_timeout").and_then(|v| v.as_u64()).unwrap_or(300)));
            s3_config.insert("use_direct_url".to_string(), json!(data.get("use_direct_url").and_then(|v| v.as_bool()).unwrap_or(false)));

            // 处理 custom_domain - 如果是空字符串或 null，则不设置该字段
            if let Some(domain) = data.get("custom_domain").and_then(|v| v.as_str()) {
                if !domain.is_empty() {
                    s3_config.insert("custom_domain".to_string(), json!(domain));
                }
            }

            updates["storage"]["s3"] = JsonValue::Object(s3_config);
        }
        "local" => {
            let local_config = json!({
                "base_dir": data.get("base_dir").and_then(|v| v.as_str()).unwrap_or("data/tmp")
            });
            updates["storage"]["local"] = local_config;
        }
        _ => {
            return Err(ApiError::invalid_request("Invalid storage type"));
        }
    }

    // 更新配置
    update_config(&updates).await.map_err(|e| ApiError::server(e.to_string()))?;

    // 重新加载媒体存储（热更新）
    match crate::core::media_storage::reload_media_storage().await {
        Ok(_) => {
            tracing::info!("Media storage reloaded successfully");
            Ok(Json(json!({
                "status": "success",
                "message": "存储配置已更新并立即生效"
            })).into_response())
        }
        Err(e) => {
            tracing::error!("Failed to reload media storage: {:?}", e);
            // 即使重载失败，配置也已保存，下次启动会生效
            Ok(Json(json!({
                "status": "warning",
                "message": format!("存储配置已保存，但重载失败: {:?}。将在下次启动时生效", e)
            })).into_response())
        }
    }
}

/// 测试存储连接
async fn test_storage_api(headers: HeaderMap, Json(data): Json<JsonValue>) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let storage_type = data.get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::invalid_request("Missing storage type"))?;

    use crate::core::media_storage::{create_storage, StorageConfig};

    let config = match storage_type {
        "s3" => {
            StorageConfig::S3 {
                endpoint: data.get("endpoint").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                region: data.get("region").and_then(|v| v.as_str()).unwrap_or("us-east-1").to_string(),
                access_key: data.get("access_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                secret_key: data.get("secret_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                bucket: data.get("bucket").and_then(|v| v.as_str()).unwrap_or("grok-media").to_string(),
                custom_domain: data.get("custom_domain").and_then(|v| v.as_str()).map(|s| s.to_string()),
                path_prefix: data.get("path_prefix").and_then(|v| v.as_str()).unwrap_or("grok/").to_string(),
                upload_timeout: data.get("upload_timeout").and_then(|v| v.as_u64()).unwrap_or(300),
                use_direct_url: data.get("use_direct_url").and_then(|v| v.as_bool()).unwrap_or(false),
            }
        }
        "local" => {
            StorageConfig::Local {
                base_dir: data.get("base_dir").and_then(|v| v.as_str()).unwrap_or("data/tmp").to_string(),
            }
        }
        _ => {
            return Err(ApiError::invalid_request("Invalid storage type"));
        }
    };

    // 创建存储实例并测试
    let storage = create_storage(config).await.map_err(|e| ApiError::from(e))?;

    storage.health_check().await.map_err(|e| ApiError::from(e))?;

    Ok(Json(json!({
        "status": "success",
        "message": "存储连接测试成功"
    })).into_response())
}

