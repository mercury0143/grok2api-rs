use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Multipart, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use tokio::sync::{Mutex, RwLock};

use crate::core::auth::verify_api_key;
use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::media::{VideoResult, VideoService};
use crate::services::grok::model::{Cost, ModelService};
use crate::services::grok::processor::BaseProcessor;
use crate::services::token::{EffortType, TokenService};

// --- Video Task storage ---

#[derive(Debug, Clone, Serialize)]
struct VideoTaskError {
    message: String,
    code: String,
}

#[derive(Debug, Clone, Serialize)]
struct VideoTask {
    id: String,
    object: String,
    model: String,
    status: String,
    progress: i64,
    created_at: i64,
    completed_at: i64,
    expires_at: i64,
    seconds: String,
    size: String,
    video_url: String,
    remixed_from_video_id: String,
    error: Option<VideoTaskError>,
}

static VIDEO_TASKS: Lazy<RwLock<HashMap<String, Arc<Mutex<VideoTask>>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn router() -> Router {
    Router::new()
        .route("/v1/videos", post(create_video))
        .route("/v1/videos/:task_id", get(get_video_task))
}

async fn create_video(headers: HeaderMap, mut multipart: Multipart) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let enabled: bool = get_config("downstream.enable_images", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }

    let mut model = String::new();
    let mut prompt = String::new();
    let mut aspect_ratio = String::from("3:2");
    let mut seconds = 6i32;
    let mut size = String::from("SD");
    let mut _input_reference: Option<Vec<u8>> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        ApiError::invalid_request(format!("Failed to parse multipart: {}", e))
    })? {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "model" => {
                model = field.text().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read model: {}", e))
                })?;
            }
            "prompt" => {
                prompt = field.text().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read prompt: {}", e))
                })?;
            }
            "aspect_ratio" => {
                aspect_ratio = field.text().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read aspect_ratio: {}", e))
                })?;
            }
            "seconds" => {
                let seconds_str = field.text().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read seconds: {}", e))
                })?;
                seconds = seconds_str.parse().unwrap_or(6);
            }
            "size" => {
                size = field.text().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read size: {}", e))
                })?;
            }
            "input_reference" => {
                let data = field.bytes().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read input_reference: {}", e))
                })?;
                if !data.is_empty() {
                    _input_reference = Some(data.to_vec());
                }
            }
            _ => {}
        }
    }

    if model.is_empty() {
        return Err(ApiError::invalid_request("model is required"));
    }
    if prompt.is_empty() {
        return Err(ApiError::invalid_request("prompt is required"));
    }

    let final_seconds = match model.as_str() {
        "grok-video-3" => 3,
        "grok-video-3-pro" => 6,
        _ => seconds,
    };

    let model_info = ModelService::get(&model)
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "The model `{}` does not exist or you do not have access to it.",
                model
            ))
            .with_param("model")
            .with_code("model_not_found")
        })?;

    if !model_info.is_video {
        return Err(ApiError::invalid_request(format!(
            "Model `{}` is not a video model",
            model
        )));
    }

    let messages = vec![json!({"role": "user", "content": prompt})];

    let result = VideoService::completions(
        &model,
        messages,
        Some(false),
        None,
        &aspect_ratio,
        final_seconds,
        &size,
        "custom",
    )
    .await?;

    let line_stream = match result {
        VideoResult::Stream {
            stream: s,
            token,
            model: m,
            ..
        } => (s, token, m),
        VideoResult::Json(j) => {
            return Ok((StatusCode::OK, Json(j)).into_response());
        }
    };
    let (stream, token, resolved_model) = line_stream;

    let now = chrono::Utc::now().timestamp();
    let task_id = format!("video-{}", uuid::Uuid::new_v4().simple());
    let task = VideoTask {
        id: task_id.clone(),
        object: "video".to_string(),
        model: resolved_model.clone(),
        status: "queued".to_string(),
        progress: 0,
        created_at: now,
        completed_at: 0,
        expires_at: now + 3600,
        seconds: final_seconds.to_string(),
        size: size.clone(),
        video_url: String::new(),
        remixed_from_video_id: String::new(),
        error: None,
    };

    let snapshot = serde_json::to_value(&task).unwrap_or(json!({}));
    let task_arc = Arc::new(Mutex::new(task));
    VIDEO_TASKS
        .write()
        .await
        .insert(task_id.clone(), task_arc.clone());

    let effort = if model_info.cost == Cost::High {
        EffortType::High
    } else {
        EffortType::Low
    };

    // Spawn background processing
    tokio::spawn(process_video_stream(
        task_arc,
        stream,
        token,
        resolved_model,
        effort,
        task_id.clone(),
    ));

    Ok((StatusCode::OK, Json(snapshot)).into_response())
}

async fn process_video_stream(
    task_arc: Arc<Mutex<VideoTask>>,
    stream: crate::services::grok::media::LineStream,
    token: String,
    model: String,
    effort: EffortType,
    task_id: String,
) {
    {
        let mut t = task_arc.lock().await;
        t.status = "processing".to_string();
    }

    let base = BaseProcessor::new(&model, &token).await;
    let mut pinned = Box::pin(stream);
    let mut final_video_url = String::new();

    while let Some(line) = pinned.next().await {
        if line.trim().is_empty() {
            continue;
        }
        let data: JsonValue = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let resp = data
            .get("result")
            .and_then(|v| v.get("response"))
            .cloned()
            .unwrap_or(JsonValue::Null);

        if let Some(video_resp) = resp.get("streamingVideoGenerationResponse") {
            let progress = video_resp
                .get("progress")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            {
                let mut t = task_arc.lock().await;
                t.progress = progress;
            }
            if progress == 100 {
                let video_url = video_resp
                    .get("videoUrl")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !video_url.is_empty() {
                    final_video_url = base.process_url(video_url, "video").await;
                }
            }
        }
    }

    let _ = TokenService::consume(&token, effort).await;

    {
        let mut t = task_arc.lock().await;
        if !final_video_url.is_empty() {
            t.status = "completed".to_string();
            t.progress = 100;
            t.video_url = final_video_url;
            t.completed_at = chrono::Utc::now().timestamp();
        } else {
            t.status = "failed".to_string();
            t.error = Some(VideoTaskError {
                message: "Video generation failed or returned no URL".to_string(),
                code: "generation_failed".to_string(),
            });
        }
    }

    // Schedule cleanup after expiry
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        VIDEO_TASKS.write().await.remove(&task_id);
    });
}

async fn get_video_task(
    headers: HeaderMap,
    Path(task_id): Path<String>,
) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let tasks = VIDEO_TASKS.read().await;
    let task_arc = tasks
        .get(&task_id)
        .ok_or_else(|| ApiError::not_found(format!("Video task '{}' not found", task_id)))?
        .clone();
    drop(tasks);

    let t = task_arc.lock().await;
    let snapshot = serde_json::to_value(&*t).unwrap_or(json!({}));
    Ok((StatusCode::OK, Json(snapshot)).into_response())
}

