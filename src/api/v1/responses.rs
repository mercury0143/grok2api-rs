use async_stream::stream;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::convert::Infallible;

use crate::core::auth::verify_api_key;
use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::chat::{ChatResult, ChatService};
use crate::services::grok::media::{VideoResult, VideoService};
use crate::services::grok::model::{Cost, ModelService};
use crate::services::grok::processor::{
    CollectProcessor, StreamProcessor, VideoCollectProcessor, VideoStreamProcessor,
};
use crate::services::token::{EffortType, TokenService};

#[derive(Debug, Deserialize)]
pub struct VideoConfig {
    pub aspect_ratio: Option<String>,
    pub video_length: Option<i32>,
    pub resolution: Option<String>,
    pub preset: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub input: Option<JsonValue>,
    pub messages: Option<Vec<JsonValue>>,
    pub stream: Option<bool>,
    pub thinking: Option<String>,
    pub video_config: Option<VideoConfig>,
}

pub fn router() -> Router {
    Router::new().route("/v1/responses", post(responses))
}

fn sse_ok(data: String) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(data))
}

fn normalize_message(msg: &JsonValue) -> JsonValue {
    let mut out = msg.clone();
    if let Some(arr) = msg.get("content").and_then(|v| v.as_array()) {
        let mut new_arr = Vec::with_capacity(arr.len());
        for block in arr {
            if let Some(typ) = block.get("type").and_then(|v| v.as_str()) {
                if typ == "input_text" {
                    let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                    new_arr.push(json!({"type": "text", "text": text}));
                    continue;
                }
                if typ == "input_image" {
                    let url = block
                        .get("image_url")
                        .and_then(|v| v.get("url"))
                        .and_then(|v| v.as_str())
                        .or_else(|| block.get("url").and_then(|v| v.as_str()))
                        .unwrap_or("");
                    if !url.is_empty() {
                        new_arr.push(json!({"type": "image_url", "image_url": {"url": url}}));
                        continue;
                    }
                }
            }
            new_arr.push(block.clone());
        }
        out["content"] = JsonValue::Array(new_arr);
    }
    out
}

fn build_messages(req: &ResponsesRequest) -> Result<Vec<JsonValue>, ApiError> {
    if let Some(list) = &req.messages {
        if !list.is_empty() {
            return Ok(list.iter().map(normalize_message).collect());
        }
    }
    if let Some(input) = &req.input {
        if let Some(text) = input.as_str() {
            if text.trim().is_empty() {
                return Err(ApiError::invalid_request("input cannot be empty"));
            }
            return Ok(vec![json!({"role": "user", "content": text})]);
        }
        if let Some(arr) = input.as_array() {
            if arr.is_empty() {
                return Err(ApiError::invalid_request("input cannot be empty"));
            }
            if arr.iter().all(|v| v.is_string()) {
                let msgs = arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| json!({"role": "user", "content": s}))
                    .collect::<Vec<_>>();
                if msgs.is_empty() {
                    return Err(ApiError::invalid_request("input cannot be empty"));
                }
                return Ok(msgs);
            }
            if arr.iter().all(|v| v.get("role").is_some()) {
                return Ok(arr.iter().map(normalize_message).collect());
            }
        }
        if input.get("role").is_some() {
            return Ok(vec![normalize_message(input)]);
        }
    }
    Err(ApiError::invalid_request("input is required"))
}

fn response_from_text(
    model: &str,
    created: i64,
    text: &str,
    usage: Option<&JsonValue>,
) -> JsonValue {
    let input_tokens = usage
        .and_then(|u| u.get("prompt_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = usage
        .and_then(|u| u.get("completion_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let total_tokens = usage
        .and_then(|u| u.get("total_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(input_tokens + output_tokens);

    let response_id = format!("resp-{}", uuid::Uuid::new_v4().simple());
    let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
    json!({
        "id": response_id,
        "object": "response",
        "created": created,
        "created_at": created,
        "status": "completed",
        "model": model,
        "output": [{
            "id": msg_id,
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}]
        }],
        "output_text": text,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens
        }
    })
}

async fn responses(
    headers: HeaderMap,
    Json(req): Json<ResponsesRequest>,
) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let enabled: bool = get_config("downstream.enable_responses", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }
    let model_info = ModelService::get(&req.model).ok_or_else(|| {
        ApiError::not_found(format!(
            "The model `{}` does not exist or you do not have access to it.",
            req.model
        ))
        .with_param("model")
        .with_code("model_not_found")
    })?;
    let messages = build_messages(&req)?;

    let stream = match req.stream {
        Some(value) => value,
        None => get_config("grok.stream", true).await,
    };
    if model_info.is_video {
        let vconf = req.video_config.unwrap_or(VideoConfig {
            aspect_ratio: Some("3:2".to_string()),
            video_length: Some(6),
            resolution: Some("SD".to_string()),
            preset: Some("custom".to_string()),
        });
        let result = VideoService::completions(
            &req.model,
            messages,
            Some(stream),
            req.thinking.clone(),
            vconf.aspect_ratio.as_deref().unwrap_or("3:2"),
            vconf.video_length.unwrap_or(6),
            vconf.resolution.as_deref().unwrap_or("SD"),
            vconf.preset.as_deref().unwrap_or("custom"),
        )
        .await?;

        match result {
            VideoResult::Stream {
                stream: line_stream,
                token,
                model,
                think,
                is_stream,
            } => {
                if is_stream {
                    let processor = VideoStreamProcessor::new(&model, &token, think).await;
                    let effort = if model_info.cost == Cost::High {
                        EffortType::High
                    } else {
                        EffortType::Low
                    };
                    let token_clone = token.clone();
                    let response_id = format!("resp-{}", uuid::Uuid::new_v4().simple());
                    let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
                    let created = chrono::Utc::now().timestamp();
                    let body_stream = stream! {
                        let created_event = json!({
                            "type": "response.created",
                            "response": {
                                "id": response_id,
                                "object": "response",
                                "created": created,
                                "created_at": created,
                                "status": "in_progress",
                                "model": model
                            }
                        });
                        yield sse_ok(format!("data: {}\n\n", created_event));

                        let mut full_text = String::new();
                        let mut inner = Box::pin(processor.process(line_stream));
                        while let Some(item) = inner.as_mut().next().await {
                            let item = match item {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let text = String::from_utf8_lossy(&item);
                            for line in text.split('\n') {
                                let line = line.trim();
                                if !line.starts_with("data: ") {
                                    continue;
                                }
                                let payload = line.trim_start_matches("data: ").trim();
                                if payload == "[DONE]" {
                                    continue;
                                }
                                if let Ok(val) = serde_json::from_str::<JsonValue>(payload) {
                                    if let Some(delta) = val.get("choices")
                                        .and_then(|v| v.get(0))
                                        .and_then(|v| v.get("delta"))
                                        .and_then(|v| v.get("content"))
                                        .and_then(|v| v.as_str()) {
                                        full_text.push_str(delta);
                                        let evt = json!({
                                            "type": "response.output_text.delta",
                                            "response_id": response_id,
                                            "output_index": 0,
                                            "content_index": 0,
                                            "delta": delta
                                        });
                                        yield sse_ok(format!("data: {}\n\n", evt));
                                    }
                                }
                            }
                        }

                        let done_evt = json!({
                            "type": "response.output_text.done",
                            "response_id": response_id,
                            "output_index": 0,
                            "content_index": 0,
                            "text": full_text
                        });
                        yield sse_ok(format!("data: {}\n\n", done_evt));

                        let completed_evt = json!({
                            "type": "response.completed",
                            "response": {
                                "id": response_id,
                                "object": "response",
                                "created": created,
                                "created_at": created,
                                "status": "completed",
                                "model": model,
                                "output": [{
                                    "id": msg_id,
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": full_text}]
                                }]
                            }
                        });
                        yield sse_ok(format!("data: {}\n\n", completed_evt));
                        yield sse_ok("data: [DONE]\n\n".to_string());
                        let _ = TokenService::consume(&token_clone, effort).await;
                    };
                    let mut headers = HeaderMap::new();
                    headers.insert("Cache-Control", "no-cache".parse().unwrap());
                    headers.insert("Connection", "keep-alive".parse().unwrap());
                    headers.insert("Content-Type", "text/event-stream".parse().unwrap());
                    Ok((headers, axum::body::Body::from_stream(body_stream)).into_response())
                } else {
                    let processor = VideoCollectProcessor::new(&model, &token).await;
                    let result = processor.process(line_stream).await;
                    let effort = if model_info.cost == Cost::High {
                        EffortType::High
                    } else {
                        EffortType::Low
                    };
                    let _ = TokenService::consume(&token, effort).await;
                    let content = result
                        .get("choices")
                        .and_then(|v| v.get(0))
                        .and_then(|v| v.get("message"))
                        .and_then(|v| v.get("content"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let created = result
                        .get("created")
                        .and_then(|v| v.as_i64())
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());
                    let resp = response_from_text(&model, created, content, result.get("usage"));
                    Ok((StatusCode::OK, Json(resp)).into_response())
                }
            }
            VideoResult::Json(json) => Ok((StatusCode::OK, Json(json)).into_response()),
        }
    } else {
        let result =
            ChatService::completions(&req.model, messages, Some(stream), req.thinking.clone())
                .await?;
        match result {
            ChatResult::Stream {
                stream: line_stream,
                token,
                model,
                is_stream,
                think,
            } => {
                if is_stream {
                    let processor = StreamProcessor::new(&model, &token, think).await;
                    let effort = if model_info.cost == Cost::High {
                        EffortType::High
                    } else {
                        EffortType::Low
                    };
                    let token_clone = token.clone();
                    let response_id = format!("resp-{}", uuid::Uuid::new_v4().simple());
                    let msg_id = format!("msg-{}", uuid::Uuid::new_v4().simple());
                    let created = chrono::Utc::now().timestamp();
                    let body_stream = stream! {
                        let created_event = json!({
                            "type": "response.created",
                            "response": {
                                "id": response_id,
                                "object": "response",
                                "created": created,
                                "created_at": created,
                                "status": "in_progress",
                                "model": model
                            }
                        });
                        yield sse_ok(format!("data: {}\n\n", created_event));

                        let mut full_text = String::new();
                        let mut inner = Box::pin(processor.process(line_stream));
                        while let Some(item) = inner.as_mut().next().await {
                            let item = match item {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let text = String::from_utf8_lossy(&item);
                            for line in text.split('\n') {
                                let line = line.trim();
                                if !line.starts_with("data: ") {
                                    continue;
                                }
                                let payload = line.trim_start_matches("data: ").trim();
                                if payload == "[DONE]" {
                                    continue;
                                }
                                if let Ok(val) = serde_json::from_str::<JsonValue>(payload) {
                                    if let Some(delta) = val.get("choices")
                                        .and_then(|v| v.get(0))
                                        .and_then(|v| v.get("delta"))
                                        .and_then(|v| v.get("content"))
                                        .and_then(|v| v.as_str()) {
                                        full_text.push_str(delta);
                                        let evt = json!({
                                            "type": "response.output_text.delta",
                                            "response_id": response_id,
                                            "output_index": 0,
                                            "content_index": 0,
                                            "delta": delta
                                        });
                                        yield sse_ok(format!("data: {}\n\n", evt));
                                    }
                                }
                            }
                        }

                        let done_evt = json!({
                            "type": "response.output_text.done",
                            "response_id": response_id,
                            "output_index": 0,
                            "content_index": 0,
                            "text": full_text
                        });
                        yield sse_ok(format!("data: {}\n\n", done_evt));

                        let completed_evt = json!({
                            "type": "response.completed",
                            "response": {
                                "id": response_id,
                                "object": "response",
                                "created": created,
                                "created_at": created,
                                "status": "completed",
                                "model": model,
                                "output": [{
                                    "id": msg_id,
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": full_text}]
                                }]
                            }
                        });
                        yield sse_ok(format!("data: {}\n\n", completed_evt));
                        yield sse_ok("data: [DONE]\n\n".to_string());
                        let _ = TokenService::consume(&token_clone, effort).await;
                    };
                    let mut headers = HeaderMap::new();
                    headers.insert("Cache-Control", "no-cache".parse().unwrap());
                    headers.insert("Connection", "keep-alive".parse().unwrap());
                    headers.insert("Content-Type", "text/event-stream".parse().unwrap());
                    Ok((headers, axum::body::Body::from_stream(body_stream)).into_response())
                } else {
                    let processor = CollectProcessor::new(&model, &token).await;
                    let result = processor.process(line_stream).await;
                    let effort = if model_info.cost == Cost::High {
                        EffortType::High
                    } else {
                        EffortType::Low
                    };
                    let _ = TokenService::consume(&token, effort).await;
                    let content = result
                        .get("choices")
                        .and_then(|v| v.get(0))
                        .and_then(|v| v.get("message"))
                        .and_then(|v| v.get("content"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let created = result
                        .get("created")
                        .and_then(|v| v.as_i64())
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());
                    let resp = response_from_text(&model, created, content, result.get("usage"));
                    Ok((StatusCode::OK, Json(resp)).into_response())
                }
            }
            ChatResult::Json(json) => Ok((StatusCode::OK, Json(json)).into_response()),
        }
    }
}
