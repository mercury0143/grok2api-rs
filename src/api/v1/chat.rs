use async_stream::stream;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

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

const VALID_ROLES: &[&str] = &["developer", "system", "user", "assistant"];
const USER_CONTENT_TYPES: &[&str] = &["text", "image_url", "input_audio", "file"];

#[derive(Debug, Deserialize)]
pub struct VideoConfig {
    pub aspect_ratio: Option<String>,
    pub video_length: Option<i32>,
    pub resolution: Option<String>,
    pub preset: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<JsonValue>,
    pub stream: Option<bool>,
    pub thinking: Option<String>,
    pub video_config: Option<VideoConfig>,
}

pub fn router() -> Router {
    Router::new().route("/v1/chat/completions", post(chat_completions))
}

fn validate_request(req: &ChatCompletionRequest) -> Result<(), ApiError> {
    if !ModelService::valid(&req.model) {
        return Err(ApiError::not_found(format!(
            "The model `{}` does not exist or you do not have access to it.",
            req.model
        ))
        .with_param("model")
        .with_code("model_not_found"));
    }

    for (idx, msg) in req.messages.iter().enumerate() {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
        if !VALID_ROLES.contains(&role) {
            return Err(ApiError::invalid_request(format!(
                "role must be one of {:?}",
                VALID_ROLES
            ))
            .with_param(format!("messages.{idx}.role")));
        }
        let content = msg.get("content");
        if content.is_none() {
            return Err(ApiError::invalid_request("Message content cannot be empty")
                .with_param(format!("messages.{idx}.content")));
        }
        let content = content.unwrap();
        if let Some(text) = content.as_str() {
            if text.trim().is_empty() {
                return Err(ApiError::invalid_request("Message content cannot be empty")
                    .with_param(format!("messages.{idx}.content")));
            }
        } else if let Some(arr) = content.as_array() {
            if arr.is_empty() {
                return Err(
                    ApiError::invalid_request("Message content cannot be an empty array")
                        .with_param(format!("messages.{idx}.content")),
                );
            }
            for (bidx, block) in arr.iter().enumerate() {
                if block.is_null() {
                    return Err(ApiError::invalid_request("Content block cannot be empty")
                        .with_param(format!("messages.{idx}.content.{bidx}")));
                }
                let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
                if block_type.is_empty() {
                    return Err(
                        ApiError::invalid_request("Content block 'type' cannot be empty")
                            .with_param(format!("messages.{idx}.content.{bidx}.type")),
                    );
                }
                if role == "user" {
                    if !USER_CONTENT_TYPES.contains(&block_type) {
                        return Err(ApiError::invalid_request(format!(
                            "Invalid content block type: '{block_type}'"
                        ))
                        .with_param(format!("messages.{idx}.content.{bidx}.type")));
                    }
                } else if block_type != "text" {
                    return Err(ApiError::invalid_request(format!(
                        "The `{}` role only supports 'text' type, got '{}'",
                        role, block_type
                    ))
                    .with_param(format!("messages.{idx}.content.{bidx}.type")));
                }

                match block_type {
                    "text" => {
                        let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                        if text.trim().is_empty() {
                            return Err(ApiError::invalid_request("Text content cannot be empty")
                                .with_param(format!("messages.{idx}.content.{bidx}.text")));
                        }
                    }
                    "image_url" => {
                        let image_url = block.get("image_url");
                        let url = image_url
                            .and_then(|v| v.get("url"))
                            .and_then(|v| v.as_str());
                        if url.is_none() {
                            return Err(ApiError::invalid_request(
                                "image_url must have a 'url' field",
                            )
                            .with_param(format!("messages.{idx}.content.{bidx}.image_url")));
                        }
                    }
                    "input_audio" => {
                        let data = block
                            .get("input_audio")
                            .and_then(|v| v.get("data"))
                            .and_then(|v| v.as_str());
                        if data.is_none() {
                            return Err(ApiError::invalid_request(
                                "input_audio must have a 'data' field",
                            )
                            .with_param(format!("messages.{idx}.content.{bidx}.input_audio")));
                        }
                    }
                    "file" => {
                        let file = block.get("file");
                        let url = file.and_then(|v| {
                            v.get("url")
                                .and_then(|v| v.as_str())
                                .or_else(|| v.get("data").and_then(|v| v.as_str()))
                        });
                        if url.is_none() {
                            return Err(ApiError::invalid_request(
                                "file must have a 'url' or 'data' field",
                            )
                            .with_param(format!("messages.{idx}.content.{bidx}.file")));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn chat_completions(
    headers: HeaderMap,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let enabled: bool = get_config("downstream.enable_chat_completions", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }
    validate_request(&req)?;

    let model_info =
        ModelService::get(&req.model).ok_or_else(|| ApiError::invalid_request("Invalid model"))?;
    if model_info.is_video {
        let vconf = req.video_config.unwrap_or(VideoConfig {
            aspect_ratio: Some("3:2".to_string()),
            video_length: Some(6),
            resolution: Some("SD".to_string()),
            preset: Some("custom".to_string()),
        });
        let result = VideoService::completions(
            &req.model,
            req.messages.clone(),
            req.stream,
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
                    let body_stream = stream! {
                        let mut inner = Box::pin(processor.process(line_stream));
                        while let Some(item) = inner.as_mut().next().await {
                            yield item;
                        }
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
                    Ok((StatusCode::OK, Json(result)).into_response())
                }
            }
            VideoResult::Json(json) => Ok((StatusCode::OK, Json(json)).into_response()),
        }
    } else {
        let result = ChatService::completions(
            &req.model,
            req.messages.clone(),
            req.stream,
            req.thinking.clone(),
        )
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
                    let body_stream = stream! {
                        let mut inner = Box::pin(processor.process(line_stream));
                        while let Some(item) = inner.as_mut().next().await {
                            yield item;
                        }
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
                    Ok((StatusCode::OK, Json(result)).into_response())
                }
            }
            ChatResult::Json(json) => Ok((StatusCode::OK, Json(json)).into_response()),
        }
    }
}
