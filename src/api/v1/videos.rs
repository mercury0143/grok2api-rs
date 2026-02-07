use axum::{routing::post, Router};
use axum::extract::Multipart;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value as JsonValue};

use crate::core::auth::verify_api_key;
use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::media::{VideoResult, VideoService};
use crate::services::grok::model::{Cost, ModelService};
use crate::services::grok::processor::{VideoCollectProcessor};
use crate::services::token::{EffortType, TokenService};

pub fn router() -> Router {
    Router::new().route("/v1/videos", post(create_video))
}

async fn create_video(headers: HeaderMap, mut multipart: Multipart) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let enabled: bool = get_config("downstream.enable_images", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }

    // 解析 multipart/form-data
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
                // 读取文件数据（如果有）
                let data = field.bytes().await.map_err(|e| {
                    ApiError::invalid_request(format!("Failed to read input_reference: {}", e))
                })?;
                if !data.is_empty() {
                    _input_reference = Some(data.to_vec());
                }
            }
            _ => {
                // 忽略未知字段
            }
        }
    }

    // 验证必填字段
    if model.is_empty() {
        return Err(ApiError::invalid_request("model is required"));
    }
    if prompt.is_empty() {
        return Err(ApiError::invalid_request("prompt is required"));
    }

    // 根据模型硬编码 seconds
    let final_seconds = match model.as_str() {
        "grok-video-3" => 3,
        "grok-video-3-pro" => 6,
        _ => seconds, // 其他模型使用传入的值
    };

    // 验证模型
    let model_info = ModelService::get(&model)
        .ok_or_else(|| ApiError::not_found(format!("The model `{}` does not exist or you do not have access to it.", model))
            .with_param("model")
            .with_code("model_not_found"))?;

    if !model_info.is_video {
        return Err(ApiError::invalid_request(format!("Model `{}` is not a video model", model)));
    }

    // 构建消息
    let messages = vec![json!({
        "role": "user",
        "content": prompt
    })];

    // 调用视频生成服务（非流式）
    let result = VideoService::completions(
        &model,
        messages,
        Some(false), // 强制非流式
        None,
        &aspect_ratio,
        final_seconds,
        &size,
        "custom",
    )
    .await?;

    match result {
        VideoResult::Stream { stream: line_stream, token, model, think: _, is_stream: _ } => {
            // 收集完整响应
            let processor = VideoCollectProcessor::new(&model, &token).await;
            let result = processor.process(line_stream).await;
            let effort = if model_info.cost == Cost::High { EffortType::High } else { EffortType::Low };
            let _ = TokenService::consume(&token, effort).await;

            // 提取视频 URL
            let content = result
                .get("choices")
                .and_then(|v| v.get(0))
                .and_then(|v| v.get("message"))
                .and_then(|v| v.get("content"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // 从内容中提取视频 URL 和封面 URL
            let (video_url, poster_url) = extract_video_urls(content);

            let response = json!({
                "id": format!("video-{}", uuid::Uuid::new_v4().simple()),
                "object": "video",
                "created": chrono::Utc::now().timestamp(),
                "model": model,
                "data": {
                    "url": video_url,
                    "poster": poster_url,
                    "prompt": prompt,
                    "aspect_ratio": aspect_ratio,
                    "seconds": final_seconds,
                    "size": size
                },
                "usage": result.get("usage").cloned().unwrap_or(json!({
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0
                }))
            });

            Ok((StatusCode::OK, Json(response)).into_response())
        }
        VideoResult::Json(json) => {
            Ok((StatusCode::OK, Json(json)).into_response())
        }
    }
}

/// 从内容中提取视频 URL 和封面 URL
/// 支持 HTML <video> 标签和 markdown ![](url) 格式
fn extract_video_urls(content: &str) -> (String, String) {
    // 尝试从 HTML <video> 标签提取
    // 提取 <source ... src="url" ...>
    if let Some(src_start) = content.find("<source") {
        let source_tag = &content[src_start..];
        let video_url = extract_attr(source_tag, "src").unwrap_or_default();
        // 提取 <video ... poster="url" ...>
        let poster_url = extract_attr(content, "poster").unwrap_or_default();
        if !video_url.is_empty() {
            return (video_url, poster_url);
        }
    }

    // 降级：尝试 markdown 格式 ![...](url)
    if let Some(start) = content.find("](") {
        if let Some(end) = content[start + 2..].find(')') {
            return (content[start + 2..start + 2 + end].to_string(), String::new());
        }
    }

    // 都没找到，返回原内容
    (content.to_string(), String::new())
}

/// 从 HTML 标签中提取指定属性的值
fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let pattern = format!("{}=\"", attr);
    if let Some(start) = tag.find(&pattern) {
        let value_start = start + pattern.len();
        if let Some(end) = tag[value_start..].find('"') {
            return Some(tag[value_start..value_start + end].to_string());
        }
    }
    None
}
