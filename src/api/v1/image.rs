use std::convert::Infallible;

use async_stream::stream;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use bytes::Bytes;
use futures::StreamExt;
use futures::future::join_all;
use rand::seq::SliceRandom;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tokio::sync::mpsc;

use crate::core::auth::verify_api_key;
use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::chat::GrokChatService;
use crate::services::grok::imagine_nsfw;
use crate::services::grok::model::{Cost, ModelInfo, ModelService};
use crate::services::grok::processor::{ImageCollectProcessor, ImageStreamProcessor};
use crate::services::token::{EffortType, TokenService};

#[derive(Debug, Deserialize)]
pub struct ImageRequest {
    pub prompt: String,
    pub model: Option<String>,
    pub n: Option<u32>,
    pub size: Option<String>,
    pub quality: Option<String>,
    pub response_format: Option<String>,
    pub style: Option<String>,
    pub stream: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImageOutputFormat {
    Url,
    Base64,
}

impl ImageOutputFormat {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "url" => Some(Self::Url),
            "base64" | "b64_json" => Some(Self::Base64),
            _ => None,
        }
    }

    fn is_base64(self) -> bool {
        matches!(self, Self::Base64)
    }
}

pub fn router() -> Router {
    Router::new()
        .route("/v1/images/generations", post(create_image))
        .route("/v1/images/generations/nsfw", post(create_image_nsfw))
}

async fn create_image(
    headers: HeaderMap,
    Json(req): Json<ImageRequest>,
) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;
    let enabled: bool = get_config("downstream.enable_images", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }

    let model_id = req
        .model
        .clone()
        .unwrap_or_else(|| "grok-imagine-1.0".to_string());
    let n = req.n.unwrap_or(1).clamp(1, 10);
    let stream = req.stream.unwrap_or(false);
    let output_format = resolve_image_output_format(req.response_format.as_deref()).await?;

    let model_info = ModelService::get(&model_id)
        .ok_or_else(|| ApiError::invalid_request("The model does not exist"))?;
    if !model_info.is_image {
        return Err(ApiError::invalid_request(format!(
            "The model `{}` is not supported for image generation.",
            model_id
        ))
        .with_code("model_not_supported"));
    }
    if req.prompt.trim().is_empty() {
        return Err(ApiError::invalid_request("Prompt cannot be empty").with_param("prompt"));
    }
    if stream && !(n == 1 || n == 2) {
        return Err(
            ApiError::invalid_request("Streaming is only supported when n=1 or n=2")
                .with_param("stream"),
        );
    }

    let token = TokenService::get_token_for_model(&model_id).await?;

    if stream {
        let processor =
            ImageStreamProcessor::new(&model_id, &token, n as usize, output_format.is_base64())
                .await;
        let response = call_grok_image(&token, &req.prompt, &model_info).await?;
        let effort = if model_info.cost == Cost::High {
            EffortType::High
        } else {
            EffortType::Low
        };
        let token_clone = token.clone();
        let body_stream = stream! {
            let mut inner = Box::pin(processor.process(response));
            while let Some(item) = inner.as_mut().next().await {
                yield item;
            }
            let _ = TokenService::consume(&token_clone, effort).await;
        };
        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", "no-cache".parse().unwrap());
        headers.insert("Connection", "keep-alive".parse().unwrap());
        headers.insert("Content-Type", "text/event-stream".parse().unwrap());
        return Ok((headers, axum::body::Body::from_stream(body_stream)).into_response());
    }

    let calls_needed = (n as usize + 1) / 2;
    let effort = if model_info.cost == Cost::High {
        EffortType::High
    } else {
        EffortType::Low
    };
    let mut all_images: Vec<JsonValue> = Vec::new();

    if calls_needed == 1 {
        match call_grok_images_once(&token, &req.prompt, &model_info, output_format.is_base64())
            .await
        {
            Ok(images) => all_images.extend(images),
            Err(err) => tracing::error!("Grok image call failed: {err}"),
        }
        let _ = TokenService::consume(&token, effort.clone()).await;
    } else {
        let tasks = (0..calls_needed)
            .map(|_| {
                call_grok_images_once(&token, &req.prompt, &model_info, output_format.is_base64())
            })
            .collect::<Vec<_>>();
        let results = join_all(tasks).await;
        for result in results {
            match result {
                Ok(images) => all_images.extend(images),
                Err(err) => tracing::error!("Concurrent image call failed: {err}"),
            }
            let _ = TokenService::consume(&token, effort.clone()).await;
        }
    }

    if all_images.len() > n as usize {
        let mut rng = rand::thread_rng();
        all_images.shuffle(&mut rng);
        all_images.truncate(n as usize);
    }

    while all_images.len() < n as usize {
        all_images.push(match output_format {
            ImageOutputFormat::Base64 => json!({ "b64_json": "error" }),
            ImageOutputFormat::Url => json!({ "url": "error" }),
        });
    }

    let created = chrono::Utc::now().timestamp() as i64;
    let usage = json!({"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0});
    let resp = json!({"created": created, "data": all_images, "usage": usage});

    Ok((StatusCode::OK, Json(resp)).into_response())
}

async fn create_image_nsfw(
    headers: HeaderMap,
    Json(req): Json<ImageRequest>,
) -> Result<Response, ApiError> {
    verify_api_key(&headers).await?;

    let enabled: bool = get_config("downstream.enable_images", true).await;
    if !enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }
    let nsfw_enabled: bool = get_config("downstream.enable_images_nsfw", true).await;
    if !nsfw_enabled {
        return Err(ApiError::not_found("Endpoint disabled"));
    }

    if req.prompt.trim().is_empty() {
        return Err(ApiError::invalid_request("Prompt cannot be empty").with_param("prompt"));
    }

    if let Some(n) = req.n {
        if n == 0 || n > 4 {
            return Err(ApiError::invalid_request("n must be between 1 and 4").with_param("n"));
        }
    }

    let stream = req.stream.unwrap_or(false);
    let response_format = req.response_format.unwrap_or_else(|| "url".to_string());
    let return_base64 = response_format == "b64_json";

    if stream {
        let (tx, mut rx) = mpsc::unbounded_channel::<imagine_nsfw::ImagineProgressEvent>();
        let prompt = req.prompt.clone();
        let size = req.size.clone();
        let n = req.n;

        let task = tokio::spawn(async move {
            imagine_nsfw::generate(&prompt, size.as_deref(), n, Some(tx)).await
        });

        let body_stream = stream! {
            while let Some(evt) = rx.recv().await {
                let payload = json!({
                    "image_id": evt.image_id,
                    "stage": evt.stage,
                    "is_final": evt.is_final,
                    "completed": evt.completed,
                    "total": evt.total,
                    "progress": format!("{}/{}", evt.completed, evt.total),
                });
                let data = format!("event: progress\ndata: {}\n\n", payload);
                yield Ok::<Bytes, Infallible>(Bytes::from(data));
            }

            let result = match task.await {
                Ok(v) => v,
                Err(err) => imagine_nsfw::ImagineResult::failed("generation_failed", format!("task join error: {err}")),
            };

            if result.success {
                let complete = json!({
                    "created": chrono::Utc::now().timestamp(),
                    "data": result.urls.into_iter().map(|url| json!({"url": url})).collect::<Vec<_>>()
                });
                let data = format!("event: complete\ndata: {}\n\n", complete);
                yield Ok::<Bytes, Infallible>(Bytes::from(data));
            } else {
                let err_payload = json!({
                    "error": result.error.unwrap_or_else(|| "Image generation failed".to_string())
                });
                let data = format!("event: error\ndata: {}\n\n", err_payload);
                yield Ok::<Bytes, Infallible>(Bytes::from(data));
            }
        };

        let mut headers = HeaderMap::new();
        headers.insert("Cache-Control", "no-cache".parse().unwrap());
        headers.insert("Connection", "keep-alive".parse().unwrap());
        headers.insert("Content-Type", "text/event-stream".parse().unwrap());
        return Ok((headers, axum::body::Body::from_stream(body_stream)).into_response());
    }

    let result = imagine_nsfw::generate(&req.prompt, req.size.as_deref(), req.n, None).await;
    if !result.success {
        let msg = result
            .error
            .unwrap_or_else(|| "Image generation failed".to_string());
        if result.error_code.as_deref() == Some("rate_limit_exceeded") {
            return Err(ApiError::rate_limit(msg));
        }
        return Err(ApiError::server(msg));
    }

    let data = if return_base64 {
        result
            .b64_list
            .into_iter()
            .map(|b64| json!({"b64_json": b64}))
            .collect::<Vec<_>>()
    } else {
        result
            .urls
            .into_iter()
            .map(|url| json!({"url": url}))
            .collect::<Vec<_>>()
    };

    let resp = json!({
        "created": chrono::Utc::now().timestamp(),
        "data": data
    });

    Ok((StatusCode::OK, Json(resp)).into_response())
}

async fn call_grok_image(
    token: &str,
    prompt: &str,
    model_info: &ModelInfo,
) -> Result<impl futures::Stream<Item = String> + Send + 'static, ApiError> {
    let chat_service = GrokChatService::new().await;
    chat_service
        .chat(
            token,
            &format!("Image Generation:{prompt}"),
            &model_info.grok_model,
            &model_info.model_mode,
            Some(false),
            true,
            &[],
            &[],
        )
        .await
}

async fn call_grok_images_once(
    token: &str,
    prompt: &str,
    model_info: &ModelInfo,
    return_base64: bool,
) -> Result<Vec<JsonValue>, ApiError> {
    let response = call_grok_image(token, prompt, model_info).await?;
    let processor = ImageCollectProcessor::new(&model_info.model_id, token, return_base64).await;
    Ok(processor.process(response).await)
}

async fn resolve_image_output_format(
    response_format: Option<&str>,
) -> Result<ImageOutputFormat, ApiError> {
    let request_format = response_format.map(|s| s.trim()).filter(|s| !s.is_empty());
    if let Some(fmt) = request_format {
        return ImageOutputFormat::parse(fmt).ok_or_else(|| {
            ApiError::invalid_request("response_format must be one of: url, b64_json")
                .with_param("response_format")
        });
    }

    let config_format: String = get_config("app.image_format", "url".to_string()).await;
    Ok(ImageOutputFormat::parse(&config_format).unwrap_or(ImageOutputFormat::Url))
}
