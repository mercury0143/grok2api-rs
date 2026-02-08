use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_stream::stream;
use bytes::Bytes;
use futures::Stream;
use serde_json::Value as JsonValue;

use crate::core::config::get_config;
use crate::services::grok::assets::DownloadService;

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

pub struct BaseProcessor {
    pub model: String,
    pub token: String,
    pub created: i64,
    pub app_url: String,
}

impl BaseProcessor {
    pub async fn new(model: &str, token: &str) -> Self {
        let app_url: String = get_config("app.app_url", String::new()).await;
        Self {
            model: model.to_string(),
            token: token.to_string(),
            created: now_ts(),
            app_url,
        }
    }

    pub async fn process_url(&self, path: &str, media_type: &str) -> String {
        let mut url_path = path.to_string();
        if url_path.starts_with("http") {
            if let Ok(parsed) = url::Url::parse(&url_path) {
                url_path = parsed.path().to_string();
            }
        }
        if !url_path.starts_with('/') {
            url_path = format!("/{url_path}");
        }
        if self.app_url.is_empty() {
            return format!("https://assets.grok.com{url_path}");
        }
        let dl = DownloadService::new().await;
        // 使用新的 download_and_upload 方法，会自动上传到配置的存储
        match dl.download_and_upload(&url_path, &self.token, media_type).await {
            Ok(url) => url,
            Err(e) => {
                tracing::error!("Failed to download and upload media: {}", e);
                // 降级为原始 CDN URL
                format!("https://assets.grok.com{url_path}")
            }
        }
    }

    fn sse_chunk(
        &self,
        response_id: &str,
        fingerprint: &str,
        content: Option<&str>,
        role: Option<&str>,
        finish: Option<&str>,
    ) -> String {
        let mut delta = serde_json::json!({});
        if let Some(role) = role {
            delta["role"] = JsonValue::String(role.to_string());
            delta["content"] = JsonValue::String(String::new());
        } else if let Some(content) = content {
            delta["content"] = JsonValue::String(content.to_string());
        }
        let chunk = serde_json::json!({
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "system_fingerprint": fingerprint,
            "choices": [{
                "index": 0,
                "delta": delta,
                "logprobs": null,
                "finish_reason": finish,
            }]
        });
        format!("data: {}\n\n", chunk.to_string())
    }
}

pub struct StreamProcessor {
    base: BaseProcessor,
    response_id: Option<String>,
    fingerprint: String,
    think_opened: bool,
    role_sent: bool,
    filter_tags: Vec<String>,
    image_format: String,
    show_think: bool,
}

impl StreamProcessor {
    pub async fn new(model: &str, token: &str, think: Option<bool>) -> Self {
        let show = match think {
            Some(v) => v,
            None => get_config("grok.thinking", false).await,
        };
        let filter_tags: Vec<String> = get_config("grok.filter_tags", Vec::<String>::new()).await;
        let image_format: String = get_config("app.image_format", "url".to_string()).await;
        Self {
            base: BaseProcessor::new(model, token).await,
            response_id: None,
            fingerprint: String::new(),
            think_opened: false,
            role_sent: false,
            filter_tags,
            image_format,
            show_think: show,
        }
    }

    pub fn process<S>(mut self, input: S) -> impl Stream<Item = Result<Bytes, Infallible>>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        stream! {
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
                if line.trim().is_empty() {
                    continue;
                }
                let data: JsonValue = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let resp = data.get("result").and_then(|v| v.get("response")).cloned().unwrap_or(JsonValue::Null);

                if let Some(llm) = resp.get("llmInfo") {
                    if self.fingerprint.is_empty() {
                        if let Some(hash) = llm.get("modelHash").and_then(|v| v.as_str()) {
                            self.fingerprint = hash.to_string();
                        }
                    }
                }
                if let Some(rid) = resp.get("responseId").and_then(|v| v.as_str()) {
                    self.response_id = Some(rid.to_string());
                }

                if !self.role_sent {
                    let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                    let chunk = self.base.sse_chunk(&id, &self.fingerprint, None, Some("assistant"), None);
                    self.role_sent = true;
                    yield Ok(Bytes::from(chunk));
                }

                if let Some(img) = resp.get("streamingImageGenerationResponse") {
                    if self.show_think {
                        if !self.think_opened {
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some("<think>\n"), None, None);
                            self.think_opened = true;
                            yield Ok(Bytes::from(chunk));
                        }
                        let idx = img.get("imageIndex").and_then(|v| v.as_i64()).unwrap_or(0) + 1;
                        let progress = img.get("progress").and_then(|v| v.as_i64()).unwrap_or(0);
                        let msg = format!("正在生成第{idx}张图片中，当前进度{progress}%\n");
                        let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                        let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(&msg), None, None);
                        yield Ok(Bytes::from(chunk));
                    }
                    continue;
                }

                if let Some(mr) = resp.get("modelResponse") {
                    if self.think_opened && self.show_think {
                        if let Some(msg) = mr.get("message").and_then(|v| v.as_str()) {
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(&(msg.to_string() + "\n")), None, None);
                            yield Ok(Bytes::from(chunk));
                        }
                        let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                        let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some("</think>\n"), None, None);
                        self.think_opened = false;
                        yield Ok(Bytes::from(chunk));
                    }

                    if let Some(urls) = mr.get("generatedImageUrls").and_then(|v| v.as_array()) {
                        for url_val in urls {
                            if let Some(url) = url_val.as_str() {
                                let parts: Vec<&str> = url.split('/').collect();
                                let img_id = parts.get(parts.len().saturating_sub(2)).copied().unwrap_or("image");
                                let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                                if self.image_format == "base64" {
                                    let dl = DownloadService::new().await;
                                    if let Ok(b64) = dl.to_base64(url, &self.base.token, "image").await {
                                        let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(&format!("![{img_id}]({b64})\n")), None, None);
                                        yield Ok(Bytes::from(chunk));
                                    } else {
                                        let final_url = self.base.process_url(url, "image").await;
                                        let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(&format!("![{img_id}]({final_url})\n")), None, None);
                                        yield Ok(Bytes::from(chunk));
                                    }
                                } else {
                                    let final_url = self.base.process_url(url, "image").await;
                                    let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(&format!("![{img_id}]({final_url})\n")), None, None);
                                    yield Ok(Bytes::from(chunk));
                                }
                            }
                        }
                    }
                    if let Some(meta) = mr.get("metadata") {
                        if let Some(hash) = meta.get("llm_info").and_then(|v| v.get("modelHash")).and_then(|v| v.as_str()) {
                            self.fingerprint = hash.to_string();
                        }
                    }
                    continue;
                }

                if let Some(token_val) = resp.get("token") {
                    if let Some(token) = token_val.as_str() {
                        if !token.is_empty() && !self.filter_tags.iter().any(|t| token.contains(t)) {
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some(token), None, None);
                            yield Ok(Bytes::from(chunk));
                        }
                    }
                }
            }
            if self.think_opened {
                let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                let chunk = self.base.sse_chunk(&id, &self.fingerprint, Some("</think>\n"), None, None);
                yield Ok(Bytes::from(chunk));
            }
            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
            let chunk = self.base.sse_chunk(&id, &self.fingerprint, None, None, Some("stop"));
            yield Ok(Bytes::from(chunk));
            yield Ok(Bytes::from("data: [DONE]\n\n"));
        }
    }
}

pub struct CollectProcessor {
    base: BaseProcessor,
    image_format: String,
}

impl CollectProcessor {
    pub async fn new(model: &str, token: &str) -> Self {
        let image_format: String = get_config("app.image_format", "url".to_string()).await;
        Self {
            base: BaseProcessor::new(model, token).await,
            image_format,
        }
    }

    pub fn process<S>(self, input: S) -> impl std::future::Future<Output = JsonValue>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        async move {
            let mut response_id = String::new();
            let mut fingerprint = String::new();
            let mut content = String::new();
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
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
                if let Some(llm) = resp.get("llmInfo") {
                    if fingerprint.is_empty() {
                        if let Some(hash) = llm.get("modelHash").and_then(|v| v.as_str()) {
                            fingerprint = hash.to_string();
                        }
                    }
                }
                if let Some(rid) = resp.get("responseId").and_then(|v| v.as_str()) {
                    response_id = rid.to_string();
                }
                if let Some(mr) = resp.get("modelResponse") {
                    if let Some(msg) = mr.get("message").and_then(|v| v.as_str()) {
                        content.push_str(msg);
                    }
                    if let Some(urls) = mr.get("generatedImageUrls").and_then(|v| v.as_array()) {
                        for url_val in urls {
                            if let Some(url) = url_val.as_str() {
                                let final_url = if self.image_format == "base64" {
                                    let dl = DownloadService::new().await;
                                    dl.to_base64(url, &self.base.token, "image")
                                        .await
                                        .unwrap_or_else(|_| url.to_string())
                                } else {
                                    self.base.process_url(url, "image").await
                                };
                                content.push_str(&format!("![]({})\n", final_url));
                            }
                        }
                    }
                }
                if let Some(token_val) = resp.get("token") {
                    if let Some(token) = token_val.as_str() {
                        content.push_str(token);
                    }
                }
            }
            serde_json::json!({
                "id": response_id,
                "object": "chat.completion",
                "created": self.base.created,
                "model": self.base.model,
                "system_fingerprint": fingerprint,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": content, "refusal": null, "annotations": []},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "prompt_tokens_details": {"cached_tokens": 0, "text_tokens": 0, "audio_tokens": 0, "image_tokens": 0},
                    "completion_tokens_details": {"text_tokens": 0, "audio_tokens": 0, "reasoning_tokens": 0}
                }
            })
        }
    }
}

pub struct VideoStreamProcessor {
    base: BaseProcessor,
    response_id: Option<String>,
    think_opened: bool,
    role_sent: bool,
    show_think: bool,
}

impl VideoStreamProcessor {
    pub async fn new(model: &str, token: &str, think: Option<bool>) -> Self {
        let show = match think {
            Some(v) => v,
            None => get_config("grok.thinking", false).await,
        };
        Self {
            base: BaseProcessor::new(model, token).await,
            response_id: None,
            think_opened: false,
            role_sent: false,
            show_think: show,
        }
    }

    fn build_video_html(video_url: &str, thumbnail_url: &str) -> String {
        let poster = if thumbnail_url.is_empty() {
            "".to_string()
        } else {
            format!(" poster=\"{}\"", thumbnail_url)
        };
        format!(
            "<video id=\"video\" controls=\"\" preload=\"none\"{poster}>\n  <source id=\"mp4\" src=\"{video_url}\" type=\"video/mp4\">\n</video>"
        )
    }

    pub fn process<S>(mut self, input: S) -> impl Stream<Item = Result<Bytes, Infallible>>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        stream! {
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
                if line.trim().is_empty() { continue; }
                let data: JsonValue = match serde_json::from_str(&line) { Ok(v) => v, Err(_) => continue };
                let resp = data.get("result").and_then(|v| v.get("response")).cloned().unwrap_or(JsonValue::Null);

                if let Some(rid) = resp.get("responseId").and_then(|v| v.as_str()) {
                    self.response_id = Some(rid.to_string());
                }
                if !self.role_sent {
                    let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                    let chunk = self.base.sse_chunk(&id, "", None, Some("assistant"), None);
                    self.role_sent = true;
                    yield Ok(Bytes::from(chunk));
                }

                if let Some(video_resp) = resp.get("streamingVideoGenerationResponse") {
                    let progress = video_resp.get("progress").and_then(|v| v.as_i64()).unwrap_or(0);
                    if self.show_think {
                        if !self.think_opened {
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, "", Some("<think>\n"), None, None);
                            self.think_opened = true;
                            yield Ok(Bytes::from(chunk));
                        }
                        let msg = format!("正在生成视频中，当前进度{progress}%\n");
                        let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                        let chunk = self.base.sse_chunk(&id, "", Some(&msg), None, None);
                        yield Ok(Bytes::from(chunk));
                    }
                    if progress == 100 {
                        if self.think_opened && self.show_think {
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, "", Some("</think>\n"), None, None);
                            self.think_opened = false;
                            yield Ok(Bytes::from(chunk));
                        }
                        let video_url = video_resp.get("videoUrl").and_then(|v| v.as_str()).unwrap_or("");
                        let thumb_url = video_resp.get("thumbnailImageUrl").and_then(|v| v.as_str()).unwrap_or("");
                        if !video_url.is_empty() {
                            let final_video = self.base.process_url(video_url, "video").await;
                            let final_thumb = if thumb_url.is_empty() { String::new() } else { self.base.process_url(thumb_url, "image").await };
                            let html = Self::build_video_html(&final_video, &final_thumb);
                            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                            let chunk = self.base.sse_chunk(&id, "", Some(&html), None, None);
                            yield Ok(Bytes::from(chunk));
                        }
                    }
                }
            }
            if self.think_opened {
                let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
                let chunk = self.base.sse_chunk(&id, "", Some("</think>\n"), None, None);
                yield Ok(Bytes::from(chunk));
            }
            let id = self.response_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().simple()));
            let chunk = self.base.sse_chunk(&id, "", None, None, Some("stop"));
            yield Ok(Bytes::from(chunk));
            yield Ok(Bytes::from("data: [DONE]\n\n"));
        }
    }
}

pub struct VideoCollectProcessor {
    base: BaseProcessor,
}

impl VideoCollectProcessor {
    pub async fn new(model: &str, token: &str) -> Self {
        Self {
            base: BaseProcessor::new(model, token).await,
        }
    }

    fn build_video_html(video_url: &str, thumbnail_url: &str) -> String {
        let poster = if thumbnail_url.is_empty() {
            "".to_string()
        } else {
            format!(" poster=\"{}\"", thumbnail_url)
        };
        format!(
            "<video id=\"video\" controls=\"\" preload=\"none\"{poster}>\n  <source id=\"mp4\" src=\"{video_url}\" type=\"video/mp4\">\n</video>"
        )
    }

    pub fn process<S>(self, input: S) -> impl std::future::Future<Output = JsonValue>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        async move {
            let mut response_id = String::new();
            let mut content = String::new();
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
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
                    if video_resp.get("progress").and_then(|v| v.as_i64()) == Some(100) {
                        response_id = resp
                            .get("responseId")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let video_url = video_resp
                            .get("videoUrl")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let thumb_url = video_resp
                            .get("thumbnailImageUrl")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if !video_url.is_empty() {
                            let final_video = self.base.process_url(video_url, "video").await;
                            let final_thumb = if thumb_url.is_empty() {
                                String::new()
                            } else {
                                self.base.process_url(thumb_url, "image").await
                            };
                            content = Self::build_video_html(&final_video, &final_thumb);
                        }
                    }
                }
            }
            serde_json::json!({
                "id": response_id,
                "object": "chat.completion",
                "created": self.base.created,
                "model": self.base.model,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": content, "refusal": null},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            })
        }
    }
}

pub struct ImageStreamProcessor {
    base: BaseProcessor,
    partial_index: usize,
    n: usize,
    target_index: Option<usize>,
    return_base64: bool,
}

impl ImageStreamProcessor {
    pub async fn new(model: &str, token: &str, n: usize, return_base64: bool) -> Self {
        let target_index = if n == 1 {
            Some(rand::random::<usize>() % 2)
        } else {
            None
        };
        Self {
            base: BaseProcessor::new(model, token).await,
            partial_index: 0,
            n,
            target_index,
            return_base64,
        }
    }

    fn sse_event(event: &str, data: JsonValue) -> String {
        format!("event: {}\ndata: {}\n\n", event, data.to_string())
    }

    pub fn process<S>(mut self, input: S) -> impl Stream<Item = Result<Bytes, Infallible>>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        stream! {
            let mut final_images: Vec<JsonValue> = Vec::new();
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
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

                if let Some(img) = resp.get("streamingImageGenerationResponse") {
                    let image_index = img.get("imageIndex").and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                    let progress = img.get("progress").and_then(|v| v.as_i64()).unwrap_or(0);
                    if self.n == 1 {
                        if let Some(target) = self.target_index {
                            if image_index != target {
                                continue;
                            }
                        }
                    }
                    let out_index = if self.n == 1 { 0 } else { image_index };
                    let mut payload = serde_json::json!({
                        "type": "image_generation.partial_image",
                        "index": out_index,
                        "progress": progress,
                    });
                    if self.return_base64 {
                        payload["b64_json"] = JsonValue::String(String::new());
                    } else {
                        payload["url"] = JsonValue::String(String::new());
                    }
                    yield Ok(Bytes::from(Self::sse_event("image_generation.partial_image", payload)));
                    continue;
                }

                if let Some(mr) = resp.get("modelResponse") {
                    if let Some(urls) = mr.get("generatedImageUrls").and_then(|v| v.as_array()) {
                        for url in urls {
                            if let Some(url) = url.as_str() {
                                if self.return_base64 {
                                    let dl = DownloadService::new().await;
                                    if let Ok(b64) = dl.to_base64(url, &self.base.token, "image").await {
                                        let b64_str = if let Some(idx) = b64.find(',') {
                                            b64[idx + 1..].to_string()
                                        } else {
                                            b64
                                        };
                                        final_images.push(serde_json::json!({"b64_json": b64_str}));
                                    }
                                } else {
                                    let final_url = self.base.process_url(url, "image").await;
                                    final_images.push(serde_json::json!({"url": final_url}));
                                }
                            }
                        }
                    }
                }
            }

            for (index, image) in final_images.iter().enumerate() {
                let out_index = if self.n == 1 {
                    if let Some(target) = self.target_index {
                        if index != target {
                            continue;
                        }
                    }
                    0
                } else {
                    index
                };
                let mut payload = serde_json::json!({
                    "type": "image_generation.completed",
                    "index": out_index,
                    "usage": {
                        "total_tokens": 50,
                        "input_tokens": 25,
                        "output_tokens": 25,
                        "input_tokens_details": {"text_tokens": 5, "image_tokens": 20}
                    }
                });
                if let Some(b64) = image.get("b64_json").and_then(|v| v.as_str()) {
                    payload["b64_json"] = JsonValue::String(b64.to_string());
                }
                if let Some(url) = image.get("url").and_then(|v| v.as_str()) {
                    payload["url"] = JsonValue::String(url.to_string());
                }
                yield Ok(Bytes::from(Self::sse_event("image_generation.completed", payload)));
            }
        }
    }
}

pub struct ImageCollectProcessor {
    base: BaseProcessor,
    return_base64: bool,
}

impl ImageCollectProcessor {
    pub async fn new(model: &str, token: &str, return_base64: bool) -> Self {
        Self {
            base: BaseProcessor::new(model, token).await,
            return_base64,
        }
    }

    pub fn process<S>(self, input: S) -> impl std::future::Future<Output = Vec<JsonValue>>
    where
        S: Stream<Item = String> + Send + 'static,
    {
        async move {
            let mut images = Vec::new();
            let mut stream = Box::pin(input);
            while let Some(line) = stream.next().await {
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
                if let Some(mr) = resp.get("modelResponse") {
                    if let Some(urls) = mr.get("generatedImageUrls").and_then(|v| v.as_array()) {
                        for url in urls {
                            if let Some(url) = url.as_str() {
                                if self.return_base64 {
                                    let dl = DownloadService::new().await;
                                    if let Ok(b64) =
                                        dl.to_base64(url, &self.base.token, "image").await
                                    {
                                        let b64_str = if let Some(idx) = b64.find(',') {
                                            b64[idx + 1..].to_string()
                                        } else {
                                            b64
                                        };
                                        images.push(serde_json::json!({"b64_json": b64_str}));
                                    }
                                } else {
                                    let final_url = self.base.process_url(url, "image").await;
                                    images.push(serde_json::json!({"url": final_url}));
                                }
                            }
                        }
                    }
                }
            }
            images
        }
    }
}

use futures::StreamExt;
