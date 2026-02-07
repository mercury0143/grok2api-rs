use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha1::{Digest, Sha1};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::core::config::{get_config, project_root};
use crate::services::token::get_token_manager;

const GROK_WS_URL: &str = "wss://grok.com/ws/imagine/listen";
const AGE_VERIFY_URL: &str = "https://grok.com/rest/auth/set-birth-date";
const RESET_INTERVAL_SECS: f64 = 86_400.0;

#[derive(Debug, Clone)]
pub struct ImagineProgressEvent {
    pub image_id: String,
    pub stage: String,
    pub is_final: bool,
    pub completed: usize,
    pub total: usize,
}

#[derive(Debug, Clone)]
pub struct ImagineResult {
    pub success: bool,
    pub urls: Vec<String>,
    pub b64_list: Vec<String>,
    pub count: usize,
    pub error_code: Option<String>,
    pub error: Option<String>,
}

impl ImagineResult {
    fn ok(urls: Vec<String>, b64_list: Vec<String>) -> Self {
        Self {
            success: true,
            count: urls.len(),
            urls,
            b64_list,
            error_code: None,
            error: None,
        }
    }

    pub fn failed(error_code: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            success: false,
            urls: Vec::new(),
            b64_list: Vec::new(),
            count: 0,
            error_code: Some(error_code.into()),
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone)]
struct ImageProgress {
    image_id: String,
    stage: String,
    blob: String,
    blob_size: usize,
    url: String,
    is_final: bool,
}

#[derive(Debug, Clone)]
struct GenerationProgress {
    total: usize,
    images: HashMap<String, ImageProgress>,
    completed: usize,
}

impl GenerationProgress {
    fn new(total: usize) -> Self {
        Self {
            total,
            images: HashMap::new(),
            completed: 0,
        }
    }

    fn check_blocked(&self) -> bool {
        let has_medium = self.images.values().any(|img| img.stage == "medium");
        let has_final = self.images.values().any(|img| img.is_final);
        has_medium && !has_final
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyUsage {
    count: i32,
    last_used: f64,
    first_used: f64,
    failed: bool,
    age_verified: i32,
}

impl Default for KeyUsage {
    fn default() -> Self {
        Self {
            count: 0,
            last_used: 0.0,
            first_used: now_secs(),
            failed: false,
            age_verified: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RotationStateData {
    last_reset: f64,
    current_index: usize,
    usage: HashMap<String, KeyUsage>,
}

#[derive(Debug)]
struct RotationStore {
    loaded: bool,
    path: PathBuf,
    state: RotationStateData,
}

impl RotationStore {
    fn new() -> Self {
        Self {
            loaded: false,
            path: project_root().join("data").join("imagine_nsfw_state.json"),
            state: RotationStateData::default(),
        }
    }
}

static ROTATION_STORE: Lazy<Arc<Mutex<RotationStore>>> =
    Lazy::new(|| Arc::new(Mutex::new(RotationStore::new())));

fn now_secs() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn key_hash(token: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(token.as_bytes());
    let full = format!("{:x}", hasher.finalize());
    full.chars().take(12).collect()
}

fn sanitize_token(token: &str) -> String {
    token.trim().trim_start_matches("sso=").to_string()
}

async fn load_rotation_state(store: &mut RotationStore) {
    if store.loaded {
        return;
    }
    store.loaded = true;
    match tokio::fs::read_to_string(&store.path).await {
        Ok(content) => {
            if let Ok(parsed) = serde_json::from_str::<RotationStateData>(&content) {
                store.state = parsed;
                tracing::info!("[ImagineNSFW] loaded rotation state");
            }
        }
        Err(_) => {}
    }
}

async fn save_rotation_state(store: &RotationStore) {
    if let Some(parent) = store.path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    if let Ok(content) = serde_json::to_string_pretty(&store.state) {
        let _ = tokio::fs::write(&store.path, content).await;
    }
}

fn ensure_usage<'a>(state: &'a mut RotationStateData, token: &str) -> &'a mut KeyUsage {
    let key = key_hash(token);
    state.usage.entry(key).or_insert_with(KeyUsage::default)
}

fn get_usage(state: &RotationStateData, token: &str) -> KeyUsage {
    state
        .usage
        .get(&key_hash(token))
        .cloned()
        .unwrap_or_default()
}

fn check_daily_reset(state: &mut RotationStateData) {
    let now = now_secs();
    if state.last_reset == 0.0 {
        state.last_reset = now;
        return;
    }
    if now - state.last_reset < RESET_INTERVAL_SECS {
        return;
    }
    for usage in state.usage.values_mut() {
        usage.count = 0;
        usage.failed = false;
    }
    state.last_reset = now;
    tracing::info!("[ImagineNSFW] daily usage reset");
}

fn get_available_tokens(
    tokens: &[String],
    state: &RotationStateData,
    daily_limit: i32,
) -> Vec<String> {
    tokens
        .iter()
        .filter_map(|token| {
            let usage = get_usage(state, token);
            if usage.failed || usage.count >= daily_limit {
                None
            } else {
                Some(token.clone())
            }
        })
        .collect()
}

async fn get_next_sso(tokens: &[String], daily_limit: i32) -> Option<String> {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    check_daily_reset(&mut guard.state);

    for token in tokens {
        let _ = ensure_usage(&mut guard.state, token);
    }

    let available = get_available_tokens(tokens, &guard.state, daily_limit);
    if available.is_empty() {
        let all_failed = tokens.iter().all(|t| get_usage(&guard.state, t).failed);
        if all_failed {
            for token in tokens {
                ensure_usage(&mut guard.state, token).failed = false;
            }
            save_rotation_state(&guard).await;
            tracing::info!("[ImagineNSFW] reset failed list");
            return tokens.first().cloned();
        }
        return None;
    }

    // Hybrid strategy, same as imagine2api: remaining quota + least recently used factor.
    let now = now_secs();
    let mut best_score = -1.0f64;
    let mut selected = available[0].clone();
    for token in available {
        let usage = get_usage(&guard.state, &token);
        let remaining = (daily_limit - usage.count).max(0) as f64;
        let time_factor = if usage.last_used == 0.0 {
            10.0
        } else {
            (((now - usage.last_used) / 60.0) * 0.1).min(10.0)
        };
        let score = remaining * (1.0 + time_factor);
        if score > best_score {
            best_score = score;
            selected = token;
        }
    }

    Some(selected)
}

async fn mark_failed(token: &str, reason: &str) {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    ensure_usage(&mut guard.state, token).failed = true;
    save_rotation_state(&guard).await;
    tracing::warn!(
        "[ImagineNSFW] mark failed {}... reason={}",
        &token[..token.len().min(12)],
        reason
    );
}

async fn mark_success(token: &str) {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    ensure_usage(&mut guard.state, token).failed = false;
    save_rotation_state(&guard).await;
}

async fn record_usage(token: &str) {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    let usage = ensure_usage(&mut guard.state, token);
    usage.count += 1;
    usage.last_used = now_secs();
    save_rotation_state(&guard).await;
}

async fn get_age_verified(token: &str) -> i32 {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    get_usage(&guard.state, token).age_verified
}

async fn set_age_verified(token: &str, verified: i32) {
    let mut guard = ROTATION_STORE.lock().await;
    load_rotation_state(&mut guard).await;
    ensure_usage(&mut guard.state, token).age_verified = verified;
    save_rotation_state(&guard).await;
    tracing::info!(
        "[ImagineNSFW] set age verified {}... -> {}",
        &token[..token.len().min(12)],
        verified
    );
}

async fn get_sso_tokens() -> Vec<String> {
    let mgr = get_token_manager().await;
    let mut guard = mgr.lock().await;
    guard.reload_if_stale().await;

    // Keep parity with imagine2api key-file behavior: NSFW generator can use all loaded SSO.
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for pool in guard.pools.values() {
        for info in pool.list() {
            let raw = sanitize_token(&info.token);
            if raw.is_empty() {
                continue;
            }
            if seen.insert(raw.clone()) {
                out.push(raw);
            }
        }
    }
    out
}

fn size_to_aspect_ratio(size: &str) -> &'static str {
    match size {
        "1024x1024" => "1:1",
        "1024x1536" => "2:3",
        "1536x1024" => "3:2",
        "512x512" => "1:1",
        "256x256" => "1:1",
        _ => "2:3",
    }
}

fn extract_image_id(url: &str) -> Option<String> {
    let path = if let Ok(parsed) = url::Url::parse(url) {
        parsed.path().to_string()
    } else {
        url.to_string()
    };
    let marker = "/images/";
    let start = path.find(marker)? + marker.len();
    let tail = &path[start..];
    let (image_id, ext) = tail.rsplit_once('.')?;
    if ext != "png" && ext != "jpg" {
        return None;
    }
    if image_id.is_empty() {
        return None;
    }
    if !image_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return None;
    }
    Some(image_id.to_string())
}

fn is_final_image(url: &str, blob_size: usize) -> bool {
    let path = if let Ok(parsed) = url::Url::parse(url) {
        parsed.path().to_string()
    } else {
        url.to_string()
    };
    path.ends_with(".jpg") && blob_size > 100_000
}

async fn verify_age(token: &str) -> bool {
    let cf_clearance: String = get_config("grok.cf_clearance", String::new()).await;
    if cf_clearance.trim().is_empty() {
        tracing::warn!("[ImagineNSFW] cf_clearance not configured; skip age verify");
        return false;
    }

    let timeout_secs: u64 = get_config("grok.timeout", 120u64).await;
    let proxy: String = get_config("grok.base_proxy_url", String::new()).await;

    let raw = sanitize_token(token);
    let cookie = format!(
        "sso={raw}; sso-rw={raw}; cf_clearance={}",
        cf_clearance.trim()
    );

    let client =
        match crate::services::grok::wreq_client::build_client(Some(&proxy), timeout_secs).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!("[ImagineNSFW] build wreq client failed: {err}");
                return false;
            }
        };

    let response = match client
        .post(AGE_VERIFY_URL)
        .timeout(Duration::from_secs(timeout_secs.max(1)))
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
        .header("Origin", "https://grok.com")
        .header("Referer", "https://grok.com/")
        .header("Accept", "*/*")
        .header("Cookie", cookie)
        .header("Content-Type", "application/json")
        .body(r#"{"birthDate":"2001-01-01T16:00:00.000Z"}"#)
        .send()
        .await
    {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!("[ImagineNSFW] age verify request failed: {err}");
            return false;
        }
    };

    let status = response.status().as_u16();
    if status == 200 {
        tracing::info!("[ImagineNSFW] age verify success");
        true
    } else {
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let body = response.text().await.unwrap_or_else(|_| String::new());
        let preview = crate::services::grok::wreq_client::body_preview(&body, 220);
        tracing::warn!(
            "[ImagineNSFW] age verify status={} content_type={} body={}",
            status,
            content_type,
            preview
        );
        false
    }
}

async fn save_final_images(progress: GenerationProgress, n: usize) -> (Vec<String>, Vec<String>) {
    let mut imgs = progress.images.values().cloned().collect::<Vec<_>>();
    imgs.sort_by(|a, b| {
        b.is_final
            .cmp(&a.is_final)
            .then_with(|| b.blob_size.cmp(&a.blob_size))
    });

    let mut result_urls = Vec::new();
    let mut result_b64 = Vec::new();
    let mut saved_ids = HashSet::new();

    let app_url: String = get_config("app.app_url", String::new()).await;
    let image_dir = project_root().join("data").join("tmp").join("image");
    let _ = tokio::fs::create_dir_all(&image_dir).await;

    for img in imgs {
        if saved_ids.contains(&img.image_id) {
            continue;
        }
        if saved_ids.len() >= n {
            break;
        }

        let image_data = match base64::engine::general_purpose::STANDARD.decode(&img.blob) {
            Ok(data) => data,
            Err(err) => {
                tracing::warn!("[ImagineNSFW] decode image failed: {err}");
                continue;
            }
        };

        let ext = if img.is_final { "jpg" } else { "png" };
        let filename = format!("{}.{}", img.image_id, ext);
        let filepath = image_dir.join(&filename);

        if let Err(err) = tokio::fs::write(&filepath, image_data).await {
            tracing::warn!("[ImagineNSFW] save image failed: {err}");
            continue;
        }

        let url = if app_url.trim().is_empty() {
            format!("/images/{filename}")
        } else {
            format!("{}/images/{filename}", app_url.trim_end_matches('/'))
        };

        result_urls.push(url);
        result_b64.push(img.blob);
        saved_ids.insert(img.image_id);
    }

    (result_urls, result_b64)
}

async fn do_generate(
    sso: &str,
    prompt: &str,
    aspect_ratio: &str,
    n: usize,
    enable_nsfw: bool,
    progress_tx: Option<mpsc::UnboundedSender<ImagineProgressEvent>>,
) -> ImagineResult {
    let request_id = uuid::Uuid::new_v4().to_string();
    let raw = sanitize_token(sso);

    let mut request = match GROK_WS_URL.into_client_request() {
        Ok(req) => req,
        Err(err) => {
            return ImagineResult::failed(
                "connection_failed",
                format!("request build failed: {err}"),
            );
        }
    };

    if let Ok(val) = format!("sso={raw}; sso-rw={raw}").parse() {
        request.headers_mut().insert("Cookie", val);
    }
    if let Ok(val) = "https://grok.com".parse() {
        request.headers_mut().insert("Origin", val);
    }
    if let Ok(val) = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".parse() {
        request.headers_mut().insert("User-Agent", val);
    }
    if let Ok(val) = "zh-CN,zh;q=0.9,en;q=0.8".parse() {
        request.headers_mut().insert("Accept-Language", val);
    }
    request.headers_mut().insert(
        "Cache-Control",
        axum::http::HeaderValue::from_static("no-cache"),
    );
    request
        .headers_mut()
        .insert("Pragma", axum::http::HeaderValue::from_static("no-cache"));

    let timeout_secs: u64 = get_config("grok.timeout", 120u64).await;

    let (mut ws, _) = match connect_async(request).await {
        Ok(v) => v,
        Err(err) => {
            let err_str = err.to_string();
            if err_str.contains("401") {
                return ImagineResult::failed("unauthorized", err_str);
            }
            return ImagineResult::failed("connection_failed", err_str);
        }
    };

    let payload = json!({
        "type": "conversation.item.create",
        "timestamp": chrono::Utc::now().timestamp_millis(),
        "item": {
            "type": "message",
            "content": [{
                "requestId": request_id,
                "text": prompt,
                "type": "input_text",
                "properties": {
                    "section_count": 0,
                    "is_kids_mode": false,
                    "enable_nsfw": enable_nsfw,
                    "skip_upsampler": false,
                    "is_initial": false,
                    "aspect_ratio": aspect_ratio
                }
            }]
        }
    });

    if let Err(err) = ws.send(Message::Text(payload.to_string().into())).await {
        return ImagineResult::failed("send_failed", err.to_string());
    }

    let mut progress = GenerationProgress::new(n);
    let mut error_info: Option<(String, String)> = None;
    let start_time = Instant::now();
    let mut last_activity = Instant::now();
    let mut medium_received_time: Option<Instant> = None;

    while start_time.elapsed() < Duration::from_secs(timeout_secs) {
        match timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(txt)))) => {
                last_activity = Instant::now();
                let msg: JsonValue = match serde_json::from_str(&txt) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");

                if msg_type == "image" {
                    let blob = msg.get("blob").and_then(|v| v.as_str()).unwrap_or("");
                    let url = msg.get("url").and_then(|v| v.as_str()).unwrap_or("");
                    if blob.is_empty() || url.is_empty() {
                        continue;
                    }

                    let image_id = match extract_image_id(url) {
                        Some(v) => v,
                        None => continue,
                    };

                    let blob_size = blob.len();
                    let is_final = is_final_image(url, blob_size);
                    let stage = if is_final {
                        "final"
                    } else if blob_size > 30_000 {
                        if medium_received_time.is_none() {
                            medium_received_time = Some(Instant::now());
                        }
                        "medium"
                    } else {
                        "preview"
                    };

                    let img_progress = ImageProgress {
                        image_id: image_id.clone(),
                        stage: stage.to_string(),
                        blob: blob.to_string(),
                        blob_size,
                        url: url.to_string(),
                        is_final,
                    };

                    let should_update = progress
                        .images
                        .get(&image_id)
                        .map(|existing| !existing.is_final)
                        .unwrap_or(true);

                    if should_update {
                        progress
                            .images
                            .insert(image_id.clone(), img_progress.clone());
                        progress.completed =
                            progress.images.values().filter(|img| img.is_final).count();

                        if let Some(tx) = &progress_tx {
                            let _ = tx.send(ImagineProgressEvent {
                                image_id,
                                stage: img_progress.stage,
                                is_final: img_progress.is_final,
                                completed: progress.completed,
                                total: progress.total,
                            });
                        }
                    }
                } else if msg_type == "error" {
                    let error_code = msg
                        .get("err_code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let error_msg = msg
                        .get("err_msg")
                        .and_then(|v| v.as_str())
                        .unwrap_or("generation failed")
                        .to_string();
                    error_info = Some((error_code.clone(), error_msg.clone()));
                    if error_code == "rate_limit_exceeded" {
                        return ImagineResult::failed(error_code, error_msg);
                    }
                }

                if progress.completed >= n {
                    break;
                }

                if let Some(medium_at) = medium_received_time {
                    if progress.completed == 0 && medium_at.elapsed() > Duration::from_secs(15) {
                        return ImagineResult::failed("blocked", "生成被阻止，无法获取最终图片");
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(data)))) => {
                let _ = ws.send(Message::Pong(data)).await;
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Ok(Message::Frame(_)))) => {
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => {
                return ImagineResult::failed("connection_failed", err.to_string());
            }
            Ok(None) => {
                break;
            }
            Err(_) => {
                if let Some(medium_at) = medium_received_time {
                    if progress.completed == 0 && medium_at.elapsed() > Duration::from_secs(10) {
                        return ImagineResult::failed("blocked", "生成被阻止，无法获取最终图片");
                    }
                }
                if progress.completed > 0 && last_activity.elapsed() > Duration::from_secs(10) {
                    break;
                }
                continue;
            }
        }
    }

    let (urls, b64_list) = save_final_images(progress.clone(), n).await;
    if !urls.is_empty() {
        ImagineResult::ok(urls, b64_list)
    } else if let Some((code, msg)) = error_info {
        ImagineResult::failed(code, msg)
    } else if progress.check_blocked() {
        ImagineResult::failed("blocked", "生成被阻止，无法获取最终图片")
    } else {
        ImagineResult::failed("generation_failed", "未收到图片数据")
    }
}

pub async fn generate(
    prompt: &str,
    size: Option<&str>,
    n: Option<u32>,
    progress_tx: Option<mpsc::UnboundedSender<ImagineProgressEvent>>,
) -> ImagineResult {
    let tokens = get_sso_tokens().await;
    if tokens.is_empty() {
        return ImagineResult::failed("no_available_sso", "没有可用的 SSO");
    }

    let default_count: u32 = get_config("grok.imagine_default_image_count", 4u32).await;
    let mut target_n = n.unwrap_or(default_count.max(1));
    if target_n == 0 {
        target_n = 1;
    }
    if target_n > 4 {
        target_n = 4;
    }

    let daily_limit: i32 = get_config("grok.imagine_sso_daily_limit", 10i32).await;
    let max_blocked_retries: usize = get_config("grok.imagine_blocked_retry", 3usize).await;
    let max_retries: usize = get_config("grok.imagine_max_retries", 5usize).await.max(1);
    let aspect_ratio = size_to_aspect_ratio(size.unwrap_or("1024x1536"));

    let mut blocked_retries = 0usize;
    let mut last_error: Option<ImagineResult> = None;

    for _attempt in 0..max_retries {
        let Some(current_sso) = get_next_sso(&tokens, daily_limit).await else {
            return last_error
                .unwrap_or_else(|| ImagineResult::failed("no_available_sso", "没有可用的 SSO"));
        };

        if get_age_verified(&current_sso).await == 0 {
            if verify_age(&current_sso).await {
                set_age_verified(&current_sso, 1).await;
            }
        }

        let result = do_generate(
            &current_sso,
            prompt,
            aspect_ratio,
            target_n as usize,
            true,
            progress_tx.clone(),
        )
        .await;

        if result.success {
            mark_success(&current_sso).await;
            record_usage(&current_sso).await;
            return result;
        }

        let error_code = result.error_code.clone().unwrap_or_default();
        let error_msg = result.error.clone().unwrap_or_default();

        if error_code == "blocked" {
            blocked_retries += 1;
            mark_failed(&current_sso, "blocked - 无法生成最终图片").await;
            if blocked_retries >= max_blocked_retries {
                return ImagineResult::failed(
                    "blocked",
                    format!("连续 {max_blocked_retries} 次被 blocked，请稍后重试"),
                );
            }
            continue;
        }

        if error_code == "rate_limit_exceeded" || error_code == "unauthorized" {
            mark_failed(&current_sso, &error_msg).await;
            last_error = Some(result);
            continue;
        }

        return result;
    }

    last_error.unwrap_or_else(|| ImagineResult::failed("generation_failed", "所有重试都失败了"))
}
