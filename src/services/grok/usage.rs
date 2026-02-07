use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::statsig::StatsigService;
use crate::services::grok::wreq_client::{apply_headers, build_client_with_emulation};

const LIMITS_API: &str = "https://grok.com/rest/rate-limits";

pub struct UsageService;

impl UsageService {
    pub async fn new() -> Self {
        Self
    }

    async fn build_headers(&self, token: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert(
            "Accept-Encoding",
            "gzip, deflate, br, zstd".parse().unwrap(),
        );
        headers.insert("Accept-Language", "zh-CN,zh;q=0.9".parse().unwrap());
        headers.insert("Baggage", "sentry-environment=production,sentry-release=d6add6fb0460641fd482d767a335ef72b9b6abb8,sentry-public_key=b311e0f2690c81f25e2c4cf6d4f7ce1c".parse().unwrap());
        headers.insert("Cache-Control", "no-cache".parse().unwrap());
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Origin", "https://grok.com".parse().unwrap());
        headers.insert("Pragma", "no-cache".parse().unwrap());
        headers.insert("Priority", "u=1, i".parse().unwrap());
        headers.insert("Referer", "https://grok.com/".parse().unwrap());
        headers.insert(
            "Sec-Ch-Ua",
            "\"Google Chrome\";v=\"136\", \"Chromium\";v=\"136\", \"Not(A:Brand\";v=\"24\""
                .parse()
                .unwrap(),
        );
        headers.insert("Sec-Ch-Ua-Arch", "arm".parse().unwrap());
        headers.insert("Sec-Ch-Ua-Bitness", "64".parse().unwrap());
        headers.insert("Sec-Ch-Ua-Mobile", "?0".parse().unwrap());
        headers.insert("Sec-Ch-Ua-Model", "".parse().unwrap());
        headers.insert("Sec-Ch-Ua-Platform", "\"macOS\"".parse().unwrap());
        headers.insert("Sec-Fetch-Dest", "empty".parse().unwrap());
        headers.insert("Sec-Fetch-Mode", "cors".parse().unwrap());
        headers.insert("Sec-Fetch-Site", "same-origin".parse().unwrap());
        headers.insert(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
                .parse()
                .unwrap(),
        );
        let statsig = StatsigService::gen_id().await;
        headers.insert("x-statsig-id", statsig.parse().unwrap());
        headers.insert(
            "x-xai-request-id",
            uuid::Uuid::new_v4().to_string().parse().unwrap(),
        );
        let raw = token.strip_prefix("sso=").unwrap_or(token);
        let cf: String = get_config("grok.cf_clearance", String::new()).await;
        let cookie = if cf.is_empty() {
            format!("sso={raw}")
        } else {
            format!("sso={raw};cf_clearance={cf}")
        };
        headers.insert("Cookie", cookie.parse().unwrap());
        headers
    }

    async fn get_via_wreq(&self, token: &str, model_name: &str) -> Result<JsonValue, ApiError> {
        let headers = self.build_headers(token).await;
        let payload = serde_json::json!({
            "requestKind": "DEFAULT",
            "modelName": model_name,
        });
        let timeout: u64 = get_config("grok.timeout", 10u64).await;
        let proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let usage_emulation: String = get_config("grok.wreq_emulation_usage", String::new()).await;
        let emulation_override = if usage_emulation.trim().is_empty() {
            None
        } else {
            Some(usage_emulation.trim())
        };

        let client = build_client_with_emulation(Some(&proxy), timeout, emulation_override).await?;
        let response = apply_headers(client.post(LIMITS_API), &headers)
            .timeout(Duration::from_secs(timeout.max(1)))
            .body(payload.to_string())
            .send()
            .await
            .map_err(|e| ApiError::upstream(format!("Usage request failed: {e}")))?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| ApiError::upstream(format!("Usage response read failed: {e}")))?;

        let body_text = String::from_utf8_lossy(&bytes).to_string();

        if status != 200 {
            let preview = body_preview(&body_text);
            return Err(ApiError::upstream(format!(
                "Failed to get usage stats: {status}; content-type: {content_type}; body: {preview}"
            )));
        }

        let normalized = normalize_json_text(&body_text);
        if normalized.is_empty() {
            return Err(ApiError::upstream(format!(
                "Usage parse error: empty response body; content-type: {content_type}"
            )));
        }

        serde_json::from_str(&normalized).map_err(|e| {
            let preview = body_preview(&normalized);
            let compression_hint = if bytes.starts_with(&[0x1f, 0x8b]) {
                "; hint: upstream body still looks gzip-compressed"
            } else {
                ""
            };
            ApiError::upstream(format!(
                "Usage parse error: {e}; content-type: {content_type}; body: {preview}{compression_hint}"
            ))
        })
    }

    pub async fn get(&self, token: &str, model_name: &str) -> Result<JsonValue, ApiError> {
        self.get_via_wreq(token, model_name).await
    }
}

fn normalize_json_text(raw: &str) -> String {
    let mut text = raw.trim_start_matches('\u{feff}').trim_start().to_string();

    if text.starts_with(")]}'") {
        if let Some((_, rest)) = text.split_once('\n') {
            text = rest.trim_start().to_string();
        }
    }

    if text.starts_with("for (;;);") {
        text = text.trim_start_matches("for (;;);").trim_start().to_string();
    }

    text.trim().to_string()
}

fn body_preview(text: &str) -> String {
    text.chars()
        .take(200)
        .flat_map(|ch| ch.escape_default())
        .collect::<String>()
}
