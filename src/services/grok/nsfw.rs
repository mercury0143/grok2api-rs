use std::time::Duration;

use urlencoding::decode;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::grpc_web::{
    encode_grpc_web_payload, get_grpc_status, parse_grpc_web_response,
};
use crate::services::grok::wreq_client::{
    apply_headers, body_preview as body_preview_text, build_client_with_emulation,
};

const NSFW_API: &str = "https://grok.com/auth_mgmt.AuthManagement/UpdateUserFeatureControls";
const AGE_VERIFY_API: &str = "https://grok.com/rest/auth/set-birth-date";
const NSFW_FALLBACK_EMULATION: &str = "chrome_116";
const NSFW_FEATURES_PATH: &str = "features";
const NSFW_FEATURES_ENABLED_PATH: &str = "features.enabled";

// Legacy protobuf layout kept as compatibility fallback.
const NSFW_PROTO_PAYLOAD_LEGACY: &[u8] = &[0x08, 0x01, 0x10, 0x01];

#[derive(Debug, Clone)]
pub struct NsfwResult {
    pub success: bool,
    pub http_status: u16,
    pub grpc_status: Option<i32>,
    pub grpc_message: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct NsfwService;

impl NsfwService {
    pub async fn new() -> Self {
        Self
    }

    async fn build_headers(&self, token: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("accept", "*/*".parse().unwrap());
        headers.insert(
            "content-type",
            "application/grpc-web+proto".parse().unwrap(),
        );
        headers.insert("origin", "https://grok.com".parse().unwrap());
        headers.insert("referer", "https://grok.com/".parse().unwrap());
        headers.insert(
            "user-agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                .parse()
                .unwrap(),
        );
        headers.insert("x-grpc-web", "1".parse().unwrap());
        headers.insert("x-user-agent", "connect-es/2.1.1".parse().unwrap());
        let cookie = build_cookie(token).await;
        headers.insert("cookie", cookie.parse().unwrap());
        headers
    }

    pub async fn enable(&self, token: &str) -> NsfwResult {
        let preferred_emulation: String =
            get_config("grok.wreq_emulation_nsfw", String::new()).await;
        let preferred_emulation = preferred_emulation.trim().to_string();
        let preferred = if preferred_emulation.is_empty() {
            None
        } else {
            Some(preferred_emulation.as_str())
        };

        let first = self.try_enable(token, preferred).await;
        if first.success {
            return first;
        }

        if should_retry_with_fallback(&first)
            && !emulation_equals(preferred_emulation.as_str(), NSFW_FALLBACK_EMULATION)
        {
            tracing::warn!(
                "NSFW enable primary attempt failed (status={}, grpc={:?}); retry with fallback emulation {}",
                first.http_status,
                first.grpc_status,
                NSFW_FALLBACK_EMULATION
            );
            let mut second = self.try_enable(token, Some(NSFW_FALLBACK_EMULATION)).await;
            if second.success {
                return second;
            }

            second.error = Some(format!(
                "primary attempt failed: {}; fallback attempt failed: {}",
                first.error.unwrap_or_else(|| "unknown".to_string()),
                second.error.unwrap_or_else(|| "unknown".to_string())
            ));
            return second;
        }

        first
    }

    async fn try_enable(&self, token: &str, emulation_override: Option<&str>) -> NsfwResult {
        match self.enable_via_wreq(token, emulation_override).await {
            Ok(result) => result,
            Err(err) => NsfwResult {
                success: false,
                http_status: 0,
                grpc_status: None,
                grpc_message: None,
                error: Some(err.to_string()),
            },
        }
    }

    async fn enable_via_wreq(
        &self,
        token: &str,
        emulation_override: Option<&str>,
    ) -> Result<NsfwResult, ApiError> {
        // Prefer request shape with explicit FieldMask (field #2), which upstream may require.
        let primary_payload = build_proto_payload(2, NSFW_FEATURES_PATH);
        let mut result = self
            .send_enable_request(token, emulation_override, &primary_payload)
            .await?;
        if result.success {
            return Ok(result);
        }

        if should_retry_with_alternate_mask_field(&result) {
            tracing::warn!(
                "NSFW enable reports missing field mask (grpc={:?}); retry with alternate field mask field #3",
                result.grpc_status,
            );
            let payload = build_proto_payload(3, NSFW_FEATURES_PATH);
            let mut second = self
                .send_enable_request(token, emulation_override, &payload)
                .await?;
            if second.success {
                return Ok(second);
            }
            second.error = Some(format!(
                "primary payload failed: {}; alternate mask field failed: {}",
                result.error.unwrap_or_else(|| "unknown".to_string()),
                second.error.unwrap_or_else(|| "unknown".to_string()),
            ));
            result = second;
        }

        if should_retry_with_alternate_mask_path(&result) {
            tracing::warn!(
                "NSFW enable reports invalid field mask path (grpc={:?}); retry with `features.enabled` path",
                result.grpc_status,
            );
            let payload = build_proto_payload(2, NSFW_FEATURES_ENABLED_PATH);
            let mut third = self
                .send_enable_request(token, emulation_override, &payload)
                .await?;
            if third.success {
                return Ok(third);
            }
            third.error = Some(format!(
                "previous payload failed: {}; alternate mask path failed: {}",
                result.error.unwrap_or_else(|| "unknown".to_string()),
                third.error.unwrap_or_else(|| "unknown".to_string()),
            ));
            result = third;
        }

        if should_retry_with_legacy_payload(&result) {
            tracing::warn!(
                "NSFW enable payload decode failed (grpc={:?}); retry with legacy protobuf payload",
                result.grpc_status,
            );
            let mut legacy = self
                .send_enable_request(token, emulation_override, NSFW_PROTO_PAYLOAD_LEGACY)
                .await?;
            if legacy.success {
                return Ok(legacy);
            }
            legacy.error = Some(format!(
                "previous payload failed: {}; legacy payload failed: {}",
                result.error.unwrap_or_else(|| "unknown".to_string()),
                legacy.error.unwrap_or_else(|| "unknown".to_string()),
            ));
            result = legacy;
        }

        if should_retry_with_age_verify(&result) {
            tracing::warn!(
                "NSFW gRPC endpoint rejected payload shape (grpc={:?}); fallback to age verification endpoint",
                result.grpc_status
            );
            let mut age_result = self.verify_age_via_rest(token, emulation_override).await?;
            if age_result.success {
                return Ok(age_result);
            }
            age_result.error = Some(format!(
                "gRPC update failed: {}; age-verify fallback failed: {}",
                result.error.unwrap_or_else(|| "unknown".to_string()),
                age_result.error.unwrap_or_else(|| "unknown".to_string()),
            ));
            result = age_result;
        }

        Ok(result)
    }

    async fn send_enable_request(
        &self,
        token: &str,
        emulation_override: Option<&str>,
        proto_payload: &[u8],
    ) -> Result<NsfwResult, ApiError> {
        let headers = self.build_headers(token).await;
        let payload = encode_grpc_web_payload(proto_payload);
        let timeout: u64 = get_config("grok.timeout", 30u64).await;
        let proxy: String = get_config("grok.base_proxy_url", String::new()).await;

        let client = build_client_with_emulation(Some(&proxy), timeout, emulation_override).await?;
        let response = apply_headers(client.post(NSFW_API), &headers)
            .timeout(Duration::from_secs(timeout.max(1)))
            .body(payload)
            .send()
            .await
            .map_err(|e| ApiError::upstream(format!("NSFW request failed: {e}")))?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let grpc_status_header = response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());
        let grpc_message_header = response
            .headers()
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());

        let bytes = response
            .bytes()
            .await
            .map_err(|e| ApiError::upstream(format!("NSFW response read failed: {e}")))?;

        if status != 200 {
            let preview = body_preview(&bytes);
            tracing::warn!(
                "NSFW enable HTTP failure status={} content_type={} body={}",
                status,
                content_type,
                preview
            );
            return Ok(NsfwResult {
                success: false,
                http_status: status,
                grpc_status: grpc_status_header
                    .as_deref()
                    .and_then(|v| v.parse::<i32>().ok()),
                grpc_message: grpc_message_header,
                error: Some(format!(
                    "HTTP {status}; content-type: {content_type}; body: {preview}"
                )),
            });
        }

        let (_, mut trailers) = parse_grpc_web_response(&bytes, Some(&content_type), None);
        if let Some(code) = grpc_status_header {
            trailers.entry("grpc-status".to_string()).or_insert(code);
        }
        if let Some(message) = grpc_message_header {
            let decoded = decode(&message).map(|v| v.to_string()).unwrap_or(message);
            trailers.entry("grpc-message".to_string()).or_insert(decoded);
        }

        let grpc = get_grpc_status(&trailers);
        let success = grpc.code == -1 || grpc.ok();
        let grpc_message = if grpc.message.is_empty() {
            None
        } else {
            Some(grpc.message.clone())
        };

        let error = if success {
            None
        } else {
            let preview = body_preview(&bytes);
            tracing::warn!(
                "NSFW enable gRPC failure status={} grpc_code={} grpc_message={} body={}",
                status,
                grpc.code,
                grpc.message,
                preview
            );
            Some(format!(
                "gRPC error: code={}, message={}, body={}",
                grpc.code, grpc.message, preview
            ))
        };

        Ok(NsfwResult {
            success,
            http_status: status,
            grpc_status: Some(grpc.code),
            grpc_message,
            error,
        })
    }

    async fn verify_age_via_rest(
        &self,
        token: &str,
        emulation_override: Option<&str>,
    ) -> Result<NsfwResult, ApiError> {
        let timeout: u64 = get_config("grok.timeout", 30u64).await;
        let proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let cookie = build_cookie(token).await;

        let client = build_client_with_emulation(Some(&proxy), timeout, emulation_override).await?;
        let response = client
            .post(AGE_VERIFY_API)
            .timeout(Duration::from_secs(timeout.max(1)))
            .header(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            )
            .header("Origin", "https://grok.com")
            .header("Referer", "https://grok.com/")
            .header("Accept", "*/*")
            .header("Cookie", cookie)
            .header("Content-Type", "application/json")
            .body(r#"{"birthDate":"2001-01-01T16:00:00.000Z"}"#)
            .send()
            .await
            .map_err(|e| ApiError::upstream(format!("NSFW age verify request failed: {e}")))?;

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let body = response.text().await.unwrap_or_else(|_| String::new());
        let preview = body_preview_text(&body, 220);

        if status == 200 {
            tracing::info!("NSFW fallback age verify success");
            return Ok(NsfwResult {
                success: true,
                http_status: 200,
                grpc_status: Some(0),
                grpc_message: Some("fallback age verify success".to_string()),
                error: None,
            });
        }

        tracing::warn!(
            "NSFW fallback age verify failed status={} content_type={} body={}",
            status,
            content_type,
            preview
        );
        Ok(NsfwResult {
            success: false,
            http_status: status,
            grpc_status: Some(3),
            grpc_message: Some("age verify fallback failed".to_string()),
            error: Some(format!(
                "HTTP {status}; content-type: {content_type}; body: {preview}"
            )),
        })
    }
}

async fn build_cookie(token: &str) -> String {
    let raw = token.strip_prefix("sso=").unwrap_or(token);
    let cf: String = get_config("grok.cf_clearance", String::new()).await;
    if cf.trim().is_empty() {
        format!("sso={raw}; sso-rw={raw}")
    } else {
        format!("sso={raw}; sso-rw={raw}; cf_clearance={}", cf.trim())
    }
}

fn build_proto_payload(mask_field_number: u8, mask_path: &str) -> Vec<u8> {
    let mut out = vec![0x0A, 0x04, 0x08, 0x01, 0x10, 0x01];

    // field_mask: google.protobuf.FieldMask { paths: [mask_path] }
    let path_bytes = mask_path.as_bytes();
    if path_bytes.len() > u8::MAX as usize {
        return out;
    }

    let mut field_mask = Vec::with_capacity(path_bytes.len() + 2);
    field_mask.push(0x0A); // paths field #1 (string)
    field_mask.push(path_bytes.len() as u8);
    field_mask.extend_from_slice(path_bytes);

    if field_mask.len() > u8::MAX as usize {
        return out;
    }

    let top_level_tag = (mask_field_number << 3) | 0x02; // length-delimited
    out.push(top_level_tag);
    out.push(field_mask.len() as u8);
    out.extend_from_slice(&field_mask);

    out
}

fn body_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .take(220)
        .flat_map(|ch| ch.escape_default())
        .collect::<String>()
}

fn emulation_equals(left: &str, right: &str) -> bool {
    normalize_emulation(left) == normalize_emulation(right)
}

fn normalize_emulation(input: &str) -> String {
    input
        .trim()
        .to_ascii_lowercase()
        .replace(['-', '_'], "")
        .replace(' ', "")
}

fn is_auth_related(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("auth")
        || lower.contains("permission")
        || lower.contains("token")
}

fn is_payload_decode_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("failed to decode protobuf")
        || lower.contains("invalid wire type")
        || lower.contains("updateuserfeaturecontrolsrequest.features")
}

fn should_retry_with_alternate_mask_field(result: &NsfwResult) -> bool {
    if result.success || result.grpc_status != Some(3) {
        return false;
    }

    result
        .grpc_message
        .as_deref()
        .map(is_field_mask_missing)
        .unwrap_or(false)
        || result
            .error
            .as_deref()
            .map(is_field_mask_missing)
            .unwrap_or(false)
}

fn should_retry_with_alternate_mask_path(result: &NsfwResult) -> bool {
    if result.success || result.grpc_status != Some(3) {
        return false;
    }

    result
        .grpc_message
        .as_deref()
        .map(is_invalid_field_mask_path)
        .unwrap_or(false)
        || result
            .error
            .as_deref()
            .map(is_invalid_field_mask_path)
            .unwrap_or(false)
}

fn should_retry_with_age_verify(result: &NsfwResult) -> bool {
    if result.success || result.grpc_status != Some(3) {
        return false;
    }

    result
        .grpc_message
        .as_deref()
        .map(is_rejected_features_field)
        .unwrap_or(false)
        || result
            .error
            .as_deref()
            .map(is_rejected_features_field)
            .unwrap_or(false)
}

fn is_field_mask_missing(text: &str) -> bool {
    text.to_ascii_lowercase()
        .contains("field mask must be provided")
}

fn is_invalid_field_mask_path(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("invalid field mask")
        || lower.contains("fieldmask")
        || lower.contains("cannot find field")
        || lower.contains("unknown path")
}

fn is_rejected_features_field(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("invalid field: features")
        || lower.contains("field mask must be provided")
        || lower.contains("invalid field")
}

fn should_retry_with_legacy_payload(result: &NsfwResult) -> bool {
    if result.success || result.grpc_status != Some(13) {
        return false;
    }

    if let Some(message) = &result.grpc_message {
        if is_payload_decode_error(message) {
            return true;
        }
    }

    if let Some(error) = &result.error {
        if is_payload_decode_error(error) {
            return true;
        }
    }

    false
}

fn should_retry_with_fallback(result: &NsfwResult) -> bool {
    if result.success {
        return false;
    }

    if result.http_status == 401 || result.http_status == 403 {
        return true;
    }

    if matches!(result.grpc_status, Some(7) | Some(16)) {
        return true;
    }

    if let Some(message) = &result.grpc_message {
        if is_auth_related(message) {
            return true;
        }
    }

    if let Some(error) = &result.error {
        if is_auth_related(error) {
            return true;
        }
    }

    false
}
