use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt};
use reqwest::header::HeaderMap as ReqwestHeaderMap;
use wreq::{Client, Proxy, RequestBuilder};
use wreq_util::Emulation;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;

pub async fn build_client(proxy: Option<&str>, timeout_secs: u64) -> Result<Client, ApiError> {
    build_client_with_emulation(proxy, timeout_secs, None).await
}

pub async fn build_client_with_emulation(
    proxy: Option<&str>,
    timeout_secs: u64,
    emulation_override: Option<&str>,
) -> Result<Client, ApiError> {
    let emulation = if let Some(raw) = emulation_override {
        parse_emulation(raw)
    } else {
        let emulation_raw: String = get_config("grok.wreq_emulation", String::new()).await;
        parse_emulation(emulation_raw.trim())
    };

    let mut builder = Client::builder()
        .emulation(emulation)
        .timeout(Duration::from_secs(timeout_secs.max(1)))
        .connect_timeout(Duration::from_secs(timeout_secs.clamp(5, 30)));

    if let Some(proxy_url) = proxy {
        let trimmed = proxy_url.trim();
        if !trimmed.is_empty() {
            let proxy = Proxy::all(trimmed)
                .map_err(|e| ApiError::upstream(format!("Invalid proxy URL: {e}")))?;
            builder = builder.proxy(proxy);
        }
    }

    builder
        .build()
        .map_err(|e| ApiError::upstream(format!("Build wreq client failed: {e}")))
}

fn parse_emulation(raw: &str) -> Emulation {
    let text = raw.trim().to_ascii_lowercase();
    if text.is_empty() {
        return Emulation::Chrome136;
    }

    let normalized = text.replace('-', "").replace('_', "");

    if normalized.starts_with("chrome") {
        let version: String = normalized
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        return match version.as_str() {
            "100" => Emulation::Chrome100,
            "101" => Emulation::Chrome101,
            "104" => Emulation::Chrome104,
            "105" => Emulation::Chrome105,
            "106" => Emulation::Chrome106,
            "107" => Emulation::Chrome107,
            "108" => Emulation::Chrome108,
            "109" => Emulation::Chrome109,
            "110" => Emulation::Chrome110,
            "114" => Emulation::Chrome114,
            "116" => Emulation::Chrome116,
            "117" => Emulation::Chrome117,
            "118" => Emulation::Chrome118,
            "119" => Emulation::Chrome119,
            "120" => Emulation::Chrome120,
            "123" => Emulation::Chrome123,
            "124" => Emulation::Chrome124,
            "126" => Emulation::Chrome126,
            "127" => Emulation::Chrome127,
            "128" => Emulation::Chrome128,
            "129" => Emulation::Chrome129,
            "130" => Emulation::Chrome130,
            "131" => Emulation::Chrome131,
            "132" => Emulation::Chrome132,
            "133" => Emulation::Chrome133,
            "134" => Emulation::Chrome134,
            "135" => Emulation::Chrome135,
            "136" => Emulation::Chrome136,
            "137" => Emulation::Chrome137,
            "138" => Emulation::Chrome138,
            "139" => Emulation::Chrome139,
            "140" => Emulation::Chrome140,
            "141" => Emulation::Chrome141,
            "142" => Emulation::Chrome142,
            "143" => Emulation::Chrome143,
            _ => Emulation::Chrome136,
        };
    }

    if normalized.starts_with("edge") {
        let version: String = normalized
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        return match version.as_str() {
            "101" => Emulation::Edge101,
            "122" => Emulation::Edge122,
            "127" => Emulation::Edge127,
            "131" => Emulation::Edge131,
            "134" => Emulation::Edge134,
            "135" => Emulation::Edge135,
            "136" => Emulation::Edge136,
            "137" => Emulation::Edge137,
            "138" => Emulation::Edge138,
            "139" => Emulation::Edge139,
            "140" => Emulation::Edge140,
            "141" => Emulation::Edge141,
            "142" => Emulation::Edge142,
            _ => Emulation::Edge136,
        };
    }

    if normalized.starts_with("firefox") {
        let version: String = normalized
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        return match version.as_str() {
            "109" => Emulation::Firefox109,
            "117" => Emulation::Firefox117,
            "128" => Emulation::Firefox128,
            "133" => Emulation::Firefox133,
            "135" => Emulation::Firefox135,
            "136" => Emulation::Firefox136,
            "139" => Emulation::Firefox139,
            "142" => Emulation::Firefox142,
            "143" => Emulation::Firefox143,
            "144" => Emulation::Firefox144,
            "145" => Emulation::Firefox145,
            "146" => Emulation::Firefox146,
            _ => Emulation::Firefox136,
        };
    }

    match normalized.as_str() {
        "safari153" => Emulation::Safari15_3,
        "safari155" => Emulation::Safari15_5,
        "safari16" => Emulation::Safari16,
        "safari165" => Emulation::Safari16_5,
        "safari170" => Emulation::Safari17_0,
        "safari1721" => Emulation::Safari17_2_1,
        "safari1741" => Emulation::Safari17_4_1,
        _ => Emulation::Chrome136,
    }
}

pub fn apply_headers(mut builder: RequestBuilder, headers: &ReqwestHeaderMap) -> RequestBuilder {
    for (name, value) in headers {
        if let Ok(val) = value.to_str() {
            builder = builder.header(name.as_str(), val);
        }
    }
    builder
}

pub fn headers_to_pairs(headers: &wreq::header::HeaderMap) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (name, value) in headers {
        if let Ok(val) = value.to_str() {
            out.push((name.as_str().to_string(), val.to_string()));
        }
    }
    out
}

pub fn line_stream_from_response(response: wreq::Response) -> PinLineStream {
    let body_stream = response.bytes_stream();
    let stream = stream! {
        let mut buffer: Vec<u8> = Vec::new();
        let mut inner = Box::pin(body_stream);

        while let Some(chunk) = inner.as_mut().next().await {
            match chunk {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                    while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                        let mut line = buffer.drain(..=pos).collect::<Vec<u8>>();
                        if matches!(line.last(), Some(b'\n')) {
                            line.pop();
                        }
                        if matches!(line.last(), Some(b'\r')) {
                            line.pop();
                        }
                        yield String::from_utf8_lossy(&line).to_string();
                    }
                }
                Err(err) => {
                    tracing::warn!("wreq stream read failed: {err}");
                    break;
                }
            }
        }

        if !buffer.is_empty() {
            let line = String::from_utf8_lossy(&buffer).to_string();
            if !line.is_empty() {
                yield line;
            }
        }
    };

    Box::pin(stream)
}



pub fn body_preview_from_bytes(bytes: &[u8], max_chars: usize) -> String {
    body_preview(&String::from_utf8_lossy(bytes), max_chars)
}

pub fn body_preview(text: &str, max_chars: usize) -> String {
    text.chars()
        .take(max_chars)
        .flat_map(|ch| ch.escape_default())
        .collect::<String>()
}

pub type PinLineStream = std::pin::Pin<Box<dyn Stream<Item = String> + Send>>;
