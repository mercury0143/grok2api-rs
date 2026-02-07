use std::collections::HashMap;

use base64::Engine;
use urlencoding::decode;

pub fn encode_grpc_web_payload(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + data.len());
    out.push(0x00);
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
    out
}

fn maybe_decode_grpc_web_text(body: &[u8], content_type: Option<&str>) -> Vec<u8> {
    let ct = content_type.unwrap_or("").to_lowercase();
    if ct.contains("grpc-web-text") {
        let compact: Vec<u8> = body
            .iter()
            .cloned()
            .filter(|b| !b"\r\n \t".contains(b))
            .collect();
        return base64::engine::general_purpose::STANDARD
            .decode(compact)
            .unwrap_or_else(|_| body.to_vec());
    }

    let head = &body[..body.len().min(2048)];
    if head.iter().all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' | b'=' | b'\r' | b'\n')) {
        let compact: Vec<u8> = body.iter().cloned().filter(|b| !b"\r\n \t".contains(b)).collect();
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(compact) {
            return decoded;
        }
    }
    body.to_vec()
}

fn parse_trailer_block(payload: &[u8]) -> HashMap<String, String> {
    let text = String::from_utf8_lossy(payload);
    let mut map = HashMap::new();
    for line in text
        .split(|c| c == '\n' || c == '\r')
        .filter(|l| !l.is_empty())
    {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_lowercase();
            let mut val = v.trim().to_string();
            if key == "grpc-message" {
                if let Ok(decoded) = decode(&val) {
                    val = decoded.to_string();
                }
            }
            map.insert(key, val);
        }
    }
    map
}

pub fn parse_grpc_web_response(
    body: &[u8],
    content_type: Option<&str>,
    headers: Option<&reqwest::header::HeaderMap>,
) -> (Vec<Vec<u8>>, HashMap<String, String>) {
    let decoded = maybe_decode_grpc_web_text(body, content_type);
    let mut messages = Vec::new();
    let mut trailers = HashMap::new();

    let mut i = 0;
    while i + 5 <= decoded.len() {
        let flag = decoded[i];
        let len = u32::from_be_bytes([
            decoded[i + 1],
            decoded[i + 2],
            decoded[i + 3],
            decoded[i + 4],
        ]) as usize;
        i += 5;
        if i + len > decoded.len() {
            break;
        }
        let payload = &decoded[i..i + len];
        i += len;
        if flag & 0x80 == 0x80 {
            trailers.extend(parse_trailer_block(payload));
        } else {
            messages.push(payload.to_vec());
        }
    }

    if let Some(h) = headers {
        if let Some(v) = h.get("grpc-status") {
            trailers
                .entry("grpc-status".to_string())
                .or_insert_with(|| v.to_str().unwrap_or("").to_string());
        }
        if let Some(v) = h.get("grpc-message") {
            trailers
                .entry("grpc-message".to_string())
                .or_insert_with(|| {
                    decode(v.to_str().unwrap_or(""))
                        .unwrap_or_else(|_| "".into())
                        .to_string()
                });
        }
    }

    (messages, trailers)
}

#[derive(Debug, Clone)]
pub struct GrpcStatus {
    pub code: i32,
    pub message: String,
}

impl GrpcStatus {
    pub fn ok(&self) -> bool {
        self.code == 0
    }

    pub fn http_equiv(&self) -> u16 {
        match self.code {
            0 => 200,
            16 => 401,
            7 => 403,
            8 => 429,
            4 => 504,
            14 => 503,
            _ => 502,
        }
    }
}

pub fn get_grpc_status(trailers: &HashMap<String, String>) -> GrpcStatus {
    let raw = trailers.get("grpc-status").cloned().unwrap_or_default();
    let msg = trailers.get("grpc-message").cloned().unwrap_or_default();
    let code = raw.parse::<i32>().unwrap_or(-1);
    GrpcStatus { code, message: msg }
}
