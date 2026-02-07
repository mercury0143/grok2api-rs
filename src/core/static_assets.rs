use std::borrow::Cow;

use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use mime_guess::from_path;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "static"]
struct EmbeddedAssets;

pub fn get_bytes(path: &str) -> Option<Cow<'static, [u8]>> {
    EmbeddedAssets::get(path).map(|f| f.data)
}

pub fn get_text(path: &str) -> Option<String> {
    get_bytes(path).and_then(|data| String::from_utf8(data.into_owned()).ok())
}

pub async fn static_handler(Path(path): Path<String>) -> Response {
    let clean = path.trim_start_matches('/');
    if clean.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(data) = get_bytes(clean) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let mime = from_path(clean).first_or_octet_stream();
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, mime.to_string().parse().unwrap());
    headers.insert(
        header::CACHE_CONTROL,
        "public, max-age=31536000, immutable".parse().unwrap(),
    );

    (headers, Body::from(data.into_owned())).into_response()
}
