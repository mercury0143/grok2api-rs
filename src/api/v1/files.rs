use axum::http::{HeaderMap, StatusCode};
use axum::{
    Router,
    extract::Path,
    response::{IntoResponse, Response},
    routing::get,
};
use mime_guess::MimeGuess;

use crate::core::config::get_config;

pub fn router() -> Router {
    Router::new()
        .route("/v1/files/image/*file", get(get_image))
        .route("/images/*file", get(get_image_alias))
        .route("/v1/files/video/*file", get(get_video))
        .route("/v1/files/s3/*file", get(get_s3_file))
}

async fn serve_file(file: String, media_type: &str) -> Response {
    let safe = file.replace('/', "-");

    // 首先尝试从媒体存储读取
    if let Some(storage) = crate::core::media_storage::get_media_storage().await {
        let file_path = format!("/{}", file.trim_start_matches('/'));
        match storage.download(&file_path).await {
            Ok((data, mime)) => {
                let mut headers = HeaderMap::new();
                headers.insert("Cache-Control", "public, max-age=31536000, immutable".parse().unwrap());
                headers.insert("Content-Type", mime.parse().unwrap());
                return (headers, data).into_response();
            }
            Err(e) => {
                tracing::error!("Failed to download from storage: {:?}", e);
            }
        }
    }

    // 降级：尝试从本地缓存读取（兼容旧数据）
    let base = crate::core::config::project_root().join("data").join("tmp");
    let dir = if media_type == "image" {
        base.join("image")
    } else {
        base.join("video")
    };
    let local_path = dir.join(&safe);
    if let Ok(bytes) = tokio::fs::read(&local_path).await {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            "public, max-age=31536000, immutable".parse().unwrap(),
        );
        let mime = if media_type == "image" {
            MimeGuess::from_path(&local_path)
                .first_or_octet_stream()
                .to_string()
        } else {
            "video/mp4".to_string()
        };
        headers.insert("Content-Type", mime.parse().unwrap());
        return (headers, bytes).into_response();
    }

    (StatusCode::NOT_FOUND, "File not found").into_response()
}

async fn get_image(Path(file): Path<String>) -> Response {
    let enabled: bool = get_config("downstream.enable_files", true).await;
    if !enabled {
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    }
    serve_file(file, "image").await
}

async fn get_image_alias(Path(file): Path<String>) -> Response {
    let enabled: bool = get_config("downstream.enable_files", true).await;
    if !enabled {
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    }
    serve_file(file, "image").await
}

async fn get_video(Path(file): Path<String>) -> Response {
    let enabled: bool = get_config("downstream.enable_files", true).await;
    if !enabled {
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    }
    serve_file(file, "video").await
}

async fn get_s3_file(Path(file): Path<String>) -> Response {
    let enabled: bool = get_config("downstream.enable_files", true).await;
    if !enabled {
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    }

    // URL 解码文件路径（路径可能被 urlencoding::encode 编码过）
    let decoded = urlencoding::decode(&file).unwrap_or(std::borrow::Cow::Borrowed(&file));
    let file_path = decoded.as_ref();

    // 从媒体存储读取
    if let Some(storage) = crate::core::media_storage::get_media_storage().await {
        match storage.download(file_path).await {
            Ok((data, mime)) => {
                let mut headers = HeaderMap::new();
                headers.insert("Cache-Control", "public, max-age=31536000, immutable".parse().unwrap());
                headers.insert("Content-Type", mime.parse().unwrap());
                return (headers, data).into_response();
            }
            Err(e) => {
                tracing::error!("Failed to download from storage: {:?}", e);
            }
        }
    }

    // 降级：尝试从本地缓存读取
    let safe = file_path.replace('/', "-").trim_start_matches('-').to_string();
    let base = crate::core::config::project_root().join("data").join("tmp");

    // 尝试 image 和 video 目录
    for subdir in &["image", "video"] {
        let local_path = base.join(subdir).join(&safe);
        if let Ok(bytes) = tokio::fs::read(&local_path).await {
            let mut headers = HeaderMap::new();
            headers.insert("Cache-Control", "public, max-age=31536000, immutable".parse().unwrap());
            let mime = MimeGuess::from_path(&local_path)
                .first_or_octet_stream()
                .to_string();
            headers.insert("Content-Type", mime.parse().unwrap());
            return (headers, bytes).into_response();
        }
    }

    (StatusCode::NOT_FOUND, "File not found").into_response()
}
