use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use base64::Engine;
use fs2::FileExt;
use sha1::Digest;
use reqwest::Client;
use serde_json::Value as JsonValue;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use crate::core::config::get_config;
use crate::core::exceptions::ApiError;
use crate::services::grok::statsig::StatsigService;

const UPLOAD_API: &str = "https://grok.com/rest/app-chat/upload-file";
const LIST_API: &str = "https://grok.com/rest/assets";
const DELETE_API: &str = "https://grok.com/rest/assets-metadata";
const DOWNLOAD_API: &str = "https://assets.grok.com";

const DEFAULT_MIME: &str = "application/octet-stream";

fn parse_header_block(block: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(block);
    let mut headers = Vec::new();
    for line in text.lines() {
        if line.starts_with("HTTP/") {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    headers
}

fn split_headers_body(data: &[u8]) -> (Vec<(String, String)>, Vec<u8>) {
    let needle = b"\r\n\r\n";
    if data.len() < needle.len() {
        return (Vec::new(), data.to_vec());
    }
    let mut idx = data.len().saturating_sub(needle.len());
    loop {
        if &data[idx..idx + needle.len()] == needle {
            let headers = parse_header_block(&data[..idx]);
            let body = data[idx + needle.len()..].to_vec();
            return (headers, body);
        }
        if idx == 0 {
            break;
        }
        idx -= 1;
    }
    (Vec::new(), data.to_vec())
}

async fn curl_request(
    proxy: &str,
    timeout: u64,
    method: &str,
    url: &str,
    headers: &reqwest::header::HeaderMap,
    body: Option<&[u8]>,
    capture_headers: bool,
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), ApiError> {
    let use_curl: bool = get_config("grok.use_curl_impersonate", true).await;
    if !use_curl {
        return Err(ApiError::upstream("curl-impersonate is required for Grok requests".to_string()));
    }
    let curl_path: String = get_config("grok.curl_path", "curl-impersonate".to_string()).await;
    let impersonate: String = get_config("grok.curl_impersonate", "chrome136".to_string()).await;
    let resolved_path = if curl_path.trim().is_empty() {
        "curl-impersonate".to_string()
    } else {
        curl_path
    };

    let mut cmd = Command::new(resolved_path);
    cmd.arg("-sS")
        .arg("--compressed")
        .arg("--http2")
        .arg("-X")
        .arg(method)
        .arg(url)
        .arg("--max-time")
        .arg(timeout.to_string())
        .arg("-w")
        .arg("\\n%{http_code}");

    if capture_headers {
        cmd.arg("-i");
    }

    if !proxy.trim().is_empty() {
        cmd.arg("-x").arg(proxy.trim());
    }

    if !impersonate.trim().is_empty() {
        cmd.arg("--impersonate").arg(impersonate.trim());
    }

    for (name, value) in headers.iter() {
        let val = value.to_str().unwrap_or("");
        cmd.arg("-H").arg(format!("{}: {}", name.as_str(), val));
    }

    if body.is_some() {
        cmd.arg("--data-binary").arg("@-");
        cmd.stdin(Stdio::piped());
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| ApiError::upstream(format!("Curl error: {e}")))?;

    if let Some(payload) = body {
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(payload)
                .await
                .map_err(|e| ApiError::upstream(format!("Curl stdin error: {e}")))?;
        }
    }

    let output = child
        .wait_with_output()
        .await
        .map_err(|e| ApiError::upstream(format!("Curl wait error: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ApiError::upstream(format!("Curl failed: {stderr}")));
    }

    let stdout = output.stdout;
    let split = stdout.iter().rposition(|b| *b == b'\n');
    let (data, code_bytes) = match split {
        Some(idx) => (&stdout[..idx], &stdout[idx + 1..]),
        None => (stdout.as_slice(), &stdout[..0]),
    };
    let status: u16 = std::str::from_utf8(code_bytes)
        .unwrap_or("")
        .trim()
        .parse()
        .unwrap_or(0);
    let (parsed_headers, body_bytes) = if capture_headers {
        split_headers_body(data)
    } else {
        (Vec::new(), data.to_vec())
    };
    Ok((status, parsed_headers, body_bytes))
}

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    let key = name.to_lowercase();
    for (k, v) in headers {
        if k.to_lowercase() == key {
            return Some(v.clone());
        }
    }
    None
}

fn lock_dir() -> PathBuf {
    crate::core::config::project_root().join("data").join(".locks")
}

async fn acquire_file_lock(name: &str, timeout: u64) -> Result<std::fs::File, ApiError> {
    let dir = lock_dir();
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| ApiError::server(format!("create lock dir failed: {e}")))?;
    let path = dir.join(format!("{name}.lock"));
    let start = Instant::now();
    loop {
        let file = std::fs::File::options()
            .create(true)
            .write(true)
            .open(&path)
            .map_err(|e| ApiError::server(format!("open lock file failed: {e}")))?;
        if file.try_lock_exclusive().is_ok() {
            return Ok(file);
        }
        if start.elapsed() >= Duration::from_secs(timeout) {
            return Err(ApiError::server(format!("lock timeout: {name}")));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Clone)]
pub struct BaseService {
    pub proxy: String,
    pub timeout: u64,
    client: Client,
    lock: ArcMutex,
}

type ArcMutex = std::sync::Arc<Mutex<()>>;

impl BaseService {
    pub async fn new(proxy: Option<String>) -> Self {
        let proxy = proxy.unwrap_or_default();
        let mut builder = Client::builder();
        if !proxy.is_empty() {
            if let Ok(proxy) = reqwest::Proxy::all(&proxy) {
                builder = builder.proxy(proxy);
            }
        }
        let client = builder.build().unwrap();
        let timeout = get_config("grok.timeout", 120u64).await;
        Self {
            proxy,
            timeout,
            client,
            lock: std::sync::Arc::new(Mutex::new(())),
        }
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn is_url(input: &str) -> bool {
        input.starts_with("http://") || input.starts_with("https://")
    }

    pub async fn headers(&self, token: &str, referer: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "*/*".parse().unwrap());
        headers.insert("Accept-Encoding", "gzip, deflate, br, zstd".parse().unwrap());
        headers.insert("Accept-Language", "zh-CN,zh;q=0.9".parse().unwrap());
        headers.insert("Baggage", "sentry-environment=production,sentry-release=d6add6fb0460641fd482d767a335ef72b9b6abb8,sentry-public_key=b311e0f2690c81f25e2c4cf6d4f7ce1c".parse().unwrap());
        headers.insert("Cache-Control", "no-cache".parse().unwrap());
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("Origin", "https://grok.com".parse().unwrap());
        headers.insert("Pragma", "no-cache".parse().unwrap());
        headers.insert("Priority", "u=1, i".parse().unwrap());
        headers.insert("Referer", referer.parse().unwrap());
        headers.insert("Sec-Ch-Ua", "\"Google Chrome\";v=\"136\", \"Chromium\";v=\"136\", \"Not(A:Brand\";v=\"24\"".parse().unwrap());
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
        headers.insert("x-xai-request-id", Uuid::new_v4().to_string().parse().unwrap());
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

    pub async fn dl_headers(&self, token: &str, file_path: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8".parse().unwrap());
        headers.insert("Sec-Fetch-Dest", "document".parse().unwrap());
        headers.insert("Sec-Fetch-Mode", "navigate".parse().unwrap());
        headers.insert("Sec-Fetch-Site", "same-site".parse().unwrap());
        headers.insert("Sec-Fetch-User", "?1".parse().unwrap());
        headers.insert("Upgrade-Insecure-Requests", "1".parse().unwrap());
        headers.insert("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36".parse().unwrap());
        let raw = token.strip_prefix("sso=").unwrap_or(token);
        let cf: String = get_config("grok.cf_clearance", String::new()).await;
        let cookie = if cf.is_empty() {
            format!("sso={raw}")
        } else {
            format!("sso={raw};cf_clearance={cf}")
        };
        headers.insert("Cookie", cookie.parse().unwrap());
        headers.insert("Referer", "https://grok.com/".parse().unwrap());
        headers
    }

    fn parse_b64(&self, input: &str) -> Result<(String, String, String), ApiError> {
        if let Some(rest) = input.strip_prefix("data:") {
            if let Some((meta, data)) = rest.split_once(',') {
                let mime = meta.split(';').next().unwrap_or(DEFAULT_MIME);
                let b64 = data.trim().to_string();
                let filename = format!("file-{}.bin", uuid::Uuid::new_v4().to_string());
                return Ok((filename, b64, mime.to_string()));
            }
        }
        let filename = format!("file-{}.bin", uuid::Uuid::new_v4().to_string());
        Ok((filename, input.trim().to_string(), DEFAULT_MIME.to_string()))
    }

    async fn fetch_url(&self, url: &str) -> Result<(String, String, String), ApiError> {
        let resp = self
            .client
            .get(url)
            .timeout(std::time::Duration::from_secs(self.timeout))
            .send()
            .await
            .map_err(|e| ApiError::upstream(format!("Fetch failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(ApiError::upstream(format!("Fetch failed: {}", resp.status().as_u16())));
        }
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.map_err(|e| ApiError::upstream(format!("Fetch read failed: {e}")))?;
        let mime = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or(DEFAULT_MIME).to_string())
            .unwrap_or_else(|| DEFAULT_MIME.to_string());
        let filename = Path::new(url)
            .file_name()
            .and_then(|v| v.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("file-{}.bin", uuid::Uuid::new_v4()));
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        Ok((filename, b64, mime))
    }

    pub fn to_b64(&self, path: &Path, mime: &str) -> Result<String, ApiError> {
        let bytes = std::fs::read(path).map_err(|e| ApiError::server(format!("read file failed: {e}")))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        Ok(format!("data:{mime};base64,{b64}"))
    }
}

#[derive(Clone)]
pub struct UploadService {
    base: BaseService,
}

impl UploadService {
    pub async fn new() -> Self {
        let proxy: String = get_config("grok.asset_proxy_url", String::new()).await;
        let base_proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let proxy = if proxy.is_empty() { base_proxy } else { proxy };
        Self { base: BaseService::new(Some(proxy)).await }
    }

    pub async fn upload(&self, file_input: &str, token: &str) -> Result<(String, String), ApiError> {
        let (filename, b64, mime) = if BaseService::is_url(file_input) {
            self.base.fetch_url(file_input).await?
        } else {
            self.base.parse_b64(file_input)?
        };

        let headers = self.base.headers(token, "https://grok.com/").await;
        let payload = serde_json::json!({
            "fileName": filename,
            "fileMimeType": mime,
            "content": b64,
        });
        let body = payload.to_string();
        let (status, _resp_headers, resp_body) = curl_request(
            &self.base.proxy,
            self.base.timeout,
            "POST",
            UPLOAD_API,
            &headers,
            Some(body.as_bytes()),
            false,
        )
        .await?;

        if status == 200 {
            let value: JsonValue = serde_json::from_slice(&resp_body)
                .map_err(|e| ApiError::upstream(format!("Upload parse error: {e}")))?;
            let file_id = value.get("fileMetadataId").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let file_uri = value.get("fileUri").and_then(|v| v.as_str()).unwrap_or("").to_string();
            return Ok((file_id, file_uri));
        }
        Err(ApiError::upstream(format!("Upload failed: {status}")))
    }
}

#[derive(Clone)]
pub struct ListService {
    base: BaseService,
}

impl ListService {
    pub async fn new() -> Self {
        let proxy: String = get_config("grok.asset_proxy_url", String::new()).await;
        let base_proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let proxy = if proxy.is_empty() { base_proxy } else { proxy };
        Self { base: BaseService::new(Some(proxy)).await }
    }

    pub async fn list(&self, token: &str) -> Result<Vec<JsonValue>, ApiError> {
        let mut assets = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let mut params = vec![
                ("pageSize", "50".to_string()),
                ("orderBy", "ORDER_BY_LAST_USE_TIME".to_string()),
                ("source", "SOURCE_ANY".to_string()),
                ("isLatest", "true".to_string()),
            ];
            if let Some(ref token_val) = page_token {
                if seen.contains(token_val) {
                    break;
                }
                seen.insert(token_val.clone());
                params.push(("pageToken", token_val.clone()));
            }

            let headers = self.base.headers(token, "https://grok.com/files").await;
            let mut url = Url::parse(LIST_API).map_err(|e| ApiError::upstream(format!("List url error: {e}")))?;
            {
                let mut pairs = url.query_pairs_mut();
                for (k, v) in params.iter() {
                    pairs.append_pair(k, v);
                }
            }
            let url = url.to_string();
            let (status, _resp_headers, resp_body) = curl_request(
                &self.base.proxy,
                self.base.timeout,
                "GET",
                &url,
                &headers,
                None,
                false,
            )
            .await?;
            if status != 200 {
                return Err(ApiError::upstream(format!("List failed: {status}")));
            }
            let value: JsonValue = serde_json::from_slice(&resp_body)
                .map_err(|e| ApiError::upstream(format!("List parse error: {e}")))?;
            let page_assets = value.get("assets").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            assets.extend(page_assets);
            page_token = value.get("nextPageToken").and_then(|v| v.as_str()).map(|s| s.to_string());
            if page_token.is_none() {
                break;
            }
        }
        Ok(assets)
    }

    pub async fn count(&self, token: &str) -> Result<usize, ApiError> {
        let assets = self.list(token).await?;
        Ok(assets.len())
    }
}

#[derive(Clone)]
pub struct DeleteService {
    base: BaseService,
}

impl DeleteService {
    pub async fn new() -> Self {
        let proxy: String = get_config("grok.asset_proxy_url", String::new()).await;
        let base_proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let proxy = if proxy.is_empty() { base_proxy } else { proxy };
        Self { base: BaseService::new(Some(proxy)).await }
    }

    pub async fn delete(&self, token: &str, asset_id: &str) -> Result<bool, ApiError> {
        let headers = self.base.headers(token, "https://grok.com/files").await;
        let url = format!("{DELETE_API}/{asset_id}");
        let (status, _resp_headers, _resp_body) = curl_request(
            &self.base.proxy,
            self.base.timeout,
            "DELETE",
            &url,
            &headers,
            None,
            false,
        )
        .await?;
        if status == 200 {
            return Ok(true);
        }
        Err(ApiError::upstream(format!("Delete failed: {status}")))
    }

    pub async fn delete_all(&self, token: &str) -> Result<JsonValue, ApiError> {
        let list = ListService::new().await;
        let assets = list.list(token).await.unwrap_or_default();
        if assets.is_empty() {
            return Ok(serde_json::json!({"total":0,"success":0,"failed":0,"skipped":true}));
        }
        let mut total = 0;
        let mut success = 0;
        let mut failed = 0;
        for asset in assets {
            total += 1;
            if let Some(asset_id) = asset.get("assetId").and_then(|v| v.as_str()) {
                match self.delete(token, asset_id).await {
                    Ok(true) => success += 1,
                    _ => failed += 1,
                }
            }
        }
        Ok(serde_json::json!({"total": total, "success": success, "failed": failed}))
    }
}

#[derive(Clone)]
pub struct DownloadService {
    base: BaseService,
    base_dir: PathBuf,
    image_dir: PathBuf,
    video_dir: PathBuf,
    cleanup_running: ArcMutex,
}

impl DownloadService {
    pub async fn new() -> Self {
        let proxy: String = get_config("grok.asset_proxy_url", String::new()).await;
        let base_proxy: String = get_config("grok.base_proxy_url", String::new()).await;
        let proxy = if proxy.is_empty() { base_proxy } else { proxy };
        let base = BaseService::new(Some(proxy)).await;
        let base_dir = crate::core::config::project_root().join("data").join("tmp");
        let image_dir = base_dir.join("image");
        let video_dir = base_dir.join("video");
        let _ = tokio::fs::create_dir_all(&image_dir).await;
        let _ = tokio::fs::create_dir_all(&video_dir).await;
        Self {
            base,
            base_dir,
            image_dir,
            video_dir,
            cleanup_running: std::sync::Arc::new(Mutex::new(())),
        }
    }

    fn cache_path(&self, file_path: &str, media_type: &str) -> PathBuf {
        let dir = if media_type == "image" { &self.image_dir } else { &self.video_dir };
        let filename = file_path.trim_start_matches('/').replace('/', "-");
        dir.join(filename)
    }

    pub async fn download(&self, file_path: &str, token: &str, media_type: &str) -> Result<(PathBuf, String), ApiError> {
        let cache_path = self.cache_path(file_path, media_type);
        if cache_path.exists() {
            let mime = mime_guess::from_path(&cache_path).first_or_octet_stream();
            return Ok((cache_path, mime.to_string()));
        }
        let mut hasher = sha1::Sha1::new();
        hasher.update(file_path.as_bytes());
        let digest = format!("{:x}", hasher.finalize());
        let lock_name = format!("download_{media_type}_{digest}");
        let _file_lock = acquire_file_lock(&lock_name, 10).await?;
        if cache_path.exists() {
            let mime = mime_guess::from_path(&cache_path).first_or_octet_stream();
            return Ok((cache_path, mime.to_string()));
        }
        let mut path = file_path.to_string();
        if !path.starts_with('/') {
            path = format!("/{path}");
        }
        let url = format!("{DOWNLOAD_API}{path}");
        let headers = self.base.dl_headers(token, &path).await;
        let (status, resp_headers, resp_body) = curl_request(
            &self.base.proxy,
            self.base.timeout,
            "GET",
            &url,
            &headers,
            None,
            true,
        )
        .await?;
        if status != 200 {
            return Err(ApiError::upstream(format!("Download failed: {status}")));
        }
        let mime = header_value(&resp_headers, "content-type")
            .and_then(|v| v.split(';').next().map(|s| s.to_string()))
            .unwrap_or_else(|| DEFAULT_MIME.to_string());
        let tmp_path = cache_path.with_extension(format!("{}tmp", cache_path.extension().and_then(|s| s.to_str()).unwrap_or("")));
        tokio::fs::write(&tmp_path, &resp_body)
            .await
            .map_err(|e| ApiError::server(format!("Write tmp file failed: {e}")))?;
        tokio::fs::rename(&tmp_path, &cache_path)
            .await
            .map_err(|e| ApiError::server(format!("Rename tmp file failed: {e}")))?;
        let _ = self.check_limit().await;
        Ok((cache_path, mime))
    }

    pub async fn to_base64(&self, file_path: &str, token: &str, media_type: &str) -> Result<String, ApiError> {
        let (path, mime) = self.download(file_path, token, media_type).await?;
        let data = self.base.to_b64(&path, &mime)?;
        let _ = tokio::fs::remove_file(path).await;
        Ok(data)
    }

    /// 下载文件并上传到配置的存储，返回访问 URL
    pub async fn download_and_upload(&self, file_path: &str, token: &str, media_type: &str) -> Result<String, ApiError> {
        // 先下载到本地（利用现有的下载和缓存逻辑）
        let (local_path, mime) = self.download(file_path, token, media_type).await?;

        // 获取媒体存储实例
        let storage = crate::core::media_storage::get_media_storage().await;

        let url = if let Some(storage) = storage {
            // 读取文件数据
            let data = tokio::fs::read(&local_path).await
                .map_err(|e| ApiError::server(format!("Read file failed: {e}")))?;

            // 上传到存储
            let storage_path = storage.upload(file_path, &data, &mime).await
                .map_err(|e| ApiError::from(e))?;

            // 获取访问 URL（使用代理模式，direct=false）
            storage.get_url(&storage_path, false).await
                .map_err(|e| ApiError::from(e))?
        } else {
            // 如果存储未初始化，降级为本地代理 URL
            let app_url: String = get_config("app.app_url", "http://127.0.0.1:8000".to_string()).await;
            format!("{}/v1/files/{}{}", app_url.trim_end_matches('/'), media_type, file_path)
        };

        Ok(url)
    }

    pub fn get_stats(&self, media_type: &str) -> JsonValue {
        let dir = if media_type == "image" { &self.image_dir } else { &self.video_dir };
        if !dir.exists() {
            return serde_json::json!({"count":0,"size_mb":0.0});
        }
        let mut count = 0usize;
        let mut total_size = 0u64;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        count += 1;
                        total_size += meta.len();
                    }
                }
            }
        }
        let size_mb = (total_size as f64) / 1024.0 / 1024.0;
        serde_json::json!({"count": count, "size_mb": (size_mb*100.0).round()/100.0})
    }

    pub fn list_files(&self, media_type: &str, page: usize, page_size: usize) -> JsonValue {
        let dir = if media_type == "image" { &self.image_dir } else { &self.video_dir };
        if !dir.exists() {
            return serde_json::json!({"total":0,"page":page,"page_size":page_size,"items":[]});
        }
        let mut items = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        let mtime_ms = meta
                            .modified()
                            .ok()
                            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        items.push(serde_json::json!({
                            "name": name,
                            "size_bytes": meta.len(),
                            "mtime_ms": mtime_ms,
                        }));
                    }
                }
            }
        }
        items.sort_by(|a, b| b.get("mtime_ms").and_then(|v| v.as_i64()).cmp(&a.get("mtime_ms").and_then(|v| v.as_i64())));
        let total = items.len();
        let start = page.saturating_sub(1) * page_size;
        let end = (start + page_size).min(total);
        let mut paged = items[start..end].to_vec();

        if media_type == "image" {
            for item in paged.iter_mut() {
                if let Some(name) = item.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()) {
                    item["view_url"] = JsonValue::String(format!("/v1/files/image/{name}"));
                }
            }
        } else {
            let mut preview_map = std::collections::HashMap::new();
            if self.image_dir.exists() {
                if let Ok(entries) = std::fs::read_dir(&self.image_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.is_file() {
                            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                                    preview_map.insert(stem.to_string(), name.to_string());
                                }
                            }
                        }
                    }
                }
            }
            for item in paged.iter_mut() {
                if let Some(name) = item.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()) {
                    item["view_url"] = JsonValue::String(format!("/v1/files/video/{name}"));
                    if let Some(stem) = Path::new(&name).file_stem().and_then(|s| s.to_str()) {
                        if let Some(preview) = preview_map.get(stem) {
                            item["preview_url"] = JsonValue::String(format!("/v1/files/image/{preview}"));
                        }
                    }
                }
            }
        }

        serde_json::json!({"total": total, "page": page, "page_size": page_size, "items": paged})
    }

    pub fn delete_file(&self, media_type: &str, name: &str) -> JsonValue {
        let dir = if media_type == "image" { &self.image_dir } else { &self.video_dir };
        let safe = name.replace('/', "-");
        let path = dir.join(safe);
        if !path.exists() {
            return serde_json::json!({"deleted": false});
        }
        if std::fs::remove_file(&path).is_ok() {
            serde_json::json!({"deleted": true})
        } else {
            serde_json::json!({"deleted": false})
        }
    }

    pub fn clear(&self, media_type: &str) -> JsonValue {
        let dir = if media_type == "image" { &self.image_dir } else { &self.video_dir };
        if !dir.exists() {
            return serde_json::json!({"count":0,"size_mb":0.0});
        }
        let mut count = 0usize;
        let mut total_size = 0u64;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_file() {
                        total_size += meta.len();
                        if std::fs::remove_file(entry.path()).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }
        let size_mb = total_size as f64 / 1024.0 / 1024.0;
        serde_json::json!({"count": count, "size_mb": (size_mb*100.0).round()/100.0})
    }

    pub async fn check_limit(&self) -> Result<(), ApiError> {
        let _guard = self.cleanup_running.lock().await;
        let enable: bool = get_config("cache.enable_auto_clean", true).await;
        if !enable {
            return Ok(());
        }
        let limit_mb: f64 = get_config("cache.limit_mb", 1024f64).await;
        let mut total_size = 0u64;
        let mut files = Vec::new();
        for dir in [&self.image_dir, &self.video_dir] {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        if meta.is_file() {
                            let mtime = meta.modified().ok().and_then(|m| m.elapsed().ok()).map(|e| e.as_secs_f64()).unwrap_or(0.0);
                            total_size += meta.len();
                            files.push((entry.path(), mtime, meta.len()));
                        }
                    }
                }
            }
        }
        let current_mb = total_size as f64 / 1024.0 / 1024.0;
        if current_mb <= limit_mb {
            return Ok(());
        }
        files.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let target_mb = limit_mb * 0.8;
        for (path, _, size) in files {
            let _ = std::fs::remove_file(&path);
            total_size = total_size.saturating_sub(size);
            if (total_size as f64 / 1024.0 / 1024.0) <= target_mb {
                break;
            }
        }
        Ok(())
    }

    pub async fn get_public_url(&self, file_path: &str) -> String {
        let app_url: String = get_config("app.app_url", String::new()).await;
        if app_url.is_empty() {
            let path = if file_path.starts_with('/') { file_path.to_string() } else { format!("/{file_path}") };
            return format!("{DOWNLOAD_API}{path}");
        }
        let path = if file_path.starts_with('/') { file_path.to_string() } else { format!("/{file_path}") };
        format!("{}/v1/files{}", app_url.trim_end_matches('/'), path)
    }
}
