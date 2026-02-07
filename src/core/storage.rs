use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fs2::FileExt;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::core::config::{config_to_toml, project_root, toml_to_json};

#[derive(Debug, Clone)]
pub struct StorageError(pub String);

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StorageError {}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn load_config(&self) -> Result<JsonValue, StorageError>;
    async fn save_config(&self, data: &JsonValue) -> Result<(), StorageError>;
    async fn load_tokens(&self) -> Result<JsonValue, StorageError>;
    async fn save_tokens(&self, data: &JsonValue) -> Result<(), StorageError>;
    async fn with_lock<F, Fut, T>(&self, name: &str, timeout: u64, f: F) -> Result<T, StorageError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, StorageError>> + Send,
        T: Send;
}

pub struct LocalStorage {
    lock: Mutex<()>,
}

impl LocalStorage {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
    }

    fn config_path() -> PathBuf {
        project_root().join("data").join("config.toml")
    }

    fn token_path() -> PathBuf {
        project_root().join("data").join("token.json")
    }

    fn lock_dir() -> PathBuf {
        project_root().join("data").join(".locks")
    }

    async fn acquire_file_lock(name: &str, timeout: u64) -> Result<File, StorageError> {
        let lock_dir = Self::lock_dir();
        if let Err(err) = fs::create_dir_all(&lock_dir) {
            return Err(StorageError(format!("create lock dir failed: {err}")));
        }
        let path = lock_dir.join(format!("{name}.lock"));
        let start = Instant::now();
        loop {
            let file = File::options()
                .create(true)
                .write(true)
                .open(&path)
                .map_err(|e| StorageError(format!("open lock file failed: {e}")))?;
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(file),
                Err(_) => {
                    if start.elapsed() >= Duration::from_secs(timeout) {
                        return Err(StorageError(format!("lock timeout: {name}")));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn load_config(&self) -> Result<JsonValue, StorageError> {
        let path = Self::config_path();
        if !path.exists() {
            return Ok(JsonValue::Object(Default::default()));
        }
        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| StorageError(format!("read config failed: {e}")))?;
        let value: toml::Value = content
            .parse()
            .map_err(|e| StorageError(format!("parse config failed: {e}")))?;
        Ok(toml_to_json(value))
    }

    async fn save_config(&self, data: &JsonValue) -> Result<(), StorageError> {
        let path = Self::config_path();
        let dir = path.parent().unwrap_or(Path::new("."));
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| StorageError(format!("create config dir failed: {e}")))?;
        let toml_value = config_to_toml(data);
        let content = toml::to_string(&toml_value)
            .map_err(|e| StorageError(format!("serialize config failed: {e}")))?;
        let tmp_path = path.with_extension("toml.tmp");
        tokio::fs::write(&tmp_path, content)
            .await
            .map_err(|e| StorageError(format!("write tmp config failed: {e}")))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| StorageError(format!("rename config failed: {e}")))?;
        Ok(())
    }

    async fn load_tokens(&self) -> Result<JsonValue, StorageError> {
        let path = Self::token_path();
        if !path.exists() {
            return Ok(JsonValue::Object(Default::default()));
        }
        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| StorageError(format!("read tokens failed: {e}")))?;
        let value: JsonValue = serde_json::from_str(&content)
            .map_err(|e| StorageError(format!("parse tokens failed: {e}")))?;
        Ok(value)
    }

    async fn save_tokens(&self, data: &JsonValue) -> Result<(), StorageError> {
        let path = Self::token_path();
        let dir = path.parent().unwrap_or(Path::new("."));
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| StorageError(format!("create token dir failed: {e}")))?;
        let content = serde_json::to_string_pretty(data)
            .map_err(|e| StorageError(format!("serialize tokens failed: {e}")))?;
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, content)
            .await
            .map_err(|e| StorageError(format!("write tmp tokens failed: {e}")))?;
        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| StorageError(format!("rename tokens failed: {e}")))?;
        Ok(())
    }

    async fn with_lock<F, Fut, T>(&self, name: &str, timeout: u64, f: F) -> Result<T, StorageError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, StorageError>> + Send,
        T: Send,
    {
        let _guard = tokio::time::timeout(Duration::from_secs(timeout), self.lock.lock())
            .await
            .map_err(|_| StorageError(format!("lock timeout: {name}")))?;
        let file = Self::acquire_file_lock(name, timeout).await?;
        let result = f().await;
        let _ = file.unlock();
        result
    }
}

static STORAGE: once_cell::sync::OnceCell<std::sync::Arc<LocalStorage>> =
    once_cell::sync::OnceCell::new();

pub fn get_storage() -> std::sync::Arc<LocalStorage> {
    STORAGE
        .get_or_init(|| {
            let storage_type =
                std::env::var("SERVER_STORAGE_TYPE").unwrap_or_else(|_| "local".to_string());
            if storage_type.to_lowercase() != "local" {
                tracing::warn!(
                    "Only local storage is supported in Rust version. Requested: {storage_type}"
                );
            }
            std::sync::Arc::new(LocalStorage::new())
        })
        .clone()
}

// ========== 本地媒体存储实现 ==========

use crate::core::media_storage::{MediaStorage, StorageError as MediaStorageError};
use bytes::Bytes;

/// 本地媒体文件存储
pub struct LocalMediaStorage {
    base_dir: PathBuf,
}

impl LocalMediaStorage {
    pub fn new(base_dir: String) -> Result<Self, MediaStorageError> {
        let path = if Path::new(&base_dir).is_absolute() {
            PathBuf::from(base_dir)
        } else {
            project_root().join(&base_dir)
        };

        // 创建基础目录
        std::fs::create_dir_all(&path).map_err(|e| MediaStorageError::IoError(e))?;

        Ok(Self { base_dir: path })
    }

    /// 将文件路径转换为安全的本地路径
    fn safe_path(&self, file_path: &str) -> PathBuf {
        let safe = file_path
            .replace('/', "-")
            .trim_start_matches('-')
            .to_string();
        self.base_dir.join(safe)
    }

    /// 检测媒体类型（image/video）
    fn detect_media_type(&self, mime_type: &str) -> &'static str {
        if mime_type.starts_with("image/") {
            "image"
        } else if mime_type.starts_with("video/") {
            "video"
        } else {
            "media"
        }
    }
}

#[async_trait]
impl MediaStorage for LocalMediaStorage {
    async fn upload(
        &self,
        file_path: &str,
        data: &[u8],
        mime_type: &str,
    ) -> Result<String, MediaStorageError> {
        let media_type = self.detect_media_type(mime_type);
        let dir = self.base_dir.join(media_type);
        tokio::fs::create_dir_all(&dir).await?;

        let local_path = dir.join(file_path.replace('/', "-").trim_start_matches('-'));

        // 原子写入
        let tmp_path = local_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, data).await?;
        tokio::fs::rename(&tmp_path, &local_path).await?;

        tracing::info!("Uploaded to local storage: {}", local_path.display());

        Ok(file_path.to_string())
    }

    async fn get_url(&self, file_path: &str, _direct: bool) -> Result<String, MediaStorageError> {
        // 本地存储总是返回代理 URL
        let app_url =
            crate::core::config::get_config("app.app_url", "http://127.0.0.1:8000".to_string())
                .await;

        // 检测文件类型
        let local_path = self.safe_path(file_path);
        let mime = mime_guess::from_path(&local_path).first_or_octet_stream();
        let media_type = if mime.type_() == "image" {
            "image"
        } else if mime.type_() == "video" {
            "video"
        } else {
            "media"
        };

        Ok(format!(
            "{}/v1/files/{}/{}",
            app_url.trim_end_matches('/'),
            media_type,
            urlencoding::encode(file_path)
        ))
    }

    async fn download(&self, file_path: &str) -> Result<(Bytes, String), MediaStorageError> {
        let local_path = self.safe_path(file_path);

        if !local_path.exists() {
            return Err(MediaStorageError::NotFound);
        }

        let data = tokio::fs::read(&local_path).await?;
        let mime = mime_guess::from_path(&local_path)
            .first_or_octet_stream()
            .to_string();

        Ok((Bytes::from(data), mime))
    }

    async fn delete(&self, file_path: &str) -> Result<(), MediaStorageError> {
        let local_path = self.safe_path(file_path);

        if local_path.exists() {
            tokio::fs::remove_file(&local_path).await?;
            tracing::info!("Deleted from local storage: {}", local_path.display());
        }

        Ok(())
    }

    async fn health_check(&self) -> Result<(), MediaStorageError> {
        // 检查基础目录是否可访问
        if !self.base_dir.exists() {
            tokio::fs::create_dir_all(&self.base_dir).await?;
        }

        // 尝试创建测试文件
        let test_file = self.base_dir.join(".health_check");
        tokio::fs::write(&test_file, b"ok").await?;
        tokio::fs::remove_file(&test_file).await?;

        Ok(())
    }

    fn storage_type(&self) -> &'static str {
        "local"
    }
}

/*
  1. LocalStorage

  - 用途：存储配置文件和Token数据
  - 存储内容：
    - data/config.toml - 应用配置
    - data/token.json - Token信息
  - 特点：
    - 使用文件锁机制保证并发安全
    - 支持 TOML 和 JSON 格式
    - 实现 Storage trait

  2. LocalMediaStorage

  - 用途：存储媒体文件（图片/视频）
  - 存储内容：
    - data/tmp/image/ - 图片文件
    - data/tmp/video/ - 视频文件
  - 特点：
    - 处理二进制数据
    - 自动检测 MIME 类型
    - 生成访问 URL
    - 实现 MediaStorage trait

  为什么不能混用？

  // LocalStorage 的接口
  trait Storage {
      async fn load_config() -> JsonValue;
      async fn save_config(data: &JsonValue);
      async fn load_tokens() -> JsonValue;
      async fn save_tokens(data: &JsonValue);
  }

  // LocalMediaStorage 的接口
  trait MediaStorage {
      async fn upload(file_path: &str, data: &[u8], mime_type: &str) -> String;
      async fn get_url(file_path: &str, direct: bool) -> String;
      async fn download(file_path: &str) -> (Bytes, String);
      async fn delete(file_path: &str);
  }

  它们的接口完全不同，职责也不同：
  - LocalStorage 是配置管理
  - LocalMediaStorage 是媒体文件管理

  架构设计

  这是一个很好的关注点分离设计：
  - 配置存储需要事务性、锁机制
  - 媒体存储需要 URL 生成、MIME 类型处理、S3 兼容

  如果混在一起会导致代码混乱，职责不清。

  总结：这两个存储虽然都叫"Storage"，但是完全不同的东西，不能也不应该合并。
*/
