use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::core::exceptions::ApiError;

/// 媒体存储错误
#[derive(Debug)]
pub enum StorageError {
    IoError(std::io::Error),
    S3Error(String),
    ConfigError(String),
    UploadTimeout,
    NotFound,
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::IoError(e)
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound => ApiError::not_found("File not found in storage"),
            StorageError::UploadTimeout => ApiError::invalid_request("Upload timeout"),
            StorageError::ConfigError(msg) => ApiError::invalid_request(msg),
            StorageError::IoError(e) => ApiError::server(format!("Storage IO error: {}", e)),
            StorageError::S3Error(e) => ApiError::server(format!("S3 error: {}", e)),
        }
    }
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    Local {
        base_dir: String,
    },
    S3 {
        endpoint: String,
        region: String,
        access_key: String,
        secret_key: String,
        bucket: String,
        #[serde(default)]
        custom_domain: Option<String>,
        #[serde(default = "default_path_prefix")]
        path_prefix: String,
        #[serde(default = "default_upload_timeout")]
        upload_timeout: u64,
        #[serde(default)]
        use_direct_url: bool,
    },
}

fn default_path_prefix() -> String {
    "grok/".to_string()
}

fn default_upload_timeout() -> u64 {
    300
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Local {
            base_dir: "data/tmp".to_string(),
        }
    }
}

/// 媒体存储抽象接口
#[async_trait]
pub trait MediaStorage: Send + Sync {
    /// 上传文件到存储
    ///
    /// # 参数
    /// - `file_path`: 文件路径（如 "/media/xxx/yyy.jpg"）
    /// - `data`: 文件二进制数据
    /// - `mime_type`: MIME 类型
    ///
    /// # 返回
    /// 存储后的文件路径标识
    async fn upload(&self, file_path: &str, data: &[u8], mime_type: &str)
        -> Result<String, StorageError>;

    /// 获取文件访问 URL
    ///
    /// # 参数
    /// - `file_path`: 文件路径标识
    /// - `direct`: 是否返回直链（true: S3直链, false: 代理URL）
    ///
    /// # 返回
    /// 可访问的 URL
    async fn get_url(&self, file_path: &str, direct: bool)
        -> Result<String, StorageError>;

    /// 下载文件（用于代理模式）
    ///
    /// # 返回
    /// (文件数据, MIME类型)
    async fn download(&self, file_path: &str)
        -> Result<(Bytes, String), StorageError>;

    /// 删除文件
    async fn delete(&self, file_path: &str) -> Result<(), StorageError>;

    /// 健康检查
    async fn health_check(&self) -> Result<(), StorageError>;

    /// 获取存储类型名称
    fn storage_type(&self) -> &'static str;

    /// 根据已有的完整 S3 key 直接构建公开访问 URL（不再拼 prefix）
    fn get_public_url(&self, key: &str) -> String;
}

/// 存储工厂：根据配置创建存储实例
pub async fn create_storage(config: StorageConfig) -> Result<Arc<dyn MediaStorage>, StorageError> {
    match config {
        StorageConfig::Local { base_dir } => {
            let storage = super::storage::LocalMediaStorage::new(base_dir)?;
            Ok(Arc::new(storage))
        }
        StorageConfig::S3 {
            endpoint,
            region,
            access_key,
            secret_key,
            bucket,
            custom_domain,
            path_prefix,
            upload_timeout,
            use_direct_url,
        } => {
            let storage = S3Storage::new(
                endpoint,
                region,
                access_key,
                secret_key,
                bucket,
                custom_domain,
                path_prefix,
                upload_timeout,
                use_direct_url,
            ).await?;
            Ok(Arc::new(storage))
        }
    }
}

/// S3/MinIO 存储实现
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    bucket: String,
    custom_domain: Option<String>,
    path_prefix: String,
    upload_timeout: u64,
    use_direct_url: bool,
    endpoint: String,
}

impl S3Storage {
    pub async fn new(
        endpoint: String,
        region: String,
        access_key: String,
        secret_key: String,
        bucket: String,
        custom_domain: Option<String>,
        path_prefix: String,
        upload_timeout: u64,
        use_direct_url: bool,
    ) -> Result<Self, StorageError> {
        use aws_credential_types::Credentials;
        use aws_sdk_s3::config::{Region, Builder};

        // 创建凭证
        let creds = Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "static",
        );

        // 构建配置
        let mut config_builder = Builder::new()
            .region(Region::new(region))
            .credentials_provider(creds);

        // 如果提供了自定义 endpoint（MinIO 或其他 S3 兼容服务）
        if !endpoint.is_empty() && !endpoint.contains("amazonaws.com") {
            config_builder = config_builder
                .endpoint_url(&endpoint)
                .force_path_style(true); // MinIO 需要路径样式
        }

        let config = config_builder.build();
        let client = aws_sdk_s3::Client::from_conf(config);

        Ok(Self {
            client,
            bucket,
            custom_domain,
            path_prefix,
            upload_timeout,
            use_direct_url,
            endpoint,
        })
    }

    /// 构建 S3 对象键
    fn build_key(&self, file_path: &str) -> String {
        let clean_path = file_path.trim_start_matches('/');
        format!("{}{}", self.path_prefix, clean_path)
    }

    /// 构建直链 URL
    fn build_direct_url(&self, key: &str) -> String {
        if let Some(domain) = &self.custom_domain {
            format!("{}/{}", domain.trim_end_matches('/'), key)
        } else if self.endpoint.contains("amazonaws.com") {
            // AWS S3 标准 URL
            format!("https://{}.s3.amazonaws.com/{}", self.bucket, key)
        } else {
            // MinIO 或其他 S3 兼容服务
            format!("{}/{}/{}", self.endpoint.trim_end_matches('/'), self.bucket, key)
        }
    }
}

#[async_trait]
impl MediaStorage for S3Storage {
    async fn upload(&self, file_path: &str, data: &[u8], mime_type: &str)
        -> Result<String, StorageError> {
        let key = self.build_key(file_path);

        let upload_future = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(data.to_vec().into())
            .content_type(mime_type)
            .send();

        // 带超时的上传
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(self.upload_timeout),
            upload_future,
        )
        .await
        .map_err(|_| StorageError::UploadTimeout)?
        .map_err(|e| {
            tracing::error!("S3 PutObject error for key '{}': {:?}", key, e);
            StorageError::S3Error(e.to_string())
        })?;

        tracing::info!("Uploaded to S3: {} (etag: {:?})", key, result.e_tag());
        Ok(key)
    }

    async fn get_url(&self, file_path: &str, _direct: bool)
        -> Result<String, StorageError> {
        let key = self.build_key(file_path);
        // 始终返回直链（custom_domain + path）
        Ok(self.build_direct_url(&key))
    }

    async fn download(&self, file_path: &str)
        -> Result<(Bytes, String), StorageError> {
        let key = self.build_key(file_path);

        let result = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                if e.to_string().contains("NoSuchKey") {
                    StorageError::NotFound
                } else {
                    StorageError::S3Error(e.to_string())
                }
            })?;

        let mime_type = result.content_type()
            .unwrap_or("application/octet-stream")
            .to_string();

        let data = result.body
            .collect()
            .await
            .map_err(|e| StorageError::S3Error(e.to_string()))?
            .into_bytes();

        Ok((data, mime_type))
    }

    async fn delete(&self, file_path: &str) -> Result<(), StorageError> {
        let key = self.build_key(file_path);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| StorageError::S3Error(e.to_string()))?;

        tracing::info!("Deleted from S3: {}", key);
        Ok(())
    }

    async fn health_check(&self) -> Result<(), StorageError> {
        // 尝试列出 bucket（只获取 1 个对象）
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(1)
            .send()
            .await
            .map_err(|e| StorageError::S3Error(format!("Health check failed: {}", e)))?;

        Ok(())
    }

    fn storage_type(&self) -> &'static str {
        "s3"
    }

    fn get_public_url(&self, key: &str) -> String {
        self.build_direct_url(key)
    }
}

// ========== 全局存储管理器 ==========

use once_cell::sync::OnceCell;
use tokio::sync::RwLock;

/// 带降级功能的存储包装器
pub struct FallbackStorage {
    primary: Arc<dyn MediaStorage>,
    fallback: Arc<dyn MediaStorage>,
}

impl FallbackStorage {
    pub fn new(primary: Arc<dyn MediaStorage>, fallback: Arc<dyn MediaStorage>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl MediaStorage for FallbackStorage {
    async fn upload(&self, file_path: &str, data: &[u8], mime_type: &str)
        -> Result<String, StorageError> {
        // 直接上传到主存储（S3），失败则报错，不降级到本地
        match self.primary.upload(file_path, data, mime_type).await {
            Ok(path) => {
                tracing::info!("Uploaded to primary storage ({}): {}", self.primary.storage_type(), path);
                Ok(path)
            }
            Err(e) => {
                tracing::error!("Primary storage ({}) upload failed: {:?}",
                    self.primary.storage_type(), e);
                Err(e)
            }
        }
    }

    async fn get_url(&self, file_path: &str, direct: bool)
        -> Result<String, StorageError> {
        // 先尝试从主存储获取 URL
        match self.primary.get_url(file_path, direct).await {
            Ok(url) => Ok(url),
            Err(_) => {
                // 如果主存储失败，尝试备用存储
                self.fallback.get_url(file_path, direct).await
            }
        }
    }

    async fn download(&self, file_path: &str)
        -> Result<(Bytes, String), StorageError> {
        // 先尝试从主存储下载
        match self.primary.download(file_path).await {
            Ok(result) => Ok(result),
            Err(_) => {
                // 如果主存储失败，尝试备用存储
                self.fallback.download(file_path).await
            }
        }
    }

    async fn delete(&self, file_path: &str) -> Result<(), StorageError> {
        // 尝试从两个存储中删除（忽略错误）
        let _ = self.primary.delete(file_path).await;
        let _ = self.fallback.delete(file_path).await;
        Ok(())
    }

    async fn health_check(&self) -> Result<(), StorageError> {
        // 检查主存储健康状态
        self.primary.health_check().await
    }

    fn storage_type(&self) -> &'static str {
        "fallback"
    }

    fn get_public_url(&self, key: &str) -> String {
        self.primary.get_public_url(key)
    }
}

static MEDIA_STORAGE: OnceCell<RwLock<Option<Arc<dyn MediaStorage>>>> = OnceCell::new();

/// 初始化媒体存储（在应用启动时调用）
pub async fn init_media_storage() -> Result<(), StorageError> {
    let storage_type: String = crate::core::config::get_config("storage.type", "local".to_string()).await;
    let enable_fallback: bool = crate::core::config::get_config("storage.enable_fallback", true).await;

    let storage: Arc<dyn MediaStorage> = match storage_type.as_str() {
        "s3" => {
            let endpoint: String = crate::core::config::get_config("storage.s3.endpoint", String::new()).await;
            let region: String = crate::core::config::get_config("storage.s3.region", "us-east-1".to_string()).await;
            let access_key: String = crate::core::config::get_config("storage.s3.access_key", String::new()).await;
            let secret_key: String = crate::core::config::get_config("storage.s3.secret_key", String::new()).await;
            let bucket: String = crate::core::config::get_config("storage.s3.bucket", "grok-media".to_string()).await;
            let custom_domain: Option<String> = crate::core::config::get_config("storage.s3.custom_domain", None).await;
            let path_prefix: String = crate::core::config::get_config("storage.s3.path_prefix", "grok/".to_string()).await;
            let upload_timeout: u64 = crate::core::config::get_config("storage.s3.upload_timeout", 300).await;
            let use_direct_url: bool = crate::core::config::get_config("storage.s3.use_direct_url", false).await;

            tracing::info!(
                "S3 config: endpoint={}, region={}, bucket={}, path_prefix={}, access_key_len={}, secret_key_len={}",
                endpoint, region, bucket, path_prefix, access_key.len(), secret_key.len()
            );

            let s3_config = StorageConfig::S3 {
                endpoint,
                region,
                access_key,
                secret_key,
                bucket,
                custom_domain,
                path_prefix,
                upload_timeout,
                use_direct_url,
            };

            let s3_storage = create_storage(s3_config).await?;

            // 如果启用降级，创建带降级的存储
            if enable_fallback {
                let base_dir: String = crate::core::config::get_config("storage.local.base_dir", "data/tmp".to_string()).await;
                let local_config = StorageConfig::Local { base_dir };
                let local_storage = create_storage(local_config).await?;

                tracing::info!("Media storage initialized: S3 with local fallback");
                Arc::new(FallbackStorage::new(s3_storage, local_storage))
            } else {
                tracing::info!("Media storage initialized: S3 only (no fallback)");
                s3_storage
            }
        }
        _ => {
            let base_dir: String = crate::core::config::get_config("storage.local.base_dir", "data/tmp".to_string()).await;
            let local_config = StorageConfig::Local { base_dir };
            let storage = create_storage(local_config).await?;
            tracing::info!("Media storage initialized: local");
            storage
        }
    };

    let cell = MEDIA_STORAGE.get_or_init(|| RwLock::new(None));
    *cell.write().await = Some(storage);

    Ok(())
}

/// 获取媒体存储实例
pub async fn get_media_storage() -> Option<Arc<dyn MediaStorage>> {
    let cell = MEDIA_STORAGE.get_or_init(|| RwLock::new(None));
    cell.read().await.clone()
}

/// 重新加载媒体存储配置
pub async fn reload_media_storage() -> Result<(), StorageError> {
    init_media_storage().await
}
