use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use tokio::sync::{OnceCell, RwLock};

use crate::core::storage::{Storage, StorageError, get_storage};

static CONFIG: OnceCell<Arc<Config>> = OnceCell::const_new();

#[derive(Debug)]
pub struct Config {
    inner: RwLock<JsonValue>,
    defaults: RwLock<Option<JsonValue>>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(JsonValue::Object(Default::default())),
            defaults: RwLock::new(None),
        }
    }

    async fn ensure_defaults(&self) {
        let mut defaults = self.defaults.write().await;
        if defaults.is_some() {
            return;
        }
        let value = load_defaults().unwrap_or(JsonValue::Object(Default::default()));
        *defaults = Some(value);
    }

    pub async fn load(&self) -> Result<(), StorageError> {
        self.ensure_defaults().await;
        let defaults = self
            .defaults
            .read()
            .await
            .clone()
            .unwrap_or(JsonValue::Object(Default::default()));
        let storage = get_storage();

        let mut from_remote = true;
        let mut config_data = storage.load_config().await.ok();
        if config_data.is_none() {
            from_remote = false;
            let local = crate::core::storage::LocalStorage::new();
            config_data = local.load_config().await.ok();
        }
        let config_data = config_data.unwrap_or(JsonValue::Object(Default::default()));
        let merged = deep_merge(&defaults, &config_data);

        let should_persist = !from_remote || merged != config_data;
        if should_persist {
            storage
                .with_lock("config_save", 10, || async {
                    storage.save_config(&merged).await
                })
                .await?;
        }

        let mut inner = self.inner.write().await;
        *inner = merged;
        Ok(())
    }

    pub async fn update(&self, new_config: &JsonValue) -> Result<(), StorageError> {
        self.ensure_defaults().await;
        let defaults = self
            .defaults
            .read()
            .await
            .clone()
            .unwrap_or(JsonValue::Object(Default::default()));
        let current = self.inner.read().await.clone();
        let base = deep_merge(&defaults, &current);
        let merged = deep_merge(&base, new_config);
        let storage = get_storage();
        storage
            .with_lock("config_save", 10, || async {
                storage.save_config(&merged).await
            })
            .await?;
        let mut inner = self.inner.write().await;
        *inner = merged;
        Ok(())
    }

    pub async fn get_value(&self, key: &str) -> Option<JsonValue> {
        let inner = self.inner.read().await;
        get_value(&inner, key)
    }
}

pub async fn load_config() -> Result<(), StorageError> {
    let cfg = CONFIG
        .get_or_init(|| async { Arc::new(Config::new()) })
        .await;
    cfg.load().await
}

pub async fn update_config(new_config: &JsonValue) -> Result<(), StorageError> {
    let cfg = CONFIG
        .get_or_init(|| async { Arc::new(Config::new()) })
        .await;
    cfg.update(new_config).await
}

pub async fn get_config_value(key: &str) -> Option<JsonValue> {
    let cfg = CONFIG
        .get_or_init(|| async { Arc::new(Config::new()) })
        .await;
    cfg.get_value(key).await
}

pub async fn get_all_config() -> JsonValue {
    let cfg = CONFIG
        .get_or_init(|| async { Arc::new(Config::new()) })
        .await;
    cfg.inner.read().await.clone()
}

pub async fn get_config<T: DeserializeOwned>(key: &str, default: T) -> T {
    if let Some(value) = get_config_value(key).await {
        if let Ok(parsed) = serde_json::from_value::<T>(value) {
            return parsed;
        }
    }
    default
}

fn get_value(config: &JsonValue, key: &str) -> Option<JsonValue> {
    if !key.contains('.') {
        return config.get(key).cloned();
    }
    let mut iter = key.split('.');
    let section = iter.next()?;
    let rest = iter.next()?;
    match config.get(section) {
        Some(JsonValue::Object(map)) => map.get(rest).cloned(),
        _ => None,
    }
}

fn deep_merge(base: &JsonValue, override_value: &JsonValue) -> JsonValue {
    match (base, override_value) {
        (JsonValue::Object(base_map), JsonValue::Object(override_map)) => {
            let mut result = base_map.clone();
            for (k, v) in override_map {
                let merged = match result.get(k) {
                    Some(existing) => deep_merge(existing, v),
                    None => v.clone(),
                };
                result.insert(k.clone(), merged);
            }
            JsonValue::Object(result)
        }
        (_, other) => other.clone(),
    }
}

fn load_defaults() -> Option<JsonValue> {
    let path = project_root().join("config.defaults.toml");
    if !path.exists() {
        return None;
    }
    let content = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = content.parse().ok()?;
    Some(toml_to_json(value))
}

pub fn project_root() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf())
}

pub fn toml_to_json(value: toml::Value) -> JsonValue {
    match value {
        toml::Value::String(s) => JsonValue::String(s),
        toml::Value::Integer(i) => JsonValue::Number(i.into()),
        toml::Value::Float(f) => JsonValue::Number(serde_json::Number::from_f64(f).unwrap()),
        toml::Value::Boolean(b) => JsonValue::Bool(b),
        toml::Value::Datetime(dt) => JsonValue::String(dt.to_string()),
        toml::Value::Array(arr) => JsonValue::Array(arr.into_iter().map(toml_to_json).collect()),
        toml::Value::Table(table) => {
            let mut map = serde_json::Map::new();
            for (k, v) in table {
                map.insert(k, toml_to_json(v));
            }
            JsonValue::Object(map)
        }
    }
}

fn json_to_toml(value: &JsonValue) -> toml::Value {
    match value {
        JsonValue::Null => toml::Value::String("".to_string()),
        JsonValue::Bool(b) => toml::Value::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                toml::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                toml::Value::Float(f)
            } else {
                toml::Value::String(n.to_string())
            }
        }
        JsonValue::String(s) => toml::Value::String(s.clone()),
        JsonValue::Array(arr) => toml::Value::Array(arr.iter().map(json_to_toml).collect()),
        JsonValue::Object(map) => {
            let mut table = toml::value::Table::new();
            for (k, v) in map {
                table.insert(k.clone(), json_to_toml(v));
            }
            toml::Value::Table(table)
        }
    }
}

pub fn config_to_toml(value: &JsonValue) -> toml::Value {
    json_to_toml(value)
}
