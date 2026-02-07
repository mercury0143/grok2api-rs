use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tokio::sync::OnceCell;

use crate::core::config::get_config;
use crate::core::storage::{Storage, get_storage};
use crate::services::grok::usage::UsageService;
use crate::services::token::models::{
    DEFAULT_QUOTA, EffortType, FAIL_THRESHOLD, TokenInfo, TokenPoolStats, TokenStatus,
};
use crate::services::token::pool::TokenPool;

#[derive(Debug)]
pub struct TokenManager {
    pub pools: HashMap<String, TokenPool>,
    initialized: bool,
    last_reload_at: Instant,
}

impl TokenManager {
    pub fn new() -> Self {
        Self {
            pools: HashMap::new(),
            initialized: false,
            last_reload_at: Instant::now(),
        }
    }

    pub async fn load(&mut self) {
        if self.initialized {
            return;
        }
        let storage = get_storage();
        let data = storage
            .load_tokens()
            .await
            .unwrap_or(JsonValue::Object(Default::default()));
        let mut pools: HashMap<String, TokenPool> = HashMap::new();
        if let Some(obj) = data.as_object() {
            for (pool_name, list) in obj {
                let mut pool = TokenPool::new(pool_name);
                if let Some(arr) = list.as_array() {
                    for token_val in arr {
                        if let Some(token_info) = token_from_value(token_val) {
                            pool.add(token_info);
                        }
                    }
                }
                pools.insert(pool_name.to_string(), pool);
            }
        }
        self.pools = pools;
        self.initialized = true;
        self.last_reload_at = Instant::now();
        let total: usize = self.pools.values().map(|p| p.count()).sum();
        tracing::info!(
            "TokenManager initialized: {} pools with {} tokens",
            self.pools.len(),
            total
        );
    }

    pub async fn reload(&mut self) {
        self.initialized = false;
        self.load().await;
    }

    pub async fn reload_if_stale(&mut self) {
        let interval: f64 = get_config("token.reload_interval_sec", 30f64).await;
        if interval <= 0.0 {
            return;
        }
        if self.last_reload_at.elapsed() < Duration::from_secs_f64(interval) {
            return;
        }
        self.reload().await;
    }

    async fn save(&self) {
        let storage = get_storage();
        let mut map = serde_json::Map::new();
        for (name, pool) in &self.pools {
            let tokens: Vec<JsonValue> = pool
                .list()
                .into_iter()
                .map(|t| serde_json::to_value(t).unwrap_or(JsonValue::Null))
                .collect();
            map.insert(name.clone(), JsonValue::Array(tokens));
        }
        let data = JsonValue::Object(map);
        let _ = storage
            .with_lock("tokens_save", 10, || async {
                storage.save_tokens(&data).await
            })
            .await;
    }

    pub fn get_token(&self, pool_name: &str) -> Option<String> {
        let pool = self.pools.get(pool_name)?;
        let token = pool.select()?;
        Some(token.token.trim_start_matches("sso=").to_string())
    }

    pub async fn consume(&mut self, token_str: &str, effort: EffortType) -> bool {
        let raw = token_str.trim_start_matches("sso=");
        for pool in self.pools.values_mut() {
            if let Some(token) = pool.get_mut(raw) {
                token.consume(&effort);
                self.save().await;
                return true;
            }
        }
        false
    }

    pub async fn sync_usage(
        &mut self,
        token_str: &str,
        model_name: &str,
        fallback_effort: EffortType,
        consume_on_fail: bool,
        is_usage: bool,
    ) -> bool {
        let raw = token_str.trim_start_matches("sso=");
        let mut pool_name: Option<String> = None;
        for (name, pool) in &self.pools {
            if pool.get(raw).is_some() {
                pool_name = Some(name.clone());
                break;
            }
        }
        let pool_name = match pool_name {
            Some(p) => p,
            None => return false,
        };
        let usage_service = UsageService::new().await;
        match usage_service.get(token_str, model_name).await {
            Ok(result) => {
                if let Some(remain) = result.get("remainingTokens").and_then(|v| v.as_i64()) {
                    if let Some(token) = self.pools.get_mut(&pool_name).and_then(|p| p.get_mut(raw))
                    {
                        let old_quota = token.quota;
                        token.update_quota(remain as i32);
                        token.record_success(is_usage);
                        tracing::info!(
                            "Token {} synced quota {} -> {}",
                            &raw[..raw.len().min(8)],
                            old_quota,
                            token.quota
                        );
                        self.save().await;
                        return true;
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    "Token {} API sync failed: {}",
                    &raw[..raw.len().min(8)],
                    err
                );
            }
        }
        if consume_on_fail {
            self.consume(token_str, fallback_effort).await
        } else {
            false
        }
    }

    pub async fn record_fail(&mut self, token_str: &str, status_code: u16, reason: &str) -> bool {
        let raw = token_str.trim_start_matches("sso=");
        for pool in self.pools.values_mut() {
            if let Some(token) = pool.get_mut(raw) {
                token.record_fail(status_code, reason);
                self.save().await;
                return true;
            }
        }
        false
    }

    pub async fn add(&mut self, token: &str, pool_name: &str) -> bool {
        let raw = token.trim_start_matches("sso=");
        let pool = self
            .pools
            .entry(pool_name.to_string())
            .or_insert_with(|| TokenPool::new(pool_name));
        if pool.get(raw).is_some() {
            return false;
        }
        pool.add(TokenInfo::new(raw.to_string()));
        self.save().await;
        true
    }

    pub async fn remove(&mut self, token: &str) -> bool {
        for pool in self.pools.values_mut() {
            if pool.remove(token) {
                self.save().await;
                return true;
            }
        }
        false
    }

    pub async fn reset_all(&mut self) {
        for pool in self.pools.values_mut() {
            for token in pool.list() {
                if let Some(tok) = pool.get_mut(&token.token) {
                    tok.reset();
                }
            }
        }
        self.save().await;
    }

    pub async fn reset_token(&mut self, token_str: &str) -> bool {
        let raw = token_str.trim_start_matches("sso=");
        for pool in self.pools.values_mut() {
            if let Some(token) = pool.get_mut(raw) {
                token.reset();
                self.save().await;
                return true;
            }
        }
        false
    }

    pub fn get_stats(&self) -> HashMap<String, TokenPoolStats> {
        let mut stats = HashMap::new();
        for (name, pool) in &self.pools {
            stats.insert(name.clone(), pool.stats());
        }
        stats
    }

    pub fn get_pool_tokens(&self, pool_name: &str) -> Vec<TokenInfo> {
        self.pools
            .get(pool_name)
            .map(|p| p.list())
            .unwrap_or_default()
    }

    pub fn has_tag(&self, token: &str, tag: &str) -> bool {
        let raw = token.trim_start_matches("sso=");
        for pool in self.pools.values() {
            if let Some(tok) = pool.get(raw) {
                return tok.tags.iter().any(|t| t == tag);
            }
        }
        false
    }

    pub async fn add_tag(&mut self, token: &str, tag: &str) -> bool {
        let raw = token.trim_start_matches("sso=");
        for pool in self.pools.values_mut() {
            if let Some(tok) = pool.get_mut(raw) {
                if !tok.tags.contains(&tag.to_string()) {
                    tok.tags.push(tag.to_string());
                    self.save().await;
                }
                return true;
            }
        }
        false
    }

    pub async fn mark_asset_clear(&mut self, token: &str) -> bool {
        let raw = token.trim_start_matches("sso=");
        for pool in self.pools.values_mut() {
            if let Some(tok) = pool.get_mut(raw) {
                tok.last_asset_clear_at = Some(chrono::Utc::now().timestamp_millis());
                self.save().await;
                return true;
            }
        }
        false
    }

    pub async fn refresh_cooling_tokens(&mut self) -> HashMap<&'static str, i32> {
        let interval_hours: i64 = get_config("token.refresh_interval_hours", 8i64).await;
        let mut to_refresh = Vec::new();
        for pool in self.pools.values() {
            for token in pool.list() {
                if token.need_refresh(interval_hours) {
                    to_refresh.push(token.token.clone());
                }
            }
        }
        if to_refresh.is_empty() {
            return HashMap::from([
                ("checked", 0),
                ("refreshed", 0),
                ("recovered", 0),
                ("expired", 0),
            ]);
        }
        let usage = UsageService::new().await;
        let mut refreshed = 0;
        let mut recovered = 0;
        let mut expired = 0;
        for token_str in to_refresh {
            if let Ok(result) = usage.get(&token_str, "grok-3").await {
                if let Some(remain) = result.get("remainingTokens").and_then(|v| v.as_i64()) {
                    if let Some(pool) = self
                        .pools
                        .values_mut()
                        .find(|p| p.get(&token_str).is_some())
                    {
                        if let Some(tok) = pool.get_mut(&token_str) {
                            let old_quota = tok.quota;
                            tok.update_quota(remain as i32);
                            tok.mark_synced();
                            if old_quota == 0 && tok.quota > 0 {
                                recovered += 1;
                            }
                        }
                    }
                }
            } else {
                if let Some(pool) = self
                    .pools
                    .values_mut()
                    .find(|p| p.get(&token_str).is_some())
                {
                    if let Some(tok) = pool.get_mut(&token_str) {
                        tok.status = TokenStatus::Expired;
                        tok.mark_synced();
                        expired += 1;
                    }
                }
            }
            refreshed += 1;
        }
        self.save().await;
        HashMap::from([
            ("checked", refreshed),
            ("refreshed", refreshed),
            ("recovered", recovered),
            ("expired", expired),
        ])
    }
}

fn token_from_value(v: &JsonValue) -> Option<TokenInfo> {
    let obj = v.as_object()?;
    let token = obj
        .get("token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if token.is_empty() {
        return None;
    }
    let status = match obj
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("active")
    {
        "disabled" => TokenStatus::Disabled,
        "expired" => TokenStatus::Expired,
        "cooling" => TokenStatus::Cooling,
        _ => TokenStatus::Active,
    };
    let quota = obj
        .get("quota")
        .and_then(|v| v.as_i64())
        .unwrap_or(DEFAULT_QUOTA as i64) as i32;
    let created_at = obj
        .get("created_at")
        .and_then(|v| v.as_i64())
        .unwrap_or(chrono::Utc::now().timestamp_millis());
    let last_used_at = obj.get("last_used_at").and_then(|v| v.as_i64());
    let use_count = obj.get("use_count").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let fail_count = obj.get("fail_count").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let last_fail_at = obj.get("last_fail_at").and_then(|v| v.as_i64());
    let last_fail_reason = obj
        .get("last_fail_reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let last_sync_at = obj.get("last_sync_at").and_then(|v| v.as_i64());
    let tags = obj
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_else(Vec::new);
    let note = obj
        .get("note")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let last_asset_clear_at = obj.get("last_asset_clear_at").and_then(|v| v.as_i64());

    Some(TokenInfo {
        token: token.trim_start_matches("sso=").to_string(),
        status,
        quota,
        created_at,
        last_used_at,
        use_count,
        fail_count,
        last_fail_at,
        last_fail_reason,
        last_sync_at,
        tags,
        note,
        last_asset_clear_at,
    })
}

static MANAGER: OnceCell<std::sync::Arc<Mutex<TokenManager>>> = OnceCell::const_new();

pub async fn get_token_manager() -> std::sync::Arc<Mutex<TokenManager>> {
    let mgr = MANAGER
        .get_or_init(|| async { std::sync::Arc::new(Mutex::new(TokenManager::new())) })
        .await
        .clone();
    {
        let mut guard = mgr.lock().await;
        guard.load().await;
    }
    mgr
}
