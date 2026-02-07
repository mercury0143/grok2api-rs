use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::core::config::get_config;
use crate::services::token::manager::get_token_manager;

pub struct TokenRefreshScheduler {
    interval_hours: i64,
    handle: Option<JoinHandle<()>>,
    running: bool,
}

impl TokenRefreshScheduler {
    pub fn new(interval_hours: i64) -> Self {
        Self {
            interval_hours,
            handle: None,
            running: false,
        }
    }

    pub fn start(&mut self) {
        if self.running {
            return;
        }
        self.running = true;
        let interval_secs = self.interval_hours.max(1) as u64 * 3600;
        self.handle = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
                let mgr = get_token_manager().await;
                let mut mgr = mgr.lock().await;
                let _ = mgr.refresh_cooling_tokens().await;
            }
        }));
    }

    pub fn stop(&mut self) {
        self.running = false;
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

static SCHEDULER: tokio::sync::OnceCell<Arc<tokio::sync::Mutex<TokenRefreshScheduler>>> =
    tokio::sync::OnceCell::const_new();

pub async fn get_scheduler() -> Arc<tokio::sync::Mutex<TokenRefreshScheduler>> {
    let interval: i64 = get_config("token.refresh_interval_hours", 8i64).await;
    let scheduler = SCHEDULER
        .get_or_init(|| async {
            Arc::new(tokio::sync::Mutex::new(TokenRefreshScheduler::new(
                interval,
            )))
        })
        .await
        .clone();
    scheduler
}
