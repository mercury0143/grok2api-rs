use std::net::SocketAddr;

use dotenvy::dotenv;
use tracing_subscriber::EnvFilter;

mod api;
mod app;
mod core;
mod services;

#[tokio::main]
async fn main() {
    let _ = dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    // Initialize config at startup
    if let Err(err) = core::config::load_config().await {
        tracing::warn!("Failed to load config: {err}");
    }

    // Initialize media storage
    if let Err(err) = core::media_storage::init_media_storage().await {
        tracing::warn!("Failed to initialize media storage: {:?}", err);
    }

    let auto_refresh: bool = core::config::get_config("token.auto_refresh", true).await;
    if auto_refresh {
        let scheduler = services::token::scheduler::get_scheduler().await;
        scheduler.lock().await.start();
    }

    let host = std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("SERVER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8000);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("invalid SERVER_HOST/SERVER_PORT");

    let app = app::create_app();

    tracing::info!("Starting grok2api-rs at http://{addr}");

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .unwrap();
}
