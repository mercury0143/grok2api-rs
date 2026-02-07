mod admin;
mod chat;
mod files;
mod image;
mod models;
mod responses;
mod videos;

use axum::Router;

pub fn router() -> Router {
    Router::new()
        .merge(chat::router())
        .merge(responses::router())
        .merge(image::router())
        .merge(videos::router())
        .merge(models::router())
        .merge(files::router())
        .merge(admin::router())
}
