use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    InvalidRequestError,
    AuthenticationError,
    PermissionError,
    NotFoundError,
    RateLimitError,
    ServerError,
    ServiceUnavailableError,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: ErrorType,
    pub param: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>, error_type: ErrorType) -> Self {
        Self {
            status,
            body: ErrorBody {
                message: message.into(),
                error_type,
                param: None,
                code: None,
            },
        }
    }

    pub fn with_param(mut self, param: impl Into<String>) -> Self {
        self.body.param = Some(param.into());
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.body.code = Some(code.into());
        self
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            message,
            ErrorType::InvalidRequestError,
        )
        .with_code("invalid_value")
    }

    pub fn authentication(message: impl Into<String>) -> Self {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            message,
            ErrorType::AuthenticationError,
        )
        .with_code("invalid_api_key")
    }

    pub fn permission(message: impl Into<String>) -> Self {
        ApiError::new(StatusCode::FORBIDDEN, message, ErrorType::PermissionError)
            .with_code("insufficient_quota")
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        ApiError::new(StatusCode::NOT_FOUND, message, ErrorType::NotFoundError)
            .with_code("model_not_found")
    }

    pub fn rate_limit(message: impl Into<String>) -> Self {
        ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            message,
            ErrorType::RateLimitError,
        )
        .with_code("rate_limit_exceeded")
    }

    pub fn server(message: impl Into<String>) -> Self {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
            ErrorType::ServerError,
        )
        .with_code("internal_error")
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        ApiError::new(StatusCode::BAD_GATEWAY, message, ErrorType::ServerError)
            .with_code("upstream_error")
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.body.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorResponse { error: self.body };
        (self.status, Json(body)).into_response()
    }
}
