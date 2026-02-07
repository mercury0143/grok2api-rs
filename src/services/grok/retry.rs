use std::future::Future;

use crate::core::config::get_config;

#[derive(Default)]
pub struct RetryContext {
    pub attempt: u32,
    pub max_retry: u32,
    pub retry_codes: Vec<u16>,
}

impl RetryContext {
    pub async fn new() -> Self {
        let max_retry: u32 = get_config("grok.max_retry", 1u32).await;
        let retry_codes: Vec<u16> =
            get_config("grok.retry_status_codes", vec![401u16, 429u16, 403u16]).await;
        Self {
            attempt: 0,
            max_retry,
            retry_codes,
        }
    }

    pub fn should_retry(&self, status: u16) -> bool {
        self.attempt < self.max_retry && self.retry_codes.contains(&status)
    }
}

pub async fn retry_on_status<F, Fut, T, E>(
    func: F,
    extract_status: impl Fn(&E) -> Option<u16>,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut ctx = RetryContext::new().await;
    loop {
        match func().await {
            Ok(v) => return Ok(v),
            Err(err) => {
                let status = extract_status(&err);
                if let Some(status) = status {
                    ctx.attempt += 1;
                    if ctx.should_retry(status) {
                        let delay = 0.5 * (ctx.attempt as f64 + 1.0);
                        tracing::warn!(
                            "Retry {}/{} for status {}, waiting {}s",
                            ctx.attempt,
                            ctx.max_retry,
                            status,
                            delay
                        );
                        tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
                        continue;
                    }
                }
                return Err(err);
            }
        }
    }
}
