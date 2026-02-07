use rand::seq::SliceRandom;

use crate::services::token::models::{TokenInfo, TokenPoolStats, TokenStatus};

#[derive(Debug, Default, Clone)]
pub struct TokenPool {
    pub name: String,
    tokens: Vec<TokenInfo>,
}

impl TokenPool {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tokens: Vec::new(),
        }
    }

    pub fn add(&mut self, token: TokenInfo) {
        self.tokens.push(token);
    }

    pub fn remove(&mut self, token: &str) -> bool {
        let before = self.tokens.len();
        self.tokens.retain(|t| t.token != token);
        before != self.tokens.len()
    }

    pub fn get(&self, token: &str) -> Option<TokenInfo> {
        self.tokens.iter().find(|t| t.token == token).cloned()
    }

    pub fn get_mut(&mut self, token: &str) -> Option<&mut TokenInfo> {
        self.tokens.iter_mut().find(|t| t.token == token)
    }

    pub fn list(&self) -> Vec<TokenInfo> {
        self.tokens.clone()
    }

    pub fn select(&self) -> Option<TokenInfo> {
        let mut available: Vec<_> = self
            .tokens
            .iter()
            .filter(|t| t.status == TokenStatus::Active && t.quota > 0)
            .collect();
        if available.is_empty() {
            return None;
        }
        let max_quota = available.iter().map(|t| t.quota).max().unwrap_or(0);
        available.retain(|t| t.quota == max_quota);
        let mut rng = rand::thread_rng();
        available.choose(&mut rng).cloned().cloned()
    }

    pub fn count(&self) -> usize {
        self.tokens.len()
    }

    pub fn stats(&self) -> TokenPoolStats {
        let mut stats = TokenPoolStats::default();
        stats.total = self.tokens.len();
        for t in &self.tokens {
            stats.total_quota += t.quota;
            match t.status {
                TokenStatus::Active => stats.active += 1,
                TokenStatus::Disabled => stats.disabled += 1,
                TokenStatus::Expired => stats.expired += 1,
                TokenStatus::Cooling => stats.cooling += 1,
            }
        }
        if stats.total > 0 {
            stats.avg_quota = stats.total_quota as f64 / stats.total as f64;
        }
        stats
    }
}
