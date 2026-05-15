//! Rate-limited HTTP client wrapper.

use anyhow::Result;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::{Quota, RateLimiter};
use reqwest::Client;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Wrapper around reqwest::Client with governor rate limiting.
#[derive(Clone)]
pub struct RateLimitedClient {
    client: Client,
    limiter: Arc<RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
}

impl RateLimitedClient {
    /// Create a new rate-limited client with the given requests-per-second quota.
    pub fn new(requests_per_second: u32) -> Self {
        let client = Client::new();
        let rate = NonZeroU32::new(requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap());
        let quota = Quota::per_second(rate);
        let limiter = Arc::new(RateLimiter::direct(quota));
        Self { client, limiter }
    }

    /// Wait for rate limit clearance and return the underlying reqwest client.
    pub async fn request(&self) -> &Client {
        if self.limiter.check().is_err() {
            self.limiter.until_ready().await;
        }
        &self.client
    }

    /// Perform a GET request, deserialize the JSON response into `T`.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let client = self.request().await;
        let resp = client.get(url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        let data: T = resp.json().await?;
        Ok(data)
    }

    /// Perform a POST request with a JSON body, deserialize the JSON response into `T`.
    pub async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let client = self.request().await;
        let resp = client.post(url).json(body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {url} returned {status}: {body_text}");
        }
        let data: T = resp.json().await?;
        Ok(data)
    }
}
