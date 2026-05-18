//! GMGN API client for fetching token holder count and other on-chain data.
//!
//! Uses the `gmgn-cli` CLI tool as a subprocess to query token information.
//! Includes rate limiting (0.5s delay between calls) and graceful error handling.

use anyhow::Result;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Default path to gmgn-cli binary.
pub const GMGN_CLI_DEFAULT_PATH: &str = "/home/kt/.npm-global/bin/gmgn-cli";

/// Minimum delay between GMGN CLI calls to respect rate limits.
const RATE_LIMIT_DELAY: Duration = Duration::from_millis(500);

/// Timeout for gmgn-cli subprocess calls.
const CLI_TIMEOUT: Duration = Duration::from_secs(30);

// ─── Response Types ──────────────────────────────────────────────────────────

/// Top-level response from `gmgn-cli token info --chain sol --address <CA> --raw`.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnTokenInfo {
    /// Token contract address.
    #[allow(dead_code)]
    pub address: String,
    /// Token symbol.
    #[allow(dead_code)]
    pub symbol: String,
    /// Token name.
    #[allow(dead_code)]
    pub name: String,
    /// Total number of holders.
    pub holder_count: u64,
    /// Circulating supply as a string.
    #[allow(dead_code)]
    pub circulating_supply: Option<String>,
    /// Liquidity in USD (string).
    #[allow(dead_code)]
    pub liquidity: Option<String>,
    /// Price information.
    #[allow(dead_code)]
    pub price: Option<GmgnPriceInfo>,
    /// Additional statistics (may contain another holder_count).
    #[allow(dead_code)]
    pub stat: Option<GmgnTokenStat>,
}

/// Price information from GMGN.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnPriceInfo {
    #[allow(dead_code)]
    pub price: Option<String>,
    /// 24h volume as string.
    #[allow(dead_code)]
    pub volume_24h: Option<String>,
}

/// Additional token statistics.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnTokenStat {
    /// Holder count (duplicated from top-level).
    #[allow(dead_code)]
    pub holder_count: Option<u64>,
}

// ─── Client ──────────────────────────────────────────────────────────────────

/// GMGN API client that shells out to `gmgn-cli` for token information.
///
/// Features:
/// - Rate limiting: 0.5s minimum delay between calls
/// - Timeout: 30s per subprocess call
/// - Graceful degradation: returns `None` on any error (429, timeout, parse)
pub struct GmgnClient {
    /// Path to the gmgn-cli binary.
    cli_path: String,
    /// Last call timestamp for rate limiting.
    last_call: Mutex<Instant>,
    /// Total calls made (for diagnostics).
    call_count: AtomicU64,
    /// Total failures (for diagnostics).
    failure_count: AtomicU64,
}

impl Default for GmgnClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GmgnClient {
    /// Create a new GMGN client with the default CLI path.
    pub fn new() -> Self {
        Self::with_cli_path(GMGN_CLI_DEFAULT_PATH.to_string())
    }

    /// Create a new GMGN client with a custom CLI path (useful for testing).
    pub fn with_cli_path(cli_path: String) -> Self {
        Self {
            cli_path,
            last_call: Mutex::new(Instant::now() - RATE_LIMIT_DELAY),
            call_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
        }
    }

    /// Fetch holder count for a token by calling `gmgn-cli token info`.
    ///
    /// Returns `Some(count)` on success, `None` on any error (rate limit,
    /// timeout, parse error, subprocess failure). Logs warnings on failures
    /// but never panics.
    pub async fn get_holder_count(&self, token_ca: &str) -> Option<u64> {
        // Enforce rate limit.
        {
            let mut last = self.last_call.lock().await;
            let elapsed = last.elapsed();
            if elapsed < RATE_LIMIT_DELAY {
                let sleep_time = RATE_LIMIT_DELAY - elapsed;
                tokio::time::sleep(sleep_time).await;
            }
            *last = Instant::now();
        }

        self.call_count.fetch_add(1, Ordering::Relaxed);

        let result = self.call_gmgn_cli(token_ca).await;
        match result {
            Ok(info) => {
                tracing::debug!(
                    token = &token_ca[..token_ca.len().min(12)],
                    holder_count = info.holder_count,
                    "GMGN holder count fetched"
                );
                Some(info.holder_count)
            }
            Err(e) => {
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    token = &token_ca[..token_ca.len().min(12)],
                    error = %e,
                    "GMGN holder count fetch failed — continuing without holder data"
                );
                None
            }
        }
    }

    /// Internal: call gmgn-cli subprocess and parse the output.
    async fn call_gmgn_cli(&self, token_ca: &str) -> Result<GmgnTokenInfo> {
        let output = tokio::time::timeout(
            CLI_TIMEOUT,
            tokio::process::Command::new(&self.cli_path)
                .args([
                    "token",
                    "info",
                    "--chain", "sol",
                    "--address", token_ca,
                    "--raw",
                ])
                .output(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("gmgn-cli timeout after {}s", CLI_TIMEOUT.as_secs()))??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            // Detect rate limit (HTTP 429) in output.
            if stderr.contains("429") || stdout.contains("429") || stderr.contains("rate limit") {
                anyhow::bail!("GMGN rate limited (429): {stderr}");
            }

            anyhow::bail!("gmgn-cli exited {}: {stderr}", output.status);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // gmgn-cli with --raw outputs a single JSON object (not wrapped).
        let info: GmgnTokenInfo = serde_json::from_str(&stdout)
            .map_err(|e| anyhow::anyhow!("Failed to parse gmgn-cli output: {e}"))?;

        Ok(info)
    }

    /// Fetch holder counts for multiple tokens with rate limiting between each.
    /// Returns a map of token_address -> holder_count. Tokens that fail are
    /// silently omitted from the result.
    pub async fn get_holder_counts(&self, token_cas: &[String]) -> std::collections::HashMap<String, u64> {
        let mut results = std::collections::HashMap::new();
        for ca in token_cas {
            if let Some(count) = self.get_holder_count(ca).await {
                results.insert(ca.clone(), count);
            }
        }
        results
    }

    /// Get the total number of GMGN calls made.
    pub fn call_count(&self) -> u64 {
        self.call_count.load(Ordering::Relaxed)
    }

    /// Get the total number of GMGN failures.
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that GmgnTokenInfo parses correctly from real gmgn-cli output.
    #[test]
    fn test_parse_gmgn_token_info() {
        let json = r#"{
            "address":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "symbol":"USDC",
            "name":"USD Coin",
            "decimals":6,
            "logo":"",
            "banner":"",
            "biggest_pool_address":"4TM5RGcQ3AJ7GYf2cn26wHKfeYQQMLuzyTRME8Wp9QXS",
            "open_timestamp":0,
            "migrated_timestamp":0,
            "holder_count":1666830,
            "circulating_supply":"5034942271",
            "total_supply":"5034942271",
            "max_supply":"5034942271",
            "liquidity":"7710204.072806",
            "creation_timestamp":0,
            "standard":"none",
            "trade_fee":"0",
            "total_fee":"9.979639",
            "og":true
        }"#;

        let info: GmgnTokenInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.address, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
        assert_eq!(info.symbol, "USDC");
        assert_eq!(info.holder_count, 1666830);
    }

    /// Test parsing with minimal fields.
    #[test]
    fn test_parse_minimal_token_info() {
        let json = r#"{
            "address":"So11111111111111111111111111111111111111112",
            "symbol":"SOL",
            "name":"Wrapped SOL",
            "holder_count":2500000
        }"#;

        let info: GmgnTokenInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.holder_count, 2500000);
    }

    /// Test that malformed JSON returns None gracefully.
    #[test]
    fn test_parse_malformed_json() {
        let json = r#"not valid json"#;
        let result: Result<GmgnTokenInfo> = serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("Parse error: {e}"));
        assert!(result.is_err());
    }

    /// Test that missing holder_count causes parse failure (field is required).
    #[test]
    fn test_parse_missing_holder_count() {
        let json = r#"{
            "address":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "symbol":"USDC",
            "name":"USD Coin"
        }"#;

        // holder_count is a required u64 field — missing it should fail parse.
        let result: Result<GmgnTokenInfo> = serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("Parse error: {e}"));
        assert!(result.is_err());
    }

    /// Test that get_holder_count returns None for non-existent CLI path.
    #[tokio::test]
    async fn test_get_holder_count_invalid_cli_path() {
        let client = GmgnClient::with_cli_path("/nonexistent/gmgn-cli".to_string());
        let result = client.get_holder_count("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").await;
        assert!(result.is_none());
        assert_eq!(client.failure_count(), 1);
        assert_eq!(client.call_count(), 1);
    }

    /// Test that get_holder_count returns None for invalid token CA.
    #[tokio::test]
    async fn test_get_holder_count_invalid_ca() {
        // Use real gmgn-cli but with a garbage address — should fail gracefully.
        let client = GmgnClient::new();
        let result = client.get_holder_count("invalid_address_12345").await;
        // Either None (error) or Some(0) — both are acceptable graceful results.
        if let Some(count) = result {
            assert_eq!(count, 0, "Invalid address should return 0 holders or None");
        }
        // The important thing is it didn't panic.
    }

    /// Test rate limiting: two rapid calls should take at least 500ms total.
    #[tokio::test]
    async fn test_rate_limiting_between_calls() {
        let client = GmgnClient::with_cli_path("/nonexistent/gmgn-cli".to_string());

        let start = Instant::now();
        let _ = client.get_holder_count("addr1").await;
        let _ = client.get_holder_count("addr2").await;
        let elapsed = start.elapsed();

        // Second call should be delayed by ~500ms due to rate limit.
        assert!(
            elapsed >= Duration::from_millis(400),
            "Expected ≥400ms for two rate-limited calls, got {:?}",
            elapsed
        );
    }

    /// Test get_holder_counts for multiple tokens.
    #[tokio::test]
    async fn test_get_holder_counts_batch() {
        let client = GmgnClient::with_cli_path("/nonexistent/gmgn-cli".to_string());

        let tokens = vec![
            "token1".to_string(),
            "token2".to_string(),
            "token3".to_string(),
        ];

        let results = client.get_holder_counts(&tokens).await;

        // All should fail with nonexistent CLI, so results should be empty.
        assert!(results.is_empty());
        assert_eq!(client.call_count(), 3);
        assert_eq!(client.failure_count(), 3);
    }

    /// Test that GmgnTokenInfo handles stat.holder_count correctly.
    #[test]
    fn test_parse_with_stat_holder_count() {
        let json = r#"{
            "address":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "symbol":"USDC",
            "name":"USD Coin",
            "holder_count":1000000,
            "stat": {
                "holder_count": 1000000,
                "signal_count": 0,
                "top_10_holder_rate": "0"
            }
        }"#;

        let info: GmgnTokenInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.holder_count, 1000000);
        assert_eq!(info.stat.unwrap().holder_count, Some(1000000));
    }

    // ════════════════════════════════════════════════════════════════════
    // PART B: Graceful Degradation Tests
    // ════════════════════════════════════════════════════════════════════

    /// Verify GmgnClient::get_holder_count() returns None gracefully when
    /// gmgn-cli is not available (non-existent binary path). The client
    /// must never panic — it degrades by returning None and incrementing
    /// the failure counter.
    #[tokio::test]
    async fn test_gmgn_graceful_failure() {
        let client = GmgnClient::with_cli_path("/nonexistent/gmgn-cli".to_string());

        // get_holder_count should return None (not panic) when CLI is unavailable.
        let result = client.get_holder_count("So11111111111111111111111111111111111111112").await;
        assert!(result.is_none(),
            "get_holder_count should return None when gmgn-cli is not available");

        // Failure counter should be incremented.
        assert_eq!(client.failure_count(), 1, "Failure count should be 1 after one failed call");
        assert_eq!(client.call_count(), 1, "Call count should be 1");

        // Multiple calls should also not panic.
        for i in 0..5 {
            let r = client.get_holder_count(&format!("token_{i}")).await;
            assert!(r.is_none(), "Call {i} should also return None, not panic");
        }
        assert_eq!(client.failure_count(), 6);
        assert_eq!(client.call_count(), 6);

        // get_holder_counts (batch) should also degrade gracefully.
        let tokens = vec![
            "batch_token_1".to_string(),
            "batch_token_2".to_string(),
        ];
        let batch_results = client.get_holder_counts(&tokens).await;
        assert!(batch_results.is_empty(),
            "Batch get_holder_counts should return empty map when CLI is unavailable");
    }
}
