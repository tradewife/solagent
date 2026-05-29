//! GMGN API client for fetching token data, security checks, market signals,
//! wallet profiling, and smart money tracking.
//!
//! Uses the `gmgn-cli` CLI tool as a subprocess to query token information.
//! Includes rate limiting (0.5s delay between calls) and graceful error handling.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::Stdio;
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

// ─── Token Security Response ─────────────────────────────────────────────────

/// Response from `gmgn-cli token security --chain sol --address <CA> --raw`.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnTokenSecurity {
    /// Token contract address.
    #[allow(dead_code)]
    pub address: String,
    /// Whether the token is a honeypot.
    pub is_honeypot: Option<bool>,
    /// Whether the token is open source.
    #[allow(dead_code)]
    pub is_open_source: Option<bool>,
    /// Whether mint authority is renounced.
    pub is_mintable: Option<bool>,
    /// Whether freeze authority is renounced (true = NOT renounced).
    pub is_freeze_authority: Option<bool>,
    /// Buy tax as decimal (e.g. 0.05 = 5%).
    pub buy_tax: Option<f64>,
    /// Sell tax as decimal (e.g. 0.05 = 5%).
    pub sell_tax: Option<f64>,
    /// Top 10 holder concentration as decimal.
    pub top_10_holder_rate: Option<f64>,
    /// LP burn percentage as decimal.
    pub lp_burn_percent: Option<f64>,
    /// Whether LP is burned.
    pub lp_burned: Option<bool>,
}

// ─── Dev Wallet / Token Info Response ────────────────────────────────────────

/// Full token info from GMGN including dev data, social links, and enrichment.
/// Response from `gmgn-cli token info --chain sol --address <CA> --raw`.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnTokenInfoFull {
    /// Token contract address.
    pub address: String,
    /// Token symbol.
    pub symbol: String,
    /// Token name.
    pub name: String,
    /// Total number of holders.
    pub holder_count: u64,
    /// Liquidity in USD (string).
    pub liquidity: Option<String>,
    /// Dev wallet address.
    pub dev_address: Option<String>,
    /// Whether dev still holds tokens.
    pub dev_holding: Option<bool>,
    /// Dev holding percentage.
    pub dev_holding_percent: Option<f64>,
    /// Number of tokens created by this dev.
    pub dev_created_count: Option<u64>,
    /// Best (highest ATH) token created by this dev.
    pub dev_best_ath_marketcap: Option<f64>,
    /// Whether the token is community-taken-over.
    pub is_cto: Option<bool>,
    /// KOL buyer count.
    pub kol_buyer_count: Option<u64>,
    /// Smart money buyer count.
    pub smart_degen_buyer_count: Option<u64>,
    /// Top-10 holder concentration as string (e.g. "0.15").
    pub top_10_holder_rate: Option<String>,
    /// Sniper wallet percentage.
    pub sniper_holder_percent: Option<f64>,
    /// Bundle percentage.
    pub bundle_percent: Option<f64>,
    /// Social links.
    pub twitter: Option<String>,
    pub website: Option<String>,
    pub telegram: Option<String>,
}

// ─── Dev Created Tokens Response ─────────────────────────────────────────────

/// A token created by a dev wallet.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnDevToken {
    /// Token contract address.
    pub address: String,
    /// Token symbol.
    pub symbol: Option<String>,
    /// Token name.
    pub name: Option<String>,
    /// All-time high market cap.
    pub token_ath_mc: Option<f64>,
    /// Current market cap.
    pub current_market_cap: Option<f64>,
    /// Whether the token has graduated to a DEX.
    pub is_graduated: Option<bool>,
}

// ─── Market Signal Response ──────────────────────────────────────────────────

/// A market signal from GMGN (smart money buy, KOL call, price surge, etc.).
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnMarketSignal {
    /// Token contract address.
    pub token_address: String,
    /// Token symbol.
    pub token_symbol: Option<String>,
    /// Market cap at trigger time.
    pub trigger_market_cap: Option<f64>,
    /// Current market cap.
    pub current_market_cap: Option<f64>,
    /// Trigger timestamp (Unix seconds).
    pub trigger_timestamp: Option<i64>,
    /// Signal type numeric code.
    pub signal_type: Option<i64>,
}

// ─── Smart Money Trade Response ──────────────────────────────────────────────

/// A smart money trade event from GMGN.
#[derive(Debug, Clone, Deserialize)]
pub struct GmgnSmartMoneyTrade {
    /// Wallet address.
    pub address: Option<String>,
    /// Token contract address.
    pub token_address: Option<String>,
    /// Token symbol.
    pub token_symbol: Option<String>,
    /// Side: "buy" or "sell".
    pub side: Option<String>,
    /// Amount in USD.
    pub amount_usd: Option<f64>,
    /// Token amount.
    pub token_amount: Option<f64>,
    /// Timestamp.
    pub timestamp: Option<String>,
}

/// Wallet holdings from GMGN `portfolio holdings`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmgnWalletHoldings {
    pub total_value_usd: f64,
    pub tokens: Vec<GmgnWalletToken>,
}

/// Single token in wallet holdings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmgnWalletToken {
    pub symbol: String,
    pub mint: String,
    pub quantity: f64,
    pub value_usd: f64,
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

    /// Fetch trending token addresses from GMGN (via `gmgn-cli market trending`).
    ///
    /// Returns a list of (address, symbol, price_change_24h_percent) tuples
    /// for the top trending tokens on Solana. Useful as a broader token
    /// discovery source for behavioral analysis.
    pub async fn get_trending_tokens(&self, interval: &str, limit: usize) -> Vec<(String, String, f64)> {
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

        let output = match tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(&self.cli_path)
                .args([
                    "market", "trending",
                    "--chain", "sol",
                    "--interval", interval,
                    "--limit", &limit.to_string(),
                    "--raw",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        ).await {
            Ok(Ok(o)) => o,
            _ => {
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                return Vec::new();
            }
        };

        if !output.status.success() {
            self.failure_count.fetch_add(1, Ordering::Relaxed);
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut tokens = Vec::new();

        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout) {
            // Response is { "data": { "rank": [...] } }
            let items = data.get("data")
                .and_then(|d| d.get("rank"))
                .and_then(|r| r.as_array());

            if let Some(items) = items {
                for item in items {
                    let addr = item.get("address")
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string();
                    let symbol = item.get("symbol")
                        .and_then(|s| s.as_str())
                        .unwrap_or("?")
                        .to_string();
                    let price_change = item.get("price_change_percent")
                        .and_then(|p| p.as_f64())
                        .unwrap_or(0.0);

                    if !addr.is_empty() {
                        tokens.push((addr, symbol, price_change));
                    }
                }
            }
        }

        tokens
    }

    // ─── New GMGN API Methods ─────────────────────────────────────────────

    /// Internal helper: enforce rate limit before a CLI call.
    async fn enforce_rate_limit(&self) {
        let mut last = self.last_call.lock().await;
        let elapsed = last.elapsed();
        if elapsed < RATE_LIMIT_DELAY {
            let sleep_time = RATE_LIMIT_DELAY - elapsed;
            tokio::time::sleep(sleep_time).await;
        }
        *last = Instant::now();
    }

    /// Internal helper: run a gmgn-cli command and return stdout as String.
    /// Returns None on any failure (timeout, non-zero exit, etc.).
    async fn run_cli(&self, args: &[&str]) -> Option<String> {
        self.enforce_rate_limit().await;
        self.call_count.fetch_add(1, Ordering::Relaxed);

        let output = tokio::time::timeout(
            CLI_TIMEOUT,
            tokio::process::Command::new(&self.cli_path)
                .args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        )
        .await
        .ok()?
        .ok()?;

        if !output.status.success() {
            self.failure_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        Some(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Fetch token security data from GMGN.
    /// Calls `gmgn-cli token security --chain sol --address <CA> --raw`.
    ///
    /// Returns LP burn status, honeypot check, buy/sell tax, top-10 concentration,
    /// mint/freeze authority status — all Solana-native data that Birdeye may not provide.
    pub async fn get_token_security(&self, token_ca: &str) -> Option<GmgnTokenSecurity> {
        let stdout = self.run_cli(&[
            "token", "security",
            "--chain", "sol",
            "--address", token_ca,
            "--raw",
        ]).await?;

        match serde_json::from_str::<GmgnTokenSecurity>(&stdout) {
            Ok(sec) => {
                tracing::debug!(
                    token = &token_ca[..token_ca.len().min(12)],
                    honeypot = ?sec.is_honeypot,
                    lp_burned = ?sec.lp_burned,
                    "GMGN token security fetched"
                );
                Some(sec)
            }
            Err(e) => {
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    token = &token_ca[..token_ca.len().min(12)],
                    error = %e,
                    "Failed to parse GMGN token security response"
                );
                None
            }
        }
    }

    /// Fetch full token info from GMGN including dev wallet data, social links,
    /// smart money/KOL buyer counts, and holder concentration.
    /// Calls `gmgn-cli token info --chain sol --address <CA> --raw`.
    ///
    /// This returns richer data than `get_holder_count()` which only extracts
    /// the holder_count field from a basic parse.
    pub async fn get_token_info_full(&self, token_ca: &str) -> Option<GmgnTokenInfoFull> {
        let stdout = self.run_cli(&[
            "token", "info",
            "--chain", "sol",
            "--address", token_ca,
            "--raw",
        ]).await?;

        match serde_json::from_str::<GmgnTokenInfoFull>(&stdout) {
            Ok(info) => {
                tracing::debug!(
                    token = &token_ca[..token_ca.len().min(12)],
                    dev = ?info.dev_address.as_deref().map(|d| &d[..d.len().min(12)]),
                    sm_buyers = ?info.smart_degen_buyer_count,
                    kol_buyers = ?info.kol_buyer_count,
                    "GMGN full token info fetched"
                );
                Some(info)
            }
            Err(e) => {
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    token = &token_ca[..token_ca.len().min(12)],
                    error = %e,
                    "Failed to parse GMGN full token info response"
                );
                None
            }
        }
    }

    /// Fetch all tokens created by a dev wallet.
    /// Calls `gmgn-cli portfolio created-tokens --chain sol --wallet <ADDR> --raw`.
    ///
    /// Returns each token's ATH mcap, current mcap, and graduation status.
    /// Used for dev track record safety scoring.
    pub async fn get_dev_created_tokens(&self, dev_wallet: &str) -> Vec<GmgnDevToken> {
        let stdout = match self.run_cli(&[
            "portfolio", "created-tokens",
            "--chain", "sol",
            "--wallet", dev_wallet,
            "--order-by", "token_ath_mc",
            "--raw",
        ]).await {
            Some(s) => s,
            None => return Vec::new(),
        };

        // Response may be { "list": [...] } or a direct array.
        let tokens: Vec<GmgnDevToken> = if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout) {
            let items = data.get("list")
                .and_then(|l| l.as_array())
                .cloned()
                .unwrap_or_else(|| {
                    data.as_array().cloned().unwrap_or_default()
                });

            items.iter().filter_map(|item| {
                serde_json::from_value(item.clone()).ok()
            }).collect()
        } else {
            Vec::new()
        };

        tracing::debug!(
            dev = &dev_wallet[..dev_wallet.len().min(12)],
            count = tokens.len(),
            "GMGN dev created tokens fetched"
        );

        tokens
    }

    /// Fetch market signals from GMGN.
    /// Calls `gmgn-cli market signal --chain sol --signal-type <TYPE> --raw`.
    ///
    /// Signal types:
    /// - 6: Price surge
    /// - 12: Smart money cluster buy
    /// - 13: KOL call
    /// - 18: Pump.fun claim
    ///
    /// Returns a list of recent signals with token addresses and market caps.
    pub async fn get_market_signals(&self, signal_type: i64) -> Vec<GmgnMarketSignal> {
        let stdout = match self.run_cli(&[
            "market", "signal",
            "--chain", "sol",
            "--signal-type", &signal_type.to_string(),
            "--raw",
        ]).await {
            Some(s) => s,
            None => return Vec::new(),
        };

        // Response may be { "list": [...] } or { "data": { "list": [...] } }.
        let signals: Vec<GmgnMarketSignal> = if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout) {
            let items = data.get("list")
                .or_else(|| data.get("data").and_then(|d| d.get("list")))
                .and_then(|l| l.as_array())
                .cloned()
                .unwrap_or_default();

            items.iter().filter_map(|item| {
                let addr = item.get("token_address")
                    .or_else(|| item.get("address"))
                    .and_then(|a| a.as_str())
                    .unwrap_or("")
                    .to_string();

                if addr.is_empty() {
                    return None;
                }

                Some(GmgnMarketSignal {
                    token_address: addr,
                    token_symbol: item.get("token_symbol")
                        .or_else(|| item.get("symbol"))
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    trigger_market_cap: item.get("trigger_market_cap")
                        .or_else(|| item.get("market_cap"))
                        .and_then(|v| v.as_f64()),
                    current_market_cap: item.get("current_market_cap")
                        .and_then(|v| v.as_f64()),
                    trigger_timestamp: item.get("trigger_timestamp")
                        .or_else(|| item.get("timestamp"))
                        .and_then(|v| v.as_i64()),
                    signal_type: Some(signal_type),
                })
            }).collect()
        } else {
            Vec::new()
        };

        tracing::debug!(
            signal_type,
            count = signals.len(),
            "GMGN market signals fetched"
        );

        signals
    }

    /// Fetch recent smart money trades (buy or sell side).
    /// Calls `gmgn-cli track smartmoney --chain sol [--side buy|sell] --raw`.
    ///
    /// Returns recent trades from smart money wallets. When side is "sell",
    /// can be used to detect smart money exit signals on held positions.
    pub async fn get_smart_money_trades(&self, side: Option<&str>) -> Vec<GmgnSmartMoneyTrade> {
        let mut args = vec![
            "track", "smartmoney",
            "--chain", "sol",
            "--raw",
        ];
        if let Some(s) = side {
            args.push("--side");
            args.push(s);
        }

        let stdout = match self.run_cli(&args).await {
            Some(s) => s,
            None => return Vec::new(),
        };

        // Response may be { "list": [...] } or { "data": { "list": [...] } }.
        let trades: Vec<GmgnSmartMoneyTrade> = if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout) {
            let items = data.get("list")
                .or_else(|| data.get("data").and_then(|d| d.get("list")))
                .and_then(|l| l.as_array())
                .cloned()
                .unwrap_or_default();

            items.iter().filter_map(|item| {
                let token_addr = item.get("token")
                    .or_else(|| item.get("token_address"))
                    .and_then(|a| a.as_str())
                    .unwrap_or("")
                    .to_string();

                if token_addr.is_empty() {
                    return None;
                }

                Some(GmgnSmartMoneyTrade {
                    address: item.get("address")
                        .or_else(|| item.get("wallet"))
                        .and_then(|a| a.as_str())
                        .map(|s| s.to_string()),
                    token_address: Some(token_addr),
                    token_symbol: item.get("token_symbol")
                        .or_else(|| item.get("symbol"))
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    side: item.get("side")
                        .or_else(|| item.get("action"))
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    amount_usd: item.get("amount_usd")
                        .or_else(|| item.get("value"))
                        .and_then(|v| v.as_f64()),
                    token_amount: item.get("token_amount")
                        .or_else(|| item.get("amount"))
                        .and_then(|v| v.as_f64()),
                    timestamp: item.get("timestamp")
                        .or_else(|| item.get("time"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
            }).collect()
        } else {
            Vec::new()
        };

        tracing::debug!(
            side = ?side,
            count = trades.len(),
            "GMGN smart money trades fetched"
        );

        trades
    }

    /// Get wallet token holdings via `gmgn-cli portfolio holdings`.
    ///
    /// Returns total USD value of all holdings and individual token positions.
    /// Uses GMGN API key auth (normal level).
    pub async fn get_wallet_holdings(&self, chain: &str, wallet: &str) -> Option<GmgnWalletHoldings> {
        let output = match self.run_cli(&[
            "portfolio", "holdings",
            "--chain", chain,
            "--wallet", wallet,
            "--raw",
        ]).await {
            Some(o) => o,
            None => return None,
        };

        match serde_json::from_str::<serde_json::Value>(&output) {
            Ok(val) => {
                // GMGN returns either an array of positions or {data: [...]}.
                let positions = if val.is_array() {
                    val.as_array().cloned().unwrap_or_default()
                } else if val.is_object() {
                    val.get("data")
                        .and_then(|d| d.as_array().cloned())
                        .unwrap_or_default()
                } else {
                    return None;
                };

                let mut total_usd = 0.0;
                let mut tokens = Vec::new();

                for pos in &positions {
                    let attrs = pos.get("attributes").unwrap_or(pos);
                    let value_usd = attrs.get("value")
                        .or_else(|| attrs.get("usd_value"))
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    total_usd += value_usd;

                    let token_info = attrs.get("token").or_else(|| attrs.get("fungible_info"));
                    let symbol = token_info
                        .and_then(|t| t.get("symbol"))
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string();
                    let mint = token_info
                        .and_then(|t| t.get("address").or_else(|| t.get("implementation")))
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string();
                    let quantity = attrs.get("amount")
                        .or_else(|| attrs.get("quantity"))
                        .and_then(|v| {
                            // Quantity can be a number or {numeric: ...}.
                            v.as_f64().or_else(|| v.get("numeric").and_then(|n| n.as_f64()))
                        })
                        .unwrap_or(0.0);

                    tokens.push(GmgnWalletToken {
                        symbol,
                        mint,
                        quantity,
                        value_usd,
                    });
                }

                Some(GmgnWalletHoldings {
                    total_value_usd: total_usd,
                    tokens,
                })
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse GMGN wallet holdings");
                self.failure_count.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
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
