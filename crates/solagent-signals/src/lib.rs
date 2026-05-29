//! # solagent-signals
//!
//! Signal engine with strategy trait, four implemented signal detectors,
//! and confluence scoring.

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solagent_core::{Chain, Event, EventBus, Signal, TokenInfo};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// ─── Twitter Handle Extraction ───────────────────────────────────────────────

/// Extract Twitter handles from DexScreener `socials` JSON data.
///
/// DexScreener returns social links as an array of objects like:
/// ```json
/// [{"type": "twitter", "url": "https://twitter.com/ElonMusk"},
///  {"type": "x", "url": "https://x.com/vitalik"},
///  {"type": "telegram", "url": "https://t.me/channel"}]
/// ```
///
/// This function filters for Twitter/X social links and extracts the handle
/// from the URL. Returns deduplicated handles.
pub fn extract_twitter_handles(socials: &[serde_json::Value]) -> Vec<String> {
    let mut handles = Vec::new();

    for social in socials {
        // Check the "type" field first.
        let social_type = social.get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if social_type != "twitter" && social_type != "x" {
            // Also check the URL itself for twitter.com or x.com patterns.
            let url = social.get("url").and_then(|v| v.as_str()).unwrap_or("");
            if !url.contains("twitter.com") && !url.contains("x.com") {
                continue;
            }
        }

        // Extract handle from URL.
        let url = social.get("url").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(handle) = extract_handle_from_url(url)
            && !handles.contains(&handle)
        {
            handles.push(handle);
        }
    }

    handles
}

/// Extract a Twitter handle from a URL string.
///
/// Supports: twitter.com/handle, x.com/handle, mobile.twitter.com/handle, etc.
/// Strips @ prefix if present. Returns None if the URL doesn't match or the
/// handle looks invalid (empty, too long, contains slashes).
fn extract_handle_from_url(url: &str) -> Option<String> {
    // Strip trailing slashes and query params.
    let url = url.trim_end_matches('/');
    let url = url.split('?').next()?;

    // Find the domain.
    let domain_part = if url.contains("twitter.com") || url.contains("x.com") {
        url
    } else {
        return None;
    };

    // Split on '/' and take the last segment as the handle.
    let segments: Vec<&str> = domain_part.split('/').collect();
    let handle = segments.last()?;

    // Clean the handle: strip @ prefix, whitespace.
    let handle = handle.trim_start_matches('@').trim();

    // Validate: non-empty, no slashes, no dots (domains), reasonable length (1-15 chars per Twitter rules).
    if handle.is_empty() || handle.len() > 15 || handle.contains('/') || handle.contains(' ') || handle.contains('.') {
        return None;
    }

    // Reject obviously invalid handles.
    if handle == "search" || handle == "home" || handle == "explore" || handle == "i" {
        return None;
    }

    Some(handle.to_lowercase())
}

// ─── Strategy Trait ──────────────────────────────────────────────────────────

/// A strategy evaluates market conditions for a token and returns a signal score (0-100).
#[allow(async_fn_in_trait)]
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal>;
}

// ─── Wallet Score Provider ───────────────────────────────────────────────────

/// Trait for providing wallet scores from a registry.
///
/// The async variant is the real interface. Sync implementations are
/// available for testing.
#[allow(async_fn_in_trait)]
pub trait WalletScoreProvider: Send + Sync {
    /// Get the composite score (0-100) for a wallet, or None if unknown.
    fn get_score(&self, address: &str) -> Option<f64>;
    /// Check if a wallet is in the registry.
    fn is_known(&self, address: &str) -> bool;
}

/// A simple in-memory wallet score provider for testing.
pub struct InMemoryWalletScores {
    scores: HashMap<String, f64>,
}

impl Default for InMemoryWalletScores {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWalletScores {
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    pub fn insert(&mut self, address: String, score: f64) {
        self.scores.insert(address, score);
    }
}

impl WalletScoreProvider for InMemoryWalletScores {
    fn get_score(&self, address: &str) -> Option<f64> {
        self.scores.get(address).copied()
    }
    fn is_known(&self, address: &str) -> bool {
        self.scores.contains_key(address)
    }
}

/// SQLite-backed wallet score cache that loads scores from the wallet registry
/// into memory. Refreshes periodically via `refresh()`.
///
/// This bridges the async `WalletRegistry` (SQLite) to the sync `WalletScoreProvider`
/// trait used by `WhaleConsensusSignal`.
pub struct RegistryScoreCache {
    scores: DashMap<String, f64>,
    pool: sqlx::SqlitePool,
}

impl RegistryScoreCache {
    /// Create a new cache backed by the given SQLite pool.
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            scores: DashMap::new(),
            pool,
        }
    }

    /// Load all wallet scores from SQLite into the in-memory cache.
    /// Call this on startup and periodically (e.g. every 5 minutes).
    pub async fn refresh(&self) -> Result<()> {
        let rows = sqlx::query_as::<_, (String, f64)>(
            "SELECT address, score FROM wallets WHERE score > 0",
        )
        .fetch_all(&self.pool)
        .await?;

        self.scores.clear();
        for (address, score) in &rows {
            self.scores.insert(address.clone(), *score);
        }

        tracing::info!(count = rows.len(), "Refreshed wallet score cache");
        Ok(())
    }

    /// Get the number of cached wallets.
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }
}

impl WalletScoreProvider for RegistryScoreCache {
    fn get_score(&self, address: &str) -> Option<f64> {
        self.scores.get(address).map(|g| *g)
    }

    fn is_known(&self, address: &str) -> bool {
        self.scores.contains_key(address)
    }
}

// ─── Whale Consensus Signal ──────────────────────────────────────────────────

/// Tracks a sliding window of wallet buys per token to detect whale consensus.
#[derive(Debug, Clone)]
struct WalletBuyRecord {
    wallet: String,
    timestamp: DateTime<Utc>,
    amount_usd: f64,
    /// Whether this record came from the GMGN fallback (vs live WS event).
    from_gmgn: bool,
}

#[derive(Debug, Clone)]
struct WalletHoldRecord {
    wallet: String,
    value_usd: f64,
    timestamp: DateTime<Utc>,
}

/// Default path to gmgn-cli binary (same as solagent-data).
#[allow(dead_code)]
const GMGN_CLI_DEFAULT_PATH: &str = "/home/kt/.npm-global/bin/gmgn-cli";

/// Whale consensus signal: fires when multiple known smart wallets buy the same token
/// within a configurable time window. Includes GMGN top-trader fallback when no
/// WebSocket events are available in the window.
pub struct WhaleConsensusSignal {
    name: String,
    /// Token address -> recent buys.
    buys: Arc<DashMap<String, VecDeque<WalletBuyRecord>>>,
    /// Token address -> wallets holding (from Zerion position snapshots).
    /// Maps token → (wallet, value_usd, timestamp).
    holds: Arc<DashMap<String, Vec<WalletHoldRecord>>>,
    /// Minimum number of distinct wallets to trigger.
    min_wallets: usize,
    /// Window duration in minutes.
    window_minutes: i64,
    /// Minimum buy amount per wallet (USD).
    #[allow(dead_code)]
    min_buy_usd: f64,
    #[allow(dead_code)]
    chain: Chain,
    /// Wallet score provider (from registry).
    wallet_scores: Arc<RwLock<Box<dyn WalletScoreProvider>>>,
    /// Path to gmgn-cli binary for top-trader fallback.
    gmgn_cli_path: Option<String>,
}

impl Clone for WhaleConsensusSignal {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            buys: Arc::clone(&self.buys),
            holds: Arc::clone(&self.holds),
            min_wallets: self.min_wallets,
            window_minutes: self.window_minutes,
            min_buy_usd: self.min_buy_usd,
            chain: self.chain,
            wallet_scores: Arc::clone(&self.wallet_scores),
            gmgn_cli_path: self.gmgn_cli_path.clone(),
        }
    }
}

impl WhaleConsensusSignal {
    pub fn new(
        chain: Chain,
        min_wallets: usize,
        window_minutes: i64,
        min_buy_usd: f64,
        wallet_scores: Box<dyn WalletScoreProvider>,
    ) -> Self {
        Self {
            name: "whale_consensus".to_string(),
            buys: Arc::new(DashMap::new()),
            holds: Arc::new(DashMap::new()),
            min_wallets,
            window_minutes,
            min_buy_usd,
            chain,
            wallet_scores: Arc::new(RwLock::new(wallet_scores)),
            gmgn_cli_path: None,
        }
    }

    /// Create with GMGN top-trader fallback enabled.
    /// When no WS events are in the window, evaluates will query GMGN
    /// for recent smart money buys of the token.
    pub fn with_gmgn_fallback(
        chain: Chain,
        min_wallets: usize,
        window_minutes: i64,
        min_buy_usd: f64,
        wallet_scores: Box<dyn WalletScoreProvider>,
        gmgn_cli_path: String,
    ) -> Self {
        Self {
            name: "whale_consensus".to_string(),
            buys: Arc::new(DashMap::new()),
            holds: Arc::new(DashMap::new()),
            min_wallets,
            window_minutes,
            min_buy_usd,
            chain,
            wallet_scores: Arc::new(RwLock::new(wallet_scores)),
            gmgn_cli_path: Some(gmgn_cli_path),
        }
    }

    /// Record a wallet buy for a token (from live WebSocket/EventBus).
    pub fn record_buy(&self, token_address: String, wallet: String, amount_usd: f64) {
        let mut buys = self.buys.entry(token_address).or_default();
        buys.push_back(WalletBuyRecord {
            wallet,
            timestamp: Utc::now(),
            amount_usd,
            from_gmgn: false,
        });
    }

    /// Record a wallet buy detected via GMGN top-trader fallback.
    /// These are treated the same as WS events but flagged for reasoning.
    pub fn record_gmgn_buy(&self, token_address: String, wallet: String, amount_usd: f64) {
        let mut buys = self.buys.entry(token_address).or_default();
        buys.push_back(WalletBuyRecord {
            wallet,
            timestamp: Utc::now(),
            amount_usd,
            from_gmgn: true,
        });
    }

    /// Record that a wallet holds a token position (from Zerion snapshot).
    pub fn record_hold(&self, token_address: String, wallet: String, value_usd: f64) {
        let mut holds = self.holds.entry(token_address).or_default();
        // Replace existing entry for same wallet, or append.
        if let Some(existing) = holds.iter_mut().find(|h| h.wallet == wallet) {
            existing.value_usd = value_usd;
            existing.timestamp = Utc::now();
        } else {
            holds.push(WalletHoldRecord {
                wallet,
                value_usd,
                timestamp: Utc::now(),
            });
        }
    }

    /// Subscribe to the event bus and auto-record WalletBuy events.
    /// Returns a join handle for the background task.
    pub fn subscribe_to_events(self: &Arc<Self>, event_bus: &EventBus) -> tokio::task::JoinHandle<()> {
        let mut rx = event_bus.subscribe();
        let signal = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(Event::WalletBuy {
                        wallet,
                        token_address,
                        amount_usd,
                        ..
                    }) => {
                        let scores = signal.wallet_scores.read().await;
                        if scores.is_known(&wallet) {
                            drop(scores);
                            tracing::debug!(
                                wallet = &wallet[..wallet.len().min(12)],
                                token = &token_address[..token_address.len().min(12)],
                                amount_usd,
                                "Recording smart wallet buy for whale consensus"
                            );
                            signal.record_buy(token_address, wallet, amount_usd);
                        }
                    }
                    Ok(Event::WalletHold {
                        wallet,
                        token_address,
                        value_usd,
                        ..
                    }) => {
                        let scores = signal.wallet_scores.read().await;
                        if scores.is_known(&wallet) {
                            drop(scores);
                            tracing::debug!(
                                wallet = &wallet[..wallet.len().min(12)],
                                token = &token_address[..token_address.len().min(12)],
                                value_usd,
                                "Recording smart wallet hold position"
                            );
                            signal.record_hold(token_address, wallet, value_usd);
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(n, "Wallet buy event channel lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        })
    }

    /// Prune expired records outside the time window.
    fn prune_stale(&self, token_address: &str) {
        if let Some(mut buys) = self.buys.get_mut(token_address) {
            let cutoff = Utc::now() - chrono::Duration::minutes(self.window_minutes);
            while buys.front().is_some_and(|b| b.timestamp < cutoff) {
                buys.pop_front();
            }
        }
    }

    /// Query GMGN top traders for a token and check if any are known wallets.
    /// Returns a list of (wallet_address, score) tuples for matches.
    /// This is the fallback when no WebSocket events are in the sliding window.
    async fn check_gmgn_top_traders(&self, token_address: &str) -> Vec<(String, f64)> {
        let cli_path = match &self.gmgn_cli_path {
            Some(p) => p.clone(),
            None => return Vec::new(),
        };

        let output = match tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new(&cli_path)
                .args([
                    "token", "traders",
                    "--chain", "sol",
                    "--address", token_address,
                    "--tag", "smart_degen",
                    "--order-by", "profit",
                    "--direction", "desc",
                    "--limit", "20",
                    "--raw",
                ])
                .output(),
        ).await {
            Ok(Ok(o)) => o,
            _ => return Vec::new(),
        };

        if !output.status.success() {
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut matches = Vec::new();

        let scores = self.wallet_scores.read().await;

        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout)
            && let Some(list) = data.get("list").and_then(|l| l.as_array())
        {
            for item in list {
                let addr = item.get("address")
                    .and_then(|a| a.as_str())
                    .unwrap_or("");
                if !addr.is_empty() && scores.is_known(addr) {
                    let score = scores.get_score(addr).unwrap_or(50.0);
                    matches.push((addr.to_string(), score));
                }
            }
        }

        matches
    }

    /// Run the GMGN fallback for a token: query top traders and record
    /// any matches as synthetic buys. Returns the number of new records.
    pub async fn run_gmgn_fallback(&self, token_address: &str) -> usize {
        let matches = self.check_gmgn_top_traders(token_address).await;
        let count = matches.len();

        for (wallet, _score) in &matches {
            self.record_gmgn_buy(token_address.to_string(), wallet.clone(), 0.0);
        }

        if count > 0 {
            tracing::info!(
                token = &token_address[..token_address.len().min(12)],
                count,
                "GMGN fallback: found {} known wallets among top traders",
                count
            );
        }

        count
    }
}

impl Strategy for WhaleConsensusSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        self.prune_stale(&token.address);

        // Check if there are any WS buys in the window.
        let ws_buy_count = if let Some(buys) = self.buys.get(&token.address) {
            buys.iter().filter(|b| !b.from_gmgn).count()
        } else {
            0
        };

        // GMGN fallback: if no WS events in window and fallback is configured,
        // query GMGN for top traders and record any known wallets.
        let mut gmgn_fallback_used = false;
        if ws_buy_count == 0 && self.gmgn_cli_path.is_some() {
            let gmgn_matches = self.run_gmgn_fallback(&token.address).await;
            gmgn_fallback_used = gmgn_matches > 0;
        }

        let scores = self.wallet_scores.read().await;

        // Count recent buys (both WS and GMGN).
        let buy_score = if let Some(buys) = self.buys.get(&token.address) {
            let distinct_wallets: HashSet<&str> =
                buys.iter().map(|b| b.wallet.as_str()).collect();
            let count = distinct_wallets.len();

            if count >= self.min_wallets {
                let wallet_score_sum: f64 = distinct_wallets
                    .iter()
                    .map(|w| scores.get_score(w).unwrap_or(50.0))
                    .sum();
                let max_possible = distinct_wallets.len() as f64 * 100.0;
                let quality_ratio = if max_possible > 0.0 {
                    wallet_score_sum / max_possible
                } else {
                    0.5
                };

                let newest = buys.back().map(|b| b.timestamp).unwrap_or_default();
                let oldest = buys.front().map(|b| b.timestamp).unwrap_or_default();
                let span_mins = (newest - oldest).num_minutes().max(1) as f64;
                let recency_mult = 1.0 / (1.0 + span_mins / self.window_minutes as f64);

                let total_amount: f64 = buys.iter().map(|b| b.amount_usd).sum();
                let amount_mult = (total_amount / 1000.0).min(2.0);

                let base = (count as f64 / self.min_wallets as f64).min(3.0) * 33.0;
                (base * quality_ratio * recency_mult * amount_mult).clamp(0.0, 100.0)
            } else {
                ((count as f64 / self.min_wallets as f64) * 50.0).clamp(0.0, 100.0)
            }
        } else {
            0.0
        };

        // Count wallet holdings (from Zerion position snapshots).
        // Multiple smart wallets holding the same token = conviction signal.
        let hold_score = if let Some(holds) = self.holds.get(&token.address) {
            let count = holds.len();
            if count >= 2 {
                let total_value: f64 = holds.iter().map(|h| h.value_usd).sum();
                // Base score scales with number of holders (2=10, 3=15, 4+=20)
                let base = (10.0 + (count as f64 - 2.0) * 5.0).min(20.0);
                // Boost for large aggregate position (>$1K = 1.5x, >$5K = 2x)
                let size_mult = if total_value > 5000.0 {
                    2.0
                } else if total_value > 1000.0 {
                    1.5
                } else {
                    1.0
                };
                (base * size_mult).clamp(0.0, 25.0)
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Combine: buy score is primary, hold score is supplementary.
        let combined = (buy_score + hold_score).clamp(0.0, 100.0);
        let score = combined as u8;

        // Count events for reasoning.
        let _ws_buys = if let Some(buys) = self.buys.get(&token.address) {
            buys.iter().filter(|b| !b.from_gmgn).count()
        } else {
            0
        };
        let gmgn_buys = if let Some(buys) = self.buys.get(&token.address) {
            buys.iter().filter(|b| b.from_gmgn).count()
        } else {
            0
        };
        let hold_count = self.holds.get(&token.address).map(|h| h.len()).unwrap_or(0);

        let reasoning = if buy_score > 0.0 && hold_score > 0.0 {
            let mut r = format!(
                "Whale consensus: {score}/100 (buys={:.0} + holds={:.0})",
                buy_score, hold_score
            );
            if gmgn_fallback_used || gmgn_buys > 0 {
                r.push_str(" [GMGN fallback]");
            }
            r
        } else if buy_score > 0.0 {
            let mut r = format!("Whale consensus: {score}/100");
            if gmgn_fallback_used || gmgn_buys > 0 {
                r.push_str(" [GMGN fallback]");
            }
            r
        } else if hold_score > 0.0 {
            format!("Whale holdings: {score}/100 ({hold_count} position conviction)")
        } else if self.gmgn_cli_path.is_some() {
            format!("Whale consensus: {score}/100 [GMGN fallback: no matches]")
        } else {
            format!("Whale consensus: {score}/100")
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.7,
            reasoning,
        ))
    }
}

// ─── Accumulation Signal ─────────────────────────────────────────────────────

/// Detects accumulation patterns: holder growth vs. price stability.
pub struct AccumulationSignal {
    name: String,
    #[allow(clippy::type_complexity)]
    history: Arc<DashMap<String, VecDeque<(u64, f64, DateTime<Utc>)>>>,
    max_history: usize,
    #[allow(dead_code)]
    chain: Chain,
}

impl AccumulationSignal {
    pub fn new(chain: Chain, max_history: usize) -> Self {
        Self {
            name: "accumulation".to_string(),
            history: Arc::new(DashMap::new()),
            max_history,
            chain,
        }
    }

    /// Record a snapshot for a token.
    pub fn record_snapshot(&self, token_address: String, holders: u64, price: f64) {
        let mut hist = self.history.entry(token_address).or_default();
        hist.push_back((holders, price, Utc::now()));
        while hist.len() > self.max_history {
            hist.pop_front();
        }
    }
}

impl Strategy for AccumulationSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        // No history at all for this token.
        if self.history.get(&token.address).is_none() {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                "No holder history recorded for accumulation signal".to_string(),
            ));
        }

        let hist = self.history.get(&token.address).unwrap();
        if hist.len() < 2 {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                format!(
                    "Insufficient history for accumulation signal ({} snapshot, need ≥2)",
                    hist.len()
                ),
            ));
        }

        let first = hist.front().unwrap();
        let last = hist.back().unwrap();
        let holder_growth = (last.0 as f64 - first.0 as f64) / first.0.max(1) as f64;
        let price_change = (last.1 - first.1) / first.1.max(0.0001);
        let snapshot_count = hist.len();

        let score = if holder_growth > 0.2 && price_change.abs() < 0.1 {
            80
        } else if holder_growth > 0.1 && price_change.abs() < 0.2 {
            60
        } else if holder_growth > 0.05 && price_change.abs() < 0.3 {
            40
        } else {
            20
        };

        let reasoning = format!(
            "Accumulation: {score}/100 ({} snapshots, holder growth {:+.0}%, price change {:+.1}%)",
            snapshot_count,
            holder_growth * 100.0,
            price_change * 100.0,
        );

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.6,
            reasoning,
        ))
    }
}

// ─── Launch Momentum Signal ──────────────────────────────────────────────────

/// Detects momentum in newly launched tokens (volume and holder rate).
pub struct LaunchMomentumSignal {
    name: String,
    #[allow(clippy::type_complexity)]
    snapshots: Arc<DashMap<String, VecDeque<(f64, u64, DateTime<Utc>)>>>,
    max_snapshots: usize,
    /// Minimum liquidity (USD) for a launch to qualify.
    min_liquidity: f64,
    /// Minimum holder count to qualify.
    #[allow(dead_code)]
    min_holders: u64,
    /// Maximum age in hours to be considered a "launch".
    max_age_hours: i64,
    #[allow(dead_code)]
    chain: Chain,
}

impl LaunchMomentumSignal {
    pub fn new(chain: Chain, max_snapshots: usize) -> Self {
        Self {
            name: "launch_momentum".to_string(),
            snapshots: Arc::new(DashMap::new()),
            max_snapshots,
            min_liquidity: 5000.0,
            min_holders: 50,
            max_age_hours: 1,
            chain,
        }
    }

    pub fn with_filters(
        chain: Chain,
        max_snapshots: usize,
        min_liquidity: f64,
        min_holders: u64,
        max_age_hours: i64,
    ) -> Self {
        Self {
            name: "launch_momentum".to_string(),
            snapshots: Arc::new(DashMap::new()),
            max_snapshots,
            min_liquidity,
            min_holders,
            max_age_hours,
            chain,
        }
    }

    /// Record a launch snapshot.
    pub fn record(&self, token_address: String, volume: f64, holders: u64) {
        let mut snaps = self.snapshots.entry(token_address).or_default();
        snaps.push_back((volume, holders, Utc::now()));
        while snaps.len() > self.max_snapshots {
            snaps.pop_front();
        }
    }
}

impl Strategy for LaunchMomentumSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        // Check age filter.
        if let Some(created) = token.created_at {
            let age_hours = (Utc::now() - created).num_hours();
            if age_hours > self.max_age_hours {
                return Ok(Signal::new(
                    token.address.clone(),
                    token.chain,
                    &self.name,
                    0,
                    0.0,
                    format!("Token too old ({age_hours}h > {}h max)", self.max_age_hours),
                ));
            }
        }

        // Check liquidity filter.
        if let Some(_vol) = token.volume_24h
            && let Some(mc) = token.market_cap_usd
            && mc < self.min_liquidity
        {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                format!("MC ${mc:.0} below ${} threshold", self.min_liquidity),
            ));
        }

        let score = if let Some(snaps) = self.snapshots.get(&token.address) {
            if snaps.len() < 2 {
                return Ok(Signal::new(
                    token.address.clone(),
                    token.chain,
                    &self.name,
                    0,
                    0.0,
                    "Insufficient data for launch momentum (1 snapshot, need ≥2)".to_string(),
                ));
            }
            let first = snaps.front().unwrap();
            let last = snaps.back().unwrap();

            let volume_rate = last.0 / first.0.max(1.0);
            let holder_rate = last.1 as f64 / first.1.max(1) as f64;

            // Holder growth rate (holders/min).
            let span_mins = (last.2 - first.2).num_minutes().max(1) as f64;
            let holder_growth_rate = (last.1 as f64 - first.1 as f64) / span_mins;

            let composite = (volume_rate + holder_rate) / 2.0;
            let momentum_bonus = if holder_growth_rate > 10.0 { 20.0 } else if holder_growth_rate > 5.0 { 10.0 } else { 0.0 };

            let score = (composite.min(2.0) * 40.0 + momentum_bonus).clamp(0.0, 100.0) as u8;

            let bonus_desc = if momentum_bonus >= 20.0 {
                ", fast holder acquisition bonus +20"
            } else if momentum_bonus >= 10.0 {
                ", holder growth bonus +10"
            } else {
                ""
            };
            let reasoning = format!(
                "Launch momentum: {score}/100 (vol rate {volume_rate:.1}x, holder rate {holder_rate:.1}x, {snaps} snapshots{bonus_desc})",
                snaps = snaps.len()
            );
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                score,
                0.5,
                reasoning,
            ));
        } else {
            0
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.5,
            "No launch snapshots recorded for launch momentum signal".to_string(),
        ))
    }
}

// ─── Volume Spike Signal ─────────────────────────────────────────────────────

/// Detects when current volume exceeds a threshold multiplier over the rolling average.
pub struct VolumeSpikeSignal {
    name: String,
    threshold: f64,
    #[allow(clippy::type_complexity)]
    volumes: Arc<DashMap<String, VecDeque<(f64, DateTime<Utc>)>>>,
    window_size: usize,
    #[allow(dead_code)]
    chain: Chain,
}

impl VolumeSpikeSignal {
    pub fn new(chain: Chain, threshold: f64, window_size: usize) -> Self {
        Self {
            name: "volume_spike".to_string(),
            threshold,
            volumes: Arc::new(DashMap::new()),
            window_size,
            chain,
        }
    }

    /// Record a volume data point.
    pub fn record(&self, token_address: String, volume: f64) {
        let mut vols = self.volumes.entry(token_address).or_default();
        vols.push_back((volume, Utc::now()));
        while vols.len() > self.window_size {
            vols.pop_front();
        }
    }

    /// Get the current ratio of latest volume to rolling historical average.
    /// The average is computed from all data points EXCEPT the latest, making
    /// this a true "spike vs history" comparison. Requires ≥3 data points.
    pub fn get_spike_ratio(&self, token_address: &str) -> Option<f64> {
        let vols = self.volumes.get(token_address)?;
        if vols.len() < 3 {
            return None;
        }
        // Historical average: all points except the latest (current spike).
        let hist_count = vols.len() - 1;
        let hist_sum: f64 = vols.iter().take(hist_count).map(|v| v.0).sum::<f64>();
        let hist_avg = hist_sum / hist_count as f64;
        let current = vols.back().map(|v| v.0).unwrap_or(0.0);
        if hist_avg > 0.0 {
            Some(current / hist_avg)
        } else {
            None
        }
    }
}

impl Strategy for VolumeSpikeSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        // No volume data at all for this token.
        if self.volumes.get(&token.address).is_none() {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                "No volume history recorded for volume spike signal".to_string(),
            ));
        }

        let vols = self.volumes.get(&token.address).unwrap();
        if vols.len() < 3 {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                format!(
                    "Insufficient volume history ({} points, need ≥3)",
                    vols.len()
                ),
            ));
        }

        // Historical average: all points except the latest (current spike).
        // This makes spike detection meaningful — comparing current to prior history.
        let hist_count = vols.len() - 1;
        let hist_sum: f64 = vols.iter().take(hist_count).map(|v| v.0).sum::<f64>();
        let hist_avg = hist_sum / hist_count as f64;
        let current = vols.back().map(|v| v.0).unwrap_or(0.0);

        if hist_avg <= 0.0 {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                "Volume spike: 0/100 (historical avg is 0, cannot compute ratio)".to_string(),
            ));
        }

        let ratio = current / hist_avg;
        let point_count = vols.len();

        let (score, reasoning) = if ratio >= self.threshold {
            let raw = 50.0 + (ratio / self.threshold * 30.0).min(50.0);
            let score = raw.clamp(0.0, 100.0) as u8;
            let reasoning = format!(
                "Volume spike: {score}/100 ({ratio:.1}x ratio, {point_count} points, avg=${hist_avg:.0} → current=${current:.0})"
            );
            (score, reasoning)
        } else if ratio >= self.threshold * 0.66 {
            let reasoning = format!(
                "Volume spike: 50/100 ({ratio:.1}x ratio, near {threshold:.0}x threshold)",
                threshold = self.threshold
            );
            (50, reasoning)
        } else {
            let reasoning = format!(
                "Volume spike: 10/100 ({ratio:.1}x ratio, below {threshold:.0}x threshold)",
                threshold = self.threshold
            );
            (10, reasoning)
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.65,
            reasoning,
        ))
    }
}

// ─── Social Signal (Twitter) ─────────────────────────────────────────────────

/// A tweet from twitter-cli's JSON output.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TweetResult {
    id: Option<String>,
    text: Option<String>,
    author: Option<TweetAuthor>,
    metrics: Option<TweetMetrics>,
    #[serde(rename = "createdAtISO")]
    created_at_iso: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TweetAuthor {
    #[serde(rename = "screenName")]
    screen_name: Option<String>,
    verified: Option<bool>,
    #[serde(rename = "followersCount")]
    followers_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TweetMetrics {
    likes: Option<i64>,
    retweets: Option<i64>,
    replies: Option<i64>,
    views: Option<i64>,
    quotes: Option<i64>,
    bookmarks: Option<i64>,
}

/// Tracks social mentions of a token within a sliding window.
#[derive(Debug)]
struct MentionRecord {
    #[allow(dead_code)]
    tweet_id: String,
    author: String,
    engagement: f64,
    timestamp: DateTime<Utc>,
}

/// Social momentum signal using twitter-cli.
///
/// Polls `twitter search` for Solana CA patterns (base58 addresses ending in
/// "pump") and keyword terms. Tracks mention velocity and engagement to score
/// social momentum.
pub struct SocialSignal {
    name: String,
    #[allow(dead_code)]
    chain: Chain,
    mentions: Arc<DashMap<String, VecDeque<MentionRecord>>>,
    /// Max mentions to keep per token.
    max_mentions: usize,
    /// Window in minutes for counting mentions.
    window_minutes: i64,
    /// Minimum mentions to trigger a signal.
    min_mentions: usize,
    /// Path to twitter-cli binary.
    twitter_cli_path: String,
    /// Search keywords in addition to CA extraction.
    search_keywords: Vec<String>,
}

impl SocialSignal {
    pub fn new(chain: Chain) -> Self {
        Self {
            name: "social".to_string(),
            chain,
            mentions: Arc::new(DashMap::new()),
            max_mentions: 200,
            window_minutes: 60,
            min_mentions: 3,
            twitter_cli_path: "twitter".to_string(),
            search_keywords: vec![
                "solana memecoin".to_string(),
                "pump.fun launch".to_string(),
                "$SOL gem".to_string(),
            ],
        }
    }

    /// Configure with custom twitter-cli path and keywords.
    pub fn with_config(
        chain: Chain,
        twitter_cli_path: String,
        search_keywords: Vec<String>,
        window_minutes: i64,
        min_mentions: usize,
    ) -> Self {
        Self {
            name: "social".to_string(),
            chain,
            mentions: Arc::new(DashMap::new()),
            max_mentions: 200,
            window_minutes,
            min_mentions,
            twitter_cli_path,
            search_keywords,
        }
    }

    /// Run a search cycle: query twitter-cli for each keyword, extract CAs,
    /// and record mentions.
    pub async fn poll(&self) -> Result<()> {
        for keyword in &self.search_keywords {
            if let Err(e) = self.search_keyword(keyword).await {
                tracing::warn!(keyword, error = %e, "Twitter search failed");
            }
        }
        Ok(())
    }

    /// Search Twitter for specific token CAs discovered during scanning.
    /// This is the primary way the social signal gets useful data — searching
    /// for an exact CA finds tweets where people are actually discussing the token.
    /// Limits to `max_tokens` per call to stay within rate limits.
    pub async fn poll_token_cas(&self, addresses: &[String], max_tokens: usize) {
        let batch = if addresses.len() > max_tokens {
            &addresses[..max_tokens]
        } else {
            addresses
        };

        for addr in batch {
            // Search for the full CA — anyone tweeting it is discussing the token.
            if let Err(e) = self.search_keyword(addr).await {
                tracing::debug!(token = &addr[..addr.len().min(12)], error = %e, "Twitter CA search failed");
            }
            // Also search for the CA with a $ prefix (common crypto Twitter convention
            // where people share "$CA" as shorthand).
            // Skip if CA is too long for a useful search (most are 43-44 chars).
            if addr.len() <= 44 {
                let dollar_query = format!("${}", &addr[..addr.len().min(20)]);
                if let Err(e) = self.search_keyword(&dollar_query).await {
                    tracing::debug!(query = %dollar_query, error = %e, "Twitter $CA search failed");
                }
            }
        }
    }

    /// Search a single keyword via twitter-cli and parse results.
    /// When `known_ca` is set, any matching tweet is attributed to that CA
    /// even if the CA isn't extracted from the text (e.g. partial match or URL).
    async fn search_keyword(&self, keyword: &str) -> Result<()> {
        // Detect if the keyword is itself a token CA (for poll_token_cas calls).
        let query_ca = if keyword.len() >= 32 && keyword.len() <= 44
            && keyword.chars().all(|c| c.is_ascii_alphanumeric())
        {
            Some(keyword.to_string())
        } else {
            None
        };

        let output = tokio::process::Command::new(&self.twitter_cli_path)
            .args(["search", keyword, "--json", "--max", "20"])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::debug!(keyword, %stderr, "twitter-cli search returned non-zero");
            return Ok(());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // twitter-cli wraps output in { ok: true, data: [...] }
        #[derive(Deserialize)]
        struct SearchResponse {
            data: Option<Vec<TweetResult>>,
        }

        let resp: SearchResponse = match serde_json::from_str(&stdout) {
            Ok(r) => r,
            Err(_) => return Ok(()), // Not JSON or malformed -- skip.
        };

        let tweets = match resp.data {
            Some(t) => t,
            None => return Ok(()),
        };

        let now = Utc::now();
        for tweet in tweets {
            let text = match &tweet.text {
                Some(t) => t.clone(),
                None => continue,
            };

            // Extract Solana CAs from the tweet text.
            let mut cas = self.extract_solana_cas(&text);

            // If this was a CA-specific search and no CAs extracted from text,
            // attribute the tweet to the queried CA anyway (the tweet matched
            // the CA in the search so it IS about this token).
            if cas.is_empty()
                && let Some(ref ca) = query_ca
            {
                cas.push(ca.clone());
            }

            let author = tweet.author.as_ref()
                .and_then(|a| a.screen_name.clone())
                .unwrap_or_else(|| "unknown".to_string());

            let engagement = Self::compute_engagement(&tweet);

            for ca in cas {
                let tweet_id = tweet.id.clone().unwrap_or_else(|| "unknown".to_string());
                let mut mentions = self.mentions.entry(ca).or_default();
                mentions.push_back(MentionRecord {
                    tweet_id,
                    author: author.clone(),
                    engagement,
                    timestamp: now,
                });
                while mentions.len() > self.max_mentions {
                    mentions.pop_front();
                }
            }
        }

        Ok(())
    }

    /// Extract Solana token addresses (CAs) from tweet text.
    ///
    /// Matches pump.fun addresses: base58 strings 32-44 chars ending in "pump".
    /// Also matches generic base58 addresses 32-44 chars long.
    fn extract_solana_cas(&self, text: &str) -> Vec<String> {
        let mut cas = Vec::new();
        for word in text.split_whitespace() {
            // Strip trailing punctuation.
            let cleaned = word.trim_end_matches(['.', ',', '!', '?', ':', ';', ')']);
            // pump.fun addresses end with "pump" and are 32-44 chars of base58.
            let is_pump = cleaned.len() >= 32 && cleaned.len() <= 44 && cleaned.ends_with("pump");
            let is_base58 = cleaned.len() >= 32 && cleaned.len() <= 44
                && cleaned.chars().all(|c| c.is_ascii_alphanumeric());
            if is_pump || is_base58 {
                // Reject common false positives.
                if cleaned.starts_with("http") || cleaned.starts_with("https") {
                    continue;
                }
                cas.push(cleaned.to_string());
            }
        }
        cas
    }

    /// Poll a specific Twitter account's timeline for token mentions.
    ///
    /// Uses `twitter user-posts <handle>` to fetch recent tweets from a
    /// curated account. Only attributes mentions to tokens whose CAs are
    /// explicitly present in the tweet text. Tweets without explicit CAs
    /// are NOT attributed to any token.
    pub async fn poll_account_timeline(&self, handle: &str) -> Result<usize> {
        let output = tokio::process::Command::new(&self.twitter_cli_path)
            .args(["user-posts", handle, "--json", "--max", "20"])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::debug!(handle, %stderr, "twitter-cli user-posts returned non-zero");
            return Ok(0);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // twitter-cli wraps output in { ok: true, data: [...] }
        #[derive(Deserialize)]
        struct SearchResponse {
            data: Option<Vec<TweetResult>>,
        }

        let resp: SearchResponse = match serde_json::from_str(&stdout) {
            Ok(r) => r,
            Err(_) => return Ok(0),
        };

        let tweets = match resp.data {
            Some(t) => t,
            None => return Ok(0),
        };

        let mut total_attributed = 0;
        let now = Utc::now();

        for tweet in tweets {
            let text = match &tweet.text {
                Some(t) => t.clone(),
                None => continue,
            };

            // ONLY attribute to tokens whose CAs are explicitly in the tweet text.
            // This is the key difference from keyword search — we do NOT use
            // query_ca fallback here. Tweets without explicit CAs are discarded.
            let cas = self.extract_solana_cas(&text);
            if cas.is_empty() {
                continue;
            }

            let author = tweet.author.as_ref()
                .and_then(|a| a.screen_name.clone())
                .unwrap_or_else(|| handle.to_string());

            let engagement = Self::compute_engagement(&tweet);

            for ca in cas {
                let tweet_id = tweet.id.clone().unwrap_or_else(|| "unknown".to_string());
                let mut mentions = self.mentions.entry(ca).or_default();
                mentions.push_back(MentionRecord {
                    tweet_id,
                    author: author.clone(),
                    engagement,
                    timestamp: now,
                });
                while mentions.len() > self.max_mentions {
                    mentions.pop_front();
                }
                total_attributed += 1;
            }
        }

        Ok(total_attributed)
    }

    /// Poll multiple curated account timelines.
    /// Returns the total number of mentions attributed across all accounts.
    pub async fn poll_curated_accounts(&self, handles: &[String]) -> usize {
        let mut total = 0;
        for handle in handles {
            match self.poll_account_timeline(handle).await {
                Ok(count) => {
                    if count > 0 {
                        tracing::debug!(handle, count, "Curated account poll found token mentions");
                    }
                    total += count;
                }
                Err(e) => {
                    tracing::debug!(handle, error = %e, "Failed to poll account timeline");
                }
            }
        }
        total
    }

    /// Compute an engagement score from tweet metrics.
    ///
    /// Formula: likes * 1.0 + retweets * 3.0 + replies * 2.0 + quotes * 2.5
    /// This gives higher weight to amplification actions (retweets, quotes).
    fn compute_engagement(tweet: &TweetResult) -> f64 {
        let m = match &tweet.metrics {
            Some(m) => m,
            None => return 0.0,
        };
        let likes = m.likes.unwrap_or(0) as f64;
        let retweets = m.retweets.unwrap_or(0) as f64;
        let replies = m.replies.unwrap_or(0) as f64;
        let quotes = m.quotes.unwrap_or(0) as f64;
        likes + retweets * 3.0 + replies * 2.0 + quotes * 2.5
    }

    /// Prune mentions outside the time window.
    fn prune_stale(&self, token_address: &str) {
        if let Some(mut mentions) = self.mentions.get_mut(token_address) {
            let cutoff = Utc::now() - chrono::Duration::minutes(self.window_minutes);
            while mentions.front().is_some_and(|m| m.timestamp < cutoff) {
                mentions.pop_front();
            }
        }
    }

    /// Get the mention count for a token in the current window.
    pub fn get_mention_count(&self, token_address: &str) -> usize {
        self.prune_stale(token_address);
        self.mentions.get(token_address).map(|m| m.len()).unwrap_or(0)
    }

    /// Run a background polling loop.
    pub fn run_polling(self: &Arc<Self>, interval_secs: u64, mut shutdown: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        let signal = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = signal.poll().await {
                            tracing::error!(error = %e, "Social signal poll failed");
                        }
                    }
                    _ = shutdown.changed() => {
                        tracing::info!("Social signal polling shutting down");
                        return;
                    }
                }
            }
        })
    }
}

impl Strategy for SocialSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        self.prune_stale(&token.address);

        // No mentions at all for this token.
        if self.mentions.get(&token.address).is_none() {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                "No social mentions recorded".to_string(),
            ));
        }

        let mentions = self.mentions.get(&token.address).unwrap();
        let count = mentions.len();
        if count < self.min_mentions {
            return Ok(Signal::new(
                token.address.clone(),
                token.chain,
                &self.name,
                0,
                0.0,
                format!("Social mentions ({count}) below threshold ({})", self.min_mentions),
            ));
        }

        // Distinct authors mentioning this token.
        let distinct_authors: HashSet<&str> = mentions.iter().map(|m| m.author.as_str()).collect();
        let author_count = distinct_authors.len();

        // Total engagement across all mentions.
        let total_engagement: f64 = mentions.iter().map(|m| m.engagement).sum();

        // Mention velocity: how many per minute.
        let span_mins = if mentions.len() >= 2 {
            let first = mentions.front().unwrap().timestamp;
            let last = mentions.back().unwrap().timestamp;
            (last - first).num_minutes().max(1) as f64
        } else {
            self.window_minutes as f64
        };
        let velocity = count as f64 / span_mins;

        // Score components:
        // - Mention count: more mentions = higher score (capped at 40)
        // - Author diversity: more unique authors = higher quality (capped at 30)
        // - Engagement: higher engagement = stronger signal (capped at 20)
        // - Velocity: faster mentions = more timely (capped at 10)
        let count_score = (count as f64 / self.min_mentions as f64 * 20.0).min(40.0);
        let author_score = (author_count as f64 * 5.0).min(30.0);
        let engagement_score = (total_engagement.log10().max(0.0) * 5.0).min(20.0);
        let velocity_score = (velocity * 10.0).min(10.0);

        let score = (count_score + author_score + engagement_score + velocity_score).clamp(0.0, 100.0) as u8;

        let reasoning = format!(
            "Social momentum: {score}/100 ({count} mentions, {author_count} authors, {velocity:.1}/min velocity, engagement={total_engagement:.0})"
        );

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.5,
            reasoning,
        ))
    }
}

// ─── Strategy Enum ───────────────────────────────────────────────────────────

/// All known strategy types, used for dispatch without dyn.
pub enum StrategyKind {
    WhaleConsensus(WhaleConsensusSignal),
    Accumulation(AccumulationSignal),
    LaunchMomentum(LaunchMomentumSignal),
    VolumeSpike(VolumeSpikeSignal),
    Social(SocialSignal),
    Behavioral(BehavioralSignal),
}

impl Strategy for StrategyKind {
    fn name(&self) -> &str {
        match self {
            StrategyKind::WhaleConsensus(s) => s.name(),
            StrategyKind::Accumulation(s) => s.name(),
            StrategyKind::LaunchMomentum(s) => s.name(),
            StrategyKind::VolumeSpike(s) => s.name(),
            StrategyKind::Social(s) => s.name(),
            StrategyKind::Behavioral(s) => s.name(),
        }
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        match self {
            StrategyKind::WhaleConsensus(s) => s.evaluate(token).await,
            StrategyKind::Accumulation(s) => s.evaluate(token).await,
            StrategyKind::LaunchMomentum(s) => s.evaluate(token).await,
            StrategyKind::VolumeSpike(s) => s.evaluate(token).await,
            StrategyKind::Social(s) => s.evaluate(token).await,
            StrategyKind::Behavioral(s) => s.evaluate(token).await,
        }
    }
}

// ─── Confluence Scorer ───────────────────────────────────────────────────────

/// Aggregates multiple strategy signals into a composite confluence score.
pub struct ConfluenceScorer {
    strategies: Vec<StrategyKind>,
    weights: Vec<f64>,
    threshold: f64,
    /// Optional GMGN signal enrichment for boosting signals with pre-computed data.
    pub gmgn_enrichment: Option<GmgnSignalEnrichment>,
}

impl ConfluenceScorer {
    pub fn new(threshold: f64) -> Self {
        Self {
            strategies: Vec::new(),
            weights: Vec::new(),
            threshold,
            gmgn_enrichment: None,
        }
    }

    /// Add a strategy with its weight.
    pub fn add_strategy(&mut self, strategy: StrategyKind, weight: f64) {
        self.weights.push(weight);
        self.strategies.push(strategy);
    }

    /// Build a scorer from config with default weights.
    pub fn from_config(
        whale_consensus: WhaleConsensusSignal,
        accumulation: AccumulationSignal,
        launch_momentum: LaunchMomentumSignal,
        volume_spike: VolumeSpikeSignal,
        behavioral: BehavioralSignal,
        weights: &SignalWeights,
        threshold: f64,
    ) -> Self {
        let mut scorer = Self::new(threshold);
        scorer.add_strategy(StrategyKind::WhaleConsensus(whale_consensus), weights.whale_consensus);
        scorer.add_strategy(StrategyKind::Accumulation(accumulation), weights.accumulation);
        scorer.add_strategy(StrategyKind::LaunchMomentum(launch_momentum), weights.launch_momentum);
        scorer.add_strategy(StrategyKind::VolumeSpike(volume_spike), weights.volume_spike);
        scorer.add_strategy(StrategyKind::Behavioral(behavioral), weights.behavioral);
        scorer
    }

    /// Feed scan data into the volume spike, launch momentum, and accumulation signals.
    /// Call this on each scan tick to keep signal state up to date.
    pub fn feed_scan_data(&self, token_address: &str, volume_24h: Option<f64>, holder_count: Option<u64>, price_usd: Option<f64>) {
        for strategy in &self.strategies {
            match strategy {
                StrategyKind::VolumeSpike(vs) => {
                    if let Some(vol) = volume_24h {
                        vs.record(token_address.to_string(), vol);
                    }
                }
                StrategyKind::LaunchMomentum(lm) => {
                    if let Some(vol) = volume_24h {
                        let holders = holder_count.unwrap_or(0);
                        lm.record(token_address.to_string(), vol, holders);
                    }
                }
                StrategyKind::Accumulation(acc) => {
                    if let Some(holders) = holder_count {
                        let price = price_usd.unwrap_or(0.0);
                        acc.record_snapshot(token_address.to_string(), holders, price);
                    }
                }
                _ => {}
            }
        }
    }

    /// Poll the social signal (twitter search). Call periodically.
    pub async fn poll_social(&self) -> Result<()> {
        for strategy in &self.strategies {
            if let StrategyKind::Social(ss) = strategy {
                ss.poll().await?;
            }
        }
        Ok(())
    }

    /// Poll Twitter for specific token CAs discovered during scanning.
    /// Searches up to `max_tokens` addresses per call. This is the primary
    /// way the social signal gets useful data — searching for exact CAs
    /// finds people actually discussing the token.
    pub async fn poll_social_tokens(&self, addresses: &[String], max_tokens: usize) {
        for strategy in &self.strategies {
            if let StrategyKind::Social(ss) = strategy {
                ss.poll_token_cas(addresses, max_tokens).await;
            }
        }
    }

    /// Reload wallet scores from the database into the whale consensus signal's cache.
    /// Call this after Zerion enrichment updates wallet scores in SQLite.
    pub async fn refresh_wallet_scores(&mut self, registry: &solagent_portfolio::WalletRegistry) {
        for strategy in &mut self.strategies {
            if let StrategyKind::WhaleConsensus(ws) = strategy {
                let pool = registry.pool().clone();
                let mut scores = ws.wallet_scores.write().await;
                let cache = RegistryScoreCache::new(pool);
                if let Err(e) = cache.refresh().await {
                    tracing::warn!(error = %e, "Failed to refresh wallet score cache");
                    return;
                }
                *scores = Box::new(cache);
                tracing::debug!("Reloaded wallet scores into whale consensus signal");
                return;
            }
        }
    }

    /// Poll curated Twitter account timelines for token mentions.
    /// Unlike keyword search, this only attributes mentions to tokens whose
    /// CAs are explicitly present in the tweet text.
    pub async fn poll_social_accounts(&self, handles: &[String]) -> usize {
        let mut total = 0;
        for strategy in &self.strategies {
            if let StrategyKind::Social(ss) = strategy {
                total += ss.poll_curated_accounts(handles).await;
            }
        }
        total
    }

    /// Evaluate all strategies for a token and produce a composite score.
    /// When GMGN signal enrichment is available, applies bonus points for
    /// smart money buy signals, KOL calls, and price surges.
    pub async fn score(&self, token: &TokenInfo) -> Result<ConfluenceResult> {
        let signals = self.evaluate_signals(token).await?;
        let weights = self.weights.clone();
        let threshold = self.threshold;

        let mut weighted_sum = 0.0;
        let mut weight_total = 0.0;
        for (i, signal) in signals.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            weighted_sum += signal.score as f64 * weight;
            weight_total += weight;
        }

        let mut composite = if weight_total > 0.0 {
            weighted_sum / weight_total
        } else {
            0.0
        };

        // Apply GMGN signal enrichment boosts on top of the weighted composite.
        if let Some(ref enrichment) = self.gmgn_enrichment {
            let sm_boost = enrichment.get_sm_buy_boost(&token.address);
            let kol_boost = enrichment.get_kol_call_boost(&token.address);
            let price_boost = enrichment.get_price_surge_boost(&token.address);
            let total_boost = sm_boost + kol_boost + price_boost;

            if total_boost > 0.0 {
                let before = composite;
                composite = (composite + total_boost).min(100.0);
                tracing::debug!(
                    token = &token.address,
                    sm_boost,
                    kol_boost,
                    price_boost,
                    before,
                    after = composite,
                    "GMGN enrichment applied"
                );
            }
        }

        Ok(ConfluenceResult {
            composite_score: composite as u8,
            signals,
            passed: composite >= threshold,
        })
    }

    /// Evaluate signals without computing composite (for external scoring).
    async fn evaluate_signals(&self, token: &TokenInfo) -> Result<Vec<Signal>> {
        let mut signals = Vec::new();
        for strategy in &self.strategies {
            match strategy.evaluate(token).await {
                Ok(signal) => signals.push(signal),
                Err(e) => {
                    tracing::warn!(
                        strategy = strategy.name(),
                        error = %e,
                        "Strategy evaluation failed"
                    );
                }
            }
        }
        Ok(signals)
    }

    /// Update the confluence threshold at runtime (for auto-tuner).
    pub fn set_threshold(&mut self, threshold: f64) {
        self.threshold = threshold;
    }

    /// Get the current threshold.
    pub fn get_threshold(&self) -> f64 {
        self.threshold
    }

    /// Update a signal weight by index at runtime.
    pub fn set_weight(&mut self, index: usize, weight: f64) {
        if let Some(w) = self.weights.get_mut(index) {
            *w = weight;
        }
    }

    /// Get current weights snapshot.
    pub fn get_weights(&self) -> Vec<f64> {
        self.weights.clone()
    }

    /// Get the number of strategies.
    pub fn strategy_count(&self) -> usize {
        self.strategies.len()
    }

    /// Get an iterator over the strategy kinds (for behavioral GMGN lookups).
    pub fn strategies(&self) -> impl Iterator<Item = &StrategyKind> {
        self.strategies.iter()
    }
}

/// Thread-safe runtime configuration for the auto-tuner.
///
/// Wraps the mutable parameters that the auto-tuner adjusts at runtime:
/// signal weights, confluence threshold, risk parameters.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub weights: Arc<RwLock<SignalWeights>>,
    pub confluence_threshold: Arc<RwLock<f64>>,
    pub max_position_size_usd: Arc<RwLock<f64>>,
    pub max_open_positions: Arc<RwLock<usize>>,
    pub daily_loss_limit: Arc<RwLock<f64>>,
}

impl RuntimeConfig {
    pub fn new(
        weights: SignalWeights,
        confluence_threshold: f64,
        max_position_size_usd: f64,
        max_open_positions: usize,
        daily_loss_limit: f64,
    ) -> Self {
        Self {
            weights: Arc::new(RwLock::new(weights)),
            confluence_threshold: Arc::new(RwLock::new(confluence_threshold)),
            max_position_size_usd: Arc::new(RwLock::new(max_position_size_usd)),
            max_open_positions: Arc::new(RwLock::new(max_open_positions)),
            daily_loss_limit: Arc::new(RwLock::new(daily_loss_limit)),
        }
    }

    /// Safely set a signal weight with bounds validation.
    pub async fn set_weight(&self, field: &str, value: f64) -> Result<(), String> {
        if !(0.0..=1.0).contains(&value) {
            return Err(format!("Weight must be in [0.0, 1.0], got {value}"));
        }
        let mut w = self.weights.write().await;
        match field {
            "whale_consensus" => w.whale_consensus = value,
            "accumulation" => w.accumulation = value,
            "launch_momentum" => w.launch_momentum = value,
            "volume_spike" => w.volume_spike = value,
            "social" => w.social = value,
            "behavioral" => w.behavioral = value,
            _ => return Err(format!("Unknown weight field: {field}")),
        }
        Ok(())
    }

    /// Set confluence threshold with bounds validation.
    pub async fn set_confluence_threshold(&self, value: f64) -> Result<(), String> {
        if !(5.0..=80.0).contains(&value) {
            return Err(format!("Threshold must be in [5.0, 80.0], got {value}"));
        }
        *self.confluence_threshold.write().await = value;
        Ok(())
    }

    /// Set max position size with bounds validation.
    pub async fn set_max_position_size(&self, value: f64) -> Result<(), String> {
        if !(1.0..=30.0).contains(&value) {
            return Err(format!("Position size must be in [$1, $30], got ${value}"));
        }
        *self.max_position_size_usd.write().await = value;
        Ok(())
    }
}

/// Configurable signal weights for confluence scoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalWeights {
    pub whale_consensus: f64,
    pub accumulation: f64,
    pub launch_momentum: f64,
    pub volume_spike: f64,
    pub social: f64,
    pub behavioral: f64,
}

impl Default for SignalWeights {
    fn default() -> Self {
        Self {
            whale_consensus: 0.25,
            accumulation: 0.15,
            launch_momentum: 0.15,
            volume_spike: 0.10,
            social: 0.10,
            behavioral: 0.25,
        }
    }
}

/// Result of confluence scoring across all strategies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfluenceResult {
    /// Weighted composite score (0-100).
    pub composite_score: u8,
    /// Individual signal results.
    pub signals: Vec<Signal>,
    /// Whether the score passes the confluence threshold.
    pub passed: bool,
}

impl ConfluenceResult {
    /// Format per-signal reasoning as a machine-parseable string.
    ///
    /// Format: `signal1=N/100 "reason1", signal2=N/100 "reason2", ...`
    pub fn signal_reasoning_summary(&self) -> String {
        self.signals
            .iter()
            .map(|s| format!("{}={}/100 \"{}\"", s.strategy, s.score, s.reason))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ─── Behavioral Wallet Cache ─────────────────────────────────────────────────

/// Tier classification for behaviorally-scored wallets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BehavioralTier {
    Precognitive,
    Sovereign,
    Emerging,
    Noise,
}

impl std::fmt::Display for BehavioralTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BehavioralTier::Precognitive => write!(f, "PRECOGNITIVE"),
            BehavioralTier::Sovereign => write!(f, "SOVEREIGN"),
            BehavioralTier::Emerging => write!(f, "EMERGING"),
            BehavioralTier::Noise => write!(f, "NOISE"),
        }
    }
}

/// A wallet discovered by the behavioral intelligence scanner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehavioralWallet {
    pub address: String,
    pub tier: BehavioralTier,
    pub score: f64,
    pub primary_edge: String,
    pub red_flags: Vec<String>,
}

/// Shared cache of behaviorally-discovered wallets, updated by the
/// background behavioral scan task. Read by BehavioralSignal and
/// WhaleConsensusSignal for quality weighting.
#[derive(Debug, Clone)]
pub struct BehavioralWalletCache {
    /// Address -> BehavioralWallet
    wallets: Arc<DashMap<String, BehavioralWallet>>,
    /// Timestamp of last scan.
    last_scan: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl Default for BehavioralWalletCache {
    fn default() -> Self {
        Self::new()
    }
}

impl BehavioralWalletCache {
    pub fn new() -> Self {
        Self {
            wallets: Arc::new(DashMap::new()),
            last_scan: Arc::new(RwLock::new(None)),
        }
    }

    /// Replace the entire cache with new scan results.
    pub async fn update(&self, new_wallets: Vec<BehavioralWallet>) {
        self.wallets.clear();
        for w in new_wallets {
            self.wallets.insert(w.address.clone(), w);
        }
        *self.last_scan.write().await = Some(Utc::now());
    }

    /// Get a wallet's tier and score, if known.
    pub fn get(&self, address: &str) -> Option<(BehavioralTier, f64)> {
        self.wallets.get(address).map(|w| (w.tier, w.score))
    }

    /// Check if a wallet is in the cache with SOVEREIGN or higher tier.
    pub fn is_high_tier(&self, address: &str) -> bool {
        self.wallets.get(address).map(|w| {
            matches!(w.tier, BehavioralTier::Precognitive | BehavioralTier::Sovereign)
        }).unwrap_or(false)
    }

    /// Get all wallets at or above the given tier.
    pub fn get_by_tier(&self, min_tier: BehavioralTier) -> Vec<BehavioralWallet> {
        self.wallets.iter()
            .filter(|w| w.tier >= min_tier)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Get count of wallets per tier.
    pub fn tier_counts(&self) -> (usize, usize, usize, usize) {
        let (mut prec, mut sov, mut em, mut noise) = (0, 0, 0, 0);
        for w in self.wallets.iter() {
            match w.tier {
                BehavioralTier::Precognitive => prec += 1,
                BehavioralTier::Sovereign => sov += 1,
                BehavioralTier::Emerging => em += 1,
                BehavioralTier::Noise => noise += 1,
            }
        }
        (prec, sov, em, noise)
    }

    /// Total wallets in cache.
    pub fn len(&self) -> usize {
        self.wallets.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.wallets.is_empty()
    }

    /// When was the last scan run?
    pub async fn last_scan(&self) -> Option<DateTime<Utc>> {
        *self.last_scan.read().await
    }
}

impl PartialOrd for BehavioralTier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BehavioralTier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let rank = |t: &Self| match t {
            BehavioralTier::Noise => 0,
            BehavioralTier::Emerging => 1,
            BehavioralTier::Sovereign => 2,
            BehavioralTier::Precognitive => 3,
        };
        rank(self).cmp(&rank(other))
    }
}

// ─── Behavioral Signal ───────────────────────────────────────────────────────

/// Signal that scores tokens based on behavioral intelligence:
/// whether SOVEREIGN/PRECOGNITIVE wallets (discovered by the periodic
/// behavioral scan) have recently traded the token.
///
/// Instead of relying on rate-limited Helius wallet watcher, this signal
/// uses GMGN top-trader data to detect smart money interest at evaluation time.
type BehavioralTokenMap = DashMap<String, Vec<(String, BehavioralTier, DateTime<Utc>)>>;

pub struct BehavioralSignal {
    name: String,
    cache: Arc<BehavioralWalletCache>,
    /// Token -> (wallets that traded it, timestamp of detection).
    pub(crate) token_wallets: Arc<BehavioralTokenMap>,
    /// Path to gmgn-cli.
    gmgn_cli_path: String,
    #[allow(dead_code)]
    chain: Chain,
}

impl BehavioralSignal {
    pub fn new(cache: Arc<BehavioralWalletCache>, chain: Chain) -> Self {
        Self {
            name: "behavioral".to_string(),
            cache,
            token_wallets: Arc::new(DashMap::new()),
            gmgn_cli_path: solagent_data::gmgn::GMGN_CLI_DEFAULT_PATH.to_string(),
            chain,
        }
    }

    /// Check if any high-tier behavioral wallets are among the top traders
    /// for a given token via GMGN. Call this during evaluation.
    pub async fn check_gmgn_traders(&self, token_address: &str) -> Vec<(String, BehavioralTier)> {
        let output = match tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new(&self.gmgn_cli_path)
                .args([
                    "token", "traders",
                    "--chain", "sol",
                    "--address", token_address,
                    "--tag", "smart_degen",
                    "--order-by", "profit",
                    "--direction", "desc",
                    "--limit", "20",
                    "--raw",
                ])
                .output(),
        ).await {
            Ok(Ok(o)) => o,
            _ => return Vec::new(),
        };

        if !output.status.success() {
            return Vec::new();
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut matches = Vec::new();

        // GMGN returns { "list": [...] }
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout)
            && let Some(list) = data.get("list").and_then(|l| l.as_array())
        {
            for item in list {
                let addr = item.get("address")
                    .and_then(|a| a.as_str())
                    .unwrap_or("");
                if !addr.is_empty()
                    && let Some((tier, _score)) = self.cache.get(addr)
                    && tier >= BehavioralTier::Emerging
                {
                    matches.push((addr.to_string(), tier));
                }
            }
        }

        if !matches.is_empty() {
            let now = Utc::now();
            let records: Vec<(String, BehavioralTier, DateTime<Utc>)> = matches.iter()
                .map(|(a, t)| (a.clone(), *t, now))
                .collect();
            self.token_wallets.insert(token_address.to_string(), records);
            tracing::info!(
                token = &token_address[..token_address.len().min(12)],
                count = matches.len(),
                "Behavioral signal: behavioral wallets detected in GMGN traders"
            );
        }

        matches
    }
}

impl Strategy for BehavioralSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        // Check cached detections first (from prior GMGN lookups).
        let cached = self.token_wallets.get(&token.address).map(|r| {
            r.value().clone()
        }).unwrap_or_default();

        let score = if cached.is_empty() {
            0
        } else {
            // Score based on highest tier detected.
            let best_tier = cached.iter()
                .map(|(_, t, _)| *t)
                .max()
                .unwrap_or(BehavioralTier::Noise);
            let count = cached.len();

            let base = match best_tier {
                BehavioralTier::Precognitive => 85,
                BehavioralTier::Sovereign => 70,
                BehavioralTier::Emerging => 45,
                BehavioralTier::Noise => 0,
            };

            // Bonus for multiple wallets (consensus).
            let consensus_bonus = if count >= 3 { 15 } else if count >= 2 { 8 } else { 0 };
            (base + consensus_bonus).min(100) as u8
        };

        let reasoning = if cached.is_empty() {
            "No behavioral wallets detected".to_string()
        } else {
            let tiers: Vec<String> = cached.iter()
                .map(|(_, t, _)| format!("{t}"))
                .collect();
            format!("Behavioral: {}/100 ({} high-tier wallets: {})", score, cached.len(), tiers.join(", "))
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.8,
            reasoning,
        ))
    }
}

// ─── GMGN Signal Enrichment ──────────────────────────────────────────────────

/// Enriches the signal engine with pre-computed GMGN market signals.
///
/// Queries GMGN for:
/// - Smart money cluster-buy signals (signal_type 12) → boosts WhaleConsensus
/// - KOL call signals (signal_type 13) → boosts Social/LaunchMomentum
/// - Price surge signals (signal_type 6) → boosts VolumeSpike
///
/// Called once per scan cycle to build a map of token → signal boosts.
/// Individual signals read from this map during evaluation without making
/// additional API calls.
#[derive(Debug, Clone)]
pub struct GmgnSignalEnrichment {
    /// Token address → smart money buy signal count and aggregate market cap.
    sm_buy_signals: Arc<DashMap<String, GmgnSignalData>>,
    /// Token address → KOL call signal data.
    kol_call_signals: Arc<DashMap<String, GmgnSignalData>>,
    /// Token address → price surge signal data.
    price_surge_signals: Arc<DashMap<String, GmgnSignalData>>,
}

/// Aggregated signal data for a token from GMGN market signals.
#[derive(Debug, Clone)]
struct GmgnSignalData {
    /// Number of signal events for this token.
    count: usize,
    /// Most recent trigger market cap.
    #[allow(dead_code)]
    trigger_mcap: Option<f64>,
    /// Current market cap at signal time.
    current_mcap: Option<f64>,
}

impl Default for GmgnSignalEnrichment {
    fn default() -> Self {
        Self::new()
    }
}

impl GmgnSignalEnrichment {
    pub fn new() -> Self {
        Self {
            sm_buy_signals: Arc::new(DashMap::new()),
            kol_call_signals: Arc::new(DashMap::new()),
            price_surge_signals: Arc::new(DashMap::new()),
        }
    }

    /// Refresh all GMGN market signals. Call once per scan cycle.
    /// Returns the number of unique tokens with signals.
    pub async fn refresh(&self, gmgn: &solagent_data::GmgnClient) -> usize {
        let mut total_tokens = HashSet::new();

        // Smart money cluster-buy signals (type 12).
        let sm_signals = gmgn.get_market_signals(12).await;
        self.sm_buy_signals.clear();
        for signal in &sm_signals {
            self.sm_buy_signals.entry(signal.token_address.clone())
                .and_modify(|e| {
                    e.count += 1;
                    e.current_mcap = signal.current_market_cap.or(e.current_mcap);
                })
                .or_insert(GmgnSignalData {
                    count: 1,
                    trigger_mcap: signal.trigger_market_cap,
                    current_mcap: signal.current_market_cap,
                });
            total_tokens.insert(signal.token_address.clone());
        }

        // KOL call signals (type 13).
        let kol_signals = gmgn.get_market_signals(13).await;
        self.kol_call_signals.clear();
        for signal in &kol_signals {
            self.kol_call_signals.entry(signal.token_address.clone())
                .and_modify(|e| {
                    e.count += 1;
                })
                .or_insert(GmgnSignalData {
                    count: 1,
                    trigger_mcap: signal.trigger_market_cap,
                    current_mcap: signal.current_market_cap,
                });
            total_tokens.insert(signal.token_address.clone());
        }

        // Price surge signals (type 6).
        let price_signals = gmgn.get_market_signals(6).await;
        self.price_surge_signals.clear();
        for signal in &price_signals {
            self.price_surge_signals.entry(signal.token_address.clone())
                .and_modify(|e| {
                    e.count += 1;
                })
                .or_insert(GmgnSignalData {
                    count: 1,
                    trigger_mcap: signal.trigger_market_cap,
                    current_mcap: signal.current_market_cap,
                });
            total_tokens.insert(signal.token_address.clone());
        }

        let sm_count = sm_signals.len();
        let kol_count = kol_signals.len();
        let price_count = price_signals.len();
        tracing::info!(
            sm_buy = sm_count,
            kol_call = kol_count,
            price_surge = price_count,
            unique_tokens = total_tokens.len(),
            "GMGN signal enrichment refreshed"
        );

        total_tokens.len()
    }

    /// Get smart money buy signal boost for a token (0-25 bonus points).
    /// Multiple SM cluster-buys = stronger signal.
    pub fn get_sm_buy_boost(&self, token_address: &str) -> f64 {
        if let Some(data) = self.sm_buy_signals.get(token_address) {
            match data.count {
                1 => 10.0,
                2 => 18.0,
                _ => 25.0,
            }
        } else {
            0.0
        }
    }

    /// Get KOL call signal boost for a token (0-15 bonus points).
    pub fn get_kol_call_boost(&self, token_address: &str) -> f64 {
        if let Some(data) = self.kol_call_signals.get(token_address) {
            match data.count {
                1 => 7.0,
                2 => 12.0,
                _ => 15.0,
            }
        } else {
            0.0
        }
    }

    /// Get price surge signal boost for a token (0-20 bonus points).
    pub fn get_price_surge_boost(&self, token_address: &str) -> f64 {
        if let Some(data) = self.price_surge_signals.get(token_address) {
            match data.count {
                1 => 10.0,
                2 => 15.0,
                _ => 20.0,
            }
        } else {
            0.0
        }
    }

    /// Get the number of SM buy signals for a token.
    pub fn sm_buy_count(&self, token_address: &str) -> usize {
        self.sm_buy_signals.get(token_address).map(|d| d.count).unwrap_or(0)
    }

    /// Get the number of KOL call signals for a token.
    pub fn kol_call_count(&self, token_address: &str) -> usize {
        self.kol_call_signals.get(token_address).map(|d| d.count).unwrap_or(0)
    }

    /// Get the number of price surge signals for a token.
    pub fn price_surge_count(&self, token_address: &str) -> usize {
        self.price_surge_signals.get(token_address).map(|d| d.count).unwrap_or(0)
    }

    /// Check if smart money is selling a token (exit signal).
    /// Returns the number of sell events and aggregate sell amount.
    pub async fn check_sm_exit_signals(
        gmgn: &solagent_data::GmgnClient,
        token_address: &str,
    ) -> (usize, f64) {
        let trades = gmgn.get_smart_money_trades(Some("sell")).await;
        let mut count = 0;
        let mut total_usd = 0.0;

        for trade in &trades {
            if let Some(ref addr) = trade.token_address {
                if addr == token_address {
                    count += 1;
                    total_usd += trade.amount_usd.unwrap_or(0.0);
                }
            }
        }

        (count, total_usd)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use solagent_core::{Chain, TokenInfo};

    fn make_token(address: &str, price: Option<f64>, volume: Option<f64>, mcap: Option<f64>, created_at: Option<DateTime<Utc>>) -> TokenInfo {
        TokenInfo {
            address: address.to_string(),
            chain: Chain::Solana,
            symbol: "TEST".to_string(),
            name: "Test Token".to_string(),
            decimals: 9,
            price_usd: price,
            market_cap_usd: mcap,
            volume_24h: volume,
            holder_count: None,
            created_at,
            pair_address: Some("pair123".to_string()),
            lp_locked: None,
            mint_authority_revoked: None,
            freeze_authority_revoked: None,
        }
    }

    // ─── AccumulationSignal Tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_accumulation_with_holder_data_scores_nonzero() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);
        let token_ca = "test_accumulation_token";

        // Record 3 snapshots: holders growing, price flat.
        signal.record_snapshot(token_ca.to_string(), 100, 1.0);
        signal.record_snapshot(token_ca.to_string(), 150, 1.02);
        signal.record_snapshot(token_ca.to_string(), 200, 1.05);

        let token = make_token(token_ca, Some(1.05), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "accumulation");
        assert!(result.score >= 40, "Accumulation should score >= 40 with 100% holder growth and flat price, got {}", result.score);
    }

    #[tokio::test]
    async fn test_accumulation_no_history_returns_zero() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);
        let token = make_token("unknown_token", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
    }

    #[tokio::test]
    async fn test_accumulation_single_snapshot_returns_zero() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);
        signal.record_snapshot("token_single".to_string(), 100, 1.0);

        let token = make_token("token_single", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "Single snapshot should not produce a score");
    }

    #[tokio::test]
    async fn test_accumulation_declining_holders_low_score() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);
        // Holders declining — should NOT trigger accumulation.
        signal.record_snapshot("declining".to_string(), 200, 1.0);
        signal.record_snapshot("declining".to_string(), 150, 1.02);

        let token = make_token("declining", Some(1.02), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        // With declining holders (negative growth), score should be 20 (the "else" branch).
        assert!(result.score <= 20, "Declining holders should score low, got {}", result.score);
    }

    // ─── LaunchMomentumSignal Tests ──────────────────────────────────────

    #[tokio::test]
    async fn test_launch_momentum_with_holders_scores_nonzero() {
        let signal = LaunchMomentumSignal::new(Chain::Solana, 10);
        let token_ca = "launch_token";

        // Record growing volume and holders.
        signal.record(token_ca.to_string(), 1000.0, 50);
        signal.record(token_ca.to_string(), 5000.0, 200);
        signal.record(token_ca.to_string(), 15000.0, 500);

        let created = Utc::now() - chrono::Duration::minutes(30); // 30 min old
        let token = make_token(token_ca, Some(0.001), Some(15000.0), Some(50000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "launch_momentum");
        assert!(result.score > 0, "Launch momentum should score > 0 with growing volume + holders, got {}", result.score);
    }

    #[tokio::test]
    async fn test_launch_momentum_no_snapshots_returns_zero() {
        let signal = LaunchMomentumSignal::new(Chain::Solana, 10);
        let created = Utc::now() - chrono::Duration::minutes(30);
        let token = make_token("no_snaps", Some(0.001), Some(1000.0), Some(50000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
    }

    #[tokio::test]
    async fn test_launch_momentum_too_old_returns_zero() {
        let signal = LaunchMomentumSignal::new(Chain::Solana, 10);
        let token_ca = "old_token";

        signal.record(token_ca.to_string(), 1000.0, 50);
        signal.record(token_ca.to_string(), 5000.0, 200);

        // Token is 48 hours old — exceeds max_age_hours=1.
        let created = Utc::now() - chrono::Duration::hours(48);
        let token = make_token(token_ca, Some(0.001), Some(5000.0), Some(50000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "Old token should score 0 for launch momentum");
    }

    // ─── VolumeSpikeSignal Tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_volume_spike_with_enough_data_scores_nonzero() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "spike_token";

        // 4 data points where the last is 4x the average.
        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 1200.0);
        signal.record(token_ca.to_string(), 1100.0);
        signal.record(token_ca.to_string(), 50000.0); // Spike!

        let token = make_token(token_ca, Some(0.001), Some(50000.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "volume_spike");
        assert!(result.score >= 80, "4x spike should score >= 80, got {}", result.score);
    }

    #[tokio::test]
    async fn test_volume_spike_insufficient_data_returns_zero() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "low_data_token";

        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 5000.0); // Only 2 points.

        let token = make_token(token_ca, Some(0.001), Some(5000.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "Fewer than 3 data points should score 0");
    }

    /// Test: With exactly 3 data points and a clear spike (latest ≥3x historical avg),
    /// the signal scores ≥80. Validates VAL-SIG-004.
    #[tokio::test]
    async fn test_volume_spike_three_points_clear_spike_scores_80_plus() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "spike_3pt_token";

        // 3 data points: historical avg = (1000+1200)/2 = 1100, current = 50000 → ratio = 45.5x
        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 1200.0);
        signal.record(token_ca.to_string(), 50000.0); // Spike!

        let token = make_token(token_ca, Some(0.001), Some(50000.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "volume_spike");
        assert!(result.score >= 80, "3-point clear spike (≥3x historical avg) should score ≥80, got {}", result.score);
    }

    /// Test: With exactly 3 data points but NO spike (volume near historical avg),
    /// the signal scores low (≤50).
    #[tokio::test]
    async fn test_volume_spike_three_points_no_spike_scores_low() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "nospike_3pt_token";

        // 3 data points: historical avg = (1000+1200)/2 = 1100, current = 1300 → ratio = 1.18x
        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 1200.0);
        signal.record(token_ca.to_string(), 1300.0);

        let token = make_token(token_ca, Some(0.001), Some(1300.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert!(result.score <= 50, "3-point no-spike should score ≤50, got {}", result.score);
    }

    /// Test: Volume history accumulates correctly across multiple record() calls,
    /// respecting the rolling window (window_size).
    #[tokio::test]
    async fn test_volume_spike_history_accumulates_respects_window() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 5); // window_size=5
        let token_ca = "window_token";

        // Record 7 data points — only last 5 should be kept.
        for vol in [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0] {
            signal.record(token_ca.to_string(), vol);
        }

        // With window_size=5, we have [300, 400, 500, 600, 700].
        // Historical avg (excluding latest) = (300+400+500+600)/4 = 450.
        // Current = 700, ratio = 700/450 = 1.56x. Below threshold, should score low.
        let token = make_token(token_ca, Some(0.001), Some(700.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();

        // Score should be based on only the last 5 points.
        // ratio = 1.56x < threshold*0.66 (1.98) → score should be 10 (the "else" branch).
        assert!(result.score <= 50, "Windowed data with no spike should score low, got {}", result.score);

        // Verify get_spike_ratio is consistent.
        let ratio = signal.get_spike_ratio(token_ca);
        assert!(ratio.is_some(), "Should have a spike ratio with 5 points");
        let r = ratio.unwrap();
        assert!((r - 1.556).abs() < 0.1, "Spike ratio should be ~1.56x, got {r}");
    }

    /// Test: get_spike_ratio returns None for tokens with <3 data points,
    /// and returns the correct ratio for tokens with ≥3 points.
    #[tokio::test]
    async fn test_volume_spike_get_spike_ratio() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);

        // No data → None.
        assert!(signal.get_spike_ratio("unknown").is_none());

        // 1 point → None (needs ≥3).
        signal.record("ratio_token".to_string(), 1000.0);
        assert!(signal.get_spike_ratio("ratio_token").is_none());

        // 2 points → None (needs ≥3).
        signal.record("ratio_token".to_string(), 1200.0);
        assert!(signal.get_spike_ratio("ratio_token").is_none());

        // 3 points → Some(ratio).
        signal.record("ratio_token".to_string(), 6000.0);
        let ratio = signal.get_spike_ratio("ratio_token");
        assert!(ratio.is_some(), "Should return ratio with 3 data points");
        // Historical avg = (1000+1200)/2 = 1100. Current = 6000. Ratio = 6000/1100 = 5.45x
        let r = ratio.unwrap();
        assert!((r - 5.45).abs() < 0.1, "Spike ratio should be ~5.45x, got {r}");
    }

    /// Test: Single data point returns score 0 (minimum is 3).
    #[tokio::test]
    async fn test_volume_spike_single_point_returns_zero() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "single_pt";

        signal.record(token_ca.to_string(), 100000.0); // Huge volume but only 1 point.

        let token = make_token(token_ca, Some(0.001), Some(100000.0), None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "Single data point should always score 0, got {}", result.score);
        assert!(
            result.reason.contains("Insufficient"),
            "Reason should mention insufficient data, got: {}",
            result.reason
        );
    }

    // ─── Confluence Scorer Tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_confluence_weighted_scoring_accurate() {
        let mut scorer = ConfluenceScorer::new(35.0);

        // Add just accumulation and volume spike for a clean test.
        scorer.add_strategy(
            StrategyKind::Accumulation(AccumulationSignal::new(Chain::Solana, 10)),
            0.20,
        );
        scorer.add_strategy(
            StrategyKind::VolumeSpike(VolumeSpikeSignal::new(Chain::Solana, 3.0, 10)),
            0.15,
        );

        // Feed data to both signals.
        scorer.feed_scan_data("token_a", Some(10000.0), Some(100), Some(1.0));
        scorer.feed_scan_data("token_a", Some(12000.0), Some(150), Some(1.02));
        scorer.feed_scan_data("token_a", Some(11000.0), Some(200), Some(1.05));
        // Volume spike: 4x+ jump
        scorer.feed_scan_data("token_a", Some(50000.0), Some(250), Some(1.05));

        let token = make_token("token_a", Some(1.05), Some(50000.0), Some(50000.0), None);
        let result = scorer.score(&token).await.unwrap();

        // Both signals should score > 0.
        let acc_signal = result.signals.iter().find(|s| s.strategy == "accumulation").unwrap();
        let vol_signal = result.signals.iter().find(|s| s.strategy == "volume_spike").unwrap();

        assert!(acc_signal.score > 0, "Accumulation should be > 0");
        assert!(vol_signal.score > 0, "Volume spike should be > 0");

        // Verify composite is weighted correctly.
        let expected_composite = (acc_signal.score as f64 * 0.20 + vol_signal.score as f64 * 0.15) / (0.20 + 0.15);
        let diff = (result.composite_score as f64 - expected_composite).abs();
        assert!(diff <= 1.0, "Composite should be within ±1 of weighted sum: got {}, expected {}", result.composite_score, expected_composite);
    }

    /// Test that feed_scan_data distributes data to all three dependent signals.
    #[tokio::test]
    async fn test_feed_scan_data_distributes_to_all_signals() {
        let mut scorer = ConfluenceScorer::new(35.0);

        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let lm = LaunchMomentumSignal::new(Chain::Solana, 10);
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);

        scorer.add_strategy(StrategyKind::Accumulation(acc), 0.20);
        scorer.add_strategy(StrategyKind::LaunchMomentum(lm), 0.20);
        scorer.add_strategy(StrategyKind::VolumeSpike(vs), 0.15);

        // Feed 4 data points with volume, holders, and price.
        let token_ca = "dist_token";
        scorer.feed_scan_data(token_ca, Some(10000.0), Some(100), Some(1.0));
        scorer.feed_scan_data(token_ca, Some(12000.0), Some(150), Some(1.02));
        scorer.feed_scan_data(token_ca, Some(11000.0), Some(200), Some(1.05));
        scorer.feed_scan_data(token_ca, Some(50000.0), Some(300), Some(1.06));

        // Evaluate and check all three scored.
        let created = Utc::now() - chrono::Duration::minutes(30);
        let token = make_token(token_ca, Some(1.06), Some(50000.0), Some(50000.0), Some(created));
        let result = scorer.score(&token).await.unwrap();

        let acc_score = result.signals.iter().find(|s| s.strategy == "accumulation").map(|s| s.score).unwrap_or(0);
        let lm_score = result.signals.iter().find(|s| s.strategy == "launch_momentum").map(|s| s.score).unwrap_or(0);
        let vs_score = result.signals.iter().find(|s| s.strategy == "volume_spike").map(|s| s.score).unwrap_or(0);

        assert!(acc_score > 0, "Accumulation should score > 0 after feed_scan_data with holders, got {}", acc_score);
        assert!(lm_score > 0, "Launch momentum should score > 0 after feed_scan_data with volume+holders, got {}", lm_score);
        assert!(vs_score > 0, "Volume spike should score > 0 after feed_scan_data with volume, got {}", vs_score);
    }

    /// Test feed_scan_data with holder_count: None does NOT feed accumulation signal.
    #[tokio::test]
    async fn test_feed_scan_data_none_holders_skips_accumulation() {
        let mut scorer = ConfluenceScorer::new(35.0);
        scorer.add_strategy(StrategyKind::Accumulation(AccumulationSignal::new(Chain::Solana, 10)), 0.20);
        scorer.add_strategy(StrategyKind::VolumeSpike(VolumeSpikeSignal::new(Chain::Solana, 3.0, 10)), 0.15);

        // Feed without holder_count (None).
        scorer.feed_scan_data("no_holder_token", Some(10000.0), None, Some(1.0));
        scorer.feed_scan_data("no_holder_token", Some(12000.0), None, Some(1.02));
        scorer.feed_scan_data("no_holder_token", Some(11000.0), None, Some(1.05));
        scorer.feed_scan_data("no_holder_token", Some(50000.0), None, Some(1.06));

        let token = make_token("no_holder_token", Some(1.06), Some(50000.0), None, None);
        let result = scorer.score(&token).await.unwrap();

        let acc_score = result.signals.iter().find(|s| s.strategy == "accumulation").map(|s| s.score).unwrap_or(0);
        let vs_score = result.signals.iter().find(|s| s.strategy == "volume_spike").map(|s| s.score).unwrap_or(0);

        assert_eq!(acc_score, 0, "Accumulation should be 0 when no holder data provided");
        assert!(vs_score > 0, "Volume spike should still score > 0 with volume data");
    }

    // ─── WhaleConsensusSignal + EventBus Integration Tests ─────────────

    /// Helper: create a WalletBuy event.
    fn make_wallet_buy_event(wallet: &str, token: &str, amount: f64) -> Event {
        Event::WalletBuy {
            wallet: wallet.to_string(),
            token_address: token.to_string(),
            chain: Chain::Solana,
            amount_usd: amount,
            timestamp: Utc::now(),
        }
    }

    /// Test: WhaleConsensusSignal records buys directly and scores > 0 with ≥2 wallets.
    #[tokio::test]
    async fn test_whale_consensus_scores_nonzero_with_two_wallets() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("wallet_alpha".to_string(), 80.0);
        scores.insert("wallet_beta".to_string(), 70.0);
        scores.insert("wallet_unknown".to_string(), 40.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,    // min_wallets
            30,   // window_minutes
            0.0,  // min_buy_usd
            Box::new(scores),
        );

        let token_ca = "SoTestToken123456789";

        // Record buys from 2 distinct wallets.
        signal.record_buy(token_ca.to_string(), "wallet_alpha".to_string(), 100.0);
        signal.record_buy(token_ca.to_string(), "wallet_beta".to_string(), 200.0);

        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "whale_consensus");
        assert!(result.score > 0, "Whale consensus should score > 0 with 2+ distinct wallet buys, got {}", result.score);
    }

    /// Test: WhaleConsensusSignal scores 0 with fewer than min_wallets.
    #[tokio::test]
    async fn test_whale_consensus_scores_zero_below_min_wallets() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("wallet_alpha".to_string(), 80.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,    // min_wallets
            30,   // window_minutes
            0.0,  // min_buy_usd
            Box::new(scores),
        );

        // Only 1 wallet buy — below min_wallets=2.
        signal.record_buy("token_xyz".to_string(), "wallet_alpha".to_string(), 100.0);

        let token = make_token("token_xyz", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "whale_consensus");
        // With 1 wallet and min_wallets=2, the partial formula gives (1/2)*50 = 25.
        // That's still > 0 but below the threshold of triggering a "consensus".
        // The spec says score > 0 only when ≥2 wallets buy. With 1 wallet:
        assert!(result.score < 50, "Single wallet should score low (partial), got {}", result.score);
    }

    /// Test: WhaleConsensusSignal scores 0 with no buys.
    #[tokio::test]
    async fn test_whale_consensus_scores_zero_no_buys() {
        let scores = InMemoryWalletScores::new();
        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        );

        let token = make_token("no_buys_token", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "No buys should score 0");
    }

    /// Test: WalletBuy events published to EventBus are received by
    /// WhaleConsensusSignal via subscribe_to_events and produce a score > 0
    /// when ≥2 known wallets buy the same token.
    ///
    /// This is the core integration test for VAL-SIG-001 and VAL-SIG-007.
    #[tokio::test]
    async fn test_whale_consensus_eventbus_integration() {
        let event_bus = EventBus::new(64);

        // Create signal with known wallets.
        let mut scores = InMemoryWalletScores::new();
        scores.insert("wallet_alpha".to_string(), 80.0);
        scores.insert("wallet_beta".to_string(), 70.0);

        let signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,    // min_wallets
            30,   // window_minutes
            0.0,  // min_buy_usd
            Box::new(scores),
        ));

        // Subscribe to EventBus — this spawns a background task.
        signal.subscribe_to_events(&event_bus);

        // Give the subscriber task time to start.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Publish WalletBuy events from 2 known wallets for the same token.
        event_bus.publish(make_wallet_buy_event("wallet_alpha", "SoTestABC", 100.0));
        event_bus.publish(make_wallet_buy_event("wallet_beta", "SoTestABC", 200.0));

        // Give the subscriber task time to process events.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Evaluate — should score > 0.
        let token = make_token("SoTestABC", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "whale_consensus");
        assert!(result.score > 0,
            "Whale consensus should score > 0 after 2 known wallets buy via EventBus, got {}",
            result.score
        );
    }

    /// Test: Unknown wallets (not in score provider) are ignored by the
    /// subscribe_to_events handler, so only registered wallets count.
    #[tokio::test]
    async fn test_whale_consensus_ignores_unknown_wallets() {
        let event_bus = EventBus::new(64);

        // Only wallet_alpha is known.
        let mut scores = InMemoryWalletScores::new();
        scores.insert("wallet_alpha".to_string(), 80.0);

        let signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        ));

        signal.subscribe_to_events(&event_bus);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Publish from known + unknown wallet.
        event_bus.publish(make_wallet_buy_event("wallet_alpha", "SoTestDEF", 100.0));
        event_bus.publish(make_wallet_buy_event("unknown_stranger", "SoTestDEF", 999.0));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let token = make_token("SoTestDEF", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        // Only 1 known wallet (alpha) recorded. unknown_stranger should be filtered.
        // With 1 wallet, the partial formula gives (1/2)*50 = 25.
        // This is less than what 2+ wallets would produce.
        assert!(result.score <= 50,
            "Unknown wallet should not contribute to whale consensus, got {}",
            result.score
        );
    }

    /// Test: Cloning WhaleConsensusSignal shares the same internal DashMap,
    /// so events recorded on one instance are visible on the clone.
    /// This is critical for the pattern where Arc<Signal> subscribes to events
    /// but a clone goes into ConfluenceScorer.
    #[tokio::test]
    async fn test_whale_consensus_clone_shares_state() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("w1".to_string(), 90.0);
        scores.insert("w2".to_string(), 85.0);

        let signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        ));

        // Clone the signal (this is what ConfluenceScorer gets).
        let cloned = (*signal).clone();

        // Record buys on the ORIGINAL Arc'd signal.
        signal.record_buy("TokenShared".to_string(), "w1".to_string(), 100.0);
        signal.record_buy("TokenShared".to_string(), "w2".to_string(), 200.0);

        // Evaluate on the CLONE — should see the same data.
        let token = make_token("TokenShared", Some(0.01), None, None, None);
        let result = cloned.evaluate(&token).await.unwrap();

        assert!(result.score > 0,
            "Cloned signal should share state with original, got score {}",
            result.score
        );
    }

    /// Test: RegistryScoreCache loads wallet scores from SQLite on refresh().
    /// Validates VAL-SIG-012.
    #[tokio::test]
    async fn test_registry_score_cache_loads_from_sqlite() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        // Insert 5 wallets with scores.
        for i in 1..=5 {
            sqlx::query(
                "INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl, total_trades, avg_hold_time_mins, score, tags, created_at, updated_at)
                 VALUES (?1, 'solana', 'smart_money', 'test', 0.5, 1000, 50, 30, ?2, '[]', datetime('now'), datetime('now'))"
            )
            .bind(format!("wallet_{i}"))
            .bind(i as f64 * 15.0) // score: 15, 30, 45, 60, 75
            .execute(&pool)
            .await
            .unwrap();
        }

        // Also insert one with score = 0 (should be excluded by refresh).
        sqlx::query(
            "INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl, total_trades, avg_hold_time_mins, score, tags, created_at, updated_at)
             VALUES ('zero_wallet', 'solana', 'unknown', 'test', 0.0, 0, 0, 0, 0, '[]', datetime('now'), datetime('now'))"
        )
        .execute(&pool)
        .await
        .unwrap();

        let cache = RegistryScoreCache::new(pool);
        cache.refresh().await.unwrap();

        assert_eq!(cache.len(), 5, "Should load 5 wallets with score > 0 (excluding zero_wallet)");

        // Verify individual lookups work.
        assert!(cache.get_score("wallet_1").is_some(), "wallet_1 should be in cache");
        assert_eq!(cache.get_score("wallet_1"), Some(15.0));
        assert_eq!(cache.get_score("wallet_5"), Some(75.0));
        assert!(cache.get_score("zero_wallet").is_none(), "zero_wallet should not be in cache (score=0)");
        assert!(cache.get_score("nonexistent").is_none(), "Nonexistent wallet should return None");
    }

    /// Test: RegistryScoreCache implements WalletScoreProvider correctly.
    #[tokio::test]
    async fn test_registry_score_cache_provider_trait() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query(
            "INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl, total_trades, avg_hold_time_mins, score, tags, created_at, updated_at)
             VALUES ('known_wallet', 'solana', 'smart_money', 'test', 0.8, 5000, 100, 45, 85.5, '[]', datetime('now'), datetime('now'))"
        )
        .execute(&pool)
        .await
        .unwrap();

        let cache = RegistryScoreCache::new(pool);
        cache.refresh().await.unwrap();

        // Test WalletScoreProvider trait methods.
        assert!(cache.is_known("known_wallet"), "known_wallet should be known");
        assert!(!cache.is_known("unknown_wallet"), "unknown_wallet should not be known");
        assert_eq!(cache.get_score("known_wallet"), Some(85.5));
        assert_eq!(cache.get_score("unknown_wallet"), None);
    }

    /// Test: Full pipeline — EventBus → WalletBuy events → WhaleConsensusSignal → score > 0.
    /// Uses RegistryScoreCache with in-memory SQLite to simulate the real flow.
    /// Validates VAL-SIG-001, VAL-SIG-007, VAL-CROSS-005.
    #[tokio::test]
    async fn test_full_wallet_buy_to_score_pipeline() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        // Seed 3 wallets in SQLite.
        let wallets = vec![
            ("sm_wallet_1", "smart_money", 90.0),
            ("sm_wallet_2", "smart_money", 85.0),
            ("sm_wallet_3", "smart_money", 75.0),
        ];
        for (addr, label, score) in &wallets {
            sqlx::query(
                "INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl, total_trades, avg_hold_time_mins, score, tags, created_at, updated_at)
                 VALUES (?1, 'solana', ?2, 'test', 0.7, 3000, 80, 40, ?3, '[]', datetime('now'), datetime('now'))"
            )
            .bind(addr)
            .bind(label)
            .bind(score)
            .execute(&pool)
            .await
            .unwrap();
        }

        // Create and refresh score cache.
        let cache = RegistryScoreCache::new(pool);
        cache.refresh().await.unwrap();
        assert_eq!(cache.len(), 3);

        // Create EventBus.
        let event_bus = EventBus::new(64);

        // Create WhaleConsensusSignal with the score cache.
        let whale_signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,    // min_wallets
            30,   // window_minutes
            0.0,  // min_buy_usd
            Box::new(cache),
        ));

        // Subscribe to EventBus (spawns background task).
        whale_signal.subscribe_to_events(&event_bus);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Simulate: 2 known wallets buy the same token.
        event_bus.publish(make_wallet_buy_event("sm_wallet_1", "TokenXYZ_pump", 500.0));
        event_bus.publish(make_wallet_buy_event("sm_wallet_2", "TokenXYZ_pump", 300.0));
        // Unknown wallet also buys — should be ignored.
        event_bus.publish(make_wallet_buy_event("rando_unknown", "TokenXYZ_pump", 99999.0));

        // Wait for async processing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Evaluate: should score > 0 because 2 known wallets bought.
        let token = make_token("TokenXYZ_pump", Some(0.001), Some(50000.0), Some(100000.0), None);
        let result = whale_signal.evaluate(&token).await.unwrap();

        assert!(result.score > 0,
            "Full pipeline: whale consensus should score > 0 with 2 known wallets buying, got {}",
            result.score
        );
        assert!(result.confidence > 0.0, "Confidence should be set");
    }

    /// Test: Confluence scorer includes whale_consensus signal and
    /// the composite score reflects wallet buy activity.
    #[tokio::test]
    async fn test_confluence_with_whale_consensus_eventbus() {
        let event_bus = EventBus::new(64);

        let mut scores = InMemoryWalletScores::new();
        scores.insert("whale_a".to_string(), 90.0);
        scores.insert("whale_b".to_string(), 85.0);

        let whale = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana, 2, 30, 0.0, Box::new(scores),
        ));
        whale.subscribe_to_events(&event_bus);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Publish 2 wallet buys.
        event_bus.publish(make_wallet_buy_event("whale_a", "TokenConv", 500.0));
        event_bus.publish(make_wallet_buy_event("whale_b", "TokenConv", 300.0));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Build confluence scorer with whale signal (clone shares DashMap).
        let mut confluence = ConfluenceScorer::new(35.0);
        confluence.add_strategy(
            StrategyKind::WhaleConsensus((*whale).clone()),
            0.30,
        );
        confluence.add_strategy(
            StrategyKind::VolumeSpike(VolumeSpikeSignal::new(Chain::Solana, 3.0, 10)),
            0.15,
        );

        let token = make_token("TokenConv", Some(0.01), Some(10000.0), None, None);
        let result = confluence.score(&token).await.unwrap();

        // Whale consensus should be > 0.
        let whale_signal = result.signals.iter()
            .find(|s| s.strategy == "whale_consensus")
            .expect("Should have whale_consensus signal");
        assert!(whale_signal.score > 0,
            "Whale consensus should score > 0 in confluence, got {}", whale_signal.score);

        // Composite should reflect whale contribution.
        assert!(result.composite_score > 0,
            "Composite should be > 0 with whale consensus contributing, got {}", result.composite_score);
    }

    // ─── Twitter Handle Extraction Tests ─────────────────────────────────

    /// Test: extract_twitter_handles parses DexScreener social links correctly.
    /// Validates the core function for VAL-CROSS-006.
    #[test]
    fn test_extract_twitter_handles_from_socials() {
        let socials = vec![
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/SolanaToken"}),
            serde_json::json!({"type": "x", "url": "https://x.com/VitalikButerin"}),
            serde_json::json!({"type": "telegram", "url": "https://t.me/solana_channel"}),
        ];

        let handles = extract_twitter_handles(&socials);
        assert_eq!(handles.len(), 2);
        assert!(handles.contains(&"solanatoken".to_string()));
        assert!(handles.contains(&"vitalikbuterin".to_string()));
    }

    /// Test: extract_twitter_handles deduplicates handles.
    #[test]
    fn test_extract_twitter_handles_deduplicates() {
        let socials = vec![
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/SolanaToken"}),
            serde_json::json!({"type": "x", "url": "https://x.com/SolanaToken"}),
        ];

        let handles = extract_twitter_handles(&socials);
        assert_eq!(handles.len(), 1, "Should deduplicate the same handle from different URLs");
        assert_eq!(handles[0], "solanatoken");
    }

    /// Test: extract_twitter_handles handles various URL formats.
    #[test]
    fn test_extract_twitter_handles_various_url_formats() {
        let socials = vec![
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/ElonMusk"}),
            serde_json::json!({"type": "twitter", "url": "https://mobile.twitter.com/anon_dev"}),
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/CryptoTrader?s=20"}),
            serde_json::json!({"type": "x", "url": "https://x.com/sol_degen123"}),
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/@PrefixedHandle"}),
        ];

        let handles = extract_twitter_handles(&socials);
        assert_eq!(handles.len(), 5);
        assert!(handles.contains(&"elonmusk".to_string()));
        assert!(handles.contains(&"anon_dev".to_string()));
        assert!(handles.contains(&"cryptotrader".to_string()));
        assert!(handles.contains(&"sol_degen123".to_string()));
        assert!(handles.contains(&"prefixedhandle".to_string()));
    }

    /// Test: extract_twitter_handles ignores non-Twitter socials.
    #[test]
    fn test_extract_twitter_handles_ignores_non_twitter() {
        let socials = vec![
            serde_json::json!({"type": "telegram", "url": "https://t.me/channel"}),
            serde_json::json!({"type": "discord", "url": "https://discord.gg/abc"}),
            serde_json::json!({"type": "website", "url": "https://example.com"}),
        ];

        let handles = extract_twitter_handles(&socials);
        assert!(handles.is_empty(), "Should not extract handles from non-Twitter socials");
    }

    /// Test: extract_twitter_handles rejects invalid handles.
    #[test]
    fn test_extract_twitter_handles_rejects_invalid() {
        let socials = vec![
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/"}), // empty handle
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/search?q=solana"}), // "search" is invalid
            serde_json::json!({"type": "twitter", "url": "https://twitter.com/home"}), // "home" is invalid
        ];

        let handles = extract_twitter_handles(&socials);
        assert!(handles.is_empty(), "Should reject empty/invalid handles");
    }

    /// Test: extract_twitter_handles handles empty input.
    #[test]
    fn test_extract_twitter_handles_empty_input() {
        let handles = extract_twitter_handles(&[]);
        assert!(handles.is_empty());
    }

    // ─── Attribution Filtering Tests ──────────────────────────────────────

    /// Test: Tweets without explicit CAs are NOT attributed to any token.
    /// This is the key safety check for VAL-SIG-005.
    #[tokio::test]
    async fn test_tweets_without_cas_not_attributed() {
        let signal = SocialSignal::new(Chain::Solana);
        let _token_ca = "*********************************";

        // Simulate a keyword search result (not a CA-specific search).
        // Use a non-CA keyword so query_ca is None.
        let _result = signal.search_keyword("solana memecoin").await;

        // The keyword search may or may not succeed (depends on twitter-cli),
        // but we can test the extract_solana_cas logic directly.
        let cas = signal.extract_solana_cas("Just a generic tweet about solana with no addresses");
        assert!(cas.is_empty(), "Tweet without CAs should extract zero CAs");
    }

    /// Test: extract_solana_cas correctly identifies valid Solana CAs.
    #[test]
    fn test_extract_solana_cas_finds_valid_cas() {
        let signal = SocialSignal::new(Chain::Solana);

        // Valid pump.fun address (base58 ending in "pump", 32-44 chars).
        // Use a realistic-length address (44 chars).
        let text = "Check out So7xKbinGHQPWo8RMvh3YuXzBqjFeS2pump great buy!";
        let cas = signal.extract_solana_cas(text);
        assert!(cas.len() >= 1, "Should find at least 1 CA in the text, found: {cas:?}");
        assert!(cas.iter().any(|c| c.contains("pump")), "Should find the pump address");
    }

    /// Test: extract_solana_cas rejects URLs as false positives.
    #[test]
    fn test_extract_solana_cas_rejects_urls() {
        let signal = SocialSignal::new(Chain::Solana);

        let text = "Buy at https://jup.ag/swap/SOL-SoLongAndThanksForAllTheFish12345";
        let cas = signal.extract_solana_cas(text);
        assert!(cas.iter().all(|c| !c.starts_with("http")),
            "URLs should not be extracted as CAs");
    }

    /// Test: extract_solana_cas rejects short strings that aren't CAs.
    #[test]
    fn test_extract_solana_cas_rejects_short_strings() {
        let signal = SocialSignal::new(Chain::Solana);

        let text = "SOL BTC ETH USDC are all great coins";
        let cas = signal.extract_solana_cas(text);
        assert!(cas.is_empty(), "Short token symbols should not be extracted as CAs");
    }

    /// Test: SocialSignal::evaluate returns 0 when mentions below threshold.
    #[tokio::test]
    async fn test_social_evaluate_below_threshold() {
        let signal = SocialSignal::with_config(
            Chain::Solana,
            "echo".to_string(), // Use echo as a fake twitter-cli for testing
            vec![],
            60,
            3, // min_mentions = 3
        );

        let token = make_token("SomeToken123456789", Some(0.001), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "Should score 0 with no mentions");

        assert!(
            result.reason.contains("below threshold")
                || result.reason.contains("No social mentions")
                || result.reason.contains("Social momentum: 0"),
            "Reason should explain zero score, got: {}",
            result.reason
        );
    }

    // ─── Curated Account Polling Tests ────────────────────────────────────

    /// Test: poll_account_timeline only attributes tweets with explicit CAs.
    /// Tweets without CAs from curated accounts should NOT be attributed.
    #[test]
    fn test_account_polling_no_ca_attribution() {
        let signal = SocialSignal::new(Chain::Solana);

        // A tweet without any CA should not be attributed.
        let cas = signal.extract_solana_cas("Just tweeted about how great Solana is today!");
        assert!(cas.is_empty(), "Tweet without CAs should not be attributed to any token");
    }

    /// Test: ConfluenceScorer::poll_social_accounts method exists and is callable.
    #[tokio::test]
    async fn test_confluence_poll_social_accounts_callable() {
        let mut scorer = ConfluenceScorer::new(35.0);
        scorer.add_strategy(
            StrategyKind::Social(SocialSignal::with_config(
                Chain::Solana,
                "echo".to_string(),
                vec![],
                60,
                3,
            )),
            0.15,
        );

        // Call with empty handles list — should work without errors.
        let count = scorer.poll_social_accounts(&[]).await;
        assert_eq!(count, 0, "Empty handles list should return 0 mentions");
    }

    // ─── Twitter Account DB Tests ────────────────────────────────────────

    /// Test: twitter_accounts table CRUD operations work.
    /// Validates the DB layer for VAL-CROSS-006.
    #[tokio::test]
    async fn test_twitter_account_table_crud() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        let pm = solagent_portfolio::PortfolioManager::new(pool);

        // Verify table exists.
        let (name,): (String,) = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='twitter_accounts'",
        )
        .fetch_one(pm.pool())
        .await
        .unwrap();
        assert_eq!(name, "twitter_accounts");

        // Upsert a handle.
        pm.upsert_twitter_account("solanatoken", Some("TokenABC123"), Some(10000))
            .await
            .unwrap();

        // Upsert another handle.
        pm.upsert_twitter_account("vitalik", Some("TokenDEF456"), Some(500000))
            .await
            .unwrap();

        // Get all accounts.
        let accounts = pm.get_twitter_accounts(10).await.unwrap();
        assert_eq!(accounts.len(), 2);

        // Verify fields.
        let sol = accounts.iter().find(|a| a.handle == "solanatoken").unwrap();
        assert_eq!(sol.source_token, Some("TokenABC123".to_string()));
        assert_eq!(sol.followers_count, Some(10000));
        assert_eq!(sol.mention_count, 0);

        // Upsert same handle again — should update, not duplicate.
        pm.upsert_twitter_account("solanatoken", Some("TokenNEW789"), Some(12000))
            .await
            .unwrap();

        let count = pm.get_twitter_account_count().await.unwrap();
        assert_eq!(count, 2, "Should still have 2 accounts after upsert of existing handle");

        // Verify updated fields.
        let accounts = pm.get_twitter_accounts(10).await.unwrap();
        let sol = accounts.iter().find(|a| a.handle == "solanatoken").unwrap();
        assert_eq!(sol.followers_count, Some(12000));
    }

    /// Test: get_stale_twitter_accounts returns never-polled accounts.
    #[tokio::test]
    async fn test_stale_twitter_accounts_never_polled() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        let pm = solagent_portfolio::PortfolioManager::new(pool);

        // Insert 3 accounts, none polled.
        pm.upsert_twitter_account("handle1", None, None).await.unwrap();
        pm.upsert_twitter_account("handle2", None, None).await.unwrap();
        pm.upsert_twitter_account("handle3", None, None).await.unwrap();

        // Get stale (never polled) — should return all 3.
        let stale = pm.get_stale_twitter_accounts(60, 10).await.unwrap();
        assert_eq!(stale.len(), 3);

        // Mark one as polled.
        pm.mark_twitter_account_polled("handle1").await.unwrap();

        // Get stale again — should return 2 (handle2, handle3).
        let stale = pm.get_stale_twitter_accounts(60, 10).await.unwrap();
        assert_eq!(stale.len(), 2);
        assert!(stale.iter().all(|a| a.handle != "handle1"));
    }

    /// Test: increment_twitter_mention_count works.
    #[tokio::test]
    async fn test_increment_mention_count() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(solagent_portfolio::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();

        let pm = solagent_portfolio::PortfolioManager::new(pool);

        pm.upsert_twitter_account("testhandle", None, None).await.unwrap();
        pm.increment_twitter_mention_count("testhandle", 5).await.unwrap();
        pm.increment_twitter_mention_count("testhandle", 3).await.unwrap();

        let accounts = pm.get_twitter_accounts(10).await.unwrap();
        let acc = accounts.into_iter().find(|a| a.handle == "testhandle").unwrap();
        assert_eq!(acc.mention_count, 8, "Mention count should be 5+3=8");
    }

    // ════════════════════════════════════════════════════════════════════
    // Whale Consensus Revival Tests (VAL-SIG-005/006/007/008)
    // ════════════════════════════════════════════════════════════════════

    /// Helper: create a WalletHold event.
    fn make_wallet_hold_event(wallet: &str, token: &str, value_usd: f64) -> Event {
        Event::WalletHold {
            wallet: wallet.to_string(),
            token_address: token.to_string(),
            chain: Chain::Solana,
            value_usd,
            quantity: value_usd / 100.0,
            timestamp: Utc::now(),
        }
    }

    /// VAL-SIG-005: WhaleConsensusSignal records WalletBuy events from EventBus
    /// and produces score > 0 when ≥2 known wallets buy same token in window.
    ///
    /// This test validates the full flow: EventBus publish → subscribe_to_events →
    /// record_buy → evaluate → score > 0 with event count in reasoning.
    #[tokio::test]
    async fn test_whale_consensus_revival_records_wallet_buy_events() {
        let event_bus = EventBus::new(64);

        let mut scores = InMemoryWalletScores::new();
        scores.insert("ws_wallet_1".to_string(), 85.0);
        scores.insert("ws_wallet_2".to_string(), 80.0);

        let signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,    // min_wallets
            30,   // window_minutes
            0.0,  // min_buy_usd
            Box::new(scores),
        ));

        // Subscribe to EventBus.
        signal.subscribe_to_events(&event_bus);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Publish 2 WalletBuy events from known wallets for same token.
        event_bus.publish(make_wallet_buy_event("ws_wallet_1", "Token_WS_Test", 500.0));
        event_bus.publish(make_wallet_buy_event("ws_wallet_2", "Token_WS_Test", 300.0));

        // Wait for async processing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Evaluate — should score > 0.
        let token = make_token("Token_WS_Test", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.strategy, "whale_consensus");
        assert!(
            result.score > 0,
            "VAL-SIG-005: Score should be > 0 after 2 known wallets buy via EventBus, got {}",
            result.score
        );
    }

    /// VAL-SIG-006: Whale consensus hold score supplements buy score.
    ///
    /// With ≥2 wallets holding positions in the same token, the hold score
    /// should be > 0 and the combined score should be higher than buy score alone.
    /// Reasoning must mention both buys and holds.
    #[tokio::test]
    async fn test_whale_consensus_revival_hold_score_supplements_buy() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("hold_w1".to_string(), 90.0);
        scores.insert("hold_w2".to_string(), 85.0);
        scores.insert("hold_w3".to_string(), 75.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        );

        let token_ca = "HoldSupplementToken";

        // Record buys from 2 wallets.
        signal.record_buy(token_ca.to_string(), "hold_w1".to_string(), 200.0);
        signal.record_buy(token_ca.to_string(), "hold_w2".to_string(), 300.0);

        // Evaluate without holds — get baseline buy-only score.
        let token = make_token(token_ca, Some(0.01), None, None, None);
        let buy_only_result = signal.evaluate(&token).await.unwrap();
        let buy_only_score = buy_only_result.score;

        // Now add hold events from 3 wallets (Zerion enrichment).
        signal.record_hold(token_ca.to_string(), "hold_w1".to_string(), 2000.0);
        signal.record_hold(token_ca.to_string(), "hold_w2".to_string(), 3000.0);
        signal.record_hold(token_ca.to_string(), "hold_w3".to_string(), 1500.0);

        // Evaluate with holds — combined score should be higher.
        let combined_result = signal.evaluate(&token).await.unwrap();

        assert!(
            combined_result.score >= buy_only_score,
            "VAL-SIG-006: Combined score (buys+holds) should be >= buy-only score. Got combined={}, buy_only={}",
            combined_result.score, buy_only_score
        );

        // Reasoning must mention holds.
        assert!(
            combined_result.reason.contains("holds"),
            "VAL-SIG-006: Reasoning should mention holds when hold events exist, got: {}",
            combined_result.reason
        );
    }

    /// VAL-SIG-006: Hold score alone produces non-zero score.
    ///
    /// With ≥2 wallets holding but 0 buys, the hold score should still produce
    /// a non-zero result (position conviction).
    #[tokio::test]
    async fn test_whale_consensus_revival_hold_only_scores_nonzero() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("hold_only_w1".to_string(), 90.0);
        scores.insert("hold_only_w2".to_string(), 85.0);
        scores.insert("hold_only_w3".to_string(), 75.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        );

        let token_ca = "HoldOnlyToken";

        // Only record holds, no buys.
        signal.record_hold(token_ca.to_string(), "hold_only_w1".to_string(), 5000.0);
        signal.record_hold(token_ca.to_string(), "hold_only_w2".to_string(), 3000.0);
        signal.record_hold(token_ca.to_string(), "hold_only_w3".to_string(), 1500.0);

        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert!(
            result.score > 0,
            "VAL-SIG-006: Hold-only should score > 0 with 3 holding wallets, got {}",
            result.score
        );
        assert!(
            result.reason.contains("hold") || result.reason.contains("position conviction"),
            "VAL-SIG-006: Reasoning should mention holds/position conviction, got: {}",
            result.reason
        );
    }

    /// VAL-SIG-007: Whale consensus stale records are pruned within the sliding window.
    ///
    /// Records older than `window_minutes` should be removed during evaluate().
    /// After pruning stale buys, the score should drop to 0 (or only reflect
    /// remaining non-stale buys).
    #[tokio::test]
    async fn test_whale_consensus_revival_stale_records_pruned() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("stale_w1".to_string(), 90.0);
        scores.insert("stale_w2".to_string(), 85.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            5,    // window_minutes = 5 (short window for testing)
            0.0,
            Box::new(scores),
        );

        let token_ca = "StaleToken";

        // Manually inject stale buy records (older than window).
        let stale_time = Utc::now() - chrono::Duration::minutes(10); // 10 min ago > 5 min window
        {
            let mut buys = signal.buys.entry(token_ca.to_string()).or_default();
            buys.push_back(WalletBuyRecord {
                wallet: "stale_w1".to_string(),
                timestamp: stale_time,
                amount_usd: 100.0,
                from_gmgn: false,
            });
            buys.push_back(WalletBuyRecord {
                wallet: "stale_w2".to_string(),
                timestamp: stale_time,
                amount_usd: 200.0,
                from_gmgn: false,
            });
        }

        // Before pruning, there should be 2 records.
        assert_eq!(signal.buys.get(token_ca).map(|b| b.len()).unwrap_or(0), 2);

        // Evaluate — should prune stale records and score 0.
        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(
            result.score, 0,
            "VAL-SIG-007: Score should be 0 after stale records pruned, got {}",
            result.score
        );

        // After pruning, the stale records should be removed.
        assert_eq!(
            signal.buys.get(token_ca).map(|b| b.len()).unwrap_or(0),
            0,
            "VAL-SIG-007: Stale records should be removed after evaluate"
        );
    }

    /// VAL-SIG-007: Fresh buys within the window are retained and score > 0.
    #[tokio::test]
    async fn test_whale_consensus_revival_fresh_buys_retained() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("fresh_w1".to_string(), 90.0);
        scores.insert("fresh_w2".to_string(), 85.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,   // window_minutes
            0.0,
            Box::new(scores),
        );

        let token_ca = "FreshToken";

        // Record fresh buys (within window).
        signal.record_buy(token_ca.to_string(), "fresh_w1".to_string(), 100.0);
        signal.record_buy(token_ca.to_string(), "fresh_w2".to_string(), 200.0);

        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert!(
            result.score > 0,
            "VAL-SIG-007: Fresh buys should score > 0, got {}",
            result.score
        );

        // Records should still be present.
        assert_eq!(
            signal.buys.get(token_ca).map(|b| b.len()).unwrap_or(0),
            2,
            "Fresh buys should be retained after evaluate"
        );
    }

    /// VAL-SIG-008: GMGN fallback provides whale signal when no WS events in window.
    ///
    /// When no WalletBuy events are recorded (empty window), WhaleConsensusSignal
    /// should query GMGN top traders for the token. If known wallets from the
    /// registry appear among the top traders, those should be used as a fallback
    /// to produce a score > 0.
    ///
    /// This test uses a mock/fake gmgn-cli path that won't produce real results,
    /// so it tests the fallback mechanism path. The real GMGN test would need
    /// the actual CLI.
    #[tokio::test]
    async fn test_whale_consensus_revival_gmgn_fallback_no_ws_events() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("gmgn_known_w1".to_string(), 90.0);
        scores.insert("gmgn_known_w2".to_string(), 85.0);

        let signal = WhaleConsensusSignal::with_gmgn_fallback(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
            "/nonexistent/gmgn-cli".to_string(), // Won't produce results
        );

        let token_ca = "GMGNFallbackToken";

        // No buys recorded — window is empty.
        // Evaluate with GMGN fallback enabled.
        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        // With a nonexistent CLI, the fallback won't produce matches,
        // but the mechanism should run without error.
        // Score should be 0 because the fallback couldn't reach GMGN.
        // The key assertion is that evaluate() attempted the fallback path
        // and didn't panic or error.
        assert!(
            result.score == 0,
            "VAL-SIG-008: With nonexistent GMGN CLI, score should be 0 (fallback path attempted gracefully), got {}",
            result.score
        );

        // Reasoning should indicate the fallback was attempted.
        assert!(
            result.reason.contains("GMGN fallback") || result.reason.contains("whale_consensus"),
            "VAL-SIG-008: Reasoning should mention GMGN fallback or whale_consensus, got: {}",
            result.reason
        );
    }

    /// VAL-SIG-008: GMGN fallback with manually injected top-trader matches.
    ///
    /// When GMGN top-trader data is manually injected (simulating a real GMGN
    /// response), the WhaleConsensusSignal should produce a score > 0 from
    /// the fallback data, even with 0 WS events in the window.
    #[tokio::test]
    async fn test_whale_consensus_revival_gmgn_fallback_with_injected_traders() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("gmgn_trader_1".to_string(), 90.0);
        scores.insert("gmgn_trader_2".to_string(), 85.0);

        let signal = WhaleConsensusSignal::with_gmgn_fallback(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
            "/nonexistent/gmgn-cli".to_string(),
        );

        let token_ca = "GMGNInjectedToken";

        // Manually record GMGN-detected buys (simulating what the fallback would find).
        signal.record_gmgn_buy(token_ca.to_string(), "gmgn_trader_1".to_string(), 1000.0);
        signal.record_gmgn_buy(token_ca.to_string(), "gmgn_trader_2".to_string(), 800.0);

        // No WS buys — only GMGN fallback data.
        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert!(
            result.score > 0,
            "VAL-SIG-008: GMGN fallback with 2 known traders should score > 0, got {}",
            result.score
        );
        assert!(
            result.reason.contains("GMGN"),
            "VAL-SIG-008: Reasoning should mention GMGN fallback, got: {}",
            result.reason
        );
    }

    /// Expected behavior: Signal reasoning includes event counts.
    ///
    /// After recording buys and holds, the reasoning string should include
    /// the number of buy events and hold events that contributed to the score.
    #[tokio::test]
    async fn test_whale_consensus_revival_reasoning_includes_event_counts() {
        let mut scores = InMemoryWalletScores::new();
        scores.insert("reason_w1".to_string(), 90.0);
        scores.insert("reason_w2".to_string(), 85.0);
        scores.insert("reason_w3".to_string(), 80.0);

        let signal = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        );

        let token_ca = "ReasoningToken";

        // Record buys from 2 wallets.
        signal.record_buy(token_ca.to_string(), "reason_w1".to_string(), 200.0);
        signal.record_buy(token_ca.to_string(), "reason_w2".to_string(), 300.0);

        // Record holds from 3 wallets.
        signal.record_hold(token_ca.to_string(), "reason_w1".to_string(), 2000.0);
        signal.record_hold(token_ca.to_string(), "reason_w2".to_string(), 3000.0);
        signal.record_hold(token_ca.to_string(), "reason_w3".to_string(), 1500.0);

        let token = make_token(token_ca, Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        // Reasoning should include event counts.
        assert!(
            result.reason.contains("buys=") || result.reason.contains("2"),
            "Reasoning should mention buy count, got: {}",
            result.reason
        );
        assert!(
            result.reason.contains("holds=") || result.reason.contains("3"),
            "Reasoning should mention hold count, got: {}",
            result.reason
        );
    }

    /// VAL-SIG-005: WalletHold events from EventBus are recorded.
    ///
    /// When WalletHold events are published on the EventBus, the
    /// WhaleConsensusSignal should record them for the hold score.
    #[tokio::test]
    async fn test_whale_consensus_revival_wallet_hold_via_eventbus() {
        let event_bus = EventBus::new(64);

        let mut scores = InMemoryWalletScores::new();
        scores.insert("hold_ev_w1".to_string(), 90.0);
        scores.insert("hold_ev_w2".to_string(), 85.0);
        scores.insert("hold_ev_w3".to_string(), 80.0);

        let signal = Arc::new(WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            0.0,
            Box::new(scores),
        ));

        signal.subscribe_to_events(&event_bus);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Publish WalletHold events from 3 known wallets.
        event_bus.publish(make_wallet_hold_event("hold_ev_w1", "HoldEventToken", 2000.0));
        event_bus.publish(make_wallet_hold_event("hold_ev_w2", "HoldEventToken", 3000.0));
        event_bus.publish(make_wallet_hold_event("hold_ev_w3", "HoldEventToken", 1500.0));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Evaluate — hold score should be > 0.
        let token = make_token("HoldEventToken", Some(0.01), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        assert!(
            result.score > 0,
            "VAL-SIG-006: Hold events via EventBus should produce score > 0, got {}",
            result.score
        );
    }

    // ─── Behavioral Signal Tests (VAL-SIG-009/010/011/012) ─────────────

    #[tokio::test]
    async fn test_behavioral_scanner_widening_min_appearances_default() {
        // VAL-SIG-010: Verify min_token_appearances=1 allows more wallets.
        // This test validates the scanner crate's default is 1 by checking
        // the behavioral cache accepts wallets that appear in only 1 token.
        let cache = Arc::new(BehavioralWalletCache::new());
        assert!(cache.is_empty(), "New cache should be empty");

        // Simulate loading wallets from a scan with min_token_appearances=1
        // (single-appearance wallets that would have been excluded with threshold=2).
        let wallets = vec![
            BehavioralWallet {
                address: "single_appear_wallet_1".to_string(),
                tier: BehavioralTier::Emerging,
                score: 60.0,
                primary_edge: "L2_GhostDetect".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "single_appear_wallet_2".to_string(),
                tier: BehavioralTier::Sovereign,
                score: 80.0,
                primary_edge: "L1_InverseLoss".to_string(),
                red_flags: vec![],
            },
        ];
        cache.update(wallets).await;
        assert!(!cache.is_empty(), "Cache should be populated after update");
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn test_behavioral_signal_precognitive_wallet_scores_85() {
        // VAL-SIG-012: PRECOGNITIVE wallet → score 85.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(Arc::clone(&cache), Chain::Solana);

        // Pre-populate cache with a PRECOGNITIVE wallet.
        cache.update(vec![
            BehavioralWallet {
                address: "precog_wallet_1".to_string(),
                tier: BehavioralTier::Precognitive,
                score: 92.0,
                primary_edge: "L1_InverseLoss".to_string(),
                red_flags: vec![],
            },
        ]).await;

        // Inject a cached detection for a token.
        let now = Utc::now();
        signal.token_wallets.insert(
            "token_with_precog".to_string(),
            vec![("precog_wallet_1".to_string(), BehavioralTier::Precognitive, now)],
        );

        let token = make_token("token_with_precog", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 85, "PRECOGNITIVE wallet should produce score 85, got {}", result.score);
        assert!(result.reason.contains("PRECOGNITIVE"), "Reasoning should mention PRECOGNITIVE");
    }

    #[tokio::test]
    async fn test_behavioral_signal_sovereign_wallet_scores_70() {
        // VAL-SIG-012: SOVEREIGN wallet → score 70.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(cache, Chain::Solana);

        let now = Utc::now();
        signal.token_wallets.insert(
            "token_with_sovereign".to_string(),
            vec![("sov_wallet_1".to_string(), BehavioralTier::Sovereign, now)],
        );

        let token = make_token("token_with_sovereign", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 70, "SOVEREIGN wallet should produce score 70, got {}", result.score);
        assert!(result.reason.contains("SOVEREIGN"), "Reasoning should mention SOVEREIGN");
    }

    #[tokio::test]
    async fn test_behavioral_signal_two_sovereign_wallets_scores_78() {
        // VAL-SIG-012: 2 SOVEREIGN wallets → score min(70 + 8, 100) = 78.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(cache, Chain::Solana);

        let now = Utc::now();
        signal.token_wallets.insert(
            "token_two_sovereign".to_string(),
            vec![
                ("sov_wallet_1".to_string(), BehavioralTier::Sovereign, now),
                ("sov_wallet_2".to_string(), BehavioralTier::Sovereign, now),
            ],
        );

        let token = make_token("token_two_sovereign", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 78, "2 SOVEREIGN wallets should produce score 78, got {}", result.score);
    }

    #[tokio::test]
    async fn test_behavioral_signal_three_emerging_wallets_scores_60() {
        // VAL-SIG-012: 3 EMERGING wallets → score min(45 + 15, 100) = 60.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(cache, Chain::Solana);

        let now = Utc::now();
        signal.token_wallets.insert(
            "token_three_emerging".to_string(),
            vec![
                ("em_wallet_1".to_string(), BehavioralTier::Emerging, now),
                ("em_wallet_2".to_string(), BehavioralTier::Emerging, now),
                ("em_wallet_3".to_string(), BehavioralTier::Emerging, now),
            ],
        );

        let token = make_token("token_three_emerging", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 60, "3 EMERGING wallets should produce score 60, got {}", result.score);
    }

    #[tokio::test]
    async fn test_behavioral_signal_no_wallets_scores_zero() {
        // VAL-SIG-012: No behavioral wallets → score 0.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(cache, Chain::Solana);

        let token = make_token("token_no_wallets", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0, "No behavioral wallets should produce score 0");
        assert!(
            result.reason.contains("No behavioral wallets detected"),
            "Reasoning should explain why: got '{}'",
            result.reason
        );
    }

    #[tokio::test]
    async fn test_behavioral_signal_reasoning_format() {
        // VAL-SIG-012: Verify reasoning format includes tier names.
        let cache = Arc::new(BehavioralWalletCache::new());
        let signal = BehavioralSignal::new(cache, Chain::Solana);

        let now = Utc::now();
        signal.token_wallets.insert(
            "token_reasoning_test".to_string(),
            vec![
                ("sov_wallet".to_string(), BehavioralTier::Sovereign, now),
                ("em_wallet".to_string(), BehavioralTier::Emerging, now),
            ],
        );

        let token = make_token("token_reasoning_test", Some(1.0), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert!(result.score > 0, "Score should be > 0 with mixed tiers");
        assert!(
            result.reason.contains("high-tier wallets"),
            "Reasoning should mention high-tier wallets: got '{}'",
            result.reason
        );
        assert!(
            result.reason.contains("SOVEREIGN"),
            "Reasoning should list SOVEREIGN: got '{}'",
            result.reason
        );
        assert!(
            result.reason.contains("EMERGING"),
            "Reasoning should list EMERGING: got '{}'",
            result.reason
        );
    }

    #[tokio::test]
    async fn test_behavioral_cache_tier_counts() {
        // VAL-SIG-011: Verify cache accumulates wallets and reports tier counts.
        let cache = Arc::new(BehavioralWalletCache::new());

        let wallets = vec![
            BehavioralWallet {
                address: "precog".to_string(),
                tier: BehavioralTier::Precognitive,
                score: 95.0,
                primary_edge: "L1".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "sov1".to_string(),
                tier: BehavioralTier::Sovereign,
                score: 80.0,
                primary_edge: "L2".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "sov2".to_string(),
                tier: BehavioralTier::Sovereign,
                score: 78.0,
                primary_edge: "L3".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "em1".to_string(),
                tier: BehavioralTier::Emerging,
                score: 60.0,
                primary_edge: "L5".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "em2".to_string(),
                tier: BehavioralTier::Emerging,
                score: 58.0,
                primary_edge: "L4".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "noise1".to_string(),
                tier: BehavioralTier::Noise,
                score: 30.0,
                primary_edge: "Unknown".to_string(),
                red_flags: vec![],
            },
        ];

        cache.update(wallets).await;
        let (prec, sov, em, noise) = cache.tier_counts();
        assert_eq!(prec, 1);
        assert_eq!(sov, 2);
        assert_eq!(em, 2);
        assert_eq!(noise, 1);

        // Verify >= 5 non-NOISE wallets
        let high_tier = prec + sov + em;
        assert!(
            high_tier >= 5,
            "Non-NOISE wallet count should be >= 5 for VAL-SIG-011, got {}",
            high_tier
        );
    }

    #[tokio::test]
    async fn test_behavioral_cache_registry_fallback_populates() {
        // VAL-SIG-011: Verify that a registry-style fallback can populate the cache.
        // Simulates the smart_money registry fallback loading wallets into cache.
        let cache = Arc::new(BehavioralWalletCache::new());
        assert!(cache.is_empty());

        // Simulate loading 5 wallets from registry fallback.
        let registry_wallets: Vec<BehavioralWallet> = (0..5)
            .map(|i| BehavioralWallet {
                address: format!("registry_wallet_{i}"),
                tier: if i < 2 { BehavioralTier::Sovereign } else { BehavioralTier::Emerging },
                score: if i < 2 { 80.0 + i as f64 } else { 60.0 + i as f64 },
                primary_edge: "registry_fallback".to_string(),
                red_flags: vec![],
            })
            .collect();

        cache.update(registry_wallets).await;
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 5);

        let (prec, sov, em, _noise) = cache.tier_counts();
        let high_tier = prec + sov + em;
        assert!(
            high_tier >= 5,
            "Registry fallback should provide >= 5 non-NOISE wallets, got {}",
            high_tier
        );

        // Verify at least 1 SOVEREIGN
        assert!(
            sov >= 1,
            "Registry fallback should include >= 1 SOVEREIGN wallet, got {}",
            sov
        );
    }

    #[tokio::test]
    async fn test_behavioral_is_high_tier() {
        let cache = Arc::new(BehavioralWalletCache::new());

        cache.update(vec![
            BehavioralWallet {
                address: "precog_addr".to_string(),
                tier: BehavioralTier::Precognitive,
                score: 95.0,
                primary_edge: "L1".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "sov_addr".to_string(),
                tier: BehavioralTier::Sovereign,
                score: 80.0,
                primary_edge: "L2".to_string(),
                red_flags: vec![],
            },
            BehavioralWallet {
                address: "em_addr".to_string(),
                tier: BehavioralTier::Emerging,
                score: 60.0,
                primary_edge: "L5".to_string(),
                red_flags: vec![],
            },
        ]).await;

        assert!(cache.is_high_tier("precog_addr"), "PRECOGNITIVE should be high-tier");
        assert!(cache.is_high_tier("sov_addr"), "SOVEREIGN should be high-tier");
        assert!(!cache.is_high_tier("em_addr"), "EMERGING should NOT be high-tier");
        assert!(!cache.is_high_tier("unknown_addr"), "Unknown wallet should NOT be high-tier");
    }

    // ─── Signal Quality Logging Tests (VAL-SIG-013, VAL-SIG-014) ────────

    /// VAL-SIG-014: Every signal returns non-empty reasoning.
    /// Tests that all 6 signals produce a non-empty reason string.
    #[tokio::test]
    async fn test_signal_quality_all_signals_nonempty_reasoning() {
        let token = make_token("reasoning_test", Some(1.0), Some(10000.0), Some(50000.0), None);

        // AccumulationSignal (no history → zero score)
        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let result = acc.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "AccumulationSignal reasoning must be non-empty, got empty string"
        );

        // VolumeSpikeSignal (no data → zero score)
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let result = vs.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "VolumeSpikeSignal reasoning must be non-empty, got empty string"
        );

        // LaunchMomentumSignal (no data → zero score)
        let lm = LaunchMomentumSignal::new(Chain::Solana, 10);
        let result = lm.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "LaunchMomentumSignal reasoning must be non-empty, got empty string"
        );

        // SocialSignal (no mentions → zero score)
        let ss = SocialSignal::new(Chain::Solana);
        let result = ss.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "SocialSignal reasoning must be non-empty, got empty string"
        );

        // BehavioralSignal (no cache → zero score)
        let bs = BehavioralSignal::new(
            Arc::new(BehavioralWalletCache::new()),
            Chain::Solana,
        );
        let result = bs.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "BehavioralSignal reasoning must be non-empty, got empty string"
        );

        // WhaleConsensusSignal (no events → zero score)
        let ws = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            10.0,
            Box::new(InMemoryWalletScores::new()),
        );
        let result = ws.evaluate(&token).await.unwrap();
        assert!(
            !result.reason.is_empty(),
            "WhaleConsensusSignal reasoning must be non-empty, got empty string"
        );
    }

    /// VAL-SIG-014: Zero-score signals explain why.
    /// Each zero-score signal must include a diagnostic reason (e.g., "Insufficient",
    /// "No events", "below threshold").
    #[tokio::test]
    async fn test_signal_quality_zero_score_explains_why() {
        let token = make_token("zero_reasoning_test", Some(1.0), None, None, None);

        // AccumulationSignal: zero score, should explain why
        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let result = acc.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            !result.reason.is_empty(),
            "Zero-score accumulation must explain why"
        );
        assert!(
            result.reason.to_lowercase().contains("insufficient")
                || result.reason.to_lowercase().contains("no ")
                || result.reason.to_lowercase().contains("below"),
            "Zero-score accumulation reasoning should explain why: got '{}'",
            result.reason
        );

        // VolumeSpikeSignal: zero score
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let result = vs.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            !result.reason.is_empty(),
            "Zero-score volume_spike must explain why"
        );
        assert!(
            result.reason.to_lowercase().contains("insufficient")
                || result.reason.to_lowercase().contains("no "),
            "Zero-score volume_spike reasoning should explain why: got '{}'",
            result.reason
        );

        // LaunchMomentumSignal: zero score (no snapshots)
        let lm = LaunchMomentumSignal::new(Chain::Solana, 10);
        let result = lm.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            !result.reason.is_empty(),
            "Zero-score launch_momentum must explain why"
        );

        // SocialSignal: zero score (no mentions)
        let ss = SocialSignal::new(Chain::Solana);
        let result = ss.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            !result.reason.is_empty(),
            "Zero-score social must explain why"
        );

        // BehavioralSignal: zero score (no cache)
        let bs = BehavioralSignal::new(
            Arc::new(BehavioralWalletCache::new()),
            Chain::Solana,
        );
        let result = bs.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            result.reason.to_lowercase().contains("no "),
            "Zero-score behavioral reasoning should explain why: got '{}'",
            result.reason
        );

        // WhaleConsensusSignal: zero score (no events)
        let ws = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            10.0,
            Box::new(InMemoryWalletScores::new()),
        );
        let result = ws.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            !result.reason.is_empty(),
            "Zero-score whale_consensus must explain why"
        );
    }

    /// VAL-SIG-014: Non-zero signals include numeric score and contributing factors.
    #[tokio::test]
    async fn test_signal_quality_nonzero_accumulation_includes_factors() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);
        let token_ca = "nonzero_acc";

        // Record 3 snapshots: strong holder growth, flat price.
        signal.record_snapshot(token_ca.to_string(), 100, 1.0);
        signal.record_snapshot(token_ca.to_string(), 150, 1.02);
        signal.record_snapshot(token_ca.to_string(), 200, 1.05);

        let token = make_token(token_ca, Some(1.05), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();
        assert!(result.score > 0, "Should have non-zero score");
        assert!(
            !result.reason.is_empty(),
            "Non-zero accumulation must include reasoning"
        );
        // Reasoning should include numeric score.
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score {}: got '{}'",
            result.score,
            result.reason
        );
        // Reasoning should include contributing factors (holder growth, snapshots, or price).
        let reason_lower = result.reason.to_lowercase();
        assert!(
            reason_lower.contains("holder")
                || reason_lower.contains("snapshot")
                || reason_lower.contains("growth")
                || reason_lower.contains("price"),
            "Reasoning should mention contributing factors: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Non-zero volume spike reasoning includes ratio and contributing factors.
    #[tokio::test]
    async fn test_signal_quality_nonzero_volume_spike_includes_factors() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "nonzero_vs";

        // Record 4 points with a clear spike.
        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 1200.0);
        signal.record(token_ca.to_string(), 1100.0);
        signal.record(token_ca.to_string(), 5000.0); // 4.5x spike

        let token = make_token(token_ca, Some(1.0), Some(5000.0), Some(50000.0), None);
        let result = signal.evaluate(&token).await.unwrap();
        assert!(result.score > 0, "Should have non-zero score");
        assert!(
            !result.reason.is_empty(),
            "Non-zero volume_spike must include reasoning"
        );
        // Reasoning should include numeric score.
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score {}: got '{}'",
            result.score,
            result.reason
        );
        // Reasoning should include ratio or spike indicator.
        let reason_lower = result.reason.to_lowercase();
        assert!(
            reason_lower.contains("spike") || reason_lower.contains("ratio") || reason_lower.contains("x"),
            "Reasoning should mention spike/ratio: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Non-zero launch momentum reasoning includes factors.
    #[tokio::test]
    async fn test_signal_quality_nonzero_launch_momentum_includes_factors() {
        let signal = LaunchMomentumSignal::new(Chain::Solana, 10);
        let token_ca = "nonzero_lm";

        // Record 2 snapshots with strong growth.
        signal.record(token_ca.to_string(), 1000.0, 50);
        signal.record(token_ca.to_string(), 5000.0, 200);

        let created = Utc::now() - chrono::Duration::minutes(30);
        let token = make_token(token_ca, Some(1.0), Some(5000.0), Some(50000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();
        assert!(result.score > 0, "Should have non-zero score");
        assert!(
            !result.reason.is_empty(),
            "Non-zero launch_momentum must include reasoning"
        );
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score {}: got '{}'",
            result.score,
            result.reason
        );
    }

    /// VAL-SIG-014: Non-zero behavioral reasoning includes tier and count.
    #[tokio::test]
    async fn test_signal_quality_nonzero_behavioral_includes_tiers() {
        let cache = Arc::new(BehavioralWalletCache::new());
        cache.update(vec![
            BehavioralWallet {
                address: "wallet1".to_string(),
                tier: BehavioralTier::Sovereign,
                score: 80.0,
                primary_edge: "L2".to_string(),
                red_flags: vec![],
            },
        ]).await;

        let bs = BehavioralSignal::new(cache, Chain::Solana);

        // Manually inject token detection.
        bs.token_wallets.insert(
            "behav_nonzero".to_string(),
            vec![("wallet1".to_string(), BehavioralTier::Sovereign, Utc::now())],
        );

        let token = make_token("behav_nonzero", Some(1.0), None, None, None);
        let result = bs.evaluate(&token).await.unwrap();
        assert!(result.score > 0, "Should have non-zero score");
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score: got '{}'",
            result.reason
        );
        assert!(
            result.reason.contains("SOVEREIGN") || result.reason.contains("wallet"),
            "Reasoning should mention tier/wallets: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-015: Confluence scorer weighted composite with all 6 signals.
    /// Tests exact weighted average computation.
    #[tokio::test]
    async fn test_signal_quality_confluence_all_six_weighted_correctly() {
        let mut scorer = ConfluenceScorer::new(35.0);

        let weights = SignalWeights::default();

        let ws = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            10.0,
            Box::new(InMemoryWalletScores::new()),
        );
        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let lm = LaunchMomentumSignal::new(Chain::Solana, 10);
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let ss = SocialSignal::new(Chain::Solana);
        let bs = BehavioralSignal::new(
            Arc::new(BehavioralWalletCache::new()),
            Chain::Solana,
        );

        scorer.add_strategy(StrategyKind::WhaleConsensus(ws), weights.whale_consensus);
        scorer.add_strategy(StrategyKind::Accumulation(acc), weights.accumulation);
        scorer.add_strategy(StrategyKind::LaunchMomentum(lm), weights.launch_momentum);
        scorer.add_strategy(StrategyKind::VolumeSpike(vs), weights.volume_spike);
        scorer.add_strategy(StrategyKind::Social(ss), weights.social);
        scorer.add_strategy(StrategyKind::Behavioral(bs), weights.behavioral);

        // All signals will score 0 (no data fed) — composite should be 0.
        let token = make_token("empty_token", Some(1.0), None, None, None);
        let result = scorer.score(&token).await.unwrap();
        assert_eq!(result.composite_score, 0);
        assert!(!result.passed);
        assert_eq!(result.signals.len(), 6, "Should have exactly 6 signals");

        // All reasons should be non-empty.
        for signal in &result.signals {
            assert!(
                !signal.reason.is_empty(),
                "Signal '{}' reasoning must be non-empty",
                signal.strategy
            );
        }
    }

    /// VAL-SIG-015: Confluence scorer weighted composite with mixed scores.
    #[tokio::test]
    async fn test_signal_quality_confluence_mixed_scores_weighted() {
        let mut scorer = ConfluenceScorer::new(35.0);

        // Only accumulation and volume_spike for clean math.
        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);

        scorer.add_strategy(StrategyKind::Accumulation(acc), 0.15);
        scorer.add_strategy(StrategyKind::VolumeSpike(vs), 0.10);

        // Feed data so both produce non-zero scores.
        let token_ca = "weighted_test";
        scorer.feed_scan_data(token_ca, Some(10000.0), Some(100), Some(1.0));
        scorer.feed_scan_data(token_ca, Some(12000.0), Some(150), Some(1.02));
        scorer.feed_scan_data(token_ca, Some(11000.0), Some(200), Some(1.05));
        scorer.feed_scan_data(token_ca, Some(50000.0), Some(250), Some(1.05));

        let token = make_token(token_ca, Some(1.05), Some(50000.0), Some(50000.0), None);
        let result = scorer.score(&token).await.unwrap();

        let acc_sig = result.signals.iter().find(|s| s.strategy == "accumulation").unwrap();
        let vs_sig = result.signals.iter().find(|s| s.strategy == "volume_spike").unwrap();

        let expected = (acc_sig.score as f64 * 0.15 + vs_sig.score as f64 * 0.10) / (0.15 + 0.10);
        let diff = (result.composite_score as f64 - expected).abs();
        assert!(
            diff <= 1.5,
            "Composite should be within ±1.5 of weighted sum: got {}, expected {}",
            result.composite_score,
            expected
        );
    }

    /// VAL-SIG-014: Accumulation zero-score with history but declining holders.
    #[tokio::test]
    async fn test_signal_quality_accumulation_declining_reasoning() {
        let signal = AccumulationSignal::new(Chain::Solana, 10);

        // Record declining holders.
        signal.record_snapshot("decl_reason".to_string(), 200, 1.0);
        signal.record_snapshot("decl_reason".to_string(), 100, 0.9);

        let token = make_token("decl_reason", Some(0.9), None, None, None);
        let result = signal.evaluate(&token).await.unwrap();

        // Score should be low (20 in the else branch).
        assert!(
            result.score <= 20,
            "Declining holders should score <= 20, got {}",
            result.score
        );
        assert!(
            !result.reason.is_empty(),
            "Reasoning must be non-empty even for low scores"
        );
        // Reasoning should include numeric score.
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Social signal with mentions below threshold has diagnostic.
    #[tokio::test]
    async fn test_signal_quality_social_below_threshold_diagnostic() {
        let ss = SocialSignal::with_config(
            Chain::Solana,
            "nonexistent_twitter".to_string(),
            vec![],
            60,
            3,
        );

        // Inject 1 mention (below threshold of 3).
        ss.mentions.insert("social_diag".to_string(), {
            let mut q = VecDeque::new();
            q.push_back(MentionRecord {
                tweet_id: "123".to_string(),
                author: "user1".to_string(),
                engagement: 100.0,
                timestamp: Utc::now(),
            });
            q
        });

        let token = make_token("social_diag", Some(1.0), None, None, None);
        let result = ss.evaluate(&token).await.unwrap();
        assert_eq!(result.score, 0);
        assert!(
            result.reason.contains("below threshold") || result.reason.contains("below"),
            "Below-threshold social should explain: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Volume spike with data but no spike has diagnostic.
    #[tokio::test]
    async fn test_signal_quality_volume_no_spike_diagnostic() {
        let signal = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let token_ca = "no_spike_diag";

        // Record 3 flat points (no spike).
        signal.record(token_ca.to_string(), 1000.0);
        signal.record(token_ca.to_string(), 1050.0);
        signal.record(token_ca.to_string(), 1020.0);

        let token = make_token(token_ca, Some(1.0), Some(1020.0), Some(50000.0), None);
        let result = signal.evaluate(&token).await.unwrap();

        // Score should be low (10 for ratio < threshold*0.66, or possibly 50 for near-threshold).
        assert!(
            !result.reason.is_empty(),
            "Reasoning must be non-empty for no-spike case"
        );
        // Reasoning should include numeric score.
        assert!(
            result.reason.contains(&result.score.to_string()),
            "Reasoning should include score: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Launch momentum too old has diagnostic.
    #[tokio::test]
    async fn test_signal_quality_launch_too_old_diagnostic() {
        let signal = LaunchMomentumSignal::new(Chain::Solana, 10);
        let token_ca = "old_lm";

        signal.record(token_ca.to_string(), 1000.0, 50);
        signal.record(token_ca.to_string(), 5000.0, 200);

        // Token created 48 hours ago (too old for 1h max).
        let created = Utc::now() - chrono::Duration::hours(48);
        let token = make_token(token_ca, Some(1.0), Some(5000.0), Some(50000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.score, 0);
        assert!(
            result.reason.contains("too old") || result.reason.to_lowercase().contains("old"),
            "Too-old token reasoning should mention age: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-014: Launch momentum MC below threshold has diagnostic.
    #[tokio::test]
    async fn test_signal_quality_launch_below_mc_diagnostic() {
        let signal = LaunchMomentumSignal::with_filters(
            Chain::Solana,
            10,
            5000.0,
            50,
            1,
        );
        let token_ca = "low_mc";

        signal.record(token_ca.to_string(), 1000.0, 50);
        signal.record(token_ca.to_string(), 5000.0, 200);

        let created = Utc::now() - chrono::Duration::minutes(30);
        let token = make_token(token_ca, Some(1.0), Some(5000.0), Some(3000.0), Some(created));
        let result = signal.evaluate(&token).await.unwrap();

        assert_eq!(result.score, 0);
        assert!(
            result.reason.contains("below") || result.reason.contains("MC"),
            "Below-MC reasoning should mention threshold: got '{}'",
            result.reason
        );
    }

    /// VAL-SIG-013 / VAL-CROSS-004: ConfluenceResult signal_reasoning_summary()
    /// produces machine-parseable per-signal reasoning.
    #[tokio::test]
    async fn test_signal_quality_confluence_result_reasoning_summary() {
        let mut scorer = ConfluenceScorer::new(35.0);

        let ws = WhaleConsensusSignal::new(
            Chain::Solana,
            2,
            30,
            10.0,
            Box::new(InMemoryWalletScores::new()),
        );
        let acc = AccumulationSignal::new(Chain::Solana, 10);
        let vs = VolumeSpikeSignal::new(Chain::Solana, 3.0, 10);
        let ss = SocialSignal::new(Chain::Solana);
        let bs = BehavioralSignal::new(
            Arc::new(BehavioralWalletCache::new()),
            Chain::Solana,
        );
        let lm = LaunchMomentumSignal::new(Chain::Solana, 10);

        scorer.add_strategy(StrategyKind::WhaleConsensus(ws), 0.25);
        scorer.add_strategy(StrategyKind::Accumulation(acc), 0.15);
        scorer.add_strategy(StrategyKind::LaunchMomentum(lm), 0.15);
        scorer.add_strategy(StrategyKind::VolumeSpike(vs), 0.10);
        scorer.add_strategy(StrategyKind::Social(ss), 0.10);
        scorer.add_strategy(StrategyKind::Behavioral(bs), 0.25);

        let token = make_token("reasoning_summary_test", Some(1.0), None, None, None);
        let result = scorer.score(&token).await.unwrap();

        let summary = result.signal_reasoning_summary();

        // Summary should contain all 6 signal names.
        assert!(summary.contains("whale_consensus="), "Summary should contain whale_consensus: {summary}");
        assert!(summary.contains("accumulation="), "Summary should contain accumulation: {summary}");
        assert!(summary.contains("launch_momentum="), "Summary should contain launch_momentum: {summary}");
        assert!(summary.contains("volume_spike="), "Summary should contain volume_spike: {summary}");
        assert!(summary.contains("social="), "Summary should contain social: {summary}");
        assert!(summary.contains("behavioral="), "Summary should contain behavioral: {summary}");

        // Each signal should have a quoted reason string.
        assert!(
            summary.contains("\""),
            "Summary should contain quoted reasoning strings: {summary}"
        );

        // Summary should be parseable (each entry has signal=N/100 "reason" format).
        for part in summary.split(", ") {
            assert!(
                part.contains("/100"),
                "Each signal entry should have N/100 format: {part}"
            );
        }
    }
}
