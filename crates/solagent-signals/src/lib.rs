//! # solagent-signals
//!
//! Signal engine with strategy trait and confluence scoring.

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solagent_core::{Chain, Signal, TokenInfo};
use std::collections::VecDeque;
use std::sync::Arc;

// ─── Strategy Trait ──────────────────────────────────────────────────────────

/// A strategy evaluates market conditions for a token and returns a signal score (0-100).
#[allow(async_fn_in_trait)]
pub trait Strategy: Send + Sync {
    /// Name of the strategy.
    fn name(&self) -> &str;

    /// Evaluate the strategy for the given token, returning a signal.
    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal>;
}

// ─── Whale Consensus Signal ──────────────────────────────────────────────────

/// Tracks a sliding window of wallet buys per token to detect whale consensus.
#[derive(Debug)]
struct WalletBuyRecord {
    wallet: String,
    timestamp: DateTime<Utc>,
    #[allow(dead_code)]
    amount_usd: f64,
}

/// Whale consensus signal: fires when multiple known smart wallets buy the same token
/// within a configurable time window.
pub struct WhaleConsensusSignal {
    name: String,
    /// Token address -> recent buys
    buys: Arc<DashMap<String, VecDeque<WalletBuyRecord>>>,
    /// Minimum number of distinct wallets to trigger
    min_wallets: usize,
    /// Window duration in minutes
    window_minutes: i64,
    /// Minimum buy amount per wallet
    #[allow(dead_code)]
    min_buy_usd: f64,
    #[allow(dead_code)]
    chain: Chain,
}

impl WhaleConsensusSignal {
    pub fn new(chain: Chain, min_wallets: usize, window_minutes: i64, min_buy_usd: f64) -> Self {
        Self {
            name: "whale_consensus".to_string(),
            buys: Arc::new(DashMap::new()),
            min_wallets,
            window_minutes,
            min_buy_usd,
            chain,
        }
    }

    /// Record a wallet buy for a token.
    pub fn record_buy(&self, token_address: String, wallet: String, amount_usd: f64) {
        let mut buys = self.buys.entry(token_address).or_default();
        buys.push_back(WalletBuyRecord {
            wallet,
            timestamp: Utc::now(),
            amount_usd,
        });
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
}

impl Strategy for WhaleConsensusSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        self.prune_stale(&token.address);
        let score = if let Some(buys) = self.buys.get(&token.address) {
            let distinct_wallets: std::collections::HashSet<&str> =
                buys.iter().map(|b| b.wallet.as_str()).collect();
            let count = distinct_wallets.len();
            if count >= self.min_wallets {
                let ratio = (count as f64 / self.min_wallets as f64).min(1.0);
                (ratio * 100.0) as u8
            } else {
                ((count as f64 / self.min_wallets as f64) * 50.0) as u8
            }
        } else {
            0
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.7,
            format!("Whale consensus: {score}/100"),
        ))
    }
}

// ─── Accumulation Signal ─────────────────────────────────────────────────────

/// Detects accumulation patterns: holder growth vs. price stability.
pub struct AccumulationSignal {
    name: String,
    /// Token address -> (holder_count, price, timestamp) history
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
        let score = if let Some(hist) = self.history.get(&token.address) {
            if hist.len() < 2 {
                return Ok(Signal::new(
                    token.address.clone(),
                    token.chain,
                    &self.name,
                    0,
                    0.0,
                    "Insufficient history for accumulation signal".to_string(),
                ));
            }
            let first = hist.front().unwrap();
            let last = hist.back().unwrap();
            let holder_growth = (last.0 as f64 - first.0 as f64) / first.0.max(1) as f64;
            let price_change = (last.1 - first.1) / first.1.max(0.0001);
            // High holder growth + low price change = accumulation
            if holder_growth > 0.2 && price_change.abs() < 0.1 {
                80
            } else if holder_growth > 0.1 && price_change.abs() < 0.2 {
                60
            } else {
                20
            }
        } else {
            0
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.6,
            format!("Accumulation score: {score}/100"),
        ))
    }
}

// ─── Launch Momentum Signal ──────────────────────────────────────────────────

/// Detects momentum in newly launched tokens (volume and holder rate).
pub struct LaunchMomentumSignal {
    name: String,
    /// Token address -> (volume, holder_count, timestamp)
    snapshots: Arc<DashMap<String, VecDeque<(f64, u64, DateTime<Utc>)>>>,
    max_snapshots: usize,
    #[allow(dead_code)]
    chain: Chain,
}

impl LaunchMomentumSignal {
    pub fn new(chain: Chain, max_snapshots: usize) -> Self {
        Self {
            name: "launch_momentum".to_string(),
            snapshots: Arc::new(DashMap::new()),
            max_snapshots,
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
        let score = if let Some(snaps) = self.snapshots.get(&token.address) {
            if snaps.len() < 2 {
                return Ok(Signal::new(
                    token.address.clone(),
                    token.chain,
                    &self.name,
                    0,
                    0.0,
                    "Insufficient data for launch momentum".to_string(),
                ));
            }
            let first = snaps.front().unwrap();
            let last = snaps.back().unwrap();
            let volume_rate = last.0 / first.0.max(1.0);
            let holder_rate = last.1 as f64 / first.1.max(1) as f64;
            let composite = (volume_rate + holder_rate) / 2.0;
            (composite.min(2.0) * 50.0) as u8
        } else {
            0
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.5,
            format!("Launch momentum: {score}/100"),
        ))
    }
}

// ─── Volume Spike Signal ─────────────────────────────────────────────────────

/// Detects when current volume exceeds 3x the rolling average.
pub struct VolumeSpikeSignal {
    name: String,
    threshold: f64,
    /// Token address -> (volume, timestamp)
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
}

impl Strategy for VolumeSpikeSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        let score = if let Some(vols) = self.volumes.get(&token.address) {
            if vols.len() < 3 {
                return Ok(Signal::new(
                    token.address.clone(),
                    token.chain,
                    &self.name,
                    0,
                    0.0,
                    "Insufficient volume history".to_string(),
                ));
            }
            let avg: f64 = vols.iter().map(|v| v.0).sum::<f64>() / vols.len() as f64;
            let current = vols.back().map(|v| v.0).unwrap_or(0.0);
            if avg > 0.0 && current / avg >= self.threshold {
                80
            } else if avg > 0.0 && current / avg >= self.threshold * 0.66 {
                50
            } else {
                10
            }
        } else {
            0
        };

        Ok(Signal::new(
            token.address.clone(),
            token.chain,
            &self.name,
            score,
            0.65,
            format!("Volume spike ({}x threshold): {score}/100", self.threshold),
        ))
    }
}

// ─── Social Signal ───────────────────────────────────────────────────────────

/// Placeholder for Twitter/social media signal integration.
pub struct SocialSignal {
    name: String,
    #[allow(dead_code)]
    chain: Chain,
}

impl SocialSignal {
    pub fn new(chain: Chain) -> Self {
        Self {
            name: "social".to_string(),
            chain,
        }
    }
}

impl Strategy for SocialSignal {
    fn name(&self) -> &str {
        &self.name
    }

    async fn evaluate(&self, _token: &TokenInfo) -> Result<Signal> {
        todo!("Integrate Twitter API for social sentiment analysis")
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
}

impl Strategy for StrategyKind {
    fn name(&self) -> &str {
        match self {
            StrategyKind::WhaleConsensus(s) => s.name(),
            StrategyKind::Accumulation(s) => s.name(),
            StrategyKind::LaunchMomentum(s) => s.name(),
            StrategyKind::VolumeSpike(s) => s.name(),
            StrategyKind::Social(s) => s.name(),
        }
    }

    async fn evaluate(&self, token: &TokenInfo) -> Result<Signal> {
        match self {
            StrategyKind::WhaleConsensus(s) => s.evaluate(token).await,
            StrategyKind::Accumulation(s) => s.evaluate(token).await,
            StrategyKind::LaunchMomentum(s) => s.evaluate(token).await,
            StrategyKind::VolumeSpike(s) => s.evaluate(token).await,
            StrategyKind::Social(s) => s.evaluate(token).await,
        }
    }
}

// ─── Confluence Scorer ───────────────────────────────────────────────────────

/// Aggregates multiple strategy signals into a composite confluence score.
pub struct ConfluenceScorer {
    strategies: Vec<StrategyKind>,
    weights: Vec<f64>,
    threshold: f64,
}

impl ConfluenceScorer {
    /// Create a new confluence scorer with strategies and their weights.
    pub fn new(threshold: f64) -> Self {
        Self {
            strategies: Vec::new(),
            weights: Vec::new(),
            threshold,
        }
    }

    /// Add a strategy with its weight.
    pub fn add_strategy(&mut self, strategy: StrategyKind, weight: f64) {
        self.weights.push(weight);
        self.strategies.push(strategy);
    }

    /// Evaluate all strategies for a token and produce a composite score.
    pub async fn score(&self, token: &TokenInfo) -> Result<ConfluenceResult> {
        let mut signals = Vec::new();
        let mut weighted_sum = 0.0;
        let mut weight_total = 0.0;

        for (i, strategy) in self.strategies.iter().enumerate() {
            let weight = self.weights.get(i).copied().unwrap_or(1.0);
            match strategy.evaluate(token).await {
                Ok(signal) => {
                    weighted_sum += signal.score as f64 * weight;
                    weight_total += weight;
                    signals.push(signal);
                }
                Err(e) => {
                    tracing::warn!(
                        strategy = strategy.name(),
                        error = %e,
                        "Strategy evaluation failed"
                    );
                }
            }
        }

        let composite = if weight_total > 0.0 {
            weighted_sum / weight_total
        } else {
            0.0
        };

        Ok(ConfluenceResult {
            composite_score: composite as u8,
            signals,
            passed: composite >= self.threshold,
        })
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
