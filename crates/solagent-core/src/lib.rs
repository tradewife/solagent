//! # solagent-core
//!
//! Core types, configuration, error handling, and event bus for the SolAgent system.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::broadcast;
use uuid::Uuid;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Top-level configuration loaded from TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub agent: AgentConfig,
    pub chains: ChainsConfig,
    pub strategies: StrategiesConfig,
    pub risk: RiskConfig,
    pub data: DataConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub name: String,
    pub poll_interval_secs: u64,
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainsConfig {
    pub solana: SolanaConfig,
    pub base: Option<BaseConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaConfig {
    pub rpc_urls: Vec<String>,
    pub ws_url: String,
    pub helius_api_key: String,
    pub private_key_bs58: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseConfig {
    pub rpc_url: String,
    pub private_key_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategiesConfig {
    pub active_strategies: Vec<String>,
    pub confluence_threshold: f64,
    pub min_signal_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_position_size_usd: f64,
    pub max_portfolio_risk_pct: f64,
    pub max_daily_loss_usd: f64,
    pub max_drawdown_pct: f64,
    pub max_open_positions: usize,
    pub default_stop_loss_pct: f64,
    pub default_take_profit_pct: f64,
    pub trailing_stop_pct: f64,
    pub cooldown_secs: u64,
    pub safety_score_threshold: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
    pub dexscreener_api_key: Option<String>,
    pub birdeye_api_key: Option<String>,
    pub jupiter_api_url: String,
    pub dexscreener_base_url: String,
    pub birdeye_base_url: String,
}

impl Config {
    /// Load configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> anyhow::Result<Self> {
        toml::from_str(toml_str).map_err(|e| anyhow::anyhow!("Config parse error: {e}"))
    }

    /// Load configuration from a TOML file.
    pub async fn load_from_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let contents = tokio::fs::read_to_string(path).await?;
        Self::from_toml(&contents)
    }
}

// ─── Chain ───────────────────────────────────────────────────────────────────

/// Supported blockchain chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Chain {
    Solana,
    Base,
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Chain::Solana => write!(f, "solana"),
            Chain::Base => write!(f, "base"),
        }
    }
}

// ─── Error ───────────────────────────────────────────────────────────────────

/// Unified error type for the SolAgent system.
#[derive(Debug, thiserror::Error)]
pub enum SolAgentError {
    #[error("API error: {0}")]
    Api(String),

    #[error("Chain error: {0}")]
    Chain(String),

    #[error("Safety check failed: {0}")]
    Safety(String),

    #[error("Risk limit exceeded: {0}")]
    Risk(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),
}

// ─── Core Data Types ─────────────────────────────────────────────────────────

/// Token metadata and on-chain information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub address: String,
    pub chain: Chain,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
    pub price_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub volume_24h: Option<f64>,
    pub holder_count: Option<u64>,
    pub created_at: Option<DateTime<Utc>>,
    pub pair_address: Option<String>,
    pub lp_locked: Option<bool>,
    pub mint_authority_revoked: Option<bool>,
    pub freeze_authority_revoked: Option<bool>,
}

/// Wallet (whale / smart money) information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletInfo {
    pub address: String,
    pub chain: Chain,
    pub label: Option<String>,
    pub pnl_30d: Option<f64>,
    pub win_rate: Option<f64>,
    pub total_trades: Option<u64>,
    pub avg_holding_time_secs: Option<u64>,
    pub tags: Vec<String>,
}

/// Signal produced by a strategy evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub id: Uuid,
    pub token_address: String,
    pub chain: Chain,
    pub strategy: String,
    pub score: u8,       // 0-100
    pub confidence: f64, // 0.0-1.0
    pub reason: String,
    pub timestamp: DateTime<Utc>,
}

impl Signal {
    pub fn new(token_address: String, chain: Chain, strategy: &str, score: u8, confidence: f64, reason: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            token_address,
            chain,
            strategy: strategy.to_string(),
            score,
            confidence,
            reason,
            timestamp: Utc::now(),
        }
    }
}

/// Trade record (buy or sell).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: Uuid,
    pub token_address: String,
    pub chain: Chain,
    pub side: TradeSide,
    pub size_usd: f64,
    pub token_amount: f64,
    pub price: f64,
    pub tx_signature: Option<String>,
    pub slippage_bps: Option<u64>,
    pub executed_at: DateTime<Utc>,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Open position tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: Uuid,
    pub token_address: String,
    pub chain: Chain,
    pub entry_price: f64,
    pub current_price: f64,
    pub size_usd: f64,
    pub token_amount: f64,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub unrealized_pnl: f64,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ─── Event Bus ───────────────────────────────────────────────────────────────

/// Events flowing through the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    TokenDiscovered {
        token: TokenInfo,
        timestamp: DateTime<Utc>,
    },
    WalletBuy {
        wallet: String,
        token_address: String,
        chain: Chain,
        amount_usd: f64,
        timestamp: DateTime<Utc>,
    },
    WalletSell {
        wallet: String,
        token_address: String,
        chain: Chain,
        amount_usd: f64,
        timestamp: DateTime<Utc>,
    },
    SignalFired {
        signal: Signal,
        timestamp: DateTime<Utc>,
    },
    TradeExecuted {
        trade: Trade,
        timestamp: DateTime<Utc>,
    },
    TradeClosed {
        position_id: Uuid,
        pnl: f64,
        reason: String,
        timestamp: DateTime<Utc>,
    },
    CircuitBreaker {
        message: String,
        timestamp: DateTime<Utc>,
    },
}

/// Typed event bus using tokio broadcast channels.
pub struct EventBus {
    sender: broadcast::Sender<Event>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event to all subscribers.
    pub fn publish(&self, event: Event) {
        // Ignore send errors (no active subscribers is fine).
        let _ = self.sender.send(event);
    }

    /// Subscribe to events. Returns a receiver that will get all future events.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Get a clone of the sender for passing to producers.
    pub fn sender(&self) -> broadcast::Sender<Event> {
        self.sender.clone()
    }
}

impl Clone for EventBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

// ─── Re-exports ──────────────────────────────────────────────────────────────

pub use anyhow;
pub use chrono;
pub use serde;
pub use serde_json;
pub use tokio;
pub use tracing;
pub use uuid;
