//! # solagent-portfolio
//!
//! Portfolio tracking with SQLite backend, position management, and PnL calculation.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use solagent_core::chrono::{DateTime, Utc};
use solagent_core::Chain;

// ─── Migration SQL ───────────────────────────────────────────────────────────

/// SQL for creating the database schema.
pub const MIGRATION_SQL: &str = r#"
-- Open and closed positions
CREATE TABLE IF NOT EXISTS positions (
    id TEXT PRIMARY KEY,
    token_address TEXT NOT NULL,
    chain TEXT NOT NULL,
    entry_price REAL NOT NULL,
    current_price REAL NOT NULL,
    size_usd REAL NOT NULL,
    token_amount REAL NOT NULL,
    stop_loss REAL,
    take_profit REAL,
    unrealized_pnl REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    updated_at TEXT NOT NULL
);

-- Trade history
CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    position_id TEXT,
    token_address TEXT NOT NULL,
    chain TEXT NOT NULL,
    side TEXT NOT NULL,
    size_usd REAL NOT NULL,
    token_amount REAL NOT NULL,
    price REAL NOT NULL,
    tx_signature TEXT,
    slippage_bps INTEGER,
    executed_at TEXT NOT NULL,
    latency_ms INTEGER,
    FOREIGN KEY (position_id) REFERENCES positions(id)
);

-- Daily portfolio snapshots for drawdown calculation
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    portfolio_value_usd REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    open_positions INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_positions_token ON positions(token_address);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);
CREATE INDEX IF NOT EXISTS idx_trades_executed ON trades(executed_at);
"#;

// ─── Portfolio Position ──────────────────────────────────────────────────────

/// Extended position data as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioPosition {
    pub id: String,
    pub token_address: String,
    pub chain: Chain,
    pub entry_price: f64,
    pub current_price: f64,
    pub size_usd: f64,
    pub token_amount: f64,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub unrealized_pnl: f64,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    Open,
    Closed,
}

impl std::fmt::Display for PositionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionStatus::Open => write!(f, "open"),
            PositionStatus::Closed => write!(f, "closed"),
        }
    }
}

/// Daily portfolio snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySnapshot {
    pub id: i64,
    pub date: String,
    pub portfolio_value_usd: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub open_positions: i64,
    pub created_at: DateTime<Utc>,
}

// ─── Portfolio Manager ───────────────────────────────────────────────────────

/// Manages portfolio state with a SQLite backend.
pub struct PortfolioManager {
    #[allow(dead_code)]
    db_path: String,
}

impl PortfolioManager {
    /// Create a new portfolio manager with the given SQLite database path.
    pub fn new(db_path: &str) -> Self {
        Self {
            db_path: db_path.to_string(),
        }
    }

    /// Initialize the database (run migrations).
    pub async fn init(&self) -> Result<()> {
        todo!("sqlx::SqlitePool::connect + execute MIGRATION_SQL")
    }

    /// Open a new position.
    pub async fn open_position(
        &self,
        _token_address: &str,
        _chain: Chain,
        _entry_price: f64,
        _size_usd: f64,
        _token_amount: f64,
        _stop_loss: Option<f64>,
        _take_profit: Option<f64>,
    ) -> Result<PortfolioPosition> {
        let _now = Utc::now();
        todo!("INSERT INTO positions VALUES (...)")
    }

    /// Close an existing position.
    pub async fn close_position(
        &self,
        _position_id: &str,
        _close_price: f64,
    ) -> Result<PortfolioPosition> {
        todo!("UPDATE positions SET status='closed', closed_at=now, current_price=close_price WHERE id=position_id")
    }

    /// Get all open positions.
    pub async fn get_open_positions(&self) -> Result<Vec<PortfolioPosition>> {
        todo!("SELECT * FROM positions WHERE status='open'")
    }

    /// Get total realized + unrealized PnL.
    pub async fn get_pnl(&self) -> Result<PnlSummary> {
        todo!("Aggregate PnL from positions and trades")
    }

    /// Get total portfolio value (open positions at current prices + cash).
    pub async fn get_portfolio_value(&self, cash_balance_usd: f64) -> Result<f64> {
        let positions = self.get_open_positions().await?;
        let position_value: f64 = positions.iter().map(|p| p.size_usd * (p.current_price / p.entry_price)).sum();
        Ok(cash_balance_usd + position_value)
    }

    /// Record a daily snapshot.
    pub async fn record_snapshot(&self, _snapshot: DailySnapshot) -> Result<()> {
        todo!("INSERT OR REPLACE INTO snapshots VALUES (...)")
    }

    /// Get recent snapshots for drawdown calculation.
    pub async fn get_snapshots(&self, _limit: usize) -> Result<Vec<DailySnapshot>> {
        todo!("SELECT * FROM snapshots ORDER BY date DESC LIMIT $limit")
    }
}

/// PnL summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlSummary {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_pnl: f64,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub win_rate: f64,
    pub avg_trade_size_usd: f64,
    pub largest_win_usd: f64,
    pub largest_loss_usd: f64,
}
