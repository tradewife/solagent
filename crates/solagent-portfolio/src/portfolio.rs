//! Portfolio manager with SQLite-backed position tracking and PnL calculation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use solagent_core::Chain;
use sqlx::SqlitePool;
use uuid::Uuid;

// ─── Position Status ─────────────────────────────────────────────────────────

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

// ─── Portfolio Position ──────────────────────────────────────────────────────

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

// ─── PnL Summary ─────────────────────────────────────────────────────────────

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

// ─── Daily Snapshot ──────────────────────────────────────────────────────────

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
    pool: SqlitePool,
}

impl PortfolioManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Open a new position.
    pub async fn open_position(
        &self,
        token_address: &str,
        chain: Chain,
        entry_price: f64,
        size_usd: f64,
        token_amount: f64,
        stop_loss: Option<f64>,
        take_profit: Option<f64>,
    ) -> Result<PortfolioPosition> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let pnl = 0.0;

        sqlx::query(
            r#"INSERT INTO positions (id, token_address, chain, entry_price, current_price,
               size_usd, token_amount, stop_loss, take_profit, unrealized_pnl, status,
               opened_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'open', ?11, ?12)"#,
        )
        .bind(&id)
        .bind(token_address)
        .bind(chain.to_string())
        .bind(entry_price)
        .bind(entry_price)
        .bind(size_usd)
        .bind(token_amount)
        .bind(stop_loss)
        .bind(take_profit)
        .bind(pnl)
        .bind(&now_str)
        .bind(&now_str)
        .execute(&self.pool)
        .await?;

        Ok(PortfolioPosition {
            id,
            token_address: token_address.to_string(),
            chain,
            entry_price,
            current_price: entry_price,
            size_usd,
            token_amount,
            stop_loss,
            take_profit,
            unrealized_pnl: pnl,
            status: PositionStatus::Open,
            opened_at: now,
            closed_at: None,
            updated_at: now,
        })
    }

    /// Close an existing position.
    pub async fn close_position(
        &self,
        position_id: &str,
        close_price: f64,
    ) -> Result<PortfolioPosition> {
        let pos = self.get_position(position_id).await?
            .ok_or_else(|| anyhow::anyhow!("Position not found: {position_id}"))?;

        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let pnl = (close_price - pos.entry_price) / pos.entry_price * pos.size_usd;

        sqlx::query(
            r#"UPDATE positions SET status='closed', current_price=?1, unrealized_pnl=?2,
               closed_at=?3, updated_at=?3 WHERE id=?4"#,
        )
        .bind(close_price)
        .bind(pnl)
        .bind(&now_str)
        .bind(position_id)
        .execute(&self.pool)
        .await?;

        Ok(PortfolioPosition {
            current_price: close_price,
            unrealized_pnl: pnl,
            status: PositionStatus::Closed,
            closed_at: Some(now),
            updated_at: now,
            ..pos
        })
    }

    /// Get a single position by ID.
    pub async fn get_position(&self, position_id: &str) -> Result<Option<PortfolioPosition>> {
        let row = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE id = ?1",
        )
        .bind(position_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into_position()))
    }

    /// Get all open positions.
    pub async fn get_open_positions(&self) -> Result<Vec<PortfolioPosition>> {
        let rows = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.into_position()).collect())
    }

    /// Update the current price of an open position.
    pub async fn update_price(&self, position_id: &str, current_price: f64) -> Result<()> {
        let pos = self.get_position(position_id).await?
            .ok_or_else(|| anyhow::anyhow!("Position not found: {position_id}"))?;

        let pnl = (current_price - pos.entry_price) / pos.entry_price * pos.size_usd;
        let now_str = Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE positions SET current_price = ?1, unrealized_pnl = ?2, updated_at = ?3 WHERE id = ?4",
        )
        .bind(current_price)
        .bind(pnl)
        .bind(&now_str)
        .bind(position_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get total realized + unrealized PnL.
    pub async fn get_pnl(&self) -> Result<PnlSummary> {
        let trades: Vec<TradeRow> = sqlx::query_as::<_, TradeRow>(
            "SELECT * FROM trades ORDER BY executed_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        let positions: Vec<PositionRow> = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE status = 'open'",
        )
        .fetch_all(&self.pool)
        .await?;

        let unrealized: f64 = positions.iter().map(|p| p.unrealized_pnl).sum();
        let total_trades = trades.len() as u64;

        let mut wins = 0u64;
        let mut losses = 0u64;
        let mut total_size = 0.0;
        let mut largest_win = 0.0f64;
        let mut largest_loss = 0.0f64;

        // Approximate realized PnL from closed positions
        let closed: Vec<PositionRow> = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE status = 'closed'",
        )
        .fetch_all(&self.pool)
        .await?;
        let realized: f64 = closed.iter().map(|p| p.unrealized_pnl).sum();

        for pos in &closed {
            let pnl = pos.unrealized_pnl;
            if pnl >= 0.0 {
                wins += 1;
                largest_win = largest_win.max(pnl);
            } else {
                losses += 1;
                largest_loss = largest_loss.min(pnl);
            }
            total_size += pos.size_usd;
        }

        let win_rate = if total_trades > 0 {
            wins as f64 / total_trades as f64
        } else {
            0.0
        };

        Ok(PnlSummary {
            realized_pnl: realized,
            unrealized_pnl: unrealized,
            total_pnl: realized + unrealized,
            total_trades,
            winning_trades: wins,
            losing_trades: losses,
            win_rate,
            avg_trade_size_usd: if total_trades > 0 { total_size / total_trades as f64 } else { 0.0 },
            largest_win_usd: largest_win,
            largest_loss_usd: largest_loss,
        })
    }

    /// Get total portfolio value (open positions at current prices + cash).
    pub async fn get_portfolio_value(&self, cash_balance_usd: f64) -> Result<f64> {
        let positions = self.get_open_positions().await?;
        let position_value: f64 = positions
            .iter()
            .map(|p| p.size_usd * (p.current_price / p.entry_price))
            .sum();
        Ok(cash_balance_usd + position_value)
    }

    /// Record a daily snapshot.
    pub async fn record_snapshot(&self, snapshot: &DailySnapshot) -> Result<()> {
        let now_str = Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO snapshots (date, portfolio_value_usd, unrealized_pnl, realized_pnl,
               open_positions, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(date) DO UPDATE SET
                 portfolio_value_usd=excluded.portfolio_value_usd,
                 unrealized_pnl=excluded.unrealized_pnl,
                 realized_pnl=excluded.realized_pnl,
                 open_positions=excluded.open_positions"#,
        )
        .bind(&snapshot.date)
        .bind(snapshot.portfolio_value_usd)
        .bind(snapshot.unrealized_pnl)
        .bind(snapshot.realized_pnl)
        .bind(snapshot.open_positions)
        .bind(&now_str)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get recent snapshots for drawdown calculation.
    pub async fn get_snapshots(&self, limit: usize) -> Result<Vec<DailySnapshot>> {
        let rows = sqlx::query_as::<_, SnapshotRow>(
            "SELECT * FROM snapshots ORDER BY date DESC LIMIT ?1",
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.into_snapshot()).collect())
    }

    /// Record a trade execution.
    pub async fn record_trade(
        &self,
        position_id: Option<&str>,
        token_address: &str,
        chain: Chain,
        side: &str,
        size_usd: f64,
        token_amount: f64,
        price: f64,
        tx_signature: Option<&str>,
        slippage_bps: Option<i64>,
        latency_ms: Option<i64>,
    ) -> Result<()> {
        let id = Uuid::new_v4().to_string();
        let now_str = Utc::now().to_rfc3339();
        sqlx::query(
            r#"INSERT INTO trades (id, position_id, token_address, chain, side, size_usd,
               token_amount, price, tx_signature, slippage_bps, executed_at, latency_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"#,
        )
        .bind(&id)
        .bind(position_id)
        .bind(token_address)
        .bind(chain.to_string())
        .bind(side)
        .bind(size_usd)
        .bind(token_amount)
        .bind(price)
        .bind(tx_signature)
        .bind(slippage_bps)
        .bind(&now_str)
        .bind(latency_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// ─── Internal Row Types ──────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow)]
struct PositionRow {
    id: String,
    token_address: String,
    chain: String,
    entry_price: f64,
    current_price: f64,
    size_usd: f64,
    token_amount: f64,
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    unrealized_pnl: f64,
    status: String,
    opened_at: String,
    closed_at: Option<String>,
    updated_at: String,
}

impl PositionRow {
    fn into_position(self) -> PortfolioPosition {
        PortfolioPosition {
            id: self.id,
            token_address: self.token_address,
            chain: match self.chain.as_str() {
                "base" => Chain::Base,
                _ => Chain::Solana,
            },
            entry_price: self.entry_price,
            current_price: self.current_price,
            size_usd: self.size_usd,
            token_amount: self.token_amount,
            stop_loss: self.stop_loss,
            take_profit: self.take_profit,
            unrealized_pnl: self.unrealized_pnl,
            status: match self.status.as_str() {
                "closed" => PositionStatus::Closed,
                _ => PositionStatus::Open,
            },
            opened_at: DateTime::parse_from_rfc3339(&self.opened_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
            closed_at: self
                .closed_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.to_utc()),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct TradeRow {
    id: String,
    position_id: Option<String>,
    token_address: String,
    chain: String,
    side: String,
    size_usd: f64,
    token_amount: f64,
    price: f64,
    tx_signature: Option<String>,
    slippage_bps: Option<i64>,
    executed_at: String,
    latency_ms: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct SnapshotRow {
    id: i64,
    date: String,
    portfolio_value_usd: f64,
    unrealized_pnl: f64,
    realized_pnl: f64,
    open_positions: i64,
    created_at: String,
}

impl SnapshotRow {
    fn into_snapshot(self) -> DailySnapshot {
        DailySnapshot {
            id: self.id,
            date: self.date,
            portfolio_value_usd: self.portfolio_value_usd,
            unrealized_pnl: self.unrealized_pnl,
            realized_pnl: self.realized_pnl,
            open_positions: self.open_positions,
            created_at: DateTime::parse_from_rfc3339(&self.created_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}
