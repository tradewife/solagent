//! Portfolio manager with SQLite-backed position tracking and PnL calculation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use solagent_core::Chain;
use sqlx::SqlitePool;
use uuid::Uuid;

// ─── Evaluation Record ───────────────────────────────────────────────────────

/// A persisted evaluation result for a token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationRecord {
    pub id: i64,
    pub token_address: String,
    pub confluence_score: u8,
    pub safety_score: u8,
    /// JSON object: {"whale_consensus": N, "accumulation": N, "launch_momentum": N, "volume_spike": N, "social": N}
    pub signal_scores: String,
    pub passed: bool,
    pub reasoning: String,
    pub created_at: DateTime<Utc>,
}

/// Statistics over all persisted evaluations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalStats {
    pub total_evaluations: i64,
    pub passed_evaluations: i64,
    pub failed_evaluations: i64,
    pub pass_rate: f64,
    pub avg_confluence_score: f64,
    pub avg_safety_score: f64,
    /// Top-scoring tokens by confluence (token_address, avg_confluence, eval_count).
    pub top_tokens: Vec<(String, f64, i64)>,
}

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

    // ─── Evaluation Persistence ──────────────────────────────────────────

    /// Insert an evaluation result into the evaluations table.
    /// `signal_scores` should be a JSON object like {"whale_consensus": 30, ...}.
    pub async fn insert_evaluation(
        &self,
        token_address: &str,
        confluence_score: u8,
        safety_score: u8,
        signal_scores: &str,
        passed: bool,
        reasoning: &str,
    ) -> Result<i64> {
        let now_str = Utc::now().to_rfc3339();
        let passed_int = if passed { 1 } else { 0 };

        let result = sqlx::query(
            r#"INSERT INTO evaluations (token_address, confluence_score, safety_score,
               signal_scores, passed, reasoning, created_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
        )
        .bind(token_address)
        .bind(confluence_score as i32)
        .bind(safety_score as i32)
        .bind(signal_scores)
        .bind(passed_int)
        .bind(reasoning)
        .bind(&now_str)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Query evaluations by token address, ordered by most recent first.
    pub async fn get_evaluations_by_token(
        &self,
        token_address: &str,
        limit: i64,
    ) -> Result<Vec<EvaluationRecord>> {
        let rows = sqlx::query_as::<_, EvaluationRow>(
            "SELECT * FROM evaluations WHERE token_address = ?1 ORDER BY created_at DESC LIMIT ?2",
        )
        .bind(token_address)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_record()).collect())
    }

    /// Get aggregate statistics over all evaluations.
    pub async fn get_eval_stats(&self) -> Result<EvalStats> {
        let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM evaluations")
            .fetch_one(&self.pool)
            .await?;

        let passed: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM evaluations WHERE passed = 1")
            .fetch_one(&self.pool)
            .await?;

        // Use COALESCE so empty tables return 0.0 instead of NULL.
        let (avg_confluence, avg_safety): (f64, f64) = sqlx::query_as(
            "SELECT COALESCE(AVG(confluence_score), 0.0), COALESCE(AVG(safety_score), 0.0) FROM evaluations",
        )
        .fetch_one(&self.pool)
        .await?;

        // Top 5 tokens by average confluence score (min 1 evaluation).
        let top_rows: Vec<(String, f64, i64)> = sqlx::query_as(
            r#"SELECT token_address, AVG(confluence_score) as avg_c, COUNT(*) as cnt
               FROM evaluations
               GROUP BY token_address
               ORDER BY avg_c DESC
               LIMIT 5"#,
        )
        .fetch_all(&self.pool)
        .await?;

        let total_evals = total.0;
        let passed_evals = passed.0;
        let failed_evals = total_evals - passed_evals;
        let pass_rate = if total_evals > 0 {
            passed_evals as f64 / total_evals as f64
        } else {
            0.0
        };

        Ok(EvalStats {
            total_evaluations: total_evals,
            passed_evaluations: passed_evals,
            failed_evaluations: failed_evals,
            pass_rate,
            avg_confluence_score: avg_confluence,
            avg_safety_score: avg_safety,
            top_tokens: top_rows,
        })
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

#[derive(Debug, sqlx::FromRow)]
struct EvaluationRow {
    id: i64,
    token_address: String,
    confluence_score: i32,
    safety_score: i32,
    signal_scores: String,
    passed: i32,
    reasoning: String,
    created_at: String,
}

impl EvaluationRow {
    fn into_record(self) -> EvaluationRecord {
        EvaluationRecord {
            id: self.id,
            token_address: self.token_address,
            confluence_score: self.confluence_score.clamp(0, 255) as u8,
            safety_score: self.safety_score.clamp(0, 255) as u8,
            signal_scores: self.signal_scores,
            passed: self.passed != 0,
            reasoning: self.reasoning,
            created_at: DateTime::parse_from_rfc3339(&self.created_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create an in-memory pool with migrations.
    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(crate::db::MIGRATION_SQL)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    async fn test_evaluations_table_created_on_startup() {
        // If this doesn't panic, the migration (including evaluations table) worked.
        let pool = test_pool().await;

        // Verify the evaluations table exists by querying its schema.
        let row: (String,) = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='evaluations'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.0, "evaluations", "evaluations table should exist after migration");
    }

    #[tokio::test]
    async fn test_insert_and_query_evaluation() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        let signal_scores = r#"{"whale_consensus":30,"accumulation":0,"launch_momentum":0,"volume_spike":80,"social":0}"#;

        let id = pm.insert_evaluation(
            "TokenABC123",
            45,
            72,
            signal_scores,
            false,
            "confluence(45<65)",
        ).await.unwrap();

        assert!(id > 0, "Insert should return a positive row ID");

        let evals = pm.get_evaluations_by_token("TokenABC123", 10).await.unwrap();
        assert_eq!(evals.len(), 1);
        assert_eq!(evals[0].token_address, "TokenABC123");
        assert_eq!(evals[0].confluence_score, 45);
        assert_eq!(evals[0].safety_score, 72);
        assert!(!evals[0].passed);
        assert_eq!(evals[0].reasoning, "confluence(45<65)");

        // Verify signal_scores is valid JSON.
        let parsed: serde_json::Value = serde_json::from_str(&evals[0].signal_scores).unwrap();
        assert_eq!(parsed["whale_consensus"], 30);
        assert_eq!(parsed["volume_spike"], 80);
    }

    #[tokio::test]
    async fn test_query_evaluations_empty() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        let evals = pm.get_evaluations_by_token("nonexistent", 10).await.unwrap();
        assert!(evals.is_empty());
    }

    #[tokio::test]
    async fn test_eval_stats_empty() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        let stats = pm.get_eval_stats().await.unwrap();
        assert_eq!(stats.total_evaluations, 0);
        assert_eq!(stats.passed_evaluations, 0);
        assert_eq!(stats.failed_evaluations, 0);
        assert!((stats.pass_rate - 0.0).abs() < f64::EPSILON);
        assert!((stats.avg_confluence_score - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_eval_stats_with_data() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Insert 5 evaluations: 2 passed, 3 failed.
        let signal_scores = "{}";
        pm.insert_evaluation("token_A", 70, 80, signal_scores, true, "passed").await.unwrap();
        pm.insert_evaluation("token_A", 65, 75, signal_scores, true, "passed").await.unwrap();
        pm.insert_evaluation("token_B", 30, 50, signal_scores, false, "failed").await.unwrap();
        pm.insert_evaluation("token_B", 25, 55, signal_scores, false, "failed").await.unwrap();
        pm.insert_evaluation("token_C", 20, 40, signal_scores, false, "failed").await.unwrap();

        let stats = pm.get_eval_stats().await.unwrap();
        assert_eq!(stats.total_evaluations, 5);
        assert_eq!(stats.passed_evaluations, 2);
        assert_eq!(stats.failed_evaluations, 3);
        assert!((stats.pass_rate - 0.4).abs() < 0.01, "pass_rate should be 0.4, got {}", stats.pass_rate);

        // Average confluence: (70+65+30+25+20)/5 = 42.0
        assert!((stats.avg_confluence_score - 42.0).abs() < 0.1,
            "avg_confluence_score should be 42.0, got {}", stats.avg_confluence_score);

        // Average safety: (80+75+50+55+40)/5 = 60.0
        assert!((stats.avg_safety_score - 60.0).abs() < 0.1,
            "avg_safety_score should be 60.0, got {}", stats.avg_safety_score);

        // Top token should be token_A (avg 67.5).
        assert!(!stats.top_tokens.is_empty(), "Should have top tokens");
        assert_eq!(stats.top_tokens[0].0, "token_A");
        assert!((stats.top_tokens[0].1 - 67.5).abs() < 0.1);
        assert_eq!(stats.top_tokens[0].2, 2); // 2 evaluations
    }

    #[tokio::test]
    async fn test_evaluations_multiple_same_token() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Insert 3 evaluations for the same token.
        for i in 0..3 {
            pm.insert_evaluation(
                "same_token",
                10 * (i + 1),
                20 * (i + 1),
                "{}",
                i == 2,
                &format!("eval {i}"),
            ).await.unwrap();
        }

        let evals = pm.get_evaluations_by_token("same_token", 10).await.unwrap();
        assert_eq!(evals.len(), 3);

        // Should be ordered by most recent first.
        assert_eq!(evals[0].confluence_score, 30);
        assert_eq!(evals[1].confluence_score, 20);
        assert_eq!(evals[2].confluence_score, 10);
    }
}
