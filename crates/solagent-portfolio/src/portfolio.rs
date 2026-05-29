//! Portfolio manager with SQLite-backed position tracking and PnL calculation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use solagent_core::Chain;
use sqlx::SqlitePool;
use uuid::Uuid;

// ─── Twitter Account ─────────────────────────────────────────────────────────

/// A curated Twitter account extracted from DexScreener social links.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwitterAccount {
    pub id: i64,
    pub handle: String,
    /// Token address that first referenced this account.
    pub source_token: Option<String>,
    pub followers_count: Option<i64>,
    pub last_polled_at: Option<DateTime<Utc>>,
    pub mention_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

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

// ─── Performance Snapshot ────────────────────────────────────────────────────

/// A snapshot of key performance metrics recorded by the auto-tuner
/// after each successful tuning cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub timestamp: String,
    pub win_rate: f64,
    pub total_trades: usize,
    pub total_pnl: f64,
    pub avg_trade_return: Option<f64>,
    pub signal_weights: String,
    pub confluence_threshold: f64,
    pub position_size: f64,
    pub open_positions: usize,
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

    /// Get a reference to the underlying SQLite pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Open a new position.
    #[allow(clippy::too_many_arguments)]
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
        let positions: Vec<PositionRow> = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE status = 'open'",
        )
        .fetch_all(&self.pool)
        .await?;

        let unrealized: f64 = positions.iter().map(|p| p.unrealized_pnl).sum();

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
        let total_trades = closed.len() as u64;

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
    #[allow(clippy::too_many_arguments)]
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

    // ─── Win Rate ───────────────────────────────────────────────────────

    /// Calculate historical win rate from closed positions.
    ///
    /// Queries closed positions and counts those with positive unrealized_pnl
    /// (which represents realized PnL at close time) as wins.
    ///
    /// Returns a value between 0.0 and 1.0, or 0.5 if no closed positions exist
    /// (neutral starting point — neither optimistic nor pessimistic).
    pub async fn get_win_rate(&self) -> Result<f64> {
        let (wins, total): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(CASE WHEN unrealized_pnl > 0 THEN 1 ELSE 0 END), 0), COUNT(*) FROM positions WHERE status = 'closed'",
        )
        .fetch_one(&self.pool)
        .await?;

        if total == 0 {
            // No trade history — return neutral 0.5 to avoid penalizing or
            // over-rewarding a fresh wallet with no track record.
            return Ok(0.5);
        }

        Ok(wins as f64 / total as f64)
    }

    // ─── Performance Metrics Snapshots ────────────────────────────────────

    /// Insert a performance snapshot into the performance_metrics table.
    pub async fn insert_performance_snapshot(
        &self,
        metrics: &PerformanceSnapshot,
    ) -> Result<i64> {
        let result = sqlx::query(
            r#"INSERT INTO performance_metrics
               (timestamp, win_rate, total_trades, total_pnl, avg_trade_return,
                signal_weights, confluence_threshold, position_size, open_positions)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
        )
        .bind(&metrics.timestamp)
        .bind(metrics.win_rate)
        .bind(metrics.total_trades as i64)
        .bind(metrics.total_pnl)
        .bind(metrics.avg_trade_return)
        .bind(&metrics.signal_weights)
        .bind(metrics.confluence_threshold)
        .bind(metrics.position_size)
        .bind(metrics.open_positions as i64)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Get the most recent performance snapshot, if any exist.
    pub async fn get_latest_metrics(&self) -> Result<Option<PerformanceSnapshot>> {
        let row = sqlx::query_as::<_, PerformanceMetricsRow>(
            "SELECT * FROM performance_metrics ORDER BY id DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into_snapshot()))
    }

    /// Get all evaluation records (useful for signal-report aggregation).
    pub async fn get_all_evaluations(&self) -> Result<Vec<EvaluationRecord>> {
        let rows = sqlx::query_as::<_, EvaluationRow>(
            "SELECT * FROM evaluations ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_record()).collect())
    }

    // ─── Agent State Persistence ────────────────────────────────────────

    /// Set an agent state key-value pair. Used by the running agent to persist
    /// runtime state (circuit breaker, effective threshold, last scan time)
    /// so the `solagent status` CLI can report it even when the agent is not running.
    pub async fn set_agent_state(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO agent_state (key, value, updated_at) VALUES (?, ?, datetime('now'))
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get an agent state value by key. Returns None if the key has never been set.
    pub async fn get_agent_state(&self, key: &str) -> Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM agent_state WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(v,)| v))
    }

    // ─── Twitter Account Management ─────────────────────────────────────

    /// Upsert a Twitter account into the twitter_accounts table.
    /// If the handle already exists, updates source_token and followers_count.
    /// Returns the row ID.
    pub async fn upsert_twitter_account(
        &self,
        handle: &str,
        source_token: Option<&str>,
        followers_count: Option<i64>,
    ) -> Result<i64> {
        let now_str = Utc::now().to_rfc3339();
        let result = sqlx::query(
            r#"INSERT INTO twitter_accounts (handle, source_token, followers_count, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?4)
               ON CONFLICT(handle) DO UPDATE SET
                 source_token = COALESCE(excluded.source_token, twitter_accounts.source_token),
                 followers_count = COALESCE(excluded.followers_count, twitter_accounts.followers_count),
                 updated_at = excluded.updated_at"#,
        )
        .bind(handle)
        .bind(source_token)
        .bind(followers_count)
        .bind(&now_str)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Get all Twitter accounts, optionally filtered by whether they've been polled.
    /// Returns handles sorted by last_polled_at ASC (oldest-first) for polling priority.
    pub async fn get_twitter_accounts(&self, limit: i64) -> Result<Vec<TwitterAccount>> {
        let rows = sqlx::query_as::<_, TwitterAccountRow>(
            "SELECT * FROM twitter_accounts ORDER BY last_polled_at ASC NULLS FIRST LIMIT ?1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_account()).collect())
    }

    /// Get handles that need polling (never polled or polled more than N minutes ago).
    pub async fn get_stale_twitter_accounts(&self, max_age_minutes: i64, limit: i64) -> Result<Vec<TwitterAccount>> {
        let cutoff = Utc::now() - chrono::Duration::minutes(max_age_minutes);
        let cutoff_str = cutoff.to_rfc3339();

        let rows = sqlx::query_as::<_, TwitterAccountRow>(
            r#"SELECT * FROM twitter_accounts
               WHERE last_polled_at IS NULL OR last_polled_at < ?1
               ORDER BY last_polled_at ASC NULLS FIRST
               LIMIT ?2"#,
        )
        .bind(&cutoff_str)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_account()).collect())
    }

    /// Mark a Twitter account as polled at the current time.
    pub async fn mark_twitter_account_polled(&self, handle: &str) -> Result<()> {
        let now_str = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE twitter_accounts SET last_polled_at = ?1, updated_at = ?1 WHERE handle = ?2",
        )
        .bind(&now_str)
        .bind(handle)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Increment the mention count for a Twitter account.
    pub async fn increment_twitter_mention_count(&self, handle: &str, count: i64) -> Result<()> {
        let now_str = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE twitter_accounts SET mention_count = mention_count + ?1, updated_at = ?2 WHERE handle = ?3",
        )
        .bind(count)
        .bind(&now_str)
        .bind(handle)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get the count of Twitter accounts in the table.
    pub async fn get_twitter_account_count(&self) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM twitter_accounts")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    /// Get an open position by token address.
    pub async fn get_position_by_token(&self, token_address: &str) -> Result<Option<PortfolioPosition>> {
        let row = sqlx::query_as::<_, PositionRow>(
            "SELECT * FROM positions WHERE token_address = ?1 AND status = 'open' LIMIT 1",
        )
        .bind(token_address)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.into_position()))
    }

    /// Update token_amount and size_usd for an existing position (e.g. after decimals fix).
    pub async fn update_position_amounts(
        &self,
        position_id: &str,
        token_amount: f64,
        size_usd: f64,
    ) -> Result<()> {
        let now_str = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE positions SET token_amount = ?1, size_usd = ?2, updated_at = ?3 WHERE id = ?4",
        )
        .bind(token_amount)
        .bind(size_usd)
        .bind(&now_str)
        .bind(position_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ─── Phantom Position Cleanup ───────────────────────────────────────

    /// Remove phantom positions caused by wrong token decimals or data corruption.
    ///
    /// A phantom position has a plausible token_amount (thousands of tokens)
    /// but an implausibly large size_usd (e.g. $12,312 for what should be $1.33).
    /// These inflate the portfolio peak value and cause the circuit breaker to
    /// HALT due to >90% drawdown when the position is eventually closed.
    ///
    /// Strategy:
    /// - Find all positions where size_usd > $100 (clearly wrong for a sub-$1 wallet).
    /// - Also find all positions where the DRPY token (or similar known-phantom address)
    ///   has both a huge and a small version of the same position.
    /// - Delete those phantom positions AND their associated trades.
    ///
    /// Returns the number of phantom position records cleaned up.
    pub async fn cleanup_phantom_positions(&self) -> Result<usize> {
        // Phase 1: Find phantom positions by implausibly large size_usd.
        // We use $100 as the threshold — the agent is configured for ~$15 max
        // position size, so anything over $100 is clearly wrong.
        let phantom_rows: Vec<PhantomPosition> = sqlx::query_as(
            "SELECT id, token_address, size_usd, token_amount FROM positions WHERE size_usd > 100.0 AND status = 'closed'",
        )
        .fetch_all(&self.pool)
        .await?;

        // Phase 2: Also find DRPY token specifically (known phantom).
        let drpy_rows: Vec<PhantomPosition> = sqlx::query_as(
            "SELECT id, token_address, size_usd, token_amount FROM positions WHERE token_address = 'EPRZgmvU4aTQ4UaC4bywgNvxJ5YmhuKqM1bx3gw4DRPY' AND size_usd > 100.0",
        )
        .fetch_all(&self.pool)
        .await?;

        // Collect all phantom position IDs.
        let phantom_ids: std::collections::HashSet<String> = phantom_rows
            .iter()
            .chain(drpy_rows.iter())
            .map(|p| p.id.clone())
            .collect();

        if phantom_ids.is_empty() {
            tracing::debug!("cleanup_phantom_positions: no phantom records found");
            return Ok(0);
        }

        // Also find the stale open DRPY position (379187fc-...).
        let stale_open: Vec<PhantomPosition> = sqlx::query_as(
            "SELECT id, token_address, size_usd, token_amount FROM positions WHERE token_address = 'EPRZgmvU4aTQ4UaC4bywgNvxJ5YmhuKqM1bx3gw4DRPY' AND status = 'open'",
        )
        .fetch_all(&self.pool)
        .await?;

        let all_ids: Vec<String> = phantom_ids
            .iter()
            .chain(stale_open.iter().map(|p| &p.id))
            .cloned()
            .collect();

        let total = all_ids.len();

        // Delete associated trades first (FK constraint).
        // Use a dynamic query since SQLite doesn't support arrays in IN clauses natively.
        for id in &all_ids {
            sqlx::query("DELETE FROM trades WHERE position_id = ?1")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }

        // Delete the phantom positions.
        for id in &all_ids {
            let result = sqlx::query("DELETE FROM positions WHERE id = ?1")
                .bind(id)
                .execute(&self.pool)
                .await?;

            if result.rows_affected() > 0 {
                tracing::warn!(
                    position_id = %id,
                    "Cleaned up phantom position (size_usd > $100 or stale DRPY)"
                );
            }
        }

        if total > 0 {
            tracing::warn!(
                cleaned = total,
                "Phantom position cleanup complete: removed {total} records with inflated size_usd"
            );
        }

        Ok(total)
    }

    // ─── On-Chain Reconciliation ─────────────────────────────────────────

    /// Reconcile positions by scanning the wallet's on-chain token accounts.
    ///
    /// For each token held on-chain (via SolanaProvider::get_all_token_balances),
    /// check if a matching open position exists in the DB. If any token is held
    /// on-chain but not recorded as an open position, create a position record.
    ///
    /// Returns the number of positions reconciled (newly created).
    pub async fn reconcile_positions(
        &self,
        on_chain_balances: &[(String, u64, u8)],
        
        dex: &solagent_data::DexScreenerClient,
    ) -> Result<usize> {
        let open_positions = self.get_open_positions().await?;
        let recorded_tokens: std::collections::HashSet<String> = open_positions
            .iter()
            .map(|p| p.token_address.clone())
            .collect();

        let mut reconciled = 0usize;

        for (mint, raw_amount, decimals) in on_chain_balances {
            // Skip SOL — only handle SPL tokens.
            if mint == "So11111111111111111111111111111111111111112" {
                continue;
            }

            let token_amount = *raw_amount as f64 / 10f64.powi(*decimals as i32);

            // Try to get current price from DexScreener.
            let (current_price, _market_cap) = match dex.get_token_info(mint).await {
                Ok(Some(pair)) => {
                    let price = pair.price_usd
                        .and_then(|p| p.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    (price, pair.market_cap)
                }
                _ => (0.0, None),
            };

            let size_usd = token_amount * current_price;

            // Update existing positions that may have stale token_amount/size_usd
            // (e.g. from before the per-token decimals fix).
            if recorded_tokens.contains(mint) {
                if let Ok(Some(existing)) = self.get_position_by_token(mint).await {
                    let amount_changed = (existing.token_amount - token_amount).abs() > token_amount * 0.01;
                    if amount_changed {
                        tracing::info!(
                            mint = %mint,
                            old_amount = existing.token_amount,
                            new_amount = token_amount,
                            old_size_usd = existing.size_usd,
                            new_size_usd = size_usd,
                            "Updating existing position with corrected on-chain balance"
                        );
                        self.update_position_amounts(&existing.id, token_amount, size_usd).await?;
                        reconciled += 1;
                    }
                    continue;
                }
                continue;
            }

            tracing::info!(
                mint = %mint,
                token_amount,
                current_price,
                size_usd,
                "Reconciling on-chain position not in DB"
            );

            // Create position record with conservative defaults.
            let sl = if current_price > 0.0 {
                Some(current_price * 0.85)
            } else {
                None
            };

            let profile = if let Some(mc) = _market_cap {
                solagent_risk::RiskManager::select_exit_profile(
                    Some(mc),
                    None,
                    50,
                )
            } else {
                solagent_risk::ExitProfile::swing()
            };

            let tp = profile.take_profit_pct.map(|pct| current_price * (1.0 + pct / 100.0));

            let _ = self.open_position(
                mint,
                Chain::Solana,
                current_price,
                size_usd,
                token_amount,
                sl,
                tp,
            ).await?;

            reconciled += 1;
        }

        if reconciled > 0 {
            tracing::info!(
                reconciled,
                "Reconciliation complete: created position records for on-chain tokens"
            );
        } else {
            tracing::debug!("Reconciliation: all on-chain positions already recorded");
        }

        Ok(reconciled)
    }
}

// ─── Internal Row Types ──────────────────────────────────────────────────────

/// Minimal row for phantom position detection.
#[derive(Debug, sqlx::FromRow)]
struct PhantomPosition {
    id: String,
    #[allow(dead_code)]
    token_address: String,
    #[allow(dead_code)]
    size_usd: f64,
    #[allow(dead_code)]
    token_amount: f64,
}

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

#[derive(Debug, sqlx::FromRow)]
struct TwitterAccountRow {
    id: i64,
    handle: String,
    source_token: Option<String>,
    followers_count: Option<i64>,
    last_polled_at: Option<String>,
    mention_count: i64,
    created_at: String,
    updated_at: String,
}

impl TwitterAccountRow {
    fn into_account(self) -> TwitterAccount {
        TwitterAccount {
            id: self.id,
            handle: self.handle,
            source_token: self.source_token,
            followers_count: self.followers_count,
            last_polled_at: self.last_polled_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.to_utc()),
            mention_count: self.mention_count,
            created_at: DateTime::parse_from_rfc3339(&self.created_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct PerformanceMetricsRow {
    #[allow(dead_code)]
    id: i64,
    timestamp: String,
    win_rate: f64,
    total_trades: i64,
    total_pnl: f64,
    avg_trade_return: Option<f64>,
    signal_weights: Option<String>,
    confluence_threshold: Option<f64>,
    position_size: Option<f64>,
    open_positions: i64,
}

impl PerformanceMetricsRow {
    fn into_snapshot(self) -> PerformanceSnapshot {
        PerformanceSnapshot {
            timestamp: self.timestamp,
            win_rate: self.win_rate,
            total_trades: self.total_trades as usize,
            total_pnl: self.total_pnl,
            avg_trade_return: self.avg_trade_return,
            signal_weights: self.signal_weights.unwrap_or_else(|| "{}".to_string()),
            confluence_threshold: self.confluence_threshold.unwrap_or(0.0),
            position_size: self.position_size.unwrap_or(0.0),
            open_positions: self.open_positions as usize,
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

    // ─── Win Rate Tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_win_rate_no_positions_returns_neutral() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        let wr = pm.get_win_rate().await.unwrap();
        assert!((wr - 0.5).abs() < f64::EPSILON, "No closed positions should return neutral 0.5, got {wr}");
    }

    #[tokio::test]
    async fn test_get_win_rate_all_wins() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Open and immediately close 3 positions, all profitable.
        for i in 0..3 {
            let pos = pm.open_position(
                &format!("token_{i}"),
                Chain::Solana,
                1.0,  // entry_price
                100.0, // size_usd
                100.0, // token_amount
                None,
                None,
            ).await.unwrap();

            // Close at 2x → profit = 100 * (2-1)/1 = +100 (positive unrealized_pnl).
            let closed = pm.close_position(&pos.id, 2.0).await.unwrap();
            assert!(closed.unrealized_pnl > 0.0);
        }

        let wr = pm.get_win_rate().await.unwrap();
        assert!((wr - 1.0).abs() < f64::EPSILON, "All wins should give 1.0, got {wr}");
    }

    #[tokio::test]
    async fn test_get_win_rate_mixed() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Two wins, one loss.
        for (i, close_price) in [(0, 2.0), (1, 0.5), (2, 3.0)] {
            let pos = pm.open_position(
                &format!("token_{i}"),
                Chain::Solana,
                1.0,  // entry_price
                100.0, // size_usd
                100.0, // token_amount
                None,
                None,
            ).await.unwrap();
            pm.close_position(&pos.id, close_price).await.unwrap();
        }

        let wr = pm.get_win_rate().await.unwrap();
        assert!((wr - 2.0 / 3.0).abs() < 0.01, "2/3 wins should give ~0.667, got {wr}");
    }

    #[tokio::test]
    async fn test_get_win_rate_all_losses() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // All positions closed at a loss.
        for i in 0..2 {
            let pos = pm.open_position(
                &format!("token_{i}"),
                Chain::Solana,
                2.0,  // entry_price
                100.0,
                100.0,
                None,
                None,
            ).await.unwrap();
            pm.close_position(&pos.id, 1.0).await.unwrap();
        }

        let wr = pm.get_win_rate().await.unwrap();
        assert!((wr - 0.0).abs() < f64::EPSILON, "All losses should give 0.0, got {wr}");
    }

    // ─── Phantom Position Cleanup Tests ─────────────────────────────────

    #[tokio::test]
    async fn test_cleanup_phantom_positions_removes_big_size_usd() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool.clone());

        // Insert a position with inflated size_usd (phantom).
        let pos_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO positions (id, token_address, chain, entry_price, current_price, size_usd, token_amount, unrealized_pnl, status, opened_at, updated_at) VALUES (?1, ?2, 'solana', 1.33, 0.0001203, ?3, 9257.0, -12311.0, 'closed', '2026-05-19T00:00:00Z', '2026-05-19T00:00:00Z')",
        )
        .bind(&pos_id)
        .bind("EPRZgmvU4aTQ4UaC4bywgNvxJ5YmhuKqM1bx3gw4DRPY")
        .bind(12312.0_f64) // inflated size_usd
        .execute(&pool)
        .await
        .unwrap();

        // Insert associated trade.
        sqlx::query(
            "INSERT INTO trades (id, position_id, token_address, chain, side, size_usd, token_amount, price, executed_at) VALUES (?1, ?2, ?3, 'solana', 'sell', 1.11, 9257.0, 0.0001203, '2026-05-19T00:00:00Z')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&pos_id)
        .bind("EPRZgmvU4aTQ4UaC4bywgNvxJ5YmhuKqM1bx3gw4DRPY")
        .execute(&pool)
        .await
        .unwrap();

        let cleaned = pm.cleanup_phantom_positions().await.unwrap();
        assert!(cleaned >= 1, "Should have cleaned at least 1 phantom position");

        // Verify position was deleted.
        let pos_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM positions WHERE id = ?1")
            .bind(&pos_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(pos_count.0, 0, "Phantom position should be deleted");

        // Verify trade was deleted.
        let trade_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM trades WHERE position_id = ?1")
            .bind(&pos_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(trade_count.0, 0, "Phantom trade should be deleted");
    }

    #[tokio::test]
    async fn test_cleanup_phantom_positions_keeps_normal() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool.clone());

        // Open and close a normal position with $1 size_usd.
        let pos = pm.open_position(
            "normal_token",
            Chain::Solana,
            1.0,
            1.0,
            1000000.0,
            None,
            None,
        ).await.unwrap();
        pm.close_position(&pos.id, 0.9).await.unwrap();

        let cleaned = pm.cleanup_phantom_positions().await.unwrap();
        assert_eq!(cleaned, 0, "Normal positions should not be cleaned up");

        // Verify the normal position still exists.
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM positions WHERE token_address = 'normal_token'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1, "Normal position should still exist");
    }

    // ─── Agent State Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_agent_state_set_and_get() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Initially no state.
        let val = pm.get_agent_state("circuit_breaker").await.unwrap();
        assert!(val.is_none(), "Should return None for unset key");

        // Set a value.
        pm.set_agent_state("circuit_breaker", "NORMAL").await.unwrap();

        // Get it back.
        let val = pm.get_agent_state("circuit_breaker").await.unwrap();
        assert_eq!(val.as_deref(), Some("NORMAL"));

        // Update the value.
        pm.set_agent_state("circuit_breaker", "HALTED").await.unwrap();
        let val = pm.get_agent_state("circuit_breaker").await.unwrap();
        assert_eq!(val.as_deref(), Some("HALTED"));
    }

    #[tokio::test]
    async fn test_agent_state_multiple_keys() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        pm.set_agent_state("circuit_breaker", "WARNING").await.unwrap();
        pm.set_agent_state("effective_threshold", "30.0").await.unwrap();
        pm.set_agent_state("last_scan", "2026-05-29T12:00:00Z").await.unwrap();

        assert_eq!(pm.get_agent_state("circuit_breaker").await.unwrap().as_deref(), Some("WARNING"));
        assert_eq!(pm.get_agent_state("effective_threshold").await.unwrap().as_deref(), Some("30.0"));
        assert_eq!(pm.get_agent_state("last_scan").await.unwrap().as_deref(), Some("2026-05-29T12:00:00Z"));
        assert!(pm.get_agent_state("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_agent_state_upsert() {
        let pool = test_pool().await;
        let pm = PortfolioManager::new(pool);

        // Insert.
        pm.set_agent_state("key1", "value1").await.unwrap();
        assert_eq!(pm.get_agent_state("key1").await.unwrap().as_deref(), Some("value1"));

        // Upsert (update).
        pm.set_agent_state("key1", "value2").await.unwrap();
        assert_eq!(pm.get_agent_state("key1").await.unwrap().as_deref(), Some("value2"));
    }
}
