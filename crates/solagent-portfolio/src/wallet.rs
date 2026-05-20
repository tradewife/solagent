//! Wallet registry, scoring, and dev-wallet blacklist.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use solagent_core::Chain;
use sqlx::SqlitePool;

// ─── Wallet Label ────────────────────────────────────────────────────────────

/// Classification labels for known wallets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalletLabel {
    SmartMoney,
    Sniper,
    Whale,
    Insider,
    MevBot,
    Dev,
    Unknown,
}

impl WalletLabel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SmartMoney => "smart_money",
            Self::Sniper => "sniper",
            Self::Whale => "whale",
            Self::Insider => "insider",
            Self::MevBot => "mev_bot",
            Self::Dev => "dev",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "smart_money" => Self::SmartMoney,
            "sniper" => Self::Sniper,
            "whale" => Self::Whale,
            "insider" => Self::Insider,
            "mev_bot" => Self::MevBot,
            "dev" => Self::Dev,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for WalletLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── Wallet Entry ────────────────────────────────────────────────────────────

/// A tracked wallet stored in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletEntry {
    pub address: String,
    pub chain: Chain,
    pub label: WalletLabel,
    pub source: String,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub total_trades: u64,
    pub avg_hold_time_mins: f64,
    /// Composite score (0-100) calculated by `recompute_score`.
    pub score: f64,
    pub tags: Vec<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ─── Dev Blacklist Entry ─────────────────────────────────────────────────────

/// A blacklisted developer wallet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevBlacklistEntry {
    pub address: String,
    pub chain: Chain,
    pub reason: String,
    pub source: String,
    pub token_address: Option<String>,
    pub added_at: DateTime<Utc>,
}

// ─── Wallet Registry ─────────────────────────────────────────────────────────

/// SQLite-backed wallet registry with scoring and blacklist.
pub struct WalletRegistry {
    pool: SqlitePool,
}

impl WalletRegistry {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get a reference to the underlying SQLite pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    // ── CRUD ──────────────────────────────────────────────────────────────

    /// Insert or update a wallet. Recomputes the composite score before saving.
    pub async fn upsert_wallet(&self, w: &WalletEntry) -> Result<()> {
        let score = Self::compute_score(w);
        let tags_json = serde_json::to_string(&w.tags)?;
        let label = w.label.as_str();
        let chain = w.chain.to_string();
        let now = Utc::now().to_rfc3339();
        let last_seen = w.last_seen_at.map(|t| t.to_rfc3339());

        sqlx::query(
            r#"INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl,
               total_trades, avg_hold_time_mins, score, tags, last_seen_at, created_at, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
               ON CONFLICT(address, chain) DO UPDATE SET
                 label=excluded.label, source=excluded.source,
                 win_rate=excluded.win_rate, total_pnl=excluded.total_pnl,
                 total_trades=excluded.total_trades, avg_hold_time_mins=excluded.avg_hold_time_mins,
                 score=excluded.score, tags=excluded.tags, last_seen_at=excluded.last_seen_at,
                 updated_at=excluded.updated_at"#,
        )
        .bind(&w.address)
        .bind(&chain)
        .bind(label)
        .bind(&w.source)
        .bind(w.win_rate)
        .bind(w.total_pnl)
        .bind(w.total_trades as i64)
        .bind(w.avg_hold_time_mins)
        .bind(score)
        .bind(&tags_json)
        .bind(&last_seen)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get a single wallet by address and chain.
    pub async fn get_wallet(&self, address: &str, chain: Chain) -> Result<Option<WalletEntry>> {
        let row = sqlx::query_as::<_, WalletRow>(
            "SELECT * FROM wallets WHERE address = ?1 AND chain = ?2",
        )
        .bind(address)
        .bind(chain.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into_entry()))
    }

    /// List wallets, optionally filtered by label and/or chain.
    pub async fn list_wallets(
        &self,
        label_filter: Option<WalletLabel>,
        chain_filter: Option<Chain>,
        limit: u32,
    ) -> Result<Vec<WalletEntry>> {
        let mut sql = String::from("SELECT * FROM wallets WHERE 1=1");
        if label_filter.is_some() {
            sql.push_str(" AND label = ? ");
        }
        if chain_filter.is_some() {
            sql.push_str(" AND chain = ? ");
        }
        sql.push_str(" ORDER BY score DESC LIMIT ?");

        let mut q = sqlx::query_as::<_, WalletRow>(&sql);
        if let Some(l) = label_filter {
            q = q.bind(l.as_str());
        }
        if let Some(c) = chain_filter {
            q = q.bind(c.to_string());
        }
        q = q.bind(limit);

        let rows = q.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| r.into_entry()).collect())
    }

    /// Get the top N wallets by composite score for a chain.
    pub async fn get_top_wallets(&self, chain: Chain, limit: u32) -> Result<Vec<WalletEntry>> {
        let rows = sqlx::query_as::<_, WalletRow>(
            "SELECT * FROM wallets WHERE chain = ?1 ORDER BY score DESC LIMIT ?2",
        )
        .bind(chain.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into_entry()).collect())
    }

    /// Remove a wallet from the registry.
    pub async fn remove_wallet(&self, address: &str, chain: Chain) -> Result<bool> {
        let result = sqlx::query("DELETE FROM wallets WHERE address = ?1 AND chain = ?2")
            .bind(address)
            .bind(chain.to_string())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Bulk-seed wallets from an iterator. Skips duplicates (upserts).
    pub async fn seed_wallets(&self, wallets: &[WalletEntry]) -> Result<u64> {
        let mut count = 0u64;
        for w in wallets {
            self.upsert_wallet(w).await?;
            count += 1;
        }
        tracing::info!(count, "Seeded wallets into registry");
        Ok(count)
    }

    /// Update the last-seen timestamp for a wallet.
    pub async fn touch_wallet(&self, address: &str, chain: Chain) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE wallets SET last_seen_at = ?1, updated_at = ?1 WHERE address = ?2 AND chain = ?3",
        )
        .bind(&now)
        .bind(address)
        .bind(chain.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get the count of registered wallets.
    pub async fn count(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM wallets")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }

    // ── Scoring ───────────────────────────────────────────────────────────

    /// Compute a composite score (0-100) for a wallet.
    ///
    /// Formula from spec:
    ///   score = win_rate * 0.3 + pnl_30d_norm * 0.3 + consistency * 0.2 + recency * 0.2
    ///
    /// win_rate: 0-1 scale, multiplied by 100 for scoring
    /// pnl_norm: total_pnl clamped to 0-100_000, normalized to 0-100
    /// consistency: based on total_trades (more = more consistent)
    /// recency: based on last_seen_at recency
    pub fn compute_score(w: &WalletEntry) -> f64 {
        let win_rate_component = w.win_rate * 30.0;

        let pnl_norm = (w.total_pnl / 1000.0).clamp(0.0, 100.0);
        let pnl_component = pnl_norm * 0.3;

        let consistency = (w.total_trades as f64 / 100.0).min(1.0) * 20.0;

        let recency = match w.last_seen_at {
            Some(t) => {
                let hours = (Utc::now() - t).num_hours() as f64;
                // Full 20 points if seen in last hour, decaying over 72 hours
                (20.0 * (1.0 - (hours / 72.0).min(1.0))).max(0.0)
            }
            None => 0.0,
        };

        (win_rate_component + pnl_component + consistency + recency).clamp(0.0, 100.0)
    }

    /// Recompute and persist the score for a single wallet.
    pub async fn recompute_score(&self, address: &str, chain: Chain) -> Result<f64> {
        let Some(w) = self.get_wallet(address, chain).await? else {
            anyhow::bail!("Wallet not found: {address}");
        };
        let score = Self::compute_score(&w);
        sqlx::query("UPDATE wallets SET score = ?1, updated_at = ?2 WHERE address = ?3 AND chain = ?4")
            .bind(score)
            .bind(Utc::now().to_rfc3339())
            .bind(address)
            .bind(chain.to_string())
            .execute(&self.pool)
            .await?;
        Ok(score)
    }

    // ── Zerion Enrichment ─────────────────────────────────────────────────

    /// Refresh wallet scores from Zerion PnL data.
    ///
    /// For each wallet in the top N by score, fetches PnL from Zerion,
    /// updates win_rate and total_pnl, then recomputes the composite score.
    /// Returns the number of wallets refreshed.
    pub async fn refresh_scores_from_zerion(
        &self,
        zerion: &solagent_data::ZerionClient,
        top_n: usize,
    ) -> Result<usize> {
        if !zerion.is_enabled() {
            return Ok(0);
        }

        let wallets = self.list_wallets(None, Some(Chain::Solana), top_n as u32).await?;
        let mut refreshed = 0usize;

        for w in &wallets {
            match zerion.get_pnl(&w.address, Some("solana")).await {
                Ok(pnl) => {
                    // Derive win_rate from ROI: positive ROI → higher win rate.
                    // Zerion gives us relative_total_gain_percentage which is a
                    // proxy for overall profitability. Map it to a 0-1 win_rate.
                    let roi_pct = pnl.relative_total_gain_pct;
                    let new_win_rate = if roi_pct > 50.0 {
                        0.9
                    } else if roi_pct > 20.0 {
                        0.75
                    } else if roi_pct > 0.0 {
                        0.6
                    } else if roi_pct > -20.0 {
                        0.4
                    } else if roi_pct > -50.0 {
                        0.25
                    } else {
                        0.1
                    };

                    // Use total_gain as the PnL, or realized_gain for a more
                    // conservative view. We use total_gain (includes unrealized).
                    let new_pnl = pnl.total_gain;

                    sqlx::query(
                        "UPDATE wallets SET win_rate = ?1, total_pnl = ?2, updated_at = ?3 \
                         WHERE address = ?4 AND chain = 'solana'"
                    )
                    .bind(new_win_rate)
                    .bind(new_pnl)
                    .bind(Utc::now().to_rfc3339())
                    .bind(&w.address)
                    .execute(&self.pool)
                    .await?;

                    // Recompute and persist the composite score.
                    let _ = self.recompute_score(&w.address, Chain::Solana).await;
                    refreshed += 1;

                    tracing::debug!(
                        address = %&w.address[..w.address.len().min(12)],
                        roi = format!("{:.1}%", roi_pct),
                        pnl = format!("${:.0}", new_pnl),
                        win_rate = format!("{:.0}%", new_win_rate * 100.0),
                        "Refreshed wallet score from Zerion"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        address = %&w.address[..w.address.len().min(12)],
                        error = %e,
                        "Zerion PnL fetch failed, skipping"
                    );
                }
            }
        }

        if refreshed > 0 {
            tracing::info!(
                refreshed,
                total = wallets.len(),
                "Zerion wallet score refresh complete"
            );
        }

        Ok(refreshed)
    }

    /// Fetch current token positions for a wallet from Zerion.
    /// Returns a list of (token_symbol, token_ca, value_usd, quantity) tuples.
    pub async fn get_zerion_positions(
        &self,
        zerion: &solagent_data::ZerionClient,
        address: &str,
    ) -> Result<Vec<(String, Option<String>, f64, f64)>> {
        if !zerion.is_enabled() {
            return Ok(Vec::new());
        }

        let positions = zerion.get_positions(address).await?;
        Ok(positions
            .into_iter()
            .filter(|p| p.value_usd > 1.0) // Filter dust
            .map(|p| {
                let ca = p.implementation
                    .as_ref()
                    .and_then(|imp| imp.split(':').next_back().map(|s| s.to_string()));
                (p.token_symbol, ca, p.value_usd, p.quantity)
            })
            .collect())
    }

    // ── Dev Blacklist ─────────────────────────────────────────────────────

    /// Add a dev wallet to the blacklist.
    pub async fn blacklist_dev(&self, entry: &DevBlacklistEntry) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO dev_blacklist (address, chain, reason, source, token_address, added_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(address, chain) DO UPDATE SET
                 reason=excluded.reason, source=excluded.source,
                 token_address=excluded.token_address"#,
        )
        .bind(&entry.address)
        .bind(entry.chain.to_string())
        .bind(&entry.reason)
        .bind(&entry.source)
        .bind(&entry.token_address)
        .bind(entry.added_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Check if a dev wallet is blacklisted.
    pub async fn is_blacklisted(&self, address: &str, chain: Chain) -> Result<bool> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM dev_blacklist WHERE address = ?1 AND chain = ?2",
        )
        .bind(address)
        .bind(chain.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0 > 0)
    }

    /// List all blacklisted devs, optionally filtered by chain.
    pub async fn list_blacklisted(&self, chain: Option<Chain>) -> Result<Vec<DevBlacklistEntry>> {
        let rows = match chain {
            Some(c) => {
                sqlx::query_as::<_, DevBlacklistRow>(
                    "SELECT * FROM dev_blacklist WHERE chain = ?1 ORDER BY added_at DESC",
                )
                .bind(c.to_string())
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, DevBlacklistRow>(
                    "SELECT * FROM dev_blacklist ORDER BY added_at DESC",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.into_iter().map(|r| r.into_entry()).collect())
    }

    /// Get the count of blacklisted devs.
    pub async fn blacklist_count(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dev_blacklist")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }
}

// ─── Internal Row Types ──────────────────────────────────────────────────────

/// Row mapping for the `wallets` table.
#[derive(Debug, sqlx::FromRow)]
struct WalletRow {
    address: String,
    chain: String,
    label: String,
    source: String,
    win_rate: f64,
    total_pnl: f64,
    total_trades: i64,
    avg_hold_time_mins: f64,
    score: f64,
    tags: String,
    last_seen_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl WalletRow {
    fn into_entry(self) -> WalletEntry {
        let tags: Vec<String> = serde_json::from_str(&self.tags).unwrap_or_default();
        WalletEntry {
            address: self.address,
            chain: match self.chain.as_str() {
                "base" => Chain::Base,
                _ => Chain::Solana,
            },
            label: WalletLabel::from_str_lossy(&self.label),
            source: self.source,
            win_rate: self.win_rate,
            total_pnl: self.total_pnl,
            total_trades: self.total_trades as u64,
            avg_hold_time_mins: self.avg_hold_time_mins,
            score: self.score,
            tags,
            last_seen_at: self
                .last_seen_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.to_utc()),
            created_at: DateTime::parse_from_rfc3339(&self.created_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
            updated_at: DateTime::parse_from_rfc3339(&self.updated_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}

/// Row mapping for the `dev_blacklist` table.
#[derive(Debug, sqlx::FromRow)]
struct DevBlacklistRow {
    address: String,
    chain: String,
    reason: String,
    source: String,
    token_address: Option<String>,
    added_at: String,
}

impl DevBlacklistRow {
    fn into_entry(self) -> DevBlacklistEntry {
        DevBlacklistEntry {
            address: self.address,
            chain: match self.chain.as_str() {
                "base" => Chain::Base,
                _ => Chain::Solana,
            },
            reason: self.reason,
            source: self.source,
            token_address: self.token_address,
            added_at: DateTime::parse_from_rfc3339(&self.added_at)
                .map(|dt| dt.to_utc())
                .unwrap_or_default(),
        }
    }
}
