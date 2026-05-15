//! Database pool management and migration execution.

use anyhow::Result;
use sqlx::SqlitePool;

/// Complete migration SQL for all tables.
pub const MIGRATION_SQL: &str = r#"
-- Known wallets (smart money, whales, snipers, etc.)
CREATE TABLE IF NOT EXISTS wallets (
    address TEXT NOT NULL,
    chain TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT 'unknown',
    source TEXT NOT NULL DEFAULT 'manual',
    win_rate REAL NOT NULL DEFAULT 0.0,
    total_pnl REAL NOT NULL DEFAULT 0.0,
    total_trades INTEGER NOT NULL DEFAULT 0,
    avg_hold_time_mins REAL NOT NULL DEFAULT 0.0,
    score REAL NOT NULL DEFAULT 0.0,
    tags TEXT NOT NULL DEFAULT '[]',
    last_seen_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (address, chain)
);

-- Dev wallet blacklist (known rug-pullers / malicious deployers)
CREATE TABLE IF NOT EXISTS dev_blacklist (
    address TEXT NOT NULL,
    chain TEXT NOT NULL DEFAULT 'solana',
    reason TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual',
    token_address TEXT,
    added_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (address, chain)
);

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

-- Evaluation results persisted for every evaluate() call
CREATE TABLE IF NOT EXISTS evaluations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT NOT NULL,
    confluence_score INTEGER NOT NULL,
    safety_score INTEGER NOT NULL,
    signal_scores TEXT NOT NULL DEFAULT '{}',
    passed INTEGER NOT NULL DEFAULT 0,
    reasoning TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_wallets_label ON wallets(label);
CREATE INDEX IF NOT EXISTS idx_wallets_score ON wallets(score DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_chain ON wallets(chain);
CREATE INDEX IF NOT EXISTS idx_dev_blacklist_chain ON dev_blacklist(chain);
CREATE INDEX IF NOT EXISTS idx_positions_token ON positions(token_address);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_address);
CREATE INDEX IF NOT EXISTS idx_trades_executed ON trades(executed_at);
CREATE INDEX IF NOT EXISTS idx_evaluations_token ON evaluations(token_address);
CREATE INDEX IF NOT EXISTS idx_evaluations_created ON evaluations(created_at);

-- Curated Twitter accounts extracted from DexScreener social links.
-- These are polled periodically for token-specific mentions.
CREATE TABLE IF NOT EXISTS twitter_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    handle TEXT NOT NULL UNIQUE,
    source_token TEXT,
    followers_count INTEGER,
    last_polled_at TEXT,
    mention_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_twitter_accounts_handle ON twitter_accounts(handle);
CREATE INDEX IF NOT EXISTS idx_twitter_accounts_last_polled ON twitter_accounts(last_polled_at);
"#;

/// Open a SQLite pool and run all migrations.
pub async fn init_pool(db_path: &str) -> Result<SqlitePool> {
    let pool = SqlitePool::connect(db_path).await?;
    sqlx::query(MIGRATION_SQL).execute(&pool).await?;
    tracing::info!(db_path, "Database initialized");
    Ok(pool)
}
