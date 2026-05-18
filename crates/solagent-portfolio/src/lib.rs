//! # solagent-portfolio
//!
//! Portfolio tracking, wallet registry, and PnL calculation with SQLite backend.

pub mod db;
pub mod portfolio;
pub mod wallet;

pub use db::{init_pool, MIGRATION_SQL};
pub use portfolio::{
    DailySnapshot, EvalStats, EvaluationRecord, PerformanceSnapshot, PnlSummary,
    PortfolioManager, PortfolioPosition, PositionStatus, TwitterAccount,
};
pub use wallet::{
    DevBlacklistEntry, WalletEntry, WalletLabel, WalletRegistry,
};
