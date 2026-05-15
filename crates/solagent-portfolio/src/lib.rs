//! # solagent-portfolio
//!
//! Portfolio tracking, wallet registry, and PnL calculation with SQLite backend.

pub mod db;
pub mod portfolio;
pub mod wallet;

pub use db::{init_pool, MIGRATION_SQL};
pub use portfolio::{
    DailySnapshot, PnlSummary, PortfolioManager, PortfolioPosition, PositionStatus,
};
pub use wallet::{
    DevBlacklistEntry, WalletEntry, WalletLabel, WalletRegistry,
};
