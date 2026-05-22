//! # solagent-behavioral
//!
//! 5-layer behavioral intelligence scanner for discovering Solana wallets
//! with genuine edge. Uses Birdeye token discovery + trader analysis,
//! then GMGN profiling for discovered wallets.

pub mod layers;
pub mod report;
pub mod scanner;

pub use report::{
    BehavioralReport, LayerScores, WalletScore, Tier, Confidence,
};
pub use scanner::BehavioralScanner;
