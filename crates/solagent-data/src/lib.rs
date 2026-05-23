//! # solagent-data
//!
//! API clients for DexScreener, Birdeye, GMGN, Helius, and Jupiter with rate limiting.
//! Also provides the WalletWatcher (polling) and WsWatcher (WebSocket-first with
//! polling fallback) for real-time wallet monitoring.

pub mod birdeye;
pub mod dexscreener;
pub mod gmgn;
pub mod helius;
pub mod http;
pub mod jupiter;
pub mod watcher;
pub mod ws_watcher;
pub mod zerion;

// Re-export the main types at the crate root for backward compatibility.
pub use birdeye::{
    BirdeyeClient, BirdeyeResponse, HolderInfo, TokenOverview, TokenPrice, TokenSecurity,
    TokenSecurityData, TraderInfo, WalletPnl, BIRDEYE_DEFAULT_BASE_URL,
};
pub use dexscreener::{
    BoostedToken, DexPair, DexPairResponse, DexScreenerClient, DexSearchResponse,
    DexTokenResponse, TokenLink,
};
pub use gmgn::{GmgnClient, GmgnTokenInfo, GMGN_CLI_DEFAULT_PATH};
pub use helius::{
    BalancesResponse, HeliusSdkClient, NativeBalanceChange, NativeTransfer, ParsedTransaction,
    SwapEvent, TokenBalance, TokenBalanceChange, TokenTransfer, TransactionEvent,
    WebhookConfig,
};
pub use http::RateLimitedClient;
pub use jupiter::{
    JupiterAccountMeta, JupiterClient, JupiterInstruction, JupiterQuote, SwapInstructions,
    SwapTransaction,
};
pub use watcher::{WalletWatcher, WatchedWallet, WatcherConfig};
pub use ws_watcher::{WsWatcher, WsWatcherConfig};
pub use zerion::{
    WalletPnl as ZerionWalletPnl, WalletPortfolio, WalletPosition, ZerionClient,
    ZERION_BASE_URL,
};
