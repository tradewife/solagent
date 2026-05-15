//! Birdeye API client for token prices, security, holders, and traders.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::RateLimitedClient;

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BirdeyeResponse<T> {
    pub success: bool,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenPriceData {
    pub value: f64,
    pub update_unix_time: i64,
    pub price_change_24h: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPrice {
    pub address: String,
    pub price_usd: f64,
    pub update_unix_time: i64,
    pub price_change_24h: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenSecurityData {
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub renounced: Option<bool>,
    pub mutable_metadata: Option<bool>,
    pub top_holders: Option<Vec<HolderInfo>>,
    pub is_honeypot: Option<bool>,
    pub buy_tax: Option<f64>,
    pub sell_tax: Option<f64>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSecurity {
    pub address: String,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub renounced: Option<bool>,
    pub mutable_metadata: Option<bool>,
    pub top_holders: Vec<HolderInfo>,
    pub is_honeypot: Option<bool>,
    pub buy_tax: Option<f64>,
    pub sell_tax: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderInfo {
    pub owner: String,
    pub amount: f64,
    pub pct: f64,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderListData {
    pub items: Vec<HolderInfo>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraderInfo {
    pub owner: String,
    pub pnl: Option<f64>,
    pub buy_count: Option<u64>,
    pub sell_count: Option<u64>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraderListData {
    pub items: Vec<TraderInfo>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPnl {
    pub address: String,
    pub total_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub trade_count: u64,
}

pub const BIRDEYE_DEFAULT_BASE_URL: &str = "https://public-api.birdeye.so";

// ─── Client ──────────────────────────────────────────────────────────────────

pub struct BirdeyeClient {
    client: RateLimitedClient,
    base_url: String,
    api_key: Option<String>,
}

impl BirdeyeClient {
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        let base_url = if base_url.is_empty() {
            BIRDEYE_DEFAULT_BASE_URL.to_string()
        } else {
            base_url
        };
        Self {
            client: RateLimitedClient::new(1), // 1 req/s for free tier
            base_url,
            api_key,
        }
    }

    pub fn with_api_key(api_key: Option<String>) -> Self {
        Self::new(BIRDEYE_DEFAULT_BASE_URL.to_string(), api_key)
    }

    async fn birdeye_get(&self, path: &str, chain: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base_url);
        let client = self.client.request().await;
        let mut req = client.get(&url).header("x-chain", chain);
        if let Some(ref key) = self.api_key {
            req = req.header("X-API-KEY", key);
        }
        req
    }

    async fn send_birdeye<T: serde::de::DeserializeOwned>(&self, req: reqwest::RequestBuilder) -> Result<T> {
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Birdeye API returned {status}: {body}");
        }
        let birdeye_resp: BirdeyeResponse<T> = resp.json().await?;
        if !birdeye_resp.success {
            anyhow::bail!("Birdeye API returned success=false");
        }
        Ok(birdeye_resp.data)
    }

    pub async fn get_token_price(&self, address: &str, chain: &str) -> Result<TokenPrice> {
        let req = self
            .birdeye_get(
                &format!("/defi/price?address={}&check_liquidity=true", address),
                chain,
            )
            .await;
        let data: TokenPriceData = self.send_birdeye(req).await?;
        Ok(TokenPrice {
            address: address.to_string(),
            price_usd: data.value,
            update_unix_time: data.update_unix_time,
            price_change_24h: data.price_change_24h,
        })
    }

    pub async fn get_token_security(&self, address: &str) -> Result<TokenSecurity> {
        let req = self
            .birdeye_get(
                &format!("/defi/token_security?address={}", address),
                "solana",
            )
            .await;
        let data: TokenSecurityData = self.send_birdeye(req).await?;
        Ok(TokenSecurity {
            address: address.to_string(),
            mint_authority: data.mint_authority,
            freeze_authority: data.freeze_authority,
            renounced: data.renounced,
            mutable_metadata: data.mutable_metadata,
            top_holders: data.top_holders.unwrap_or_default(),
            is_honeypot: data.is_honeypot,
            buy_tax: data.buy_tax,
            sell_tax: data.sell_tax,
        })
    }

    pub async fn get_top_traders(&self, address: &str) -> Result<Vec<TraderInfo>> {
        let req = self
            .birdeye_get(
                &format!(
                    "/defi/v3/token/top-traders?address={}&sort_by=pnl&sort_type=desc&offset=0&limit=10",
                    address
                ),
                "solana",
            )
            .await;
        let data: TraderListData = self.send_birdeye(req).await?;
        Ok(data.items)
    }

    pub async fn get_top_holders(&self, address: &str) -> Result<Vec<HolderInfo>> {
        let req = self
            .birdeye_get(
                &format!(
                    "/defi/v3/token/holder?address={}&sort_by=amount&sort_type=desc&offset=0&limit=10",
                    address
                ),
                "solana",
            )
            .await;
        let data: HolderListData = self.send_birdeye(req).await?;
        Ok(data.items)
    }

    pub async fn get_wallet_pnl(&self, wallet: &str) -> Result<WalletPnl> {
        let req = self
            .birdeye_get(
                &format!("/v1/wallet/{wallet}/pnl"),
                "solana",
            )
            .await;
        let data: WalletPnlData = self.send_birdeye(req).await?;
        Ok(WalletPnl {
            address: wallet.to_string(),
            total_pnl: data.total_pnl,
            realized_pnl: data.realized_pnl,
            unrealized_pnl: data.unrealized_pnl,
            trade_count: data.trade_count,
        })
    }
}

// ─── Wallet PnL Data ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalletPnlData {
    total_pnl: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    trade_count: u64,
}
