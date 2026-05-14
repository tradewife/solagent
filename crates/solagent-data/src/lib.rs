//! # solagent-data
//!
//! API clients for DexScreener, Birdeye, Helius, and Jupiter with rate limiting.

use anyhow::Result;
use governor::{Quota, RateLimiter};
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::middleware::NoOpMiddleware;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::Arc;

// ─── Rate-Limited HTTP Client ────────────────────────────────────────────────

/// Wrapper around reqwest::Client with governor rate limiting.
#[derive(Clone)]
pub struct RateLimitedClient {
    client: Client,
    limiter: Arc<RateLimiter<governor::state::NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
}

impl RateLimitedClient {
    /// Create a new rate-limited client with the given requests-per-second quota.
    pub fn new(requests_per_second: u32) -> Self {
        let client = Client::new();
        let rate = NonZeroU32::new(requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap());
        let quota = Quota::per_second(rate);
        let limiter = Arc::new(RateLimiter::direct(quota));
        Self { client, limiter }
    }

    /// Wait for rate limit clearance and return the underlying reqwest client.
    pub async fn request(&self) -> &Client {
        if self.limiter.check().is_err() {
            self.limiter.until_ready().await;
        }
        &self.client
    }

    /// Perform a GET request, deserialize the JSON response into `T`.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let client = self.request().await;
        let resp = client.get(url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        let data: T = resp.json().await?;
        Ok(data)
    }

    /// Perform a POST request with a JSON body, deserialize the JSON response into `T`.
    pub async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let client = self.request().await;
        let resp = client.post(url).json(body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {url} returned {status}: {body_text}");
        }
        let data: T = resp.json().await?;
        Ok(data)
    }
}

// ─── DexScreener Client ─────────────────────────────────────────────────────

/// Buy/sell counts for a time window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnsWindow {
    pub buys: u64,
    pub sells: u64,
}

/// Transaction counts across time windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnsInfo {
    pub m5: TxnsWindow,
    pub h1: TxnsWindow,
    pub h6: TxnsWindow,
    pub h24: TxnsWindow,
}

/// Volume across time windows (all Option since may be null).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub m5: Option<f64>,
    pub h1: Option<f64>,
    pub h6: Option<f64>,
    pub h24: Option<f64>,
}

/// Price change percentages across time windows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChangeInfo {
    pub m5: Option<f64>,
    pub h1: Option<f64>,
    pub h6: Option<f64>,
    pub h24: Option<f64>,
}

/// Liquidity information for a pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityInfo {
    pub usd: Option<f64>,
    pub base: Option<f64>,
    pub quote: Option<f64>,
}

/// Boost information for a pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoostsInfo {
    pub active: Option<u64>,
}

/// Token info within a pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexToken {
    pub address: String,
    pub name: String,
    pub symbol: String,
}

/// Info metadata (images, websites, socials).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexPairInfo {
    pub image_url: Option<String>,
    #[serde(default)]
    pub websites: Vec<serde_json::Value>,
    #[serde(default)]
    pub socials: Vec<serde_json::Value>,
}

/// DexScreener API pair – matches the exact JSON shape returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexPair {
    pub chain_id: String,
    pub dex_id: String,
    pub url: Option<String>,
    pub pair_address: String,
    pub base_token: DexToken,
    pub quote_token: DexToken,
    pub price_native: Option<String>,
    pub price_usd: Option<String>,
    pub txns: Option<TxnsInfo>,
    pub volume: Option<VolumeInfo>,
    pub price_change: Option<PriceChangeInfo>,
    pub liquidity: Option<LiquidityInfo>,
    pub fdv: Option<f64>,
    pub market_cap: Option<f64>,
    pub pair_created_at: Option<i64>,
    pub info: Option<DexPairInfo>,
    pub boosts: Option<BoostsInfo>,
}

/// Response for `GET /latest/dex/pairs/{chainId}/{pairAddress}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexPairResponse {
    pub pair: Option<DexPair>,
}

/// Response for `GET /latest/dex/tokens/{tokenAddress}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexTokenResponse {
    pub pairs: Option<Vec<DexPair>>,
}

/// Response for `GET /latest/dex/search?q=...`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexSearchResponse {
    pub pairs: Option<Vec<DexPair>>,
}

/// Token profile from `GET /token-profiles/latest/v1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoostedToken {
    pub url: String,
    pub chain_id: Option<String>,
    pub token_address: String,
    pub icon: Option<String>,
    pub header: Option<String>,
    pub open_graph: Option<String>,
    pub description: Option<String>,
    pub links: Option<Vec<TokenLink>>,
    pub cto: Option<bool>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenLink {
    pub label: Option<String>,
    #[serde(rename = "type")]
    pub link_type: Option<String>,
    pub url: Option<String>,
}

/// DexScreener API client.
pub struct DexScreenerClient {
    client: RateLimitedClient,
    base_url: String,
    #[allow(dead_code)]
    api_key: Option<String>,
}

impl DexScreenerClient {
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        Self {
            client: RateLimitedClient::new(5),
            base_url,
            api_key,
        }
    }

    /// Fetch recently boosted/promoted pairs for a chain.
    /// Uses the token-boosts endpoint to find promoted tokens, then fetches their pair data.
    pub async fn get_new_pairs(&self, chain: &str) -> Result<Vec<DexPair>> {
        // First get boosted tokens -- these are actively promoted and worth scanning.
        let boosts_url = format!("{}/token-boosts/latest/v1", self.base_url);
        let boosts: Vec<BoostedToken> = match self.client.get_json::<Vec<BoostedToken>>(&boosts_url).await {
            Ok(b) => b,
            Err(_) => {
                // Fallback to token profiles if boosts endpoint fails.
                let profiles_url = format!("{}/token-profiles/latest/v1", self.base_url);
                self.client.get_json::<Vec<BoostedToken>>(&profiles_url).await?
            }
        };

        let chain_boosts: Vec<&BoostedToken> = boosts
            .iter()
            .filter(|b| b.chain_id.as_deref() == Some(chain))
            .take(15)
            .collect();

        let mut all_pairs = Vec::new();
        for boost in chain_boosts {
            let token_url = format!(
                "{}/latest/dex/tokens/{}",
                self.base_url, boost.token_address
            );
            if let Ok(resp) = self.client.get_json::<DexTokenResponse>(&token_url).await {
                if let Some(pairs) = resp.pairs {
                    let chain_pairs: Vec<DexPair> = pairs
                        .into_iter()
                        .filter(|p| p.chain_id == chain)
                        .collect();
                    all_pairs.extend(chain_pairs);
                }
            }
        }
        Ok(all_pairs)
    }

    /// Fetch all pairs for a token address, returning the first pair (if any).
    pub async fn get_token_info(&self, token_address: &str) -> Result<Option<DexPair>> {
        let url = format!(
            "{}/latest/dex/tokens/{}",
            self.base_url, token_address
        );
        let resp: DexTokenResponse = self.client.get_json(&url).await?;
        Ok(resp.pairs.and_then(|mut p| {
            p.sort_by(|a, b| {
                let liq_a = a.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                let liq_b = b.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                liq_b.partial_cmp(&liq_a).unwrap_or(std::cmp::Ordering::Equal)
            });
            p.into_iter().next()
        }))
    }

    /// Fetch pair info by chain and pair address.
    pub async fn get_pair_info(&self, chain: &str, pair_address: &str) -> Result<DexPair> {
        let url = format!(
            "{}/latest/dex/pairs/{}/{}",
            self.base_url, chain, pair_address
        );
        let resp: DexPairResponse = self.client.get_json(&url).await?;
        resp.pair
            .ok_or_else(|| anyhow::anyhow!("Pair not found: {chain}/{pair_address}"))
    }

    /// Search for pairs by query string.
    pub async fn search_pairs(&self, query: &str) -> Result<Vec<DexPair>> {
        let url = format!(
            "{}/latest/dex/search?q={}",
            self.base_url,
            urlencoding::encode(query)
        );
        let resp: DexSearchResponse = self.client.get_json(&url).await?;
        Ok(resp.pairs.unwrap_or_default())
    }

    /// Fetch boosted / latest token profiles.
    pub async fn get_boosted_tokens(&self) -> Result<Vec<BoostedToken>> {
        let url = format!("{}/token-profiles/latest/v1", self.base_url);
        let profiles: Vec<BoostedToken> = self.client.get_json(&url).await?;
        Ok(profiles)
    }
}

// ─── Birdeye Client ──────────────────────────────────────────────────────────

/// Generic Birdeye API response wrapper.
/// All Birdeye endpoints return `{ success: bool, data: T }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BirdeyeResponse<T> {
    pub success: bool,
    pub data: T,
}

/// Inner data for the `/defi/price` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenPriceData {
    pub value: f64,
    pub update_unix_time: i64,
    pub price_change_24h: Option<f64>,
}

/// Token price info returned by `get_token_price`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenPrice {
    pub address: String,
    pub price_usd: f64,
    pub update_unix_time: i64,
    pub price_change_24h: Option<f64>,
}

/// Inner data for the `/defi/token_security` endpoint.
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
    /// Additional fields from Birdeye that we keep as untyped JSON.
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Token security info returned by `get_token_security`.
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

/// Top holder entry from the `/defi/v3/token/holder` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderInfo {
    pub owner: String,
    pub amount: f64,
    pub pct: f64,
    /// Allow additional fields.
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Inner data for the `/defi/v3/token/holder` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderListData {
    pub items: Vec<HolderInfo>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Top trader entry from the `/defi/v3/token/top-traders` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraderInfo {
    pub owner: String,
    pub pnl: Option<f64>,
    pub buy_count: Option<u64>,
    pub sell_count: Option<u64>,
    /// Allow additional fields.
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Inner data for the `/defi/v3/token/top-traders` endpoint.
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

/// Default Birdeye public API base URL.
pub const BIRDEYE_DEFAULT_BASE_URL: &str = "https://public-api.birdeye.so";

/// Birdeye API client.
pub struct BirdeyeClient {
    client: RateLimitedClient,
    base_url: String,
    api_key: Option<String>,
}

impl BirdeyeClient {
    /// Create a new Birdeye client.
    ///
    /// `base_url` defaults to `BIRDEYE_DEFAULT_BASE_URL` if empty.
    /// `api_key` is optional – some endpoints work without it on the free tier.
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        let base_url = if base_url.is_empty() {
            BIRDEYE_DEFAULT_BASE_URL.to_string()
        } else {
            base_url
        };
        Self {
            client: RateLimitedClient::new(3),
            base_url,
            api_key,
        }
    }

    /// Convenience constructor with default base URL and optional API key.
    pub fn with_api_key(api_key: Option<String>) -> Self {
        Self::new(BIRDEYE_DEFAULT_BASE_URL.to_string(), api_key)
    }

    /// Build a reqwest GET request builder with common Birdeye headers set.
    /// Respects the rate limiter before constructing the request.
    async fn birdeye_get(&self, path: &str, chain: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base_url);
        let client = self.client.request().await;
        let mut req = client.get(&url).header("x-chain", chain);
        if let Some(ref key) = self.api_key {
            req = req.header("X-API-KEY", key);
        }
        req
    }

    /// Execute a request and deserialize the Birdeye response, unwrapping the `data` field.
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

    /// Get token price.
    ///
    /// `chain` is passed as the `x-chain` header (e.g. `"solana"`, `"ethereum"`).
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

    /// Get token security info (mint/freeze authority, holders, taxes).
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

    /// Get top traders for a token.
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

    /// Get top holders for a token.
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

    /// Get wallet PnL summary.
    pub async fn get_wallet_pnl(&self, wallet: &str) -> Result<WalletPnl> {
        let _client = self.client.request().await;
        todo!("GET /v1/wallet/{wallet}/pnl")
    }
}

// ─── Helius Client ───────────────────────────────────────────────────────────

/// Helius webhook configuration for creating/listing webhooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookConfig {
    #[serde(rename = "webhookURL")]
    pub webhook_url: String,
    #[serde(default)]
    pub transaction_types: Vec<String>,
    #[serde(default)]
    pub account_addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_header: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_id: Option<String>,
}

/// A parsed transaction returned by the Helius `/addresses/{address}/transactions` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedTransaction {
    pub signature: String,
    #[serde(rename = "type")]
    pub tx_type: Option<String>,
    pub source: Option<String>,
    pub description: Option<String>,
    pub fee: Option<u64>,
    pub fee_payer: Option<String>,
    pub native_transfers: Option<Vec<serde_json::Value>>,
    pub events: Option<serde_json::Value>,
    /// Timestamp of the transaction.
    pub timestamp: Option<i64>,
    /// Allow additional fields from Helius.
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Token balance entry from the Helius `/addresses/{address}/balances` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub address: String,
    pub amount: f64,
    pub decimals: u8,
    pub mint: String,
}

/// Response wrapper for `/addresses/{address}/balances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancesResponse {
    #[serde(default)]
    pub tokens: Vec<TokenBalance>,
}

/// Helius API client for Solana enhanced RPC and DAS API.
pub struct HeliusClient {
    client: RateLimitedClient,
    api_key: Option<String>,
    base_url: String,
}

impl HeliusClient {
    const DEFAULT_BASE_URL: &'static str = "https://api.helius.xyz/v0";

    /// Create a new Helius client with an API key.
    pub fn new_with_key(api_key: String) -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: Some(api_key),
            base_url: Self::DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Create a new Helius client without an API key.
    /// All API calls will return an error since Helius requires a key.
    pub fn new_without_key() -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: None,
            base_url: Self::DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Legacy constructor kept for backwards compatibility.
    pub fn new(api_key: String, base_url: String) -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: Some(api_key),
            base_url,
        }
    }

    /// Ensure an API key is available, returning an error otherwise.
    fn require_api_key(&self) -> Result<&str> {
        self.api_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Helius API key is not configured"))
    }

    /// Build a URL with the API key query parameter appended.
    fn build_url(&self, path: &str) -> Result<String> {
        let key = self.require_api_key()?;
        let separator = if path.contains('?') { '&' } else { '?' };
        Ok(format!("{}{}{}api-key={}", self.base_url, path, separator, key))
    }

    /// Get parsed transaction history for an address.
    /// POST /addresses/{address}/transactions
    pub async fn get_transactions(
        &self,
        address: &str,
        tx_type: Option<&str>,
    ) -> Result<Vec<ParsedTransaction>> {
        let url = self.build_url(&format!("/addresses/{}/transactions", address))?;
        let mut body = serde_json::json!({});
        if let Some(t) = tx_type {
            body["type"] = serde_json::Value::String(t.to_string());
        }
        let txs: Vec<ParsedTransaction> = self.client.post_json(&url, &body).await?;
        Ok(txs)
    }

    /// Get token balances for an address.
    /// GET /addresses/{address}/balances
    pub async fn get_balances(&self, address: &str) -> Result<BalancesResponse> {
        let url = self.build_url(&format!("/addresses/{}/balances", address))?;
        let resp: BalancesResponse = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Create a webhook for on-chain events.
    /// POST /webhooks
    pub async fn create_webhook(&self, config: &WebhookConfig) -> Result<WebhookConfig> {
        let url = self.build_url("/webhooks")?;
        let body = serde_json::to_value(config)?;
        let resp: WebhookConfig = self.client.post_json(&url, &body).await?;
        Ok(resp)
    }

    /// List all webhooks.
    /// GET /webhooks
    pub async fn list_webhooks(&self) -> Result<Vec<WebhookConfig>> {
        let url = self.build_url("/webhooks")?;
        let resp: Vec<WebhookConfig> = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Register a webhook for on-chain events (legacy name, delegates to create_webhook).
    pub async fn register_webhook(&self, config: WebhookConfig) -> Result<String> {
        let resp = self.create_webhook(&config).await?;
        resp.webhook_id
            .ok_or_else(|| anyhow::anyhow!("Helius did not return a webhook ID"))
    }

    /// Get parsed transaction details by signature (alias for get_transactions filtered).
    /// Uses the parsed transactions endpoint for a single address.
    pub async fn get_parsed_transaction(&self, signature: &str) -> Result<ParsedTransaction> {
        let url = self.build_url(&format!("/transactions/{}", signature))?;
        let resp: ParsedTransaction = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Get all token balances for an owner address (legacy name, delegates to get_balances).
    pub async fn get_token_accounts(&self, owner: &str) -> Result<Vec<TokenBalance>> {
        let resp = self.get_balances(owner).await?;
        Ok(resp.tokens)
    }
}

// ─── Jupiter Client ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInfo {
    pub amm_key: String,
    pub label: Option<String>,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    pub fee_amount: String,
    pub fee_mint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutePlanStep {
    pub swap_info: SwapInfo,
    pub percentage: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JupiterQuote {
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    #[serde(default)]
    pub other_amount_threshold: String,
    #[serde(default)]
    pub price_impact_pct: String,
    pub route_plan: Vec<RoutePlanStep>,
    #[serde(default)]
    pub context_ago: Option<u64>,
    // Allow additional fields from the Jupiter response
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapTransaction {
    pub swap_transaction: String, // base64-encoded
    pub last_valid_block_height: u64,
    pub prioritization_fee_lamports: Option<serde_json::Value>,
}

/// Jupiter Aggregator API client.
pub struct JupiterClient {
    client: RateLimitedClient,
    #[allow(dead_code)]
    base_url: String,
}

impl JupiterClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: RateLimitedClient::new(10),
            base_url,
        }
    }

    /// Get a swap quote.
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount: u64,
        slippage_bps: u32,
    ) -> Result<JupiterQuote> {
        let url = format!(
            "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
            self.base_url, input_mint, output_mint, amount, slippage_bps
        );
        let quote: JupiterQuote = self.client.get_json(&url).await?;
        Ok(quote)
    }

    /// Get a serialized swap transaction from a quote.
    pub async fn get_swap_transaction(
        &self,
        quote_response: &JupiterQuote,
        user_public_key: &str,
    ) -> Result<SwapTransaction> {
        let url = format!("{}/swap", self.base_url);
        let quote_value = serde_json::to_value(quote_response)?;
        let body = serde_json::json!({
            "quoteResponse": quote_value,
            "userPublicKey": user_public_key,
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": "auto"
        });
        let swap_tx: SwapTransaction = self.client.post_json(&url, &body).await?;
        Ok(swap_tx)
    }
}
