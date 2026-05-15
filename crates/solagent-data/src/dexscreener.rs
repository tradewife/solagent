//! DexScreener API client.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::RateLimitedClient;

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnsWindow {
    pub buys: u64,
    pub sells: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnsInfo {
    pub m5: TxnsWindow,
    pub h1: TxnsWindow,
    pub h6: TxnsWindow,
    pub h24: TxnsWindow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub m5: Option<f64>,
    pub h1: Option<f64>,
    pub h6: Option<f64>,
    pub h24: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChangeInfo {
    pub m5: Option<f64>,
    pub h1: Option<f64>,
    pub h6: Option<f64>,
    pub h24: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityInfo {
    pub usd: Option<f64>,
    pub base: Option<f64>,
    pub quote: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoostsInfo {
    pub active: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexToken {
    pub address: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexPairInfo {
    pub image_url: Option<String>,
    #[serde(default)]
    pub websites: Vec<serde_json::Value>,
    #[serde(default)]
    pub socials: Vec<serde_json::Value>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexPairResponse {
    pub pair: Option<DexPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexTokenResponse {
    pub pairs: Option<Vec<DexPair>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DexSearchResponse {
    pub pairs: Option<Vec<DexPair>>,
}

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

// ─── Client ──────────────────────────────────────────────────────────────────

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

    pub async fn get_new_pairs(&self, chain: &str) -> Result<Vec<DexPair>> {
        let boosts_url = format!("{}/token-boosts/latest/v1", self.base_url);
        let boosts: Vec<BoostedToken> = match self.client.get_json::<Vec<BoostedToken>>(&boosts_url).await {
            Ok(b) => b,
            Err(_) => {
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

    pub async fn get_pair_info(&self, chain: &str, pair_address: &str) -> Result<DexPair> {
        let url = format!(
            "{}/latest/dex/pairs/{}/{}",
            self.base_url, chain, pair_address
        );
        let resp: DexPairResponse = self.client.get_json(&url).await?;
        resp.pair
            .ok_or_else(|| anyhow::anyhow!("Pair not found: {chain}/{pair_address}"))
    }

    pub async fn search_pairs(&self, query: &str) -> Result<Vec<DexPair>> {
        let url = format!(
            "{}/latest/dex/search?q={}",
            self.base_url,
            urlencoding::encode(query)
        );
        let resp: DexSearchResponse = self.client.get_json(&url).await?;
        Ok(resp.pairs.unwrap_or_default())
    }

    pub async fn get_boosted_tokens(&self) -> Result<Vec<BoostedToken>> {
        let url = format!("{}/token-profiles/latest/v1", self.base_url);
        let profiles: Vec<BoostedToken> = self.client.get_json(&url).await?;
        Ok(profiles)
    }
}
