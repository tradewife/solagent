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
            if let Ok(resp) = self.client.get_json::<DexTokenResponse>(&token_url).await
                && let Some(pairs) = resp.pairs
            {
                let chain_pairs: Vec<DexPair> = pairs
                    .into_iter()
                    .filter(|p| p.chain_id == chain)
                    .collect();
                all_pairs.extend(chain_pairs);
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
            // Score pairs by liquidity × volume/liquidity ratio.
            // This penalizes pools with high liquidity but zero real volume
            // (e.g. manipulated or dead pools where price is meaningless).
            p.sort_by(|a, b| {
                let score = |pair: &DexPair| -> f64 {
                    let liq = pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                    let vol = pair.volume.as_ref().and_then(|v| v.h24).unwrap_or(0.0);
                    if liq <= 0.0 { return 0.0; }
                    let ratio = (vol / liq).min(1.0);
                    liq * ratio
                };
                let score_a = score(a);
                let score_b = score(b);
                score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
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

    /// Fetch Solana pairs sorted by 24h price change via the search endpoint.
    /// Uses DexScreener's token search to discover pairs with extreme price
    /// movements. Returns pairs filtered to Solana with valid price change data.
    pub async fn get_solana_trending_pairs(&self, min_liquidity_usd: f64, limit: usize) -> Result<Vec<DexPair>> {
        // DexScreener's search returns pairs matching the query. We use
        // broad queries to discover active Solana tokens.
        let queries = ["SOL", "pump", "solana"];
        let mut all_pairs: Vec<DexPair> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for q in &queries {
            let url = format!(
                "{}/latest/dex/search?q={}",
                self.base_url,
                urlencoding::encode(q),
            );
            if let Ok(resp) = self.client.get_json::<DexSearchResponse>(&url).await
                && let Some(pairs) = resp.pairs
            {
                for pair in pairs {
                    if pair.chain_id != "solana" { continue; }
                    let liq = pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                    if liq < min_liquidity_usd { continue; }
                    if seen.contains(&pair.base_token.address) { continue; }
                    seen.insert(pair.base_token.address.clone());
                    all_pairs.push(pair);
                }
            }
            // Small delay to stay within DexScreener rate limits.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        // Sort by absolute 24h price change (most volatile first).
        all_pairs.sort_by(|a, b| {
            let change_a = a.price_change.as_ref().and_then(|c| c.h24).unwrap_or(0.0).abs();
            let change_b = b.price_change.as_ref().and_then(|c| c.h24).unwrap_or(0.0).abs();
            change_b.partial_cmp(&change_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        all_pairs.truncate(limit);
        Ok(all_pairs)
    }
}
