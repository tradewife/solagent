//! Jupiter Aggregator V6 API client.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::RateLimitedClient;

// ─── Types ───────────────────────────────────────────────────────────────────

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
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapTransaction {
    pub swap_transaction: String,
    pub last_valid_block_height: u64,
    pub prioritization_fee_lamports: Option<serde_json::Value>,
}

// ─── Client ──────────────────────────────────────────────────────────────────

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
