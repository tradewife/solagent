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
    #[serde(default)]
    pub fee_amount: Option<String>,
    #[serde(default)]
    pub fee_mint: Option<String>,
    #[serde(flatten)]
    pub other: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlanStep {
    pub swap_info: SwapInfo,
    #[serde(rename = "percent")]
    pub percentage: u32,
    #[serde(default)]
    pub bps: Option<serde_json::Value>,
    #[serde(flatten)]
    pub other: serde_json::Value,
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

/// A single instruction account meta from Jupiter's `/swap-instructions` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JupiterAccountMeta {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

/// A single instruction from Jupiter's `/swap-instructions` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JupiterInstruction {
    pub program_id: String,
    pub accounts: Vec<JupiterAccountMeta>,
    /// Base64-encoded instruction data.
    pub data: String,
}

impl JupiterInstruction {
    /// Convert to a `solana_sdk::instruction::Instruction`.
    pub fn to_solana_instruction(&self) -> Result<solana_sdk::instruction::Instruction> {
        let program_id: solana_sdk::pubkey::Pubkey = self.program_id.parse()?;
        let accounts: Vec<solana_sdk::instruction::AccountMeta> = self
            .accounts
            .iter()
            .map(|a| {
                let pubkey: solana_sdk::pubkey::Pubkey = a.pubkey.parse()?;
                Ok(solana_sdk::instruction::AccountMeta {
                    pubkey,
                    is_signer: a.is_signer,
                    is_writable: a.is_writable,
                })
            })
            .collect::<Result<_>>()?;
        let data = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &self.data,
        )?;
        Ok(solana_sdk::instruction::Instruction {
            program_id,
            accounts,
            data,
        })
    }
}

/// Response from Jupiter's `/swap-instructions` endpoint.
///
/// Returns individual instructions instead of a pre-built serialized transaction,
/// enabling use with Helius Smart Transaction Sender for optimized sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructions {
    /// Compute unit limit for the swap.
    pub compute_unit_limit: Option<u64>,
    /// Priority fee in microLamports per compute unit.
    pub compute_unit_price: Option<u64>,
    /// Setup instructions (e.g., create ATA, wrap SOL).
    #[serde(default)]
    pub setup_instructions: Vec<JupiterInstruction>,
    /// The main swap instruction.
    pub swap_instruction: JupiterInstruction,
    /// Cleanup instruction (e.g., close WSOL account).
    pub cleanup_instruction: Option<JupiterInstruction>,
    /// Address lookup table addresses used by the swap.
    #[serde(default)]
    pub address_lookup_table_addresses: Vec<String>,
    /// Last valid block height for the swap.
    #[serde(default)]
    pub last_valid_block_height: Option<u64>,
    /// Prioritization fee info.
    #[serde(default)]
    pub prioritization_fee_lamports: Option<serde_json::Value>,
}

impl SwapInstructions {
    /// Collect all instructions in execution order: setup → swap → cleanup.
    pub fn all_instructions(&self) -> Vec<&JupiterInstruction> {
        let mut instrs: Vec<&JupiterInstruction> = self.setup_instructions.iter().collect();
        instrs.push(&self.swap_instruction);
        if let Some(ref cleanup) = self.cleanup_instruction {
            instrs.push(cleanup);
        }
        instrs
    }

    /// Convert all instructions to `solana_sdk::instruction::Instruction`.
    pub fn to_solana_instructions(&self) -> Result<Vec<solana_sdk::instruction::Instruction>> {
        self.all_instructions()
            .iter()
            .map(|ji| ji.to_solana_instruction())
            .collect()
    }
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

    /// Get individual swap instructions from Jupiter V6.
    ///
    /// Unlike `get_swap_transaction()` which returns a pre-built serialized transaction,
    /// this returns the individual instructions that can be assembled into a transaction
    /// using the Helius Smart Transaction Sender for optimized routing and retries.
    pub async fn get_swap_instructions(
        &self,
        quote_response: &JupiterQuote,
        user_public_key: &str,
    ) -> Result<SwapInstructions> {
        let url = format!("{}/swap-instructions", self.base_url);
        let quote_value = serde_json::to_value(quote_response)?;
        let body = serde_json::json!({
            "quoteResponse": quote_value,
            "userPublicKey": user_public_key,
            "wrapAndUnwrapSol": true,
            "dynamicComputeUnitLimit": true,
            "prioritizationFeeLamports": "auto"
        });
        let swap_instrs: SwapInstructions = self.client.post_json(&url, &body).await?;
        Ok(swap_instrs)
    }
}
