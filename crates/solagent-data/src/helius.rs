//! Helius API client for Solana enhanced RPC, DAS API, and webhooks.
//!
//! Response types match the official Helius Enhanced Transactions API schema
//! as verified against helius-labs/helius-rust-sdk.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::RateLimitedClient;

// ─── Enhanced Transaction Types ──────────────────────────────────────────────

/// A human-readable, enhanced representation of a Solana transaction.
///
/// Matches the Helius Enhanced Transactions API response schema exactly.
/// See: https://docs.helius.dev/enhanced-transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedTransaction {
    /// The transaction signature (base-58).
    pub signature: String,
    /// Transaction type classification (e.g. "SWAP", "TRANSFER", "NFT_SALE").
    #[serde(rename = "type")]
    pub tx_type: Option<String>,
    /// Protocol or marketplace source (e.g. "JUPITER", "RAYDIUM", "ORCA").
    pub source: Option<String>,
    /// Human-readable description.
    pub description: Option<String>,
    /// Transaction fee in lamports.
    pub fee: Option<i64>,
    /// Fee payer public key.
    pub fee_payer: Option<String>,
    /// Slot number.
    #[serde(default)]
    pub slot: Option<i64>,
    /// Per-account balance snapshots.
    #[serde(default)]
    pub account_data: Vec<AccountData>,
    /// Native SOL transfers.
    pub native_transfers: Option<Vec<NativeTransfer>>,
    /// SPL token transfers.
    pub token_transfers: Option<Vec<TokenTransfer>>,
    /// Structured events (swap, nft, compressed, etc.).
    #[serde(default)]
    pub events: Option<TransactionEvent>,
    /// Transaction error, if any.
    pub transaction_error: Option<serde_json::Value>,
    /// Top-level instructions.
    #[serde(default)]
    pub instructions: Vec<Instruction>,
    /// Unix timestamp (seconds).
    pub timestamp: Option<i64>,
    /// Catch-all for fields we don't explicitly handle.
    #[serde(flatten)]
    pub other: serde_json::Value,
}

/// Balance snapshot for a single account in a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountData {
    pub account: String,
    pub native_token_balance: Option<serde_json::Number>,
    pub token_balance_changes: Option<Vec<TokenBalanceChange>>,
}

/// SPL token balance change within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalanceChange {
    pub user_account: String,
    pub token_account: String,
    pub raw_token_amount: RawTokenAmount,
    pub mint: String,
}

/// Raw token amount with decimals.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTokenAmount {
    pub token_amount: String,
    pub decimals: serde_json::Number,
}

/// Native SOL transfer between accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTransfer {
    pub from_user_account: Option<String>,
    pub to_user_account: Option<String>,
    pub amount: serde_json::Number,
}

/// SPL token transfer between accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenTransfer {
    pub from_user_account: Option<String>,
    pub to_user_account: Option<String>,
    pub from_token_account: Option<String>,
    pub to_token_account: Option<String>,
    pub token_amount: serde_json::Number,
    pub token_standard: Option<String>,
    pub mint: String,
}

/// Structured event data extracted from a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TransactionEvent {
    /// NFT marketplace event.
    pub nft: Option<serde_json::Value>,
    /// Token swap event.
    pub swap: Option<SwapEvent>,
    /// Compressed NFT events.
    pub compressed: Option<Vec<serde_json::Value>>,
    /// Authority change events.
    #[serde(rename = "setAuthority")]
    pub set_authority: Option<Vec<serde_json::Value>>,
}

/// A token swap (DEX trade) event.
///
/// This is the authoritative structure from the Helius API.
/// `native_input` / `native_output` contain SOL amounts in lamports.
/// `token_inputs` / `token_outputs` contain the SPL tokens with mint addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapEvent {
    /// SOL sent into the swap, if any.
    pub native_input: Option<NativeBalanceChange>,
    /// SOL received from the swap, if any.
    pub native_output: Option<NativeBalanceChange>,
    /// SPL tokens sent as input.
    #[serde(default)]
    pub token_inputs: Vec<TokenBalanceChange>,
    /// SPL tokens received as output.
    #[serde(default)]
    pub token_outputs: Vec<TokenBalanceChange>,
    /// Token fees.
    #[serde(default)]
    pub token_fees: Vec<TokenBalanceChange>,
    /// Native fees.
    #[serde(default)]
    pub native_fees: Vec<NativeBalanceChange>,
    /// Individual swap hops for routed trades.
    #[serde(default)]
    pub inner_swaps: Vec<serde_json::Value>,
}

/// Change in native SOL balance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeBalanceChange {
    pub account: String,
    pub amount: serde_json::Number,
}

/// A top-level instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    pub accounts: Vec<String>,
    pub data: String,
    #[serde(rename = "programId")]
    pub program_id: String,
    #[serde(rename = "innerInstructions", default)]
    pub inner_instructions: Vec<serde_json::Value>,
}

// ─── Webhook Types ────────────────────────────────────────────────────────────

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

// ─── Balance Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub address: String,
    pub amount: f64,
    pub decimals: u8,
    pub mint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancesResponse {
    #[serde(default)]
    pub tokens: Vec<TokenBalance>,
}

// ─── Helius Client ───────────────────────────────────────────────────────────

pub struct HeliusClient {
    client: RateLimitedClient,
    api_key: Option<String>,
    base_url: String,
}

impl HeliusClient {
    const DEFAULT_BASE_URL: &'static str = "https://api.helius.xyz/v0";

    pub fn new_with_key(api_key: String) -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: Some(api_key),
            base_url: Self::DEFAULT_BASE_URL.to_string(),
        }
    }

    pub fn new_without_key() -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: None,
            base_url: Self::DEFAULT_BASE_URL.to_string(),
        }
    }

    pub fn new(api_key: String, base_url: String) -> Self {
        Self {
            client: RateLimitedClient::new(10),
            api_key: Some(api_key),
            base_url,
        }
    }

    fn require_api_key(&self) -> Result<&str> {
        self.api_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Helius API key is not configured"))
    }

    fn build_url(&self, path: &str) -> Result<String> {
        let key = self.require_api_key()?;
        let separator = if path.contains('?') { '&' } else { '?' };
        Ok(format!("{}{}{}api-key={}", self.base_url, path, separator, key))
    }

    /// Get parsed transaction history for an address.
    pub async fn get_transactions(
        &self,
        address: &str,
        tx_type: Option<&str>,
    ) -> Result<Vec<ParsedTransaction>> {
        let mut url = self.build_url(&format!("/addresses/{}/transactions", address))?;
        if let Some(t) = tx_type {
            url = format!("{}&type={}", url, t);
        }
        let txs: Vec<ParsedTransaction> = self.client.get_json(&url).await?;
        Ok(txs)
    }

    /// Get token balances for an address.
    pub async fn get_balances(&self, address: &str) -> Result<BalancesResponse> {
        let url = self.build_url(&format!("/addresses/{}/balances", address))?;
        let resp: BalancesResponse = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Create a webhook.
    pub async fn create_webhook(&self, config: &WebhookConfig) -> Result<WebhookConfig> {
        let url = self.build_url("/webhooks")?;
        let body = serde_json::to_value(config)?;
        let resp: WebhookConfig = self.client.post_json(&url, &body).await?;
        Ok(resp)
    }

    /// List all webhooks.
    pub async fn list_webhooks(&self) -> Result<Vec<WebhookConfig>> {
        let url = self.build_url("/webhooks")?;
        let resp: Vec<WebhookConfig> = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Register a webhook (legacy name).
    pub async fn register_webhook(&self, config: WebhookConfig) -> Result<String> {
        let resp = self.create_webhook(&config).await?;
        resp.webhook_id
            .ok_or_else(|| anyhow::anyhow!("Helius did not return a webhook ID"))
    }

    /// Get a single parsed transaction by signature.
    pub async fn get_parsed_transaction(&self, signature: &str) -> Result<ParsedTransaction> {
        let url = self.build_url(&format!("/transactions/{}", signature))?;
        let resp: ParsedTransaction = self.client.get_json(&url).await?;
        Ok(resp)
    }

    /// Get all token balances for an owner address (legacy name).
    pub async fn get_token_accounts(&self, owner: &str) -> Result<Vec<TokenBalance>> {
        let resp = self.get_balances(owner).await?;
        Ok(resp.tokens)
    }
}
