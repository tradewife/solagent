//! Helius API client built on the official Helius Rust SDK.
//!
//! Provides:
//! - **Enhanced transaction history** via `parsed_transaction_history()`
//! - **DAS API token balances** via `get_assets_by_owner()`
//! - **Webhook management** (delegated to SDK)
//!
//! The custom HTTP `HeliusClient` has been replaced by `HeliusSdkClient` which
//! wraps `helius::Helius` (official SDK v1.x). Our internal types like
//! `ParsedTransaction` and `SwapEvent` are preserved for backward compatibility;
//! SDK types are converted at the boundary.

use anyhow::Result;
use helius::types::{
    Cluster, DisplayOptions, GetAssetsByOwner, ParsedTransactionHistoryRequest,
};
use serde::{Deserialize, Serialize};

// ─── Enhanced Transaction Types (backward-compatible) ────────────────────────

/// A human-readable, enhanced representation of a Solana transaction.
///
/// Converted from the SDK's `EnhancedTransaction` at the API boundary.
/// Matches the Helius Enhanced Transactions API response schema.
/// See: <https://docs.helius.dev/enhanced-transactions>
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

// ─── SDK Type Conversions ────────────────────────────────────────────────────

/// Convert an SDK `EnhancedTransaction` to our backward-compatible `ParsedTransaction`.
fn convert_enhanced_transaction(etx: &helius::types::EnhancedTransaction) -> ParsedTransaction {
    ParsedTransaction {
        signature: etx.signature.clone(),
        tx_type: Some(format!("{:?}", etx.transaction_type)),
        source: Some(format!("{:?}", etx.source)),
        description: if etx.description.is_empty() {
            None
        } else {
            Some(etx.description.clone())
        },
        fee: Some(etx.fee as i64),
        fee_payer: if etx.fee_payer.is_empty() {
            None
        } else {
            Some(etx.fee_payer.clone())
        },
        slot: Some(etx.slot as i64),
        account_data: etx.account_data.iter().map(convert_account_data).collect(),
        native_transfers: etx
            .native_transfers
            .as_ref()
            .map(|v| v.iter().map(convert_native_transfer).collect()),
        token_transfers: etx
            .token_transfers
            .as_ref()
            .map(|v| v.iter().map(convert_token_transfer).collect()),
        events: Some(convert_transaction_event(&etx.events)),
        transaction_error: etx
            .transaction_error
            .as_ref()
            .map(|e| serde_json::to_value(e).unwrap_or(serde_json::Value::Null)),
        instructions: etx.instructions.iter().map(convert_instruction).collect(),
        timestamp: Some(etx.timestamp as i64),
        other: serde_json::Value::Object(serde_json::Map::new()),
    }
}

fn convert_account_data(ad: &helius::types::AccountData) -> AccountData {
    AccountData {
        account: ad.account.clone(),
        native_token_balance: ad.native_token_balance.clone(),
        token_balance_changes: ad
            .token_balance_changes
            .as_ref()
            .map(|v| v.iter().map(convert_token_balance_change).collect()),
    }
}

fn convert_token_balance_change(
    tbc: &helius::types::TokenBalanceChange,
) -> TokenBalanceChange {
    TokenBalanceChange {
        user_account: tbc.user_account.clone(),
        token_account: tbc.token_account.clone(),
        raw_token_amount: RawTokenAmount {
            token_amount: tbc.raw_token_amount.token_amount.clone(),
            decimals: tbc.raw_token_amount.decimals.clone(),
        },
        mint: tbc.mint.clone(),
    }
}

fn convert_native_transfer(nt: &helius::types::NativeTransfer) -> NativeTransfer {
    NativeTransfer {
        from_user_account: nt.user_accounts.from_user_account.clone(),
        to_user_account: nt.user_accounts.to_user_account.clone(),
        amount: nt.amount.clone(),
    }
}

fn convert_token_transfer(tt: &helius::types::TokenTransfer) -> TokenTransfer {
    TokenTransfer {
        from_user_account: tt.user_accounts.from_user_account.clone(),
        to_user_account: tt.user_accounts.to_user_account.clone(),
        from_token_account: tt.from_token_account.clone(),
        to_token_account: tt.to_token_account.clone(),
        token_amount: tt.token_amount.clone(),
        token_standard: Some(format!("{:?}", tt.token_standard)),
        mint: tt.mint.clone(),
    }
}

fn convert_transaction_event(te: &helius::types::TransactionEvent) -> TransactionEvent {
    TransactionEvent {
        nft: te.nft.as_ref().map(|e| serde_json::to_value(e).unwrap_or(serde_json::Value::Null)),
        swap: te.swap.as_ref().map(convert_swap_event),
        compressed: te.compressed.as_ref().map(|v| {
            v.iter()
                .map(|e| serde_json::to_value(e).unwrap_or(serde_json::Value::Null))
                .collect()
        }),
        set_authority: te.set_authority.as_ref().map(|v| {
            v.iter()
                .map(|e| serde_json::to_value(e).unwrap_or(serde_json::Value::Null))
                .collect()
        }),
    }
}

fn convert_swap_event(se: &helius::types::SwapEvent) -> SwapEvent {
    SwapEvent {
        native_input: se.native_input.as_ref().map(convert_native_balance_change),
        native_output: se.native_output.as_ref().map(convert_native_balance_change),
        token_inputs: se.token_inputs.iter().map(convert_token_balance_change).collect(),
        token_outputs: se.token_outputs.iter().map(convert_token_balance_change).collect(),
        token_fees: se.token_fees.iter().map(convert_token_balance_change).collect(),
        native_fees: se.native_fees.iter().map(convert_native_balance_change).collect(),
        inner_swaps: se
            .inner_swaps
            .iter()
            .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null))
            .collect(),
    }
}

fn convert_native_balance_change(
    nbc: &helius::types::NativeBalanceChange,
) -> NativeBalanceChange {
    NativeBalanceChange {
        account: nbc.account.clone(),
        amount: nbc.amount.clone(),
    }
}

fn convert_instruction(inst: &helius::types::Instruction) -> Instruction {
    Instruction {
        accounts: inst.accounts.clone(),
        data: inst.data.clone(),
        program_id: inst.program_id.clone(),
        inner_instructions: inst
            .inner_instructions
            .iter()
            .map(|ii| serde_json::to_value(ii).unwrap_or(serde_json::Value::Null))
            .collect(),
    }
}

// ─── Helius SDK Client ───────────────────────────────────────────────────────

/// Helius client wrapping the official `helius::Helius` Rust SDK.
///
/// Constructed from an API key, targets Solana mainnet. Provides:
/// - Enhanced transaction history via `parsed_transaction_history()`
/// - DAS API token balances via `get_assets_by_owner()`
/// - Webhook management (delegated to SDK)
pub struct HeliusSdkClient {
    helius: helius::Helius,
    api_key: String,
}

impl HeliusSdkClient {
    /// Create a new Helius SDK client targeting Solana mainnet.
    ///
    /// Validates the API key is non-empty (per SDK's `ApiKey::new()`).
    /// Construction is synchronous — for WebSocket support use `HeliusBuilder`.
    pub fn new(api_key: &str) -> Result<Self> {
        let helius = helius::Helius::new(api_key, Cluster::MainnetBeta)
            .map_err(|e| anyhow::anyhow!("Failed to create Helius SDK client: {e}"))?;

        tracing::info!(
            "Helius SDK client initialized for mainnet"
        );

        Ok(Self {
            helius,
            api_key: api_key.to_string(),
        })
    }

    /// Returns the configured API key (for health-check logging).
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns a reference to the underlying SDK client (for advanced usage).
    pub fn inner(&self) -> &helius::Helius {
        &self.helius
    }

    // ─── Enhanced Transaction History ────────────────────────────────────────

    /// Get parsed (enhanced) transaction history for an address.
    ///
    /// Uses the official Helius SDK's `parsed_transaction_history()` method.
    /// Results are converted to our backward-compatible `ParsedTransaction` type.
    pub async fn get_transactions(
        &self,
        address: &str,
        tx_type: Option<&str>,
    ) -> Result<Vec<ParsedTransaction>> {
        let mut request = ParsedTransactionHistoryRequest {
            address: address.to_string(),
            before: None,
            until: None,
            commitment: None,
            source: None,
            transaction_type: None,
            limit: Some(100),
        };

        if let Some(t) = tx_type {
            // Map string type to SDK TransactionType enum.
            request.transaction_type = Some(Self::parse_transaction_type(t));
        }

        let enhanced_txs = self
            .helius
            .parsed_transaction_history(request)
            .await
            .map_err(|e| anyhow::anyhow!("Helius transaction history error: {e}"))?;

        let txs: Vec<ParsedTransaction> =
            enhanced_txs.iter().map(convert_enhanced_transaction).collect();

        tracing::debug!(
            address = address,
            count = txs.len(),
            "Fetched enhanced transaction history via Helius SDK"
        );

        Ok(txs)
    }

    /// Get a single parsed transaction by signature.
    ///
    /// Uses the SDK's `parse_transactions()` method with a single signature.
    pub async fn get_parsed_transaction(&self, signature: &str) -> Result<ParsedTransaction> {
        let request = helius::types::ParseTransactionsRequest {
            transactions: vec![signature.to_string()],
        };

        let enhanced_txs = self
            .helius
            .parse_transactions(request)
            .await
            .map_err(|e| anyhow::anyhow!("Helius parse transaction error: {e}"))?;

        enhanced_txs
            .first()
            .map(convert_enhanced_transaction)
            .ok_or_else(|| anyhow::anyhow!("No transaction returned for signature {}", signature))
    }

    // ─── DAS API Token Balances ─────────────────────────────────────────────

    /// Get all token balances for a wallet via the DAS API `getAssetsByOwner`.
    ///
    /// Returns tuples of `(mint_address, raw_amount, decimals)` for fungible tokens
    /// with non-zero balance. This replaces the old `spl-token CLI` fallback.
    pub async fn get_token_balances_das(
        &self,
        owner_address: &str,
    ) -> Result<Vec<(String, u64, u8)>> {
        let request = GetAssetsByOwner {
            owner_address: owner_address.to_string(),
            page: 1,
            limit: Some(1000),
            before: None,
            after: None,
            display_options: Some(DisplayOptions {
                show_fungible: true,
                ..Default::default()
            }),
            sort_by: None,
            cursor: None,
        };

        let asset_list = self
            .helius
            .rpc()
            .get_assets_by_owner(request)
            .await
            .map_err(|e| {
                anyhow::anyhow!("DAS API getAssetsByOwner failed for {}: {e}", owner_address)
            })?;

        let mut balances = Vec::new();

        for asset in &asset_list.items {
            // Only process fungible tokens (skip NFTs, compressed NFTs).
            if !matches!(
                asset.interface,
                helius::types::Interface::FungibleToken
                    | helius::types::Interface::FungibleAsset
            ) {
                continue;
            }

            // Extract balance and decimals from token_info.
            if let Some(ref token_info) = asset.token_info {
                let balance = token_info.balance.unwrap_or(0);
                let decimals = token_info.decimals.unwrap_or(0) as u8;

                if balance > 0 {
                    balances.push((asset.id.clone(), balance, decimals));
                }
            }
        }

        tracing::info!(
            wallet = owner_address,
            count = balances.len(),
            "DAS API returned token balances for wallet"
        );

        Ok(balances)
    }

    // ─── Legacy Balance Methods (kept for backward compat) ──────────────────

    /// Get token balances for an address.
    ///
    /// Kept for backward compatibility — internally uses DAS API.
    pub async fn get_balances(&self, address: &str) -> Result<BalancesResponse> {
        let das_balances = self.get_token_balances_das(address).await?;
        let tokens: Vec<TokenBalance> = das_balances
            .into_iter()
            .map(|(mint, raw_amount, decimals)| {
                let amount = if decimals > 0 {
                    raw_amount as f64 / 10f64.powi(decimals as i32)
                } else {
                    raw_amount as f64
                };
                TokenBalance {
                    address: mint.clone(),
                    amount,
                    decimals,
                    mint,
                }
            })
            .collect();

        Ok(BalancesResponse { tokens })
    }

    /// Get all token balances for an owner address (legacy name).
    pub async fn get_token_accounts(&self, owner: &str) -> Result<Vec<TokenBalance>> {
        let resp = self.get_balances(owner).await?;
        Ok(resp.tokens)
    }

    // ─── Webhook Management ─────────────────────────────────────────────────

    /// Create a webhook.
    pub async fn create_webhook(&self, config: &WebhookConfig) -> Result<WebhookConfig> {
        // Convert string transaction types to SDK TransactionType enum.
        let sdk_tx_types: Vec<helius::types::TransactionType> = config
            .transaction_types
            .iter()
            .map(|t| Self::parse_transaction_type(t))
            .collect();

        let request = helius::types::CreateWebhookRequest {
            webhook_url: config.webhook_url.clone(),
            transaction_types: sdk_tx_types,
            account_addresses: config.account_addresses.clone(),
            webhook_type: helius::types::WebhookType::Enhanced,
            auth_header: config.auth_header.clone(),
            txn_status: helius::types::TransactionStatus::All,
            encoding: helius::types::AccountWebhookEncoding::JsonParsed,
        };

        let response = self
            .helius
            .create_webhook(request)
            .await
            .map_err(|e| anyhow::anyhow!("Helius create webhook error: {e}"))?;

        // Convert SDK TransactionType enums back to strings.
        let tx_types: Vec<String> = response
            .transaction_types
            .iter()
            .map(|t| format!("{:?}", t))
            .collect();

        Ok(WebhookConfig {
            webhook_url: response.webhook_url,
            transaction_types: tx_types,
            account_addresses: response.account_addresses,
            auth_header: response.auth_header,
            webhook_id: Some(response.webhook_id),
        })
    }

    /// List all webhooks.
    pub async fn list_webhooks(&self) -> Result<Vec<WebhookConfig>> {
        let webhooks = self
            .helius
            .get_all_webhooks()
            .await
            .map_err(|e| anyhow::anyhow!("Helius list webhooks error: {e}"))?;

        Ok(webhooks
            .into_iter()
            .map(|w| {
                let tx_types: Vec<String> = w
                    .transaction_types
                    .iter()
                    .map(|t| format!("{:?}", t))
                    .collect();
                WebhookConfig {
                    webhook_url: w.webhook_url,
                    transaction_types: tx_types,
                    account_addresses: w.account_addresses,
                    auth_header: w.auth_header,
                    webhook_id: Some(w.webhook_id),
                }
            })
            .collect())
    }

    /// Register a webhook (convenience method).
    pub async fn register_webhook(&self, config: WebhookConfig) -> Result<String> {
        let resp = self.create_webhook(&config).await?;
        resp.webhook_id
            .ok_or_else(|| anyhow::anyhow!("Helius did not return a webhook ID"))
    }

    // ─── Health Check ───────────────────────────────────────────────────────

    /// Verify the Helius client can reach the RPC endpoint.
    pub fn health_check(&self) -> Result<()> {
        // Use the embedded Solana RPC client for a lightweight health check.
        match self.helius.connection().get_health() {
            Ok(_) => {
                tracing::info!("Helius RPC health check passed");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "Helius RPC health check failed");
                Err(anyhow::anyhow!("Helius RPC health check failed: {e}"))
            }
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    /// Parse a transaction type string into the SDK's `TransactionType` enum.
    ///
    /// Falls back to `TransactionType::Any` if the string doesn't match.
    fn parse_transaction_type(s: &str) -> helius::types::TransactionType {
        match s.to_uppercase().as_str() {
            "SWAP" => helius::types::TransactionType::Swap,
            "TRANSFER" => helius::types::TransactionType::Transfer,
            "NFT_SALE" | "NFTSALE" => helius::types::TransactionType::NftSale,
            "NFT_MINT" | "NFTMINT" => helius::types::TransactionType::NftMint,
            "NFT_CANCEL_LISTING" | "NFTCANCELLISTING" => {
                helius::types::TransactionType::NftCancelListing
            }
            "NFT_LISTING" | "NFTLISTING" => helius::types::TransactionType::NftListing,
            "NFT_BID" | "NFTBID" => helius::types::TransactionType::NftBid,
            "NFT_AUCTION_CREATED" | "NFTAuctionCreated" => {
                helius::types::TransactionType::NftAuctionCreated
            }
            "NFT_AUCTION_UPDATED" | "NFTAuctionUpdated" => {
                helius::types::TransactionType::NftAuctionUpdated
            }
            "NFT_AUCTION_CANCELLED" | "NFTAuctionCancelled" => {
                helius::types::TransactionType::NftAuctionCancelled
            }
            "UNKNOWN" => helius::types::TransactionType::Unknown,
            _ => helius::types::TransactionType::Any,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_enhanced_transaction_preserves_signature() {
        // Verify the conversion function maps the signature correctly.
        let etx = helius::types::EnhancedTransaction {
            signature: "test_sig_123".to_string(),
            transaction_type: helius::types::TransactionType::Swap,
            source: helius::types::Source::Jupiter,
            description: "Swapped 0.1 SOL for BONK".to_string(),
            fee: 5000,
            fee_payer: "fee_payer_pubkey".to_string(),
            slot: 200_000_000,
            account_data: vec![],
            native_transfers: None,
            token_transfers: None,
            transaction_error: None,
            instructions: vec![],
            events: helius::types::TransactionEvent::default(),
            timestamp: 1700000000,
        };

        let parsed = convert_enhanced_transaction(&etx);

        assert_eq!(parsed.signature, "test_sig_123");
        assert_eq!(parsed.tx_type.as_deref(), Some("Swap"));
        assert_eq!(parsed.source.as_deref(), Some("Jupiter"));
        assert_eq!(parsed.description.as_deref(), Some("Swapped 0.1 SOL for BONK"));
        assert_eq!(parsed.fee, Some(5000));
        assert_eq!(parsed.fee_payer.as_deref(), Some("fee_payer_pubkey"));
        assert_eq!(parsed.slot, Some(200_000_000));
        assert_eq!(parsed.timestamp, Some(1700000000));
    }

    #[test]
    fn test_convert_native_transfer() {
        let sdk_nt = helius::types::NativeTransfer {
            user_accounts: helius::types::TransferUserAccounts {
                from_user_account: Some("sender_wallet".to_string()),
                to_user_account: Some("receiver_wallet".to_string()),
            },
            amount: serde_json::Number::from(1_000_000_000u64),
        };

        let our_nt = convert_native_transfer(&sdk_nt);

        assert_eq!(
            our_nt.from_user_account.as_deref(),
            Some("sender_wallet")
        );
        assert_eq!(
            our_nt.to_user_account.as_deref(),
            Some("receiver_wallet")
        );
        assert_eq!(our_nt.amount, serde_json::Number::from(1_000_000_000u64));
    }

    #[test]
    fn test_convert_swap_event() {
        let sdk_swap = helius::types::SwapEvent {
            native_input: Some(helius::types::NativeBalanceChange {
                account: "wallet1".to_string(),
                amount: serde_json::Number::from(100_000_000u64),
            }),
            native_output: None,
            token_inputs: vec![],
            token_outputs: vec![helius::types::TokenBalanceChange {
                user_account: "wallet1".to_string(),
                token_account: "FAKE_TOKEN_ACCT_1111".to_string(),
                raw_token_amount: helius::types::RawTokenAmount {
                    token_amount: "1000000".to_string(),
                    decimals: serde_json::Number::from(6),
                },
                mint: "BONK_mint_address".to_string(),
            }],
            token_fees: vec![],
            native_fees: vec![],
            inner_swaps: vec![],
        };

        let our_swap = convert_swap_event(&sdk_swap);

        assert!(our_swap.native_input.is_some());
        assert!(our_swap.native_output.is_none());
        assert_eq!(our_swap.token_outputs.len(), 1);
        assert_eq!(our_swap.token_outputs[0].mint, "BONK_mint_address");
        assert_eq!(our_swap.token_outputs[0].user_account, "wallet1");
    }

    #[test]
    fn test_parse_transaction_type() {
        assert_eq!(
            HeliusSdkClient::parse_transaction_type("SWAP"),
            helius::types::TransactionType::Swap
        );
        assert_eq!(
            HeliusSdkClient::parse_transaction_type("transfer"),
            helius::types::TransactionType::Transfer
        );
        assert_eq!(
            HeliusSdkClient::parse_transaction_type("NFT_SALE"),
            helius::types::TransactionType::NftSale
        );
        assert_eq!(
            HeliusSdkClient::parse_transaction_type("unknown_type"),
            helius::types::TransactionType::Any
        );
    }

    #[test]
    fn test_helius_sdk_client_new_rejects_empty_key() {
        let result = HeliusSdkClient::new("");
        assert!(result.is_err(), "Empty API key should be rejected");
    }

    #[test]
    fn test_helius_sdk_client_new_rejects_whitespace_key() {
        let result = HeliusSdkClient::new("   ");
        assert!(result.is_err(), "Whitespace-only API key should be rejected");
    }
}
