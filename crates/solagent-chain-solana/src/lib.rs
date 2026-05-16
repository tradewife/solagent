//! # solagent-chain-solana
//!
//! Solana chain provider with RPC pool, keypair management, and pump.fun event parsing.

pub mod pump_fun;

use anyhow::Result;
use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer, Signature},
    transaction::Transaction,
};
use solana_client::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── Chain Provider Trait ────────────────────────────────────────────────────

/// Generic chain provider trait. Each chain implements this.
#[allow(async_fn_in_trait)]
pub trait ChainProvider {
    type Address;
    type TxHash;
    type Balance;

    async fn get_balance(&self, address: &Self::Address) -> Result<Self::Balance>;
    async fn get_token_balance(&self, address: &Self::Address, token_mint: &str) -> Result<Self::Balance>;
    async fn sign_and_send(&self, tx: &[u8]) -> Result<Self::TxHash>;
    async fn simulate_tx(&self, tx: &[u8]) -> Result<SimulateResult>;
}

/// Result of a transaction simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulateResult {
    pub success: bool,
    pub logs: Vec<String>,
    pub units_consumed: u64,
    pub error: Option<String>,
}

// ─── Solana Provider ─────────────────────────────────────────────────────────

/// Solana-specific chain provider with RPC pool and keypair management.
pub struct SolanaProvider {
    rpc_pool: Vec<Arc<RpcClient>>,
    current_rpc: RwLock<usize>,
    keypair: Arc<Keypair>,
    commitment: CommitmentConfig,
}

impl SolanaProvider {
    /// Create a new SolanaProvider from a list of RPC URLs and a base58-encoded private key.
    pub fn new(rpc_urls: Vec<String>, private_key_bs58: &str, commitment: CommitmentConfig) -> Result<Self> {
        let rpc_pool: Vec<Arc<RpcClient>> = rpc_urls
            .iter()
            .map(|url| Arc::new(RpcClient::new_with_commitment(url.clone(), commitment)))
            .collect();

        let keypair_bytes = bs58::decode(private_key_bs58).into_vec()?;
        let keypair = Keypair::try_from(keypair_bytes.as_slice())?;

        Ok(Self {
            rpc_pool,
            current_rpc: RwLock::new(0),
            keypair: Arc::new(keypair),
            commitment,
        })
    }

    /// Get the next RPC client in the pool (round-robin).
    async fn get_rpc(&self) -> Arc<RpcClient> {
        let mut idx = self.current_rpc.write().await;
        let rpc = self.rpc_pool[*idx].clone();
        *idx = (*idx + 1) % self.rpc_pool.len();
        rpc
    }

    /// Get the signer's public key.
    pub fn pubkeys(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Get the keypair reference.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// Get SOL balance for an address.
    pub async fn get_balance(&self, address: &Pubkey) -> Result<u64> {
        let rpc = self.get_rpc().await;
        let balance = rpc.get_balance_with_commitment(address, self.commitment)?.value;
        Ok(balance)
    }

    /// Get SPL token balance for an address and mint.
    pub async fn get_token_balance(&self, address: &Pubkey, token_mint: &str) -> Result<u64> {
        let rpc = self.get_rpc().await;
        let mint_pubkey: Pubkey = token_mint.parse()?;
        let resp = rpc.get_token_accounts_by_owner_with_commitment(
            address,
            TokenAccountsFilter::Mint(mint_pubkey),
            self.commitment,
        )?;

        // Parse the token balance from the first matching token account.
        let Some(account) = resp.value.first() else {
            return Ok(0); // No token account = 0 balance.
        };

        // Re-serialize and parse as generic JSON to extract amount reliably.
        let account_json = serde_json::to_value(&account.account)?;
        if let Some(data_val) = account_json.get("parsed").and_then(|p| p.get("info")) {
            // Try tokenAmount.amount first (standard parsed format).
            if let Some(amt) = data_val.get("tokenAmount").and_then(|t| t.get("amount")) {
                if let Some(s) = amt.as_str() {
                    if let Ok(v) = s.parse::<u64>() { return Ok(v); }
                }
            }
            // Fallback: direct "amount" field.
            if let Some(amt) = data_val.get("amount") {
                if let Some(s) = amt.as_str() {
                    if let Ok(v) = s.parse::<u64>() { return Ok(v); }
                }
                if let Some(v) = amt.as_u64() { return Ok(v); }
            }
        }

        // Legacy base64 data: decode the SPL Token Account structure.
        // The amount field is at bytes 64-72 (u64 LE).
        if let Some(b64) = account_json.get("data").and_then(|d| {
            // data is [base64_string, encoding] array.
            d.as_array().and_then(|a| a.first()).and_then(|v| v.as_str())
        }) {
            if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64) {
                if decoded.len() >= 72 {
                    let amount = u64::from_le_bytes(
                        decoded[64..72].try_into().unwrap_or([0u8; 8])
                    );
                    return Ok(amount);
                }
            }
        }

        Ok(0)
    }

    /// Build a swap transaction using Jupiter V6 quote API.
    /// This is a convenience method; the execution engine typically calls Jupiter directly.
    pub async fn build_swap_tx(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount: u64,
        slippage_bps: u32,
    ) -> Result<Transaction> {
        let jupiter = solagent_data::JupiterClient::new(
            "https://quote-api.jup.ag/v6".to_string(),
        );

        let quote = jupiter.get_quote(input_mint, output_mint, amount, slippage_bps).await?;
        let swap_tx = jupiter.get_swap_transaction(
            &quote,
            &self.keypair().pubkey().to_string(),
        ).await?;

        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(&swap_tx.swap_transaction)?;
        let mut tx: Transaction = bincode::deserialize(&tx_bytes)?;
        tx.sign(&[self.keypair()], tx.message.recent_blockhash);
        Ok(tx)
    }    /// Sign and send a transaction.
    pub async fn sign_and_send(&self, tx: &Transaction) -> Result<Signature> {
        let rpc = self.get_rpc().await;
        let signature = rpc.send_and_confirm_transaction(tx)?;
        Ok(signature)
    }

    /// Sign and send a versioned transaction (V0 with Address Lookup Tables).
    /// Jupiter V6 returns V0 transactions that require this method.
    pub async fn sign_and_send_versioned(
        &self,
        vtx: &solana_sdk::transaction::VersionedTransaction,
    ) -> Result<Signature> {
        let rpc = self.get_rpc().await;
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            max_retries: Some(0),
            ..Default::default()
        };
        let signature = rpc.send_transaction_with_config(vtx, config)?;
        // Wait for confirmation.
        let confirmed = rpc.confirm_transaction(&signature)?;
        if confirmed {
            Ok(signature)
        } else {
            anyhow::bail!("Transaction {} was not confirmed", signature);
        }
    }

    /// Simulate a transaction without sending it.
    pub async fn simulate_tx(&self, tx: &Transaction) -> Result<SimulateResult> {
        let rpc = self.get_rpc().await;
        let result = rpc.simulate_transaction(tx)?;
        Ok(SimulateResult {
            success: result.value.err.is_none(),
            logs: result.value.logs.unwrap_or_default(),
            units_consumed: result.value.units_consumed.unwrap_or(0),
            error: result.value.err.map(|e| format!("{e:?}")),
        })
    }
}

impl ChainProvider for SolanaProvider {
    type Address = Pubkey;
    type TxHash = Signature;
    type Balance = u64;

    async fn get_balance(&self, address: &Pubkey) -> Result<u64> {
        self.get_balance(address).await
    }

    async fn get_token_balance(&self, address: &Pubkey, token_mint: &str) -> Result<u64> {
        self.get_token_balance(address, token_mint).await
    }

    async fn sign_and_send(&self, tx: &[u8]) -> Result<Signature> {
        let transaction: Transaction = bincode::deserialize(tx)?;
        self.sign_and_send(&transaction).await
    }

    async fn simulate_tx(&self, tx: &[u8]) -> Result<SimulateResult> {
        let transaction: Transaction = bincode::deserialize(tx)?;
        self.simulate_tx(&transaction).await
    }
}
