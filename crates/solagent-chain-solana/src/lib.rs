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
            if let Some(amt) = data_val.get("tokenAmount").and_then(|t| t.get("amount"))
                && let Some(s) = amt.as_str()
                && let Ok(v) = s.parse::<u64>()
            {
                return Ok(v);
            }
            // Fallback: direct "amount" field.
            if let Some(amt) = data_val.get("amount") {
                if let Some(s) = amt.as_str()
                    && let Ok(v) = s.parse::<u64>()
                {
                    return Ok(v);
                }
                if let Some(v) = amt.as_u64() { return Ok(v); }
            }
        }

        // Legacy base64 data: decode the SPL Token Account structure.
        // The amount field is at bytes 64-72 (u64 LE).
        if let Some(b64) = account_json.get("data").and_then(|d| {
            // data is [base64_string, encoding] array.
            d.as_array().and_then(|a| a.first()).and_then(|v| v.as_str())
        })
            && let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64)
            && decoded.len() >= 72
        {
            let amount = u64::from_le_bytes(
                decoded[64..72].try_into().unwrap_or([0u8; 8])
            );
            return Ok(amount);
        }

        Ok(0)
    }

    /// List all SPL token accounts owned by this wallet, returning (mint_address, raw_amount).
    /// Falls back from Helius to public RPC on failure.
    pub async fn get_all_token_balances(&self) -> Result<Vec<(String, u64)>> {
        match self.get_all_token_balances_inner().await {
            Ok(balances) => Ok(balances),
            Err(e) => {
                tracing::warn!(error = %e, "Helius token balance fetch failed, trying public RPC");
                self.get_all_token_balances_public().await
            }
        }
    }

    async fn get_all_token_balances_inner(&self) -> Result<Vec<(String, u64)>> {
        let address = self.pubkeys();
        let rpc = self.get_rpc().await;
        // Use ProgramId filter for the SPL Token program to get all token accounts.
        let spl_token_program: Pubkey = spl_token::ID;
        let resp = rpc.get_token_accounts_by_owner_with_commitment(
            &address,
            TokenAccountsFilter::ProgramId(spl_token_program),
            self.commitment,
        )?;

        let mut balances = Vec::new();
        for account in resp.value {
            let account_json = serde_json::to_value(&account.account)?;

            // Try parsed format first.
            if let Some(data_val) = account_json.get("parsed").and_then(|p| p.get("info")) {
                let mint = data_val.get("mint").and_then(|m| m.as_str()).map(|s| s.to_string());
                let amount = data_val.get("tokenAmount")
                    .and_then(|t| t.get("amount"))
                    .and_then(|s| s.as_str())
                    .and_then(|s| s.parse::<u64>().ok());
                if let (Some(mint), Some(amount)) = (mint, amount) {
                    if amount > 0 {
                        balances.push((mint, amount));
                    }
                    continue;
                }
            }

            // Fallback: extract mint from account data (bytes 0-32) and amount (bytes 64-72).
            if let Some(b64) = account_json.get("data").and_then(|d| {
                d.as_array().and_then(|a| a.first()).and_then(|v| v.as_str())
            }) {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64)
                    && decoded.len() >= 72
                {
                    let mint_bytes: [u8; 32] = decoded[0..32].try_into().unwrap_or([0u8; 32]);
                    let mint = bs58::encode(mint_bytes).into_string();
                    let amount = u64::from_le_bytes(
                        decoded[64..72].try_into().unwrap_or([0u8; 8])
                    );
                    if amount > 0 {
                        balances.push((mint, amount));
                    }
                }
            }
        }

        Ok(balances)
    }

    /// Public RPC fallback for token balance fetching.
    /// Uses `spl-token accounts` CLI, which correctly discovers Associated Token Accounts
    /// (ATAs). The `ProgramId` filter used by the Solana RPC `getTokenAccountsByOwner` only
    /// returns accounts owned directly by the wallet's keypair — not ATAs owned by the
    /// ATA program `ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL`.
    async fn get_all_token_balances_public(&self) -> Result<Vec<(String, u64)>> {
        let pubkey = self.keypair.pubkey().to_string();
        tracing::info!(wallet = %pubkey, "Fetching token balances via spl-token CLI");

        let output = tokio::process::Command::new("spl-token")
            .args([
                "accounts",
                "--owner",
                &pubkey,
                "--url",
                "https://api.mainnet-beta.solana.com",
                "--output",
                "json-compact",
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("spl-token CLI failed: {stderr}");
        }

        #[derive(::serde::Deserialize)]
        struct SplTokenAccount {
            mint: String,
            #[serde(rename = "tokenAmount")]
            token_amount: SplTokenAmount,
        }
        #[derive(::serde::Deserialize)]
        struct SplTokenAmount {
            amount: String,
            #[allow(dead_code)]
            decimals: u8,
        }
        #[derive(::serde::Deserialize)]
        struct SplTokenAccountsOutput {
            accounts: Vec<SplTokenAccount>,
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: SplTokenAccountsOutput = serde_json::from_str(&stdout)?;

        let mut balances = Vec::new();
        for account in parsed.accounts {
            if let Ok(raw_amount) = account.token_amount.amount.parse::<u64>() {
                if raw_amount > 0 {
                    balances.push((account.mint, raw_amount));
                }
            }
        }

        tracing::info!(
            wallet = %pubkey,
            count = balances.len(),
            "spl-token CLI returned token balances"
        );
        Ok(balances)
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
    ///
    /// Includes retry with exponential backoff for confirmation (1s, 2s, 4s)
    /// and falls back to the public Solana RPC if the primary RPC fails.
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

        // Confirm the transaction with retry + exponential backoff.
        // Use the primary RPC first, then fall back to public Solana RPC.
        let backoff_secs = [1, 2, 4];
        for (i, delay) in backoff_secs.iter().enumerate() {
            let attempt = i + 1;
            // Sleep before retrying (skip on first attempt since we just sent).
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
            }

            // Try primary RPC first.
            match rpc.confirm_transaction(&signature) {
                Ok(true) => {
                    tracing::info!(
                        signature = %signature,
                        attempt,
                        "Transaction confirmed via primary RPC"
                    );
                    return Ok(signature);
                }
                Ok(false) => {
                    tracing::warn!(
                        signature = %signature,
                        attempt,
                        "Transaction not yet confirmed (retrying)"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        signature = %signature,
                        attempt,
                        error = %e,
                        "Primary RPC confirmation failed, trying public RPC"
                    );
                    // Fall through to public RPC fallback.
                }
            }

            // Fall back to public Solana RPC for confirmation.
            match self.confirm_via_public_rpc(&signature).await {
                Ok(true) => {
                    tracing::info!(
                        signature = %signature,
                        attempt,
                        "Transaction confirmed via public RPC fallback"
                    );
                    return Ok(signature);
                }
                Ok(false) => {
                    tracing::warn!(
                        signature = %signature,
                        attempt,
                        "Public RPC: transaction not yet confirmed (retrying)"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        signature = %signature,
                        attempt,
                        error = %e,
                        "Public RPC confirmation also failed"
                    );
                }
            }
        }

        // Final attempt: if all retries exhausted, still try the public RPC one more time.
        // Even if confirmation fails, the transaction may have been submitted successfully.
        // Return the signature so the caller can record the trade — the monitor loop
        // will reconcile positions from on-chain state.
        match self.confirm_via_public_rpc(&signature).await {
            Ok(true) => {
                tracing::info!(signature = %signature, "Transaction confirmed via public RPC on final attempt");
                Ok(signature)
            }
            Ok(false) => {
                tracing::warn!(
                    signature = %signature,
                    "Transaction MAY have succeeded but could not be confirmed. Returning signature for reconciliation."
                );
                // Return the signature anyway — the transaction was submitted.
                // The reconcile_positions() method will catch on-chain positions later.
                Ok(signature)
            }
            Err(e) => {
                tracing::error!(
                    signature = %signature,
                    error = %e,
                    "All confirmation attempts exhausted, returning signature for reconciliation"
                );
                // Still return Ok — the send itself succeeded.
                Ok(signature)
            }
        }
    }

    /// Confirm a transaction via the public Solana RPC (api.mainnet-beta.solana.com).
    async fn confirm_via_public_rpc(&self, signature: &Signature) -> Result<bool> {
        let public_rpc = RpcClient::new_with_commitment(
            "https://api.mainnet-beta.solana.com".to_string(),
            self.commitment,
        );
        public_rpc.confirm_transaction(signature).map_err(|e| {
            anyhow::anyhow!("Public RPC confirm failed: {e}")
        })
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
