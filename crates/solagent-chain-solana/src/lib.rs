//! # solagent-chain-solana
//!
//! Solana chain provider with RPC pool, keypair management, and pump.fun event parsing.

pub mod pump_fun;

use anyhow::Result;
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
        let token_accounts = rpc.get_token_accounts_by_owner_with_commitment(
            address,
            TokenAccountsFilter::Mint(mint_pubkey),
            self.commitment,
        )?;
        // Parse token balance from token accounts.
        // TODO: implement proper UiAccountData parsing for token amounts.
        let _ = token_accounts;
        todo!("Parse token balance from get_token_accounts_by_owner response")
    }

    /// Build a swap transaction (delegates to Jupiter integration).
    pub async fn build_swap_tx(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount: u64,
        _slippage_bps: u32,
    ) -> Result<Transaction> {
        let _rpc = self.get_rpc().await;
        todo!("Build swap tx via Jupiter API integration")
    }

    /// Sign and send a transaction.
    pub async fn sign_and_send(&self, tx: &Transaction) -> Result<Signature> {
        let rpc = self.get_rpc().await;
        let signature = rpc.send_and_confirm_transaction(tx)?;
        Ok(signature)
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

    async fn sign_and_send(&self, _tx: &[u8]) -> Result<Signature> {
        let _rpc = self.get_rpc().await;
        todo!("Deserialize transaction from bytes and send")
    }

    async fn simulate_tx(&self, _tx: &[u8]) -> Result<SimulateResult> {
        let _rpc = self.get_rpc().await;
        todo!("Deserialize transaction from bytes and simulate")
    }
}
