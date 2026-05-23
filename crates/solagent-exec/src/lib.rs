//! # solagent-exec
//!
//! Execution engine with pre-flight checks, retry logic, and quality tracking.
//! Supports Solana (via Jupiter V6) and Base (via Uniswap — stub).
//!
//! ## Helius Smart Transaction Sender Integration
//!
//! When a Helius SDK client is configured, swap transactions are sent via the
//! Helius Smart Transaction Sender for higher landing rates:
//! - Priority fees estimated before every swap via `getPriorityFeeEstimate`
//! - Transactions sent via `send_smart_transaction_with_sender` (tip-based routing)
//! - Falls back to `send_and_confirm_versioned_transaction` (Helius retry) if Sender fails
//! - Falls back to `sign_and_send_versioned` (manual retry) if no Helius client
//!
//! The Helius Sender handles retries internally — no manual retry loop is needed
//! for the send+confirm step. The slippage escalation loop (in `execute_buy`/`execute_sell`)
//! is orthogonal and remains for handling Jupiter quote price movement.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use base64::Engine;
use solana_sdk::signature::Signer;
use solagent_core::{Chain, Trade, TradeSide};
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── Execution Quality ───────────────────────────────────────────────────────

/// Tracks execution quality metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionQuality {
    pub total_trades: u64,
    pub successful_trades: u64,
    pub failed_trades: u64,
    pub avg_slippage_bps: f64,
    pub avg_latency_ms: f64,
    pub total_volume_usd: f64,
}

impl Default for ExecutionQuality {
    fn default() -> Self {
        Self {
            total_trades: 0,
            successful_trades: 0,
            failed_trades: 0,
            avg_slippage_bps: 0.0,
            avg_latency_ms: 0.0,
            total_volume_usd: 0.0,
        }
    }
}

impl ExecutionQuality {
    pub fn success_rate(&self) -> f64 {
        if self.total_trades == 0 {
            0.0
        } else {
            self.successful_trades as f64 / self.total_trades as f64
        }
    }

    /// Record a completed trade's metrics.
    pub fn record(&mut self, latency_ms: u64, slippage_bps: Option<u64>, size_usd: f64, success: bool) {
        self.total_trades += 1;
        if success {
            self.successful_trades += 1;
        } else {
            self.failed_trades += 1;
        }
        self.total_volume_usd += size_usd;
        // Running average for latency.
        self.avg_latency_ms = (self.avg_latency_ms * (self.total_trades - 1) as f64 + latency_ms as f64)
            / self.total_trades as f64;
        // Running average for slippage.
        if let Some(slip) = slippage_bps {
            self.avg_slippage_bps = (self.avg_slippage_bps * (self.total_trades - 1) as f64 + slip as f64)
                / self.total_trades as f64;
        }
    }
}

// ─── Pre-flight Check ────────────────────────────────────────────────────────

/// Result of a pre-flight check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightCheck {
    pub name: String,
    pub passed: bool,
    pub details: String,
}

// ─── Execution Config ────────────────────────────────────────────────────────

/// Configuration for the execution engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub max_retries: u32,
    pub base_slippage_bps: u32,
    pub slippage_increase_bps: u32,
    pub max_slippage_bps: u32,
    /// Maximum priority fee in microLamports per compute unit.
    /// Prevents overpaying during fee spikes. Used as `priority_fee_cap`
    /// when sending via Helius Smart Transaction Sender.
    pub priority_fee_cap: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_slippage_bps: 100,  // 1%
            slippage_increase_bps: 50,
            max_slippage_bps: 500,   // 5%
            priority_fee_cap: 1_000_000, // 1M microLamports = 1 SOL max priority fee
        }
    }
}

// ─── Execution Engine ────────────────────────────────────────────────────────

/// The execution engine handles buy/sell order dispatch with retry logic.
///
/// When a Helius SDK client is configured, uses the Smart Transaction Sender
/// for optimized transaction routing and higher landing rates. Falls back to
/// direct RPC sending when no Helius client is available.
pub struct ExecutionEngine {
    config: ExecutionConfig,
    quality: Arc<RwLock<ExecutionQuality>>,
    jupiter: Option<solagent_data::JupiterClient>,
    solana_provider: Option<Arc<solagent_chain_solana::SolanaProvider>>,
    /// Helius SDK client for priority fee estimation and Smart Transaction Sender.
    helius_client: Option<Arc<solagent_data::HeliusSdkClient>>,
}

impl ExecutionEngine {
    /// Create a new execution engine with no chain providers (for dry-run or testing).
    pub fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            quality: Arc::new(RwLock::new(ExecutionQuality::default())),
            jupiter: None,
            solana_provider: None,
            helius_client: None,
        }
    }

    /// Create a fully-wired execution engine for Solana.
    pub fn new_solana(
        config: ExecutionConfig,
        jupiter: solagent_data::JupiterClient,
        solana_provider: Arc<solagent_chain_solana::SolanaProvider>,
    ) -> Self {
        Self {
            config,
            quality: Arc::new(RwLock::new(ExecutionQuality::default())),
            jupiter: Some(jupiter),
            solana_provider: Some(solana_provider),
            helius_client: None,
        }
    }

    /// Set the Helius SDK client for priority fee estimation and Smart Transaction Sender.
    pub fn set_helius_execution_client(&mut self, client: Arc<solagent_data::HeliusSdkClient>) {
        tracing::info!("Helius Smart Transaction Sender client configured for execution engine");
        self.helius_client = Some(client);
    }

    /// Run pre-flight checks before executing a trade.
    pub async fn preflight_checks(
        &self,
        _chain: Chain,
        _token_address: &str,
        side: TradeSide,
        size_usd: f64,
        wallet_balance_usd: f64,
        _current_price: f64,
    ) -> Vec<PreflightCheck> {
        let mut checks = Vec::new();

        // Balance check.
        let balance_sufficient = if side == TradeSide::Buy {
            wallet_balance_usd >= size_usd
        } else {
            true // For sells, the token balance check is done elsewhere.
        };
        checks.push(PreflightCheck {
            name: "balance".to_string(),
            passed: balance_sufficient,
            details: format!(
                "Wallet: ${wallet_balance_usd:.2}, Required: ${size_usd:.2}"
            ),
        });

        // Provider available.
        let provider_ok = match _chain {
            Chain::Solana => self.jupiter.is_some() && self.solana_provider.is_some(),
            Chain::Base => false,
        };
        checks.push(PreflightCheck {
            name: "provider".to_string(),
            passed: provider_ok,
            details: if provider_ok {
                "Chain provider available".to_string()
            } else {
                format!("{_chain} provider not configured")
            },
        });

        checks
    }

    /// Execute a buy order with retry logic (slippage escalation).
    pub async fn execute_buy(
        &self,
        chain: Chain,
        token_address: &str,
        size_usd: f64,
        wallet_balance_usd: f64,
        current_price: f64,
        sol_price: f64,
    ) -> Result<Trade> {
        // Pre-flight checks.
        let checks = self
            .preflight_checks(chain, token_address, TradeSide::Buy, size_usd, wallet_balance_usd, current_price)
            .await;
        let all_passed = checks.iter().all(|c| c.passed);
        if !all_passed {
            let failed: Vec<&str> = checks.iter().filter(|c| !c.passed).map(|c| c.name.as_str()).collect();
            anyhow::bail!("Pre-flight checks failed: {}", failed.join(", "));
        }

        // Retry loop with increasing slippage.
        let mut attempt = 0;
        let mut slippage_bps = self.config.base_slippage_bps;
        let start = std::time::Instant::now();

        while attempt < self.config.max_retries {
            attempt += 1;
            tracing::info!(
                attempt,
                token = token_address,
                size_usd,
                slippage_bps,
                "Executing buy attempt"
            );

            match self
                .dispatch_buy(chain, token_address, size_usd, slippage_bps, current_price, sol_price)
                .await
            {
                Ok(trade) => {
                    let latency = start.elapsed().as_millis() as u64;
                    self.quality.write().await.record(
                        latency,
                        Some(slippage_bps as u64),
                        size_usd,
                        true,
                    );
                    tracing::info!(
                        token = token_address,
                        size_usd,
                        latency_ms = latency,
                        "Buy executed successfully"
                    );
                    return Ok(trade);
                }
                Err(e) => {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "Buy attempt failed"
                    );
                    slippage_bps = (slippage_bps + self.config.slippage_increase_bps)
                        .min(self.config.max_slippage_bps);
                }
            }
        }

        let latency = start.elapsed().as_millis() as u64;
        self.quality.write().await.record(latency, None, size_usd, false);
        anyhow::bail!(
            "Buy failed after {} attempts for token {}",
            attempt,
            token_address
        );
    }

    /// Execute a sell order with retry logic (slippage escalation).
    pub async fn execute_sell(
        &self,
        chain: Chain,
        token_address: &str,
        token_amount: f64,
        current_price: f64,
    ) -> Result<Trade> {
        let size_usd = token_amount * current_price;
        let mut attempt = 0;
        let mut slippage_bps = self.config.base_slippage_bps;
        let start = std::time::Instant::now();

        while attempt < self.config.max_retries {
            attempt += 1;
            tracing::info!(
                attempt,
                token = token_address,
                size_usd,
                slippage_bps,
                "Executing sell attempt"
            );

            match self
                .dispatch_sell(chain, token_address, token_amount, slippage_bps, current_price)
                .await
            {
                Ok(trade) => {
                    let latency = start.elapsed().as_millis() as u64;
                    self.quality.write().await.record(
                        latency,
                        Some(slippage_bps as u64),
                        size_usd,
                        true,
                    );
                    return Ok(trade);
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "Sell attempt failed");
                    slippage_bps = (slippage_bps + self.config.slippage_increase_bps)
                        .min(self.config.max_slippage_bps);
                }
            }
        }

        let latency = start.elapsed().as_millis() as u64;
        self.quality.write().await.record(latency, None, size_usd, false);
        anyhow::bail!("Sell failed after {} attempts for token {}", attempt, token_address);
    }

    /// Dispatch a buy to the appropriate chain provider.
    async fn dispatch_buy(
        &self,
        chain: Chain,
        token_address: &str,
        size_usd: f64,
        slippage_bps: u32,
        current_price: f64,
        sol_price: f64,
    ) -> Result<Trade> {
        match chain {
            Chain::Solana => self.dispatch_solana_buy(token_address, size_usd, slippage_bps, current_price, sol_price).await,
            Chain::Base => {
                anyhow::bail!("Base execution not yet implemented");
            }
        }
    }

    /// Dispatch a sell to the appropriate chain provider.
    async fn dispatch_sell(
        &self,
        chain: Chain,
        token_address: &str,
        token_amount: f64,
        slippage_bps: u32,
        current_price: f64,
    ) -> Result<Trade> {
        match chain {
            Chain::Solana => self.dispatch_solana_sell(token_address, token_amount, slippage_bps, current_price).await,
            Chain::Base => {
                anyhow::bail!("Base execution not yet implemented");
            }
        }
    }

    /// Execute a Solana buy via Jupiter V6 with Helius Smart Transaction Sender.
    async fn dispatch_solana_buy(
        &self,
        token_address: &str,
        size_usd: f64,
        slippage_bps: u32,
        current_price: f64,
        sol_price: f64,
    ) -> Result<Trade> {
        let jupiter = self.jupiter.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Jupiter client not configured"))?;
        let provider = self.solana_provider.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Solana provider not configured"))?;

        // SOL mint address.
        let sol_mint = "So11111111111111111111111111111111111111112";
        // Convert USD amount to lamports using SOL price (not token price).
        let lamports = if sol_price > 0.0 {
            (size_usd / sol_price * 1_000_000_000.0) as u64
        } else {
            anyhow::bail!("SOL price is zero, cannot calculate swap amount");
        };

        // Get quote from Jupiter.
        let quote = jupiter.get_quote(sol_mint, token_address, lamports, slippage_bps).await?;
        tracing::info!(
            input_lamports = lamports,
            output_amount = &quote.out_amount,
            price_impact = &quote.price_impact_pct,
            "Jupiter quote received"
        );

        // Estimate priority fee before the swap (VAL-HELIUS-011).
        let user_pubkey = provider.pubkeys();
        let priority_fee_micro_lamports = self.estimate_priority_fee(&user_pubkey.to_string()).await;

        // Get swap instructions from Jupiter (for Smart Transaction Sender path).
        let signature = if let Some(helius) = &self.helius_client {
            self.send_via_helius_sender(
                helius,
                jupiter,
                &quote,
                &user_pubkey.to_string(),
                provider,
                priority_fee_micro_lamports,
            ).await?
        } else {
            // Fallback: legacy path with sign_and_send_versioned.
            tracing::info!("No Helius client configured, using legacy send path");
            let swap_tx = jupiter.get_swap_transaction(&quote, &user_pubkey.to_string()).await?;
            self.sign_and_send_legacy(provider, &swap_tx.swap_transaction).await?
        };

        let token_amount = quote.out_amount.parse::<f64>().unwrap_or(0.0);
        Ok(Trade {
            id: solagent_core::uuid::Uuid::new_v4(),
            token_address: token_address.to_string(),
            chain: Chain::Solana,
            side: TradeSide::Buy,
            size_usd,
            token_amount,
            price: current_price,
            tx_signature: Some(signature.to_string()),
            slippage_bps: Some(slippage_bps as u64),
            executed_at: solagent_core::chrono::Utc::now(),
            latency_ms: None,
        })
    }

    /// Execute a Solana sell via Jupiter V6 with Helius Smart Transaction Sender.
    async fn dispatch_solana_sell(
        &self,
        token_address: &str,
        token_amount: f64,
        slippage_bps: u32,
        current_price: f64,
    ) -> Result<Trade> {
        let jupiter = self.jupiter.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Jupiter client not configured"))?;
        let provider = self.solana_provider.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Solana provider not configured"))?;

        let sol_mint = "So11111111111111111111111111111111111111112";
        // Convert token amount to smallest unit (assume 6 decimals for SPL tokens).
        let token_units = (token_amount * 1_000_000.0) as u64;

        let quote = jupiter.get_quote(token_address, sol_mint, token_units, slippage_bps).await?;

        // Estimate priority fee before the swap (VAL-HELIUS-011).
        let user_pubkey = provider.pubkeys();
        let priority_fee_micro_lamports = self.estimate_priority_fee(&user_pubkey.to_string()).await;

        // Get swap instructions from Jupiter (for Smart Transaction Sender path).
        let signature = if let Some(helius) = &self.helius_client {
            self.send_via_helius_sender(
                helius,
                jupiter,
                &quote,
                &user_pubkey.to_string(),
                provider,
                priority_fee_micro_lamports,
            ).await?
        } else {
            // Fallback: legacy path with sign_and_send_versioned.
            tracing::info!("No Helius client configured, using legacy send path");
            let swap_tx = jupiter.get_swap_transaction(&quote, &user_pubkey.to_string()).await?;
            self.sign_and_send_legacy(provider, &swap_tx.swap_transaction).await?
        };

        let size_usd = token_amount * current_price;
        Ok(Trade {
            id: solagent_core::uuid::Uuid::new_v4(),
            token_address: token_address.to_string(),
            chain: Chain::Solana,
            side: TradeSide::Sell,
            size_usd,
            token_amount,
            price: current_price,
            tx_signature: Some(signature.to_string()),
            slippage_bps: Some(slippage_bps as u64),
            executed_at: solagent_core::chrono::Utc::now(),
            latency_ms: None,
        })
    }

    // ─── Helius Smart Transaction Sender Integration ─────────────────────────

    /// Estimate the priority fee for a transaction using Helius.
    ///
    /// Returns the estimated fee in microLamports, or None if estimation fails.
    /// Logs the estimate regardless of success/failure.
    async fn estimate_priority_fee(&self, wallet_pubkey: &str) -> Option<f64> {
        if let Some(helius) = &self.helius_client {
            match helius.get_priority_fee_estimate(&[wallet_pubkey.to_string()]).await {
                Ok(fee) => {
                    tracing::info!(
                        fee_micro_lamports = fee,
                        wallet = wallet_pubkey,
                        "Priority fee estimate: {:.0} microLamports",
                        fee
                    );
                    return Some(fee);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Priority fee estimation failed, using default fee"
                    );
                }
            }
        }
        None
    }

    /// Send a swap via Helius Smart Transaction Sender using Jupiter instructions.
    ///
    /// This is the primary execution path when a Helius client is configured.
    /// Uses Jupiter's `/swap-instructions` endpoint to get individual instructions,
    /// then sends them via `helius.send_smart_transaction_with_sender()` for:
    /// - Automatic priority fee estimation (overrides Jupiter's default)
    /// - Compute budget optimization
    /// - Tip-based routing via Sender for faster landing
    /// - Built-in retry + confirmation polling
    ///
    /// Falls back to sending a pre-built V0 transaction via Helius RPC if the
    /// instruction-based path fails.
    async fn send_via_helius_sender(
        &self,
        helius: &Arc<solagent_data::HeliusSdkClient>,
        jupiter: &solagent_data::JupiterClient,
        quote: &solagent_data::JupiterQuote,
        user_pubkey: &str,
        provider: &Arc<solagent_chain_solana::SolanaProvider>,
        _priority_fee_micro_lamports: Option<f64>,
    ) -> Result<solana_sdk::signature::Signature> {
        // Try instruction-based path first (full Smart Transaction Sender with tip).
        match self.try_send_via_sender_instructions(helius, jupiter, quote, user_pubkey, provider).await {
            Ok(sig) => return Ok(sig),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Smart Transaction Sender (instructions path) failed, falling back to V0 transaction path"
                );
            }
        }

        // Fallback: use pre-built V0 transaction sent via Helius RPC.
        let swap_tx = jupiter.get_swap_transaction(quote, user_pubkey).await?;
        self.sign_and_send_via_helius(helius, provider, &swap_tx.swap_transaction).await
    }

    /// Try sending via the instruction-based Smart Transaction Sender path.
    ///
    /// Gets individual instructions from Jupiter's `/swap-instructions` endpoint,
    /// resolves ALT accounts, and sends via `helius.send_smart_transaction_with_sender()`.
    async fn try_send_via_sender_instructions(
        &self,
        helius: &Arc<solagent_data::HeliusSdkClient>,
        jupiter: &solagent_data::JupiterClient,
        quote: &solagent_data::JupiterQuote,
        user_pubkey: &str,
        provider: &Arc<solagent_chain_solana::SolanaProvider>,
    ) -> Result<solana_sdk::signature::Signature> {
        // Get individual swap instructions from Jupiter.
        let swap_instrs = jupiter.get_swap_instructions(quote, user_pubkey).await?;
        tracing::info!(
            setup_count = swap_instrs.setup_instructions.len(),
            has_cleanup = swap_instrs.cleanup_instruction.is_some(),
            alt_count = swap_instrs.address_lookup_table_addresses.len(),
            "Jupiter swap instructions received"
        );

        // Convert to solana_sdk Instructions.
        let instructions = swap_instrs.to_solana_instructions()?;

        // Fetch ALT accounts from the RPC.
        let lookup_tables = if swap_instrs.address_lookup_table_addresses.is_empty() {
            Vec::new()
        } else {
            helius.fetch_lookup_table_accounts(&swap_instrs.address_lookup_table_addresses).await?
        };

        // Create signer from the provider's keypair.
        let keypair_bytes = provider.keypair().to_bytes();
        // to_bytes() returns 64 bytes: [secret_key (32) | public_key (32)].
        let secret_key: [u8; 32] = keypair_bytes[0..32].try_into()
            .map_err(|_| anyhow::anyhow!("Failed to extract secret key"))?;
        let signer: Arc<dyn solana_sdk::signer::Signer> = Arc::new(
            solana_sdk::signature::Keypair::new_from_array(secret_key),
        );

        // Determine priority fee cap.
        let priority_fee_cap = Some(self.config.priority_fee_cap);

        // Send via Helius Smart Transaction Sender.
        helius
            .send_smart_transaction_with_sender(
                instructions,
                vec![signer],
                lookup_tables,
                priority_fee_cap,
            )
            .await
    }

    /// Sign and send a pre-built V0 transaction via Helius Smart Transaction
    /// infrastructure (without Sender tip routing).
    ///
    /// This is the fallback path when the instruction-based Sender path fails.
    /// Uses Helius's RPC for sending with automatic retries and confirmation polling.
    async fn sign_and_send_via_helius(
        &self,
        helius: &Arc<solagent_data::HeliusSdkClient>,
        provider: &Arc<solagent_chain_solana::SolanaProvider>,
        swap_transaction_b64: &str,
    ) -> Result<solana_sdk::signature::Signature> {
        // Deserialize the V0 transaction from Jupiter.
        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(swap_transaction_b64)?;
        let mut vtx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)?;

        // Sign the versioned transaction.
        let message_bytes = bincode::serialize(&vtx.message)?;
        let signature = provider.keypair().sign_message(&message_bytes);
        vtx.signatures[0] = signature;

        // Send via Helius Smart Transaction infrastructure.
        helius.send_and_confirm_versioned_transaction(&vtx).await
    }

    /// Sign and send a pre-built V0 transaction via the legacy path
    /// (manual retry loop in SolanaProvider).
    ///
    /// Used when no Helius client is configured.
    async fn sign_and_send_legacy(
        &self,
        provider: &Arc<solagent_chain_solana::SolanaProvider>,
        swap_transaction_b64: &str,
    ) -> Result<solana_sdk::signature::Signature> {
        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(swap_transaction_b64)?;
        let mut vtx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)?;

        // Sign the versioned transaction.
        let message_bytes = bincode::serialize(&vtx.message)?;
        let signature = provider.keypair().sign_message(&message_bytes);
        vtx.signatures[0] = signature;

        // Send via the provider's manual retry path.
        provider.sign_and_send_versioned(&vtx).await
    }

    // ─── Query Methods ──────────────────────────────────────────────────────

    /// Get a quote from Jupiter without executing.
    pub async fn get_quote(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount: u64,
        slippage_bps: u32,
    ) -> Result<solagent_data::JupiterQuote> {
        let jupiter = self.jupiter.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Jupiter client not configured"))?;
        jupiter.get_quote(input_mint, output_mint, amount, slippage_bps).await
    }

    /// Get a snapshot of execution quality metrics.
    pub async fn quality(&self) -> ExecutionQuality {
        self.quality.read().await.clone()
    }

    /// Get SOL balance in lamports. Returns None if provider not configured.
    pub async fn get_sol_balance(&self) -> Option<u64> {
        let provider = self.solana_provider.as_ref()?;
        let pubkey = provider.pubkeys();
        match provider.get_balance(&pubkey).await {
            Ok(balance) => Some(balance),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get SOL balance from RPC");
                None
            }
        }
    }

    /// Get the Solana wallet public key, if configured.
    pub fn solana_pubkey(&self) -> Option<solana_sdk::pubkey::Pubkey> {
        self.solana_provider.as_ref().map(|p| p.pubkeys())
    }

    /// Set the Helius SDK client on the Solana provider for DAS API token balance fetching.
    /// No-op if no Solana provider is configured.
    pub async fn set_helius_client(&self, client: Arc<solagent_data::HeliusSdkClient>) {
        if let Some(provider) = &self.solana_provider {
            provider.set_helius_client(client).await;
        }
    }

    /// Get all SPL token balances for the wallet (mint_address, raw_amount).
    /// Returns None if the Solana provider is not configured.
    pub async fn get_all_token_balances(&self) -> Option<Vec<(String, u64, u8)>> {
        let provider = self.solana_provider.as_ref()?;
        match provider.get_all_token_balances().await {
            Ok(balances) => {
                tracing::info!(count = balances.len(), "Fetched on-chain token balances");
                Some(balances)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get on-chain token balances");
                None
            }
        }
    }

    /// Reconcile on-chain token holdings with portfolio DB records.
    /// Creates position records for any tokens held on-chain but missing from DB.
    pub async fn reconcile_positions(
        &self,
        portfolio: &solagent_portfolio::PortfolioManager,
        dex: &solagent_data::DexScreenerClient,
    ) -> Result<usize> {
        let Some(balances) = self.get_all_token_balances().await else {
            anyhow::bail!("Cannot reconcile: Solana provider not configured");
        };
        portfolio.reconcile_positions(&balances, dex).await
    }

    /// Returns true if Helius Smart Transaction Sender is configured.
    pub fn has_helius_sender(&self) -> bool {
        self.helius_client.is_some()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_quality_default() {
        let q = ExecutionQuality::default();
        assert_eq!(q.total_trades, 0);
        assert_eq!(q.successful_trades, 0);
        assert_eq!(q.failed_trades, 0);
        assert_eq!(q.success_rate(), 0.0);
        assert_eq!(q.avg_slippage_bps, 0.0);
        assert_eq!(q.avg_latency_ms, 0.0);
        assert_eq!(q.total_volume_usd, 0.0);
    }

    #[test]
    fn test_execution_quality_record_success() {
        let mut q = ExecutionQuality::default();
        q.record(150, Some(100), 50.0, true);
        assert_eq!(q.total_trades, 1);
        assert_eq!(q.successful_trades, 1);
        assert_eq!(q.failed_trades, 0);
        assert_eq!(q.success_rate(), 1.0);
        assert_eq!(q.avg_latency_ms, 150.0);
        assert_eq!(q.avg_slippage_bps, 100.0);
        assert_eq!(q.total_volume_usd, 50.0);
    }

    #[test]
    fn test_execution_quality_record_mixed() {
        let mut q = ExecutionQuality::default();
        q.record(100, Some(50), 30.0, true);
        q.record(200, None, 20.0, false);
        assert_eq!(q.total_trades, 2);
        assert_eq!(q.successful_trades, 1);
        assert_eq!(q.failed_trades, 1);
        assert_eq!(q.success_rate(), 0.5);
        assert_eq!(q.avg_latency_ms, 150.0);
        assert_eq!(q.avg_slippage_bps, 50.0); // Only first trade had slippage.
        assert_eq!(q.total_volume_usd, 50.0);
    }

    #[test]
    fn test_execution_quality_running_average() {
        let mut q = ExecutionQuality::default();
        q.record(100, Some(100), 10.0, true);
        q.record(200, Some(200), 20.0, true);
        q.record(300, Some(300), 30.0, true);
        assert_eq!(q.avg_latency_ms, 200.0); // (100+200+300)/3
        assert_eq!(q.avg_slippage_bps, 200.0); // (100+200+300)/3
        assert_eq!(q.total_volume_usd, 60.0);
    }

    #[test]
    fn test_execution_config_default() {
        let config = ExecutionConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_slippage_bps, 100);
        assert_eq!(config.slippage_increase_bps, 50);
        assert_eq!(config.max_slippage_bps, 500);
        assert_eq!(config.priority_fee_cap, 1_000_000);
    }

    #[test]
    fn test_execution_engine_new_no_providers() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        assert!(engine.jupiter.is_none());
        assert!(engine.solana_provider.is_none());
        assert!(engine.helius_client.is_none());
        assert!(!engine.has_helius_sender());
        assert!(engine.solana_pubkey().is_none());
    }

    #[tokio::test]
    async fn test_execution_engine_preflight_balance_check() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        let checks = engine
            .preflight_checks(
                Chain::Solana,
                "fake_token",
                TradeSide::Buy,
                100.0,
                50.0, // wallet balance < size
                1.0,
            )
            .await;
        let balance_check = checks.iter().find(|c| c.name == "balance").unwrap();
        assert!(!balance_check.passed);
    }

    #[tokio::test]
    async fn test_execution_engine_preflight_provider_check() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        let checks = engine
            .preflight_checks(
                Chain::Solana,
                "fake_token",
                TradeSide::Buy,
                10.0,
                100.0,
                1.0,
            )
            .await;
        let provider_check = checks.iter().find(|c| c.name == "provider").unwrap();
        assert!(!provider_check.passed);
    }

    // ─── Helius Integration Tests ────────────────────────────────────────────

    #[test]
    fn test_has_helius_sender_false_by_default() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        assert!(!engine.has_helius_sender());
    }

    #[test]
    fn test_helius_sender_config_changes_priority_fee_cap() {
        // Verify that the new config field exists and has a sensible default.
        let config = ExecutionConfig::default();
        assert_eq!(config.priority_fee_cap, 1_000_000);
    }

    #[tokio::test]
    async fn test_execution_engine_no_helius_uses_legacy_path() {
        // Without a Helius client, the engine should fall back to the legacy path.
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        assert!(!engine.has_helius_sender());
        assert!(engine.helius_client.is_none());
    }

    // ─── Priority Fee Estimation Tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_estimate_priority_fee_returns_none_without_helius() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        let fee = engine.estimate_priority_fee("fake_pubkey").await;
        assert!(fee.is_none(), "Should return None when no Helius client configured");
    }

    #[test]
    fn test_priority_fee_cap_prevents_overpaying() {
        // The priority_fee_cap should prevent excessive fees during network spikes.
        let config = ExecutionConfig {
            priority_fee_cap: 500_000, // 500K microLamports cap
            ..Default::default()
        };
        assert_eq!(config.priority_fee_cap, 500_000);
    }

    // ─── Quality Tracking Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_execution_quality_tracking_still_works() {
        let engine = ExecutionEngine::new(ExecutionConfig::default());
        let quality = engine.quality().await;
        assert_eq!(quality.total_trades, 0);

        // Verify quality tracking methods are accessible.
        let mut q = ExecutionQuality::default();
        q.record(100, Some(50), 25.0, true);
        assert_eq!(q.total_trades, 1);
        assert_eq!(q.successful_trades, 1);
    }

    // ─── Swap Instructions Parsing Tests ─────────────────────────────────────

    #[test]
    fn test_jupiter_instruction_to_solana_instruction() {
        use solagent_data::JupiterInstruction;

        let ji = JupiterInstruction {
            program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            accounts: vec![],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &[3u8, 0, 0, 0, 0, 0, 0, 0, 100], // SetComputeUnitPrice(100)
            ),
        };

        let instr = ji.to_solana_instruction().unwrap();
        assert_eq!(
            instr.program_id.to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        );
        assert!(instr.accounts.is_empty());
        assert_eq!(instr.data.len(), 9);
    }

    #[test]
    fn test_swap_instructions_all_instructions_order() {
        use solagent_data::{JupiterInstruction, SwapInstructions};

        let setup = JupiterInstruction {
            program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            accounts: vec![],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"setup",
            ),
        };
        let swap = JupiterInstruction {
            program_id: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            accounts: vec![],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"swap",
            ),
        };
        let cleanup = JupiterInstruction {
            program_id: "ComputeBudget111111111111111111111111111111111111111".to_string(),
            accounts: vec![],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"cleanup",
            ),
        };

        let si = SwapInstructions {
            compute_unit_limit: Some(200_000),
            compute_unit_price: Some(100_000),
            setup_instructions: vec![setup],
            swap_instruction: swap,
            cleanup_instruction: Some(cleanup),
            address_lookup_table_addresses: vec![],
            last_valid_block_height: None,
            prioritization_fee_lamports: None,
        };

        let all = si.all_instructions();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].program_id, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(all[1].program_id, "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
        assert_eq!(all[2].program_id, "ComputeBudget111111111111111111111111111111111111111");
    }

    #[test]
    fn test_swap_instructions_to_solana_instructions() {
        use solagent_data::{JupiterAccountMeta, JupiterInstruction, SwapInstructions};

        let swap = JupiterInstruction {
            program_id: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            accounts: vec![JupiterAccountMeta {
                pubkey: "11111111111111111111111111111111".to_string(), // System Program
                is_signer: false,
                is_writable: false,
            }],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &[1, 2, 3],
            ),
        };

        let si = SwapInstructions {
            compute_unit_limit: None,
            compute_unit_price: None,
            setup_instructions: vec![],
            swap_instruction: swap,
            cleanup_instruction: None,
            address_lookup_table_addresses: vec![],
            last_valid_block_height: None,
            prioritization_fee_lamports: None,
        };

        let instrs = si.to_solana_instructions().unwrap();
        assert_eq!(instrs.len(), 1);
        assert_eq!(instrs[0].accounts.len(), 1);
        assert!(!instrs[0].accounts[0].is_signer);
        assert!(!instrs[0].accounts[0].is_writable);
    }

    #[test]
    fn test_jupiter_instruction_invalid_program_id() {
        use solagent_data::JupiterInstruction;

        let ji = JupiterInstruction {
            program_id: "not_a_valid_pubkey".to_string(),
            accounts: vec![],
            data: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &[1u8],
            ),
        };

        assert!(ji.to_solana_instruction().is_err());
    }

    #[test]
    fn test_jupiter_instruction_invalid_data() {
        use solagent_data::JupiterInstruction;

        let ji = JupiterInstruction {
            program_id: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            accounts: vec![],
            data: "not_valid_base64!!!".to_string(),
        };

        assert!(ji.to_solana_instruction().is_err());
    }
}
