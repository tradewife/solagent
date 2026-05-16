//! # solagent-exec
//!
//! Execution engine with pre-flight checks, retry logic, and quality tracking.
//! Supports Solana (via Jupiter V6) and Base (via Uniswap — stub).

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
    pub priority_fee_lamports: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_slippage_bps: 100,  // 1%
            slippage_increase_bps: 50,
            max_slippage_bps: 500,   // 5%
            priority_fee_lamports: 100_000,
        }
    }
}

// ─── Execution Engine ────────────────────────────────────────────────────────

/// The execution engine handles buy/sell order dispatch with retry logic.
pub struct ExecutionEngine {
    config: ExecutionConfig,
    quality: Arc<RwLock<ExecutionQuality>>,
    jupiter: Option<solagent_data::JupiterClient>,
    solana_provider: Option<Arc<solagent_chain_solana::SolanaProvider>>,
}

impl ExecutionEngine {
    /// Create a new execution engine with no chain providers (for dry-run or testing).
    pub fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            quality: Arc::new(RwLock::new(ExecutionQuality::default())),
            jupiter: None,
            solana_provider: None,
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
        }
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

    /// Execute a buy order with retry logic.
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

    /// Execute a sell order with retry logic.
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

    /// Execute a Solana buy via Jupiter V6.
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
        // size_usd is how much SOL to spend; divide by SOL price to get SOL amount.
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

        // Get swap transaction from Jupiter.
        let user_pubkey = provider.pubkeys().to_string();
        let swap_tx = jupiter.get_swap_transaction(&quote, &user_pubkey).await?;

        // Deserialize, sign, and send the transaction.
        // Jupiter V6 returns versioned (V0) transactions with Address Lookup Tables.
        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(&swap_tx.swap_transaction)?;
        let mut vtx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)?;
        // Sign the versioned transaction: compute signature over the message.
        let message_bytes = bincode::serialize(&vtx.message)?;
        let signature = provider.keypair().sign_message(&message_bytes);
        vtx.signatures[0] = signature;
        let signature = provider.sign_and_send_versioned(&vtx).await?;

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

    /// Execute a Solana sell via Jupiter V6.
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
        let user_pubkey = provider.pubkeys().to_string();
        let swap_tx = jupiter.get_swap_transaction(&quote, &user_pubkey).await?;

        let tx_bytes = base64::engine::general_purpose::STANDARD
            .decode(&swap_tx.swap_transaction)?;
        let mut vtx: solana_sdk::transaction::VersionedTransaction =
            bincode::deserialize(&tx_bytes)?;
        let message_bytes = bincode::serialize(&vtx.message)?;
        let signature = provider.keypair().sign_message(&message_bytes);
        vtx.signatures[0] = signature;
        let signature = provider.sign_and_send_versioned(&vtx).await?;

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

    /// Get all SPL token balances for the wallet (mint_address, raw_amount).
    /// Returns None if the Solana provider is not configured.
    pub async fn get_all_token_balances(&self) -> Option<Vec<(String, u64)>> {
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
        portfolio.reconcile_positions(&balances, 6, dex).await
    }
}
