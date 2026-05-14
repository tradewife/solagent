//! # solagent-exec
//!
//! Execution engine with pre-flight checks, retry logic, and quality tracking.

use anyhow::Result;
use serde::{Deserialize, Serialize};
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
}

impl ExecutionEngine {
    pub fn new(config: ExecutionConfig) -> Self {
        Self {
            config,
            quality: Arc::new(RwLock::new(ExecutionQuality::default())),
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

        // Slippage estimate.
        let estimated_impact = (size_usd / 10_000.0).min(5.0); // Simplified.
        checks.push(PreflightCheck {
            name: "slippage".to_string(),
            passed: estimated_impact < (self.config.base_slippage_bps as f64 / 100.0),
            details: format!("Estimated price impact: {estimated_impact:.2}%"),
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
                .dispatch_buy(chain, token_address, size_usd, slippage_bps, current_price)
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
        _chain: Chain,
        _token_address: &str,
        _size_usd: f64,
        _slippage_bps: u32,
        _current_price: f64,
    ) -> Result<Trade> {
        match _chain {
            Chain::Solana => {
                todo!("Dispatch buy via SolanaProvider -> Jupiter")
            }
            Chain::Base => {
                todo!("Dispatch buy via BaseProvider -> Uniswap")
            }
        }
    }

    /// Dispatch a sell to the appropriate chain provider.
    async fn dispatch_sell(
        &self,
        _chain: Chain,
        _token_address: &str,
        _token_amount: f64,
        _slippage_bps: u32,
        _current_price: f64,
    ) -> Result<Trade> {
        match _chain {
            Chain::Solana => {
                todo!("Dispatch sell via SolanaProvider -> Jupiter")
            }
            Chain::Base => {
                todo!("Dispatch sell via BaseProvider -> Uniswap")
            }
        }
    }

    /// Get a snapshot of execution quality metrics.
    pub async fn quality(&self) -> ExecutionQuality {
        self.quality.read().await.clone()
    }
}
