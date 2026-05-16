//! # solagent-agent
//!
//! Autonomous agent with state machine, main event loop, and decision logging.
//! Wires together all subsystems: data pipeline, signals, safety, risk, execution,
//! and portfolio management.

// std::sync::Mutex is used for subsystem access. The guards are dropped before
// long-running awaits where possible, but some .await calls happen while held
// (e.g., confluence.score().await). A full refactor to tokio::sync::Mutex is
// deferred; the current pattern is safe in practice because the agent is single-tasked.
#![allow(clippy::await_holding_lock)]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use solagent_core::chrono::{DateTime, Utc};
use solagent_core::serde_json;
use solagent_core::uuid::Uuid;
use solagent_core::{Chain, EventBus, Event, Signal, TokenInfo};
use std::sync::Arc;

// ─── Agent State Machine ─────────────────────────────────────────────────────

/// Agent states in the trading lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentState {
    /// Scanning for new tokens / opportunities.
    Scanning,
    /// Evaluating a candidate token (running strategies + safety).
    Evaluating,
    /// Running risk checks on an approved signal.
    RiskCheck,
    /// Executing a trade.
    Executing,
    /// Monitoring open positions.
    Monitoring,
}

impl std::fmt::Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentState::Scanning => write!(f, "SCANNING"),
            AgentState::Evaluating => write!(f, "EVALUATING"),
            AgentState::RiskCheck => write!(f, "RISK_CHECK"),
            AgentState::Executing => write!(f, "EXECUTING"),
            AgentState::Monitoring => write!(f, "MONITORING"),
        }
    }
}

// ─── Decision Log ────────────────────────────────────────────────────────────

/// A single decision entry with full reasoning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub state: AgentState,
    pub token_address: Option<String>,
    pub signals: Vec<Signal>,
    pub safety_score: Option<u8>,
    pub risk_report: Option<serde_json::Value>,
    pub action: String,
    pub reasoning: String,
    pub outcome: Option<String>,
}

// ─── Agent Configuration ─────────────────────────────────────────────────────

/// Agent-specific configuration beyond the base Config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub scan_interval_secs: u64,
    pub monitor_interval_secs: u64,
    pub max_concurrent_evaluations: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 30,
            monitor_interval_secs: 60,
            max_concurrent_evaluations: 5,
        }
    }
}

// ─── Progressive Threshold Lowering ──────────────────────────────────────────

/// Progressive confluence threshold lowering as a safety net.
///
/// After N consecutive failed evaluations, lowers the confluence threshold
/// by a fixed step, down to a configurable floor. Resets to the original
/// value after a successful trade.
///
/// This ensures the agent will eventually execute trades even if signals
/// are weak, while the auto-tuner works on improving signal quality.
#[derive(Debug, Clone)]
pub struct ProgressiveThreshold {
    /// The original threshold from config.
    original: f64,
    /// The current (possibly lowered) threshold.
    current: f64,
    /// Minimum threshold — never go below this.
    floor: f64,
    /// Step to lower by each time the failure count is reached.
    step: f64,
    /// How many consecutive failures before lowering by one step.
    failures_before_lowering: u32,
    /// Current count of consecutive failures since last success/reset.
    consecutive_failures: u32,
}

impl ProgressiveThreshold {
    /// Create a new progressive threshold.
    pub fn new(
        original: f64,
        floor: f64,
        step: f64,
        failures_before_lowering: u32,
    ) -> Self {
        Self {
            original,
            current: original,
            floor,
            step,
            failures_before_lowering,
            consecutive_failures: 0,
        }
    }

    /// Create with default values: step=5, floor=20, failures=50.
    pub fn with_original(original: f64) -> Self {
        Self::new(original, 20.0, 5.0, 50)
    }

    /// Create from config values.
    pub fn from_config(
        original: f64,
        failures: u32,
        step: f64,
        floor: f64,
    ) -> Self {
        Self::new(original, floor, step, failures)
    }

    /// Get the current effective threshold.
    pub fn current(&self) -> f64 {
        self.current
    }

    /// Get the original (config) threshold.
    pub fn original(&self) -> f64 {
        self.original
    }

    /// Record a failed evaluation (confluence score < threshold).
    /// Returns true if the threshold was lowered.
    pub fn record_failure(&mut self) -> bool {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.failures_before_lowering
            && self.current > self.floor
        {
            let old = self.current;
            self.current = (self.current - self.step).max(self.floor);
            self.consecutive_failures = 0; // Reset counter after each lowering
            tracing::warn!(
                old_threshold = old,
                new_threshold = self.current,
                floor = self.floor,
                "Progressive threshold lowered after {} consecutive failures",
                self.failures_before_lowering,
            );
            true
        } else {
            false
        }
    }

    /// Record a successful trade. Resets threshold to original config value.
    pub fn record_success(&mut self) {
        if self.current != self.original {
            tracing::warn!(
                old_threshold = self.current,
                new_threshold = self.original,
                "Progressive threshold RESET to original after successful trade"
            );
        }
        self.current = self.original;
        self.consecutive_failures = 0;
    }

    /// Check if the threshold has been lowered from the original.
    pub fn is_lowered(&self) -> bool {
        (self.current - self.original).abs() > f64::EPSILON && self.current < self.original
    }
}

// ─── Per-Position Exit Tracking ───────────────────────────────────────────────

/// Tracks exit parameters for an open position (peak price + trailing stop).
#[derive(Debug, Clone)]
struct PositionExit {
    peak_price: f64,
    trailing_stop_pct: f64,
}

// ─── Subsystem References ────────────────────────────────────────────────────

/// Holds references to all subsystems the agent needs.
pub struct AgentSubsystems {
    pub dex: solagent_data::DexScreenerClient,
    pub safety: solagent_safety::SafetyEvaluator,
    pub risk: std::sync::Mutex<solagent_risk::RiskManager>,
    pub exec: solagent_exec::ExecutionEngine,
    pub portfolio: solagent_portfolio::PortfolioManager,
    pub event_bus: EventBus,
    /// Confluence scorer aggregating all signal detectors.
    pub confluence: std::sync::Mutex<solagent_signals::ConfluenceScorer>,
    /// Confluence threshold (0-100). Default 35.
    pub confluence_threshold: f64,
    /// Progressive threshold: consecutive failures before lowering.
    pub progressive_threshold_failures: u32,
    /// Progressive threshold: step to lower by each time.
    pub progressive_threshold_step: f64,
    /// Progressive threshold: minimum floor (never go below).
    pub progressive_threshold_floor: f64,
    /// Wallet watcher for detecting smart money trades (Helius-backed).
    pub watcher: Option<solagent_data::WalletWatcher>,
    /// GMGN client for fetching holder count data.
    pub gmgn: solagent_data::GmgnClient,
}

// ─── Agent ───────────────────────────────────────────────────────────────────

/// The autonomous SolAgent trading agent.
pub struct Agent {
    state: Arc<tokio::sync::RwLock<AgentState>>,
    config: AgentConfig,
    subsystems: Arc<AgentSubsystems>,
    decisions: Arc<tokio::sync::RwLock<Vec<Decision>>>,
    running: Arc<tokio::sync::RwLock<bool>>,
    /// Track exit parameters per position.
    position_exits: Arc<tokio::sync::RwLock<std::collections::HashMap<String, PositionExit>>>,
    /// Recently-evaluated tokens with cooldown (address -> last eval time).
    eval_cooldown: Arc<tokio::sync::RwLock<std::collections::HashMap<String, DateTime<Utc>>>>,
    /// Progressive threshold lowering: lowers confluence threshold after
    /// N consecutive failed evaluations, resets on successful trade.
    progressive_threshold: Arc<tokio::sync::RwLock<ProgressiveThreshold>>,
}

impl Agent {
    /// Create a new agent with wired subsystems.
    pub fn new(config: AgentConfig, subsystems: AgentSubsystems) -> Self {
        let event_bus = subsystems.event_bus.clone();
        let _ = event_bus;
        let progressive = ProgressiveThreshold::from_config(
            subsystems.confluence_threshold,
            subsystems.progressive_threshold_failures,
            subsystems.progressive_threshold_step,
            subsystems.progressive_threshold_floor,
        );
        Self {
            state: Arc::new(tokio::sync::RwLock::new(AgentState::Scanning)),
            config,
            subsystems: Arc::new(subsystems),
            decisions: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            running: Arc::new(tokio::sync::RwLock::new(false)),
            position_exits: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            eval_cooldown: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            progressive_threshold: Arc::new(tokio::sync::RwLock::new(progressive)),
        }
    }

    /// Create a minimal agent for CLI use (no full subsystems).
    pub fn new_minimal(config: AgentConfig, event_bus: EventBus) -> Self {
        // This is used when the agent is created without full subsystems.
        // The subsystems field won't be usable, but scan/monitor won't be called.
        let threshold = 65.0;
        Self {
            state: Arc::new(tokio::sync::RwLock::new(AgentState::Scanning)),
            config,
            subsystems: Arc::new(AgentSubsystems {
                dex: solagent_data::DexScreenerClient::new(
                    "https://api.dexscreener.com".to_string(), None,
                ),
                safety: solagent_safety::SafetyEvaluator::new(
                    60,
                    solagent_data::BirdeyeClient::with_api_key(None),
                    solagent_safety::SqliteDevBlacklist::new(
                        sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
                    ),
                ),
                risk: std::sync::Mutex::new(solagent_risk::RiskManager::new(
                    solagent_risk::RiskConfig::default(),
                )),
                exec: solagent_exec::ExecutionEngine::new(
                    solagent_exec::ExecutionConfig::default(),
                ),
                portfolio: solagent_portfolio::PortfolioManager::new(
                    sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
                ),
                event_bus,
                confluence: std::sync::Mutex::new(solagent_signals::ConfluenceScorer::new(threshold)),
                confluence_threshold: threshold,
                progressive_threshold_failures: 50,
                progressive_threshold_step: 5.0,
                progressive_threshold_floor: 20.0,
                watcher: None,
                gmgn: solagent_data::GmgnClient::new(),
            }),
            decisions: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            running: Arc::new(tokio::sync::RwLock::new(false)),
            position_exits: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            eval_cooldown: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            progressive_threshold: Arc::new(tokio::sync::RwLock::new(
                ProgressiveThreshold::from_config(threshold, 50, 5.0, 20.0),
            )),
        }
    }

    /// Get the current agent state.
    pub async fn state(&self) -> AgentState {
        *self.state.read().await
    }

    /// Transition to a new state.
    pub async fn transition(&self, new_state: AgentState) {
        let old = *self.state.read().await;
        tracing::info!(old_state = %old, new_state = %new_state, "State transition");
        *self.state.write().await = new_state;
    }

    /// Log a decision.
    pub async fn log_decision(&self, decision: Decision) {
        tracing::info!(
            state = %decision.state,
            action = %decision.action,
            token = ?decision.token_address,
            "Decision logged"
        );
        self.decisions.write().await.push(decision);
    }

    /// Compute a heuristic safety score (0-100) from DexScreener data alone.
    /// No Birdeye/API calls -- uses liquidity, volume, buy/sell ratio, and age.
    fn compute_heuristic_safety(token: &TokenInfo) -> u8 {
        let mut score: f64 = 0.0;

        // Liquidity (0-30 pts): more liquidity = safer.
        let liq = token.volume_24h.unwrap_or(0.0).min(100_000.0);
        score += (liq / 100_000.0 * 30.0).min(30.0);

        // Market cap (0-20 pts): reasonable mcap range.
        if let Some(mc) = token.market_cap_usd {
            if (10_000.0..=100_000_000.0).contains(&mc) {
                score += 20.0;
            } else if mc >= 1_000.0 {
                score += 10.0;
            }
        }

        // Price available (0-10 pts).
        if token.price_usd.is_some() {
            score += 10.0;
        }

        // Age (0-20 pts): not brand new is safer, but not too old either.
        if let Some(created) = token.created_at {
            let age_hours = (Utc::now() - created).num_hours();
            if (1..=168).contains(&age_hours) {
                score += 20.0; // 1 hour to 7 days -- sweet spot.
            } else if age_hours > 168 {
                score += 15.0; // Established token.
            }
            // Brand new (<1h) gets 0 -- too risky.
        }

        // Base score for having pair data (0-20 pts).
        if token.pair_address.is_some() {
            score += 20.0;
        }

        score.clamp(0.0, 100.0) as u8
    }

    /// Run the scanning phase — discover new tokens from DexScreener.
    async fn scan(&self) -> Result<Vec<TokenInfo>> {
        self.transition(AgentState::Scanning).await;

        let pairs = self.subsystems.dex.get_new_pairs("solana").await?;
        let mut tokens = Vec::new();

        for pair in pairs {
            let _liq = pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
            let price = pair.price_usd.as_ref()
                .and_then(|p| p.parse::<f64>().ok())
                .or_else(|| pair.price_native.as_ref().and_then(|p| p.parse::<f64>().ok()));

            let created_at = pair.pair_created_at
                .and_then(chrono::DateTime::from_timestamp_millis);

            // Extract Twitter handles from DexScreener social links and upsert
            // into the twitter_accounts table for curated timeline polling.
            if let Some(ref info) = pair.info {
                let handles = solagent_signals::extract_twitter_handles(&info.socials);
                if !handles.is_empty() {
                    tracing::debug!(
                        token = &pair.base_token.address[..pair.base_token.address.len().min(12)],
                        handles = handles.join(","),
                        "Extracted Twitter handles from DexScreener socials"
                    );
                    for handle in &handles {
                        if let Err(e) = self.subsystems.portfolio.upsert_twitter_account(
                            handle,
                            Some(&pair.base_token.address),
                            None,
                        ).await {
                            tracing::debug!(handle, error = %e, "Failed to upsert Twitter account");
                        }
                    }
                }
            }

            tokens.push(TokenInfo {
                address: pair.base_token.address.clone(),
                chain: Chain::Solana,
                symbol: pair.base_token.symbol.clone(),
                name: pair.base_token.name.clone(),
                decimals: 0,
                price_usd: price,
                market_cap_usd: pair.market_cap,
                volume_24h: pair.volume.as_ref().and_then(|v| v.h24),
                holder_count: None,
                created_at,
                pair_address: Some(pair.pair_address.clone()),
                lp_locked: None,
                mint_authority_revoked: None,
                freeze_authority_revoked: None,
            });
        }

        tracing::info!(count = tokens.len(), "Scan discovered tokens");

        // Fetch holder counts from GMGN for discovered tokens (rate-limited).
        // Only fetch for tokens with volume/mcap data (likely legitimate).
        let tokens_with_data: Vec<String> = tokens.iter()
            .filter(|t| t.volume_24h.is_some() || t.market_cap_usd.is_some())
            .map(|t| t.address.clone())
            .collect();

        if !tokens_with_data.is_empty() {
            tracing::info!(
                count = tokens_with_data.len(),
                "Fetching holder counts from GMGN for tokens with volume/mcap data"
            );
            let holder_counts = self.subsystems.gmgn.get_holder_counts(&tokens_with_data).await;
            let fetched = holder_counts.len();
            let total = tokens_with_data.len();
            tracing::info!(
                fetched,
                total,
                "GMGN holder count fetch complete ({fetched}/{total} successful)"
            );

            // Update tokens with fetched holder counts.
            for token in &mut tokens {
                if let Some(count) = holder_counts.get(&token.address) {
                    token.holder_count = Some(*count);
                }
            }
        }

        // Feed scan data into signal detectors (volume, launch momentum, accumulation).
        for token in &tokens {
            if token.holder_count.is_some() {
                tracing::debug!(
                    token = &token.address[..token.address.len().min(12)],
                    holder_count = ?token.holder_count,
                    "Feeding scan data with holder count"
                );
            }
            self.subsystems.confluence.lock().unwrap().feed_scan_data(
                &token.address,
                token.volume_24h,
                token.holder_count,
                token.price_usd,
            );
        }

        Ok(tokens)
    }

    /// Run the evaluation phase — heuristic safety check from DexScreener data
    /// plus confluence scoring from all signal detectors.
    async fn evaluate(&self, token: &TokenInfo) -> Result<EvaluationResult> {
        self.transition(AgentState::Evaluating).await;

        // Heuristic safety score from DexScreener data (no Birdeye calls).
        let safety_score = Self::compute_heuristic_safety(token);
        let safety_passed = safety_score >= self.subsystems.safety.threshold;

        // Run confluence scoring across all signal detectors.
        let confluence = self.subsystems.confluence.lock().unwrap();
        let confluence_result = confluence.score(token).await?;
        drop(confluence);

        // Use progressive threshold (may be lowered after many failures).
        let effective_threshold = self.progressive_threshold.read().await.current();
        let confluence_passed = confluence_result.composite_score as f64 >= effective_threshold;
        let confluence_score = confluence_result.composite_score;
        let signals = confluence_result.signals;

        // Derive age in hours from creation timestamp.
        let age_hours = token.created_at.map(|t| {
            (Utc::now() - t).num_minutes() as f64 / 60.0
        });

        let passed = safety_passed && confluence_passed;

        let signal_summary: Vec<String> = signals.iter()
            .map(|s| format!("{}={}/100", s.strategy, s.score))
            .collect();
        let status = if passed { "PASS" } else { "FAIL" };
        let reasons = if !safety_passed {
            format!("safety({safety_score}<{})", self.subsystems.safety.threshold)
        } else if !confluence_passed {
            format!("confluence({confluence_score}<{effective_threshold:.0})")
        } else {
            "all passed".to_string()
        };

        tracing::info!(
            token = &token.address,
            safety_score,
            confluence_score,
            effective_threshold = effective_threshold,
            status,
            "Evaluation: [{status}] safety={safety_score} confluence={confluence_score}/{effective_threshold:.0} signals=[{}] ({reasons})",
            signal_summary.join(", "),
        );

        Ok(EvaluationResult {
            token_address: token.address.clone(),
            chain: token.chain,
            confluence_score,
            safety_score,
            signals,
            passed,
            reasoning: format!("Safety: {safety_score}/100, Confluence: {confluence_score}/100 (threshold={effective_threshold:.0}) [{reasons}]", ),
            market_cap: token.market_cap_usd,
            age_hours,
        })
    }

    /// Run risk check phase.
    async fn risk_check(&self, evaluation: &EvaluationResult) -> Result<bool> {
        self.transition(AgentState::RiskCheck).await;

        let positions = self.subsystems.portfolio.get_open_positions().await?;
        let portfolio_value = self.get_portfolio_value().await?;

        let risk = self.subsystems.risk.lock().unwrap();
        let size = risk.calculate_position_size(portfolio_value);
        let report = risk.evaluate_trade(
            &evaluation.token_address,
            size,
            positions.len(),
            portfolio_value,
            &[],
        );

        tracing::info!(
            token = &evaluation.token_address,
            passed = report.passed,
            circuit_breaker = %report.circuit_breaker,
            "Risk check completed"
        );

        Ok(report.passed && risk.can_trade())
    }

    /// Execute a trade.
    async fn execute(&self, evaluation: &EvaluationResult) -> Result<()> {
        self.transition(AgentState::Executing).await;

        let portfolio_value = self.get_portfolio_value().await?;

        let risk = self.subsystems.risk.lock().unwrap();
        let size = risk.calculate_position_size(portfolio_value);
        drop(risk);

        tracing::info!(
            token = &evaluation.token_address,
            size_usd = size,
            "Attempting to execute buy"
        );

        // Get current price from DexScreener.
        let current_price = match self.get_token_price_dex(&evaluation.token_address).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get price for execution");
                return Err(e);
            }
        };

        // Calculate dynamic exit profile based on market cap, age, and confluence.
        let (sl, tp, trailing_pct) = solagent_risk::RiskManager::calculate_exit(
            current_price,
            evaluation.market_cap,
            evaluation.age_hours,
            evaluation.confluence_score,
        );

        let profile = solagent_risk::RiskManager::select_exit_profile(
            evaluation.market_cap,
            evaluation.age_hours,
            evaluation.confluence_score,
        );
        tracing::info!(
            token = &evaluation.token_address,
            profile = %profile.name,
            sl_pct = %format!("{:.0}%", profile.stop_loss_pct),
            tp = match profile.take_profit_pct { Some(p) => format!("+{p:.0}%"), None => "none (trail only)".to_string() },
            trail_pct = %format!("{:.0}%", profile.trailing_stop_pct),
            mcap = ?evaluation.market_cap.map(|m| format!("${m:.0}")),
            age_hours = ?evaluation.age_hours.map(|a| format!("{a:.1}h")),
            "Selected exit profile"
        );

        let sol_price = self.get_sol_price().await;
        let trade = self.subsystems.exec.execute_buy(
            evaluation.chain,
            &evaluation.token_address,
            size,
            portfolio_value,
            current_price,
            sol_price,
        ).await?;

        // Record in portfolio.
        self.subsystems.portfolio.open_position(
            &trade.token_address,
            trade.chain,
            trade.price,
            trade.size_usd,
            trade.token_amount,
            Some(sl),
            tp,
        ).await?;

        self.subsystems.portfolio.record_trade(
            None,
            &trade.token_address,
            trade.chain,
            "buy",
            trade.size_usd,
            trade.token_amount,
            trade.price,
            trade.tx_signature.as_deref(),
            trade.slippage_bps.map(|s| s as i64),
            trade.latency_ms.map(|l| l as i64),
        ).await?;

        // Record exit parameters for trailing stop monitoring.
        self.position_exits.write().await.insert(
            trade.token_address.clone(),
            PositionExit {
                peak_price: trade.price,
                trailing_stop_pct: trailing_pct,
            },
        );

        tracing::info!(
            token = &trade.token_address,
            size = trade.size_usd,
            price = trade.price,
            "Trade executed and recorded"
        );

        Ok(())
    }

    /// Get token price from DexScreener (avoids Birdeye rate limits).
    async fn get_token_price_dex(&self, token_address: &str) -> Result<f64> {
        let pair = self.subsystems.dex.get_token_info(token_address).await?;
        match pair {
            Some(p) => p.price_usd
                .and_then(|p| p.parse::<f64>().ok())
                .ok_or_else(|| anyhow::anyhow!("No price from DexScreener for {token_address}")),
            None => Err(anyhow::anyhow!("Token not found on DexScreener: {token_address}")),
        }
    }

    /// Get SOL price from DexScreener.
    async fn get_sol_price(&self) -> f64 {
        // Use Wrapped SOL (So111...) which is listed on DexScreener.
        self.get_token_price_dex("So11111111111111111111111111111111111111112")
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to get SOL price from DexScreener");
                0.0
            })
    }

    /// Get real portfolio value: SOL balance (converted to USD) + open positions at current prices.
    async fn get_portfolio_value(&self) -> Result<f64> {
        // Get SOL price.
        let sol_price = self.get_sol_price().await;

        // Get SOL balance from the wallet.
        // Try to get real balance from Solana RPC if provider is available.
        // Otherwise use the portfolio's position values as a lower bound.
        let sol_balance = self.get_sol_balance().await;

        let cash_usd = sol_balance * sol_price;

        // Sum open positions at current prices.
        let positions = self.subsystems.portfolio.get_open_positions().await?;
        let position_value: f64 = positions.iter().map(|p| {
            // size_usd scaled by price change since entry.
            if p.entry_price > 0.0 {
                p.size_usd * (p.current_price / p.entry_price)
            } else {
                p.size_usd
            }
        }).sum();

        let total = cash_usd + position_value;

        // Update risk manager peak value.
        self.subsystems.risk.lock().unwrap().update_peak(total);

        tracing::debug!(
            sol_balance,
            sol_price,
            cash_usd,
            position_value,
            total,
            "Portfolio value calculated"
        );

        Ok(total)
    }

    /// Get SOL balance from the wallet. Queries the Solana RPC for the real balance.
    async fn get_sol_balance(&self) -> f64 {
        match self.subsystems.exec.get_sol_balance().await {
            Some(lamports) => {
                let sol = lamports as f64 / 1_000_000_000.0;
                tracing::debug!(sol_balance = sol, "SOL balance fetched");
                sol
            }
            None => {
                // If we can't get the balance but the provider is configured,
                // try using the portfolio value from positions as a fallback.
                tracing::warn!("Could not fetch SOL balance from RPC — using $0 balance");
                0.0
            }
        }
    }

    /// Monitor open positions (check stop-loss, take-profit, trailing stops).
    async fn monitor(&self) -> Result<()> {
        self.transition(AgentState::Monitoring).await;

        let positions = self.subsystems.portfolio.get_open_positions().await?;
        if positions.is_empty() {
            return Ok(());
        }

        for pos in &positions {
            // Get current price from DexScreener.
            let current_price = match self.get_token_price_dex(&pos.token_address).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        token = &pos.token_address,
                        error = %e,
                        "Failed to get price for monitoring"
                    );
                    continue;
                }
            };

            // Update portfolio price.
            if let Err(e) = self.subsystems.portfolio.update_price(&pos.id, current_price).await {
                tracing::warn!(error = %e, "Failed to update position price");
            }

            // Update peak price and get trailing stop for this position.
            let mut exits = self.position_exits.write().await;
            let exit = exits.entry(pos.token_address.clone()).or_insert_with(|| {
                // Fallback: use swing profile if we don't have one recorded.
                let profile = solagent_risk::ExitProfile::swing();
                PositionExit {
                    peak_price: pos.entry_price,
                    trailing_stop_pct: profile.trailing_stop_pct,
                }
            });
            if current_price > exit.peak_price {
                exit.peak_price = current_price;
            }
            let peak_price = exit.peak_price;
            let trailing_pct = exit.trailing_stop_pct;
            drop(exits);

            // Check stop conditions with per-position trailing stop.
            let risk = self.subsystems.risk.lock().unwrap();
            let should_close = risk.check_stop_conditions(
                current_price,
                pos.entry_price,
                pos.stop_loss,
                pos.take_profit,
                Some(peak_price),
                trailing_pct,
            );
            drop(risk);

            if let Some(reason) = should_close {
                tracing::info!(
                    token = &pos.token_address,
                    reason = %reason,
                    "Closing position"
                );

                // Execute sell.
                let sell_result = self.subsystems.exec.execute_sell(
                    pos.chain,
                    &pos.token_address,
                    pos.token_amount,
                    current_price,
                ).await;

                match sell_result {
                    Ok(trade) => {
                        // Close position in portfolio.
                        self.subsystems.portfolio.close_position(&pos.id, current_price).await?;
                        self.subsystems.portfolio.record_trade(
                            Some(&pos.id),
                            &trade.token_address,
                            trade.chain,
                            "sell",
                            trade.size_usd,
                            trade.token_amount,
                            trade.price,
                            trade.tx_signature.as_deref(),
                            trade.slippage_bps.map(|s| s as i64),
                            trade.latency_ms.map(|l| l as i64),
                        ).await?;

                        // Record in risk manager.
                        self.subsystems.risk.lock().unwrap().record_trade(&trade);

                        // Clean up exit tracking.
                        self.position_exits.write().await.remove(&pos.token_address);
                    }
                    Err(e) => {
                        tracing::error!(
                            token = &pos.token_address,
                            error = %e,
                            "Failed to execute sell for position close"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the agent's main event loop.
    pub async fn run(&self) -> Result<()> {
        *self.running.write().await = true;

        // Reconcile on-chain positions with the database on startup.
        // This catches positions opened before the agent crashed or trades
        // that were confirmed on-chain but whose confirmation timed out.
        tracing::info!("Running on-chain position reconciliation on startup");
        match self.subsystems.exec.reconcile_positions(
            &self.subsystems.portfolio,
            &self.subsystems.dex,
        ).await {
            Ok(count) => {
                let open_positions = self.subsystems.portfolio.get_open_positions().await
                    .map(|p| p.len()).unwrap_or(0);
                if count > 0 {
                    tracing::warn!(
                        reconciled = count,
                        open_positions,
                        "Found {} on-chain positions missing from DB — records created ({} total open positions now tracked)",
                        count, open_positions + count,
                    );
                } else {
                    tracing::info!(
                        open_positions,
                        "Reconciliation: all {} on-chain positions already in DB",
                        open_positions,
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to reconcile positions on startup (non-fatal)");
            }
        }

        // Spawn wallet watcher background task (if configured).
        let _watcher_handle = if let Some(ref watcher) = self.subsystems.watcher {
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let watcher = watcher.clone();
            let handle = tokio::spawn(async move {
                watcher.run(shutdown_rx).await;
            });
            tracing::info!("Wallet watcher background task started");
            Some((handle, shutdown_tx))
        } else {
            tracing::info!("No wallet watcher configured -- whale consensus will rely on other signals");
            None
        };

        let mut scan_interval = tokio::time::interval(
            std::time::Duration::from_secs(self.config.scan_interval_secs),
        );
        let mut monitor_interval = tokio::time::interval(
            std::time::Duration::from_secs(self.config.monitor_interval_secs),
        );

        tracing::info!(
            scan_interval = self.config.scan_interval_secs,
            monitor_interval = self.config.monitor_interval_secs,
            "Agent starting main loop"
        );

        while *self.running.read().await {
            tokio::select! {
                // Periodic scan for new tokens.
                _ = scan_interval.tick() => {
                    tracing::debug!("Scan tick");

                    // Poll social signal (twitter search) each scan cycle.
                    if let Err(e) = self.subsystems.confluence.lock().unwrap().poll_social().await {
                        tracing::debug!(error = %e, "Social signal poll skipped");
                    }

                    // Poll curated Twitter account timelines for token-specific mentions.
                    // These are handles extracted from DexScreener social links.
                    match self.subsystems.portfolio.get_stale_twitter_accounts(60, 10).await {
                        Ok(accounts) => {
                            if !accounts.is_empty() {
                                let handles: Vec<String> = accounts.iter().map(|a| a.handle.clone()).collect();
                                let count = self.subsystems.confluence.lock().unwrap().poll_social_accounts(&handles).await;
                                if count > 0 {
                                    tracing::info!(count, handles = handles.len(), "Curated Twitter accounts mentioned tokens");
                                }
                                // Mark accounts as polled.
                                for account in &accounts {
                                    if let Err(e) = self.subsystems.portfolio.mark_twitter_account_polled(&account.handle).await {
                                        tracing::debug!(handle = %account.handle, error = %e, "Failed to mark Twitter account as polled");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, "Failed to get stale Twitter accounts");
                        }
                    }

                    match self.scan().await {
                        Ok(tokens) => {
                            // Search Twitter for discovered token CAs (top 5 per cycle to respect rate limits).
                            let addresses: Vec<String> = tokens.iter().take(5).map(|t| t.address.clone()).collect();
                            self.subsystems.confluence.lock().unwrap().poll_social_tokens(&addresses, 5).await;

                            // Prune expired cooldown entries (older than 5 minutes).
                            let cooldown_duration = chrono::Duration::minutes(5);
                            {
                                let mut cd = self.eval_cooldown.write().await;
                                let now = Utc::now();
                                cd.retain(|_, t| *t + cooldown_duration > now);
                            }

                            // Only evaluate N tokens per scan to respect API rate limits.
                            // Filter cooldown FIRST, then deduplicate by token address
                            // (DexScreener returns multiple pairs for same base token),
                            // then take the limit — ensures we evaluate unique fresh tokens.
                            let eval_limit = self.config.max_concurrent_evaluations.min(tokens.len());
                            let cooldown = self.eval_cooldown.read().await;
                            let mut seen_addresses = std::collections::HashSet::new();
                            let tokens_to_eval: Vec<_> = tokens.iter()
                                .filter(|t| !cooldown.contains_key(&t.address))
                                .filter(|t| seen_addresses.insert(t.address.clone()))
                                .take(eval_limit)
                                .collect();
                            let skipped = eval_limit.saturating_sub(tokens_to_eval.len());
                            if skipped > 0 {
                                tracing::debug!(skipped, "Tokens skipped due to evaluation cooldown");
                            }
                            drop(cooldown);

                            for token in tokens_to_eval {
                                self.subsystems.event_bus.publish(Event::TokenDiscovered {
                                    token: token.clone(),
                                    timestamp: Utc::now(),
                                });

                                // Evaluate each token.
                                match self.evaluate(token).await {
                                    Ok(result) => {
                                        // Persist every evaluation to SQLite.
                                        let signal_scores_json = serde_json::json!({
                                            "whale_consensus": result.signals.iter().find(|s| s.strategy == "whale_consensus").map(|s| s.score).unwrap_or(0),
                                            "accumulation": result.signals.iter().find(|s| s.strategy == "accumulation").map(|s| s.score).unwrap_or(0),
                                            "launch_momentum": result.signals.iter().find(|s| s.strategy == "launch_momentum").map(|s| s.score).unwrap_or(0),
                                            "volume_spike": result.signals.iter().find(|s| s.strategy == "volume_spike").map(|s| s.score).unwrap_or(0),
                                            "social": result.signals.iter().find(|s| s.strategy == "social").map(|s| s.score).unwrap_or(0),
                                        }).to_string();

                                        if let Err(e) = self.subsystems.portfolio.insert_evaluation(
                                            &result.token_address,
                                            result.confluence_score,
                                            result.safety_score,
                                            &signal_scores_json,
                                            result.passed,
                                            &result.reasoning,
                                        ).await {
                                            tracing::warn!(error = %e, "Failed to persist evaluation to DB");
                                        }

                                        if result.passed {
                                            self.log_decision(Decision {
                                                id: Uuid::new_v4(),
                                                timestamp: Utc::now(),
                                                state: AgentState::Evaluating,
                                                token_address: Some(token.address.clone()),
                                                signals: result.signals.clone(),
                                                safety_score: Some(result.safety_score),
                                                risk_report: None,
                                                action: "evaluate_pass".to_string(),
                                                reasoning: result.reasoning.clone(),
                                                outcome: None,
                                            }).await;

                                            // Risk check.
                                            match self.risk_check(&result).await {
                                                Ok(true) => {
                                                    match self.execute(&result).await {
                                                        Ok(()) => {
                                                            // Successful trade — reset progressive threshold.
                                                            self.progressive_threshold.write().await.record_success();
                                                        }
                                                        Err(e) => {
                                                            tracing::error!(error = %e, "Execution failed");
                                                        }
                                                    }
                                                }
                                                Ok(false) => {
                                                    tracing::info!(
                                                        token = &token.address,
                                                        "Risk check rejected trade"
                                                    );
                                                }
                                                Err(e) => {
                                                    tracing::error!(error = %e, "Risk check error");
                                                }
                                            }
                                        } else {
                                            // Only count confluence failures for progressive threshold lowering.
                                            // Safety failures are orthogonal to confluence quality and should not
                                            // cause the agent to accept weaker signals.
                                            let effective_threshold = self.progressive_threshold.read().await.current();
                                            let confluence_passed = result.confluence_score as f64 >= effective_threshold;
                                            if !confluence_passed {
                                                self.progressive_threshold.write().await.record_failure();
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            token = &token.address,
                                            error = %e,
                                            "Evaluation failed"
                                        );
                                    }
                                }

                                // Rate-limit between evaluations to respect Birdeye free tier.
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                                // Mark token as evaluated (cooldown 5 min to avoid re-evaluating same tokens).
                                self.eval_cooldown.write().await.insert(token.address.clone(), Utc::now());
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Scan failed");
                        }
                    }
                }

                // Periodic position monitoring.
                _ = monitor_interval.tick() => {
                    if let Err(e) = self.monitor().await {
                        tracing::error!(error = %e, "Monitor failed");
                    }

                    // Periodically reconcile on-chain positions with DB
                    // to catch any positions from trades that were confirmed late.
                    if let Err(e) = self.subsystems.exec.reconcile_positions(
                        &self.subsystems.portfolio,
                        &self.subsystems.dex,
                    ).await {
                        tracing::debug!(error = %e, "Periodic reconciliation skipped (non-fatal)");
                    }
                }
            }
        }

        tracing::info!("Agent main loop exited");
        Ok(())
    }

    /// Stop the agent.
    pub async fn stop(&self) {
        *self.running.write().await = false;
        tracing::info!("Agent stop requested");
    }

    /// Get all logged decisions.
    pub async fn decisions(&self) -> Vec<Decision> {
        self.decisions.read().await.clone()
    }

    /// Check if the agent is running.
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

// ─── Evaluation Result ───────────────────────────────────────────────────────

/// Result of evaluating a token through strategies + safety.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationResult {
    pub token_address: String,
    pub chain: Chain,
    pub confluence_score: u8,
    pub safety_score: u8,
    pub signals: Vec<Signal>,
    pub passed: bool,
    pub reasoning: String,
    /// Market cap in USD (from DexScreener).
    pub market_cap: Option<f64>,
    /// Token age in hours (from pair creation time).
    pub age_hours: Option<f64>,
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── ProgressiveThreshold Tests ──────────────────────────────────────

    #[test]
    fn test_progressive_threshold_starts_at_original() {
        let pt = ProgressiveThreshold::new(35.0, 20.0, 5.0, 50);
        assert!((pt.current() - 35.0).abs() < f64::EPSILON);
        assert!((pt.original() - 35.0).abs() < f64::EPSILON);
        assert!(!pt.is_lowered());
    }

    #[test]
    fn test_progressive_threshold_with_original_default() {
        let pt = ProgressiveThreshold::with_original(35.0);
        assert!((pt.current() - 35.0).abs() < f64::EPSILON);
        assert!((pt.original() - 35.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progressive_threshold_lowers_after_n_failures() {
        let mut pt = ProgressiveThreshold::new(35.0, 20.0, 5.0, 50);

        // Record 49 failures — should NOT lower yet.
        for _ in 0..49 {
            let lowered = pt.record_failure();
            assert!(!lowered);
        }
        assert!((pt.current() - 35.0).abs() < f64::EPSILON, "Should not lower before 50 failures");

        // 50th failure — should lower to 30.
        let lowered = pt.record_failure();
        assert!(lowered, "Should lower on 50th failure");
        assert!((pt.current() - 30.0).abs() < f64::EPSILON, "Should be 30 after first lowering, got {}", pt.current());
        assert!(pt.is_lowered());
    }

    #[test]
    fn test_progressive_threshold_respects_floor() {
        // Start at 30, step=5, floor=25 → can only lower once (30→25).
        let mut pt = ProgressiveThreshold::new(30.0, 25.0, 5.0, 10);

        // 10 failures → lower to 25 (at floor).
        for _ in 0..10 {
            pt.record_failure();
        }
        assert!((pt.current() - 25.0).abs() < f64::EPSILON, "Should be at floor 25, got {}", pt.current());

        // Another 10 failures → should stay at floor.
        for _ in 0..10 {
            let lowered = pt.record_failure();
            assert!(!lowered, "Should NOT lower below floor");
        }
        assert!((pt.current() - 25.0).abs() < f64::EPSILON, "Should still be at floor 25, got {}", pt.current());
    }

    #[test]
    fn test_progressive_threshold_resets_on_success() {
        let mut pt = ProgressiveThreshold::new(35.0, 20.0, 5.0, 50);

        // Lower to 30.
        for _ in 0..50 {
            pt.record_failure();
        }
        assert!((pt.current() - 30.0).abs() < f64::EPSILON);

        // Record success — should reset to 35.
        pt.record_success();
        assert!((pt.current() - 35.0).abs() < f64::EPSILON, "Should reset to original 35, got {}", pt.current());
        assert!(!pt.is_lowered());
    }

    #[test]
    fn test_progressive_threshold_stepwise_lowering() {
        // Start at 35, step=5, floor=20, failures=10 → should lower in steps: 35→30→25→20.
        let mut pt = ProgressiveThreshold::new(35.0, 20.0, 5.0, 10);

        // First lowering: 35→30 after 10 failures.
        for _ in 0..10 {
            pt.record_failure();
        }
        assert!((pt.current() - 30.0).abs() < f64::EPSILON, "First lowering: should be 30, got {}", pt.current());

        // Second lowering: 30→25 after another 10 failures.
        for _ in 0..10 {
            pt.record_failure();
        }
        assert!((pt.current() - 25.0).abs() < f64::EPSILON, "Second lowering: should be 25, got {}", pt.current());

        // Third lowering: 25→20 (at floor) after another 10 failures.
        for _ in 0..10 {
            pt.record_failure();
        }
        assert!((pt.current() - 20.0).abs() < f64::EPSILON, "Third lowering: should be at floor 20, got {}", pt.current());

        // No more lowering possible.
        for _ in 0..10 {
            let lowered = pt.record_failure();
            assert!(!lowered, "Should not lower below floor");
        }
        assert!((pt.current() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progressive_threshold_consecutive_counter_resets_on_success() {
        let mut pt = ProgressiveThreshold::new(35.0, 20.0, 5.0, 50);

        // 30 failures.
        for _ in 0..30 {
            pt.record_failure();
        }
        assert!((pt.current() - 35.0).abs() < f64::EPSILON, "Should not have lowered yet");

        // Success resets the counter.
        pt.record_success();

        // 30 more failures — should still be at 35 (counter reset, need 50 fresh).
        for _ in 0..30 {
            pt.record_failure();
        }
        assert!((pt.current() - 35.0).abs() < f64::EPSILON, "Should still be at 35 after success reset");

        // 20 more failures = 50 total since last success → should lower.
        for _ in 0..19 {
            pt.record_failure();
        }
        assert!((pt.current() - 35.0).abs() < f64::EPSILON, "Should not lower before 50");
        let lowered = pt.record_failure(); // 50th
        assert!(lowered, "Should lower on 50th failure after success reset");
        assert!((pt.current() - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_progressive_threshold_exact_floor_step() {
        // When current - step would go below floor, clamp to floor.
        let mut pt = ProgressiveThreshold::new(23.0, 20.0, 5.0, 10);

        for _ in 0..10 {
            pt.record_failure();
        }
        // 23 - 5 = 18, but floor is 20, so should clamp to 20.
        assert!((pt.current() - 20.0).abs() < f64::EPSILON, "Should clamp to floor 20, got {}", pt.current());
    }
}
