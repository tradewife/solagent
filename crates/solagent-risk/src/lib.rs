//! # solagent-risk
//!
//! Risk management with position sizing, drawdown limits, and circuit breaker.
//! Integrates with the portfolio manager for live portfolio state.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use solagent_core::chrono::{DateTime, Utc};
use solagent_core::{Position, Trade, TradeSide};
use std::collections::HashMap;
use std::sync::Arc;

// ─── Circuit Breaker ─────────────────────────────────────────────────────────

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitBreaker {
    /// Normal operation.
    Normal,
    /// Approaching limits — reduced position sizing.
    Warning,
    /// All trading halted.
    Halted,
}

impl std::fmt::Display for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreaker::Normal => write!(f, "NORMAL"),
            CircuitBreaker::Warning => write!(f, "WARNING"),
            CircuitBreaker::Halted => write!(f, "HALTED"),
        }
    }
}

// ─── Risk Configuration ──────────────────────────────────────────────────────

/// All configurable risk parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Max position size in USD (absolute cap).
    pub max_position_size_usd: f64,
    /// Percentage of portfolio risked per trade (e.g. 2.0 = 2%).
    pub per_trade_pct: f64,
    /// Max percentage of portfolio in any single token (e.g. 5.0 = 5%).
    pub max_per_token_pct: f64,
    /// Max portfolio risk as percentage.
    pub max_portfolio_risk_pct: f64,
    /// Max daily loss in USD.
    pub max_daily_loss_usd: f64,
    /// Max daily loss as percentage of portfolio.
    pub max_daily_loss_pct: f64,
    /// Drawdown percentage from peak that triggers halt.
    pub max_drawdown_pct: f64,
    /// Maximum number of concurrent open positions.
    pub max_open_positions: usize,
    /// Default stop loss percentage (e.g. 20.0 = -20%).
    pub default_stop_loss_pct: f64,
    /// Default take profit percentage (e.g. 50.0 = +50%).
    pub default_take_profit_pct: f64,
    /// Trailing stop percentage from peak (e.g. 10.0 = -10% from high).
    pub trailing_stop_pct: f64,
    /// Cooldown in seconds after a loss trade.
    pub cooldown_secs: u64,
    /// Warning threshold as percentage of drawdown.
    pub warning_threshold_pct: f64,
    /// Halt threshold as percentage of drawdown.
    pub halt_threshold_pct: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_size_usd: 500.0,
            per_trade_pct: 2.0,
            max_per_token_pct: 5.0,
            max_portfolio_risk_pct: 10.0,
            max_daily_loss_usd: 200.0,
            max_daily_loss_pct: 5.0,
            max_drawdown_pct: 10.0,
            max_open_positions: 10,
            default_stop_loss_pct: 20.0,
            default_take_profit_pct: 50.0,
            trailing_stop_pct: 10.0,
            cooldown_secs: 300,
            warning_threshold_pct: 7.0,
            halt_threshold_pct: 10.0,
        }
    }
}

impl From<solagent_core::RiskConfig> for RiskConfig {
    fn from(c: solagent_core::RiskConfig) -> Self {
        Self {
            max_position_size_usd: c.max_position_size_usd,
            per_trade_pct: 2.0,
            max_per_token_pct: 5.0,
            max_portfolio_risk_pct: c.max_portfolio_risk_pct,
            max_daily_loss_usd: c.max_daily_loss_usd,
            max_daily_loss_pct: 5.0,
            max_drawdown_pct: c.max_drawdown_pct,
            max_open_positions: c.max_open_positions,
            default_stop_loss_pct: c.default_stop_loss_pct,
            default_take_profit_pct: c.default_take_profit_pct,
            trailing_stop_pct: c.trailing_stop_pct,
            cooldown_secs: c.cooldown_secs,
            warning_threshold_pct: c.max_drawdown_pct * 0.7,
            halt_threshold_pct: c.max_drawdown_pct,
        }
    }
}

// ─── Risk Report ─────────────────────────────────────────────────────────────

/// A logged risk decision for auditing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskReport {
    pub timestamp: DateTime<Utc>,
    pub action: String,
    pub token_address: Option<String>,
    pub checks: Vec<RiskCheck>,
    pub passed: bool,
    pub circuit_breaker: CircuitBreaker,
    pub reason: String,
}

/// Individual risk check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheck {
    pub name: String,
    pub passed: bool,
    pub current_value: String,
    pub limit: String,
}

// ─── Risk Manager ────────────────────────────────────────────────────────────

/// Manages all risk parameters and tracks portfolio state.
pub struct RiskManager {
    config: RiskConfig,
    /// Daily realized PnL tracking (reset at midnight UTC).
    daily_pnl: Arc<RwLock<f64>>,
    /// Last trade timestamp per token (for cooldown).
    last_trade_time: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    /// Peak portfolio value for drawdown calculation.
    peak_value: Arc<RwLock<f64>>,
    /// Current circuit breaker state.
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    /// Decision log.
    decision_log: Arc<RwLock<Vec<RiskReport>>>,
}

impl RiskManager {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            daily_pnl: Arc::new(RwLock::new(0.0)),
            last_trade_time: Arc::new(RwLock::new(HashMap::new())),
            peak_value: Arc::new(RwLock::new(0.0)),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::Normal)),
            decision_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get the current circuit breaker state.
    pub fn circuit_breaker(&self) -> CircuitBreaker {
        *self.circuit_breaker.read()
    }

    /// Check if a proposed position size is within limits.
    pub fn check_position_size(&self, size_usd: f64) -> RiskCheck {
        let passed = size_usd <= self.config.max_position_size_usd;
        RiskCheck {
            name: "position_size".to_string(),
            passed,
            current_value: format!("{size_usd:.2} USD"),
            limit: format!("{:.2} USD", self.config.max_position_size_usd),
        }
    }

    /// Check if we can open another position.
    pub fn check_max_positions(&self, current_open: usize) -> RiskCheck {
        let passed = current_open < self.config.max_open_positions;
        RiskCheck {
            name: "max_positions".to_string(),
            passed,
            current_value: format!("{current_open}"),
            limit: format!("{}", self.config.max_open_positions),
        }
    }

    /// Check daily loss limit.
    pub fn check_daily_loss(&self) -> RiskCheck {
        let daily = *self.daily_pnl.read();
        let passed = daily.abs() < self.config.max_daily_loss_usd;
        RiskCheck {
            name: "daily_loss".to_string(),
            passed,
            current_value: format!("{daily:.2} USD"),
            limit: format!("{:.2} USD", self.config.max_daily_loss_usd),
        }
    }

    /// Check portfolio drawdown from peak.
    pub fn check_drawdown(&self, current_value: f64) -> RiskCheck {
        let peak = *self.peak_value.read();
        let drawdown_pct = if peak > 0.0 {
            ((peak - current_value) / peak) * 100.0
        } else {
            0.0
        };
        let passed = drawdown_pct < self.config.max_drawdown_pct;
        RiskCheck {
            name: "drawdown".to_string(),
            passed,
            current_value: format!("{drawdown_pct:.1}%"),
            limit: format!("{:.1}%", self.config.max_drawdown_pct),
        }
    }

    /// Check position correlation (simplified: just checks token isn't duplicate).
    pub fn check_correlation(&self, token_address: &str, open_positions: &[Position]) -> RiskCheck {
        let duplicate = open_positions.iter().any(|p| p.token_address == token_address);
        RiskCheck {
            name: "correlation".to_string(),
            passed: !duplicate,
            current_value: if duplicate {
                "Already held".to_string()
            } else {
                "New position".to_string()
            },
            limit: "No duplicates".to_string(),
        }
    }

    /// Check cooldown period since last trade on this token.
    pub fn check_cooldown(&self, token_address: &str) -> RiskCheck {
        let times = self.last_trade_time.read();
        let passed = match times.get(token_address) {
            Some(last) => {
                let elapsed = (Utc::now() - *last).num_seconds();
                elapsed >= self.config.cooldown_secs as i64
            }
            None => true,
        };
        RiskCheck {
            name: "cooldown".to_string(),
            passed,
            current_value: match times.get(token_address) {
                Some(last) => {
                    let elapsed = (Utc::now() - *last).num_seconds();
                    format!("{elapsed}s ago")
                }
                None => "Never traded".to_string(),
            },
            limit: format!("{}s", self.config.cooldown_secs),
        }
    }

    /// Run all risk checks for a proposed trade and return the combined result.
    pub fn evaluate_trade(
        &self,
        token_address: &str,
        size_usd: f64,
        current_open_positions: usize,
        portfolio_value: f64,
        open_positions: &[Position],
    ) -> RiskReport {
        let checks = vec![
            self.check_position_size(size_usd),
            self.check_max_positions(current_open_positions),
            self.check_daily_loss(),
            self.check_drawdown(portfolio_value),
            self.check_correlation(token_address, open_positions),
            self.check_cooldown(token_address),
        ];

        let all_passed = checks.iter().all(|c| c.passed);
        let reason = if all_passed {
            "All risk checks passed".to_string()
        } else {
            let failed: Vec<&str> = checks.iter().filter(|c| !c.passed).map(|c| c.name.as_str()).collect();
            format!("Failed checks: {}", failed.join(", "))
        };

        // Update circuit breaker based on drawdown.
        {
            let peak = *self.peak_value.read();
            if peak > 0.0 {
                let dd_pct = ((peak - portfolio_value) / peak) * 100.0;
                let mut cb = self.circuit_breaker.write();
                if dd_pct >= self.config.halt_threshold_pct {
                    *cb = CircuitBreaker::Halted;
                } else if dd_pct >= self.config.warning_threshold_pct {
                    *cb = CircuitBreaker::Warning;
                } else {
                    *cb = CircuitBreaker::Normal;
                }
            }
        }

        let report = RiskReport {
            timestamp: Utc::now(),
            action: "evaluate_trade".to_string(),
            token_address: Some(token_address.to_string()),
            checks,
            passed: all_passed,
            circuit_breaker: self.circuit_breaker(),
            reason,
        };

        // Log the decision.
        self.decision_log.write().push(report.clone());

        report
    }

    /// Record that a trade was executed (updates daily PnL and cooldown).
    pub fn record_trade(&self, trade: &Trade) {
        if trade.side == TradeSide::Sell {
            let mut daily = self.daily_pnl.write();
            // Approximate realized PnL from the trade size.
            *daily -= trade.size_usd; // Simplified; real impl would track cost basis.
        }
        self.last_trade_time
            .write()
            .insert(trade.token_address.clone(), trade.executed_at);
    }

    /// Update peak portfolio value.
    pub fn update_peak(&self, current_value: f64) {
        let mut peak = self.peak_value.write();
        if current_value > *peak {
            *peak = current_value;
        }
    }

    /// Reset daily PnL (call at midnight UTC).
    pub fn reset_daily(&self) {
        *self.daily_pnl.write() = 0.0;
    }

    /// Get the decision log.
    pub fn decision_log(&self) -> Vec<RiskReport> {
        self.decision_log.read().clone()
    }

    // ─── Position Sizing ─────────────────────────────────────────────────

    /// Calculate the position size for a new trade based on portfolio value.
    pub fn calculate_position_size(&self, portfolio_value: f64) -> f64 {
        let by_pct = portfolio_value * self.config.per_trade_pct / 100.0;
        let by_token_cap = portfolio_value * self.config.max_per_token_pct / 100.0;
        let absolute_cap = self.config.max_position_size_usd;

        by_pct.min(by_token_cap).min(absolute_cap)
    }

    // ─── Dynamic Exit Profiles ──────────────────────────────────────────

    /// Select exit profile based on token characteristics.
    /// Cascading: first match wins.
    pub fn select_exit_profile(
        market_cap: Option<f64>,
        age_hours: Option<f64>,
        confluence_score: u8,
    ) -> ExitProfile {
        let mcap = market_cap.unwrap_or(f64::MAX);
        let age = age_hours.unwrap_or(f64::MAX);

        if mcap < 100_000.0 && age < 1.0 && confluence_score >= 80 {
            ExitProfile::moonbag()
        } else if mcap < 1_000_000.0 && age < 24.0 && confluence_score >= 70 {
            ExitProfile::runner()
        } else if mcap < 10_000_000.0 || age < 168.0 {
            ExitProfile::swing()
        } else {
            ExitProfile::conservative()
        }
    }

    /// Calculate SL/TP/trailing for a position based on its exit profile.
    /// Returns (stop_loss_price, take_profit_price, trailing_stop_pct).
    /// take_profit_price is None when we ride purely on trailing stop.
    pub fn calculate_exit(
        entry_price: f64,
        market_cap: Option<f64>,
        age_hours: Option<f64>,
        confluence_score: u8,
    ) -> (f64, Option<f64>, f64) {
        let profile = Self::select_exit_profile(market_cap, age_hours, confluence_score);
        let sl = entry_price * (1.0 - profile.stop_loss_pct / 100.0);
        let tp = profile.take_profit_pct
            .map(|pct| entry_price * (1.0 + pct / 100.0));
        (sl, tp, profile.trailing_stop_pct)
    }
}

// ─── Exit Profile ────────────────────────────────────────────────────────────

/// Dynamic exit strategy based on token characteristics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitProfile {
    pub name: String,
    /// Stop loss percentage (e.g. 20.0 = -20%).
    pub stop_loss_pct: f64,
    /// Take profit percentage (None = no hard TP, ride the trailing stop).
    pub take_profit_pct: Option<f64>,
    /// Trailing stop percentage from peak (e.g. 25.0 = -25% from peak).
    pub trailing_stop_pct: f64,
}

impl ExitProfile {
    /// Moonbag: tiny cap, brand new, strong signal. Ride it hard.
    /// No hard TP -- let the 25% trailing stop do the work.
    pub fn moonbag() -> Self {
        Self {
            name: "moonbag".to_string(),
            stop_loss_pct: 20.0,
            take_profit_pct: None,
            trailing_stop_pct: 25.0,
        }
    }

    /// Runner: small cap, fresh launch, decent signal.
    /// Hard TP at 500% (6x) as backstop, 20% trailing does most exits.
    pub fn runner() -> Self {
        Self {
            name: "runner".to_string(),
            stop_loss_pct: 15.0,
            take_profit_pct: Some(500.0),
            trailing_stop_pct: 20.0,
        }
    }

    /// Swing: mid-cap or a few days old. Tighter parameters.
    pub fn swing() -> Self {
        Self {
            name: "swing".to_string(),
            stop_loss_pct: 12.0,
            take_profit_pct: Some(200.0),
            trailing_stop_pct: 15.0,
        }
    }

    /// Conservative: large cap or weak signal. Take profits early.
    pub fn conservative() -> Self {
        Self {
            name: "conservative".to_string(),
            stop_loss_pct: 10.0,
            take_profit_pct: Some(50.0),
            trailing_stop_pct: 10.0,
        }
    }
}

// ─── RiskManager continued (methods that use ExitProfile) ─────────────────────

impl RiskManager {
    /// Check if a position should be stopped out.
    ///
    /// Returns `Some(reason)` if the position should be closed, `None` otherwise.
    /// `trailing_stop_pct` is per-position (from ExitProfile), not the global default.
    pub fn check_stop_conditions(
        &self,
        current_price: f64,
        entry_price: f64,
        stop_loss: Option<f64>,
        take_profit: Option<f64>,
        peak_price: Option<f64>,
        trailing_stop_pct: f64,
    ) -> Option<String> {
        // Hard stop loss.
        let sl = stop_loss.unwrap_or_else(|| entry_price * (1.0 - self.config.default_stop_loss_pct / 100.0));
        if current_price <= sl {
            return Some(format!("Stop loss hit: ${current_price:.6} <= ${sl:.6}"));
        }

        // Take profit (may be None for moonbag profile).
        if let Some(tp) = take_profit {
            if current_price >= tp {
                return Some(format!("Take profit hit: ${current_price:.6} >= ${tp:.6}"));
            }
        }

        // Trailing stop -- uses the per-position trailing pct.
        if let Some(peak) = peak_price {
            if peak > entry_price {
                let trail = peak * (1.0 - trailing_stop_pct / 100.0);
                if current_price <= trail {
                    return Some(format!(
                        "Trailing stop hit: ${current_price:.6} <= ${trail:.6} (peak: ${peak:.6}, trail: -{trailing_stop_pct:.0}%)"
                    ));
                }
            }
        }

        None
    }

    /// Check if the circuit breaker would allow a new trade.
    pub fn can_trade(&self) -> bool {
        !matches!(self.circuit_breaker(), CircuitBreaker::Halted)
    }
}
