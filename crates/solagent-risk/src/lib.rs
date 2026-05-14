//! # solagent-risk
//!
//! Risk management with position sizing, drawdown limits, and circuit breaker.

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
    pub max_position_size_usd: f64,
    pub max_portfolio_risk_pct: f64,
    pub max_daily_loss_usd: f64,
    pub max_drawdown_pct: f64,
    pub max_open_positions: usize,
    pub default_stop_loss_pct: f64,
    pub default_take_profit_pct: f64,
    pub cooldown_secs: u64,
    pub warning_threshold_pct: f64,
    pub halt_threshold_pct: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_size_usd: 500.0,
            max_portfolio_risk_pct: 10.0,
            max_daily_loss_usd: 200.0,
            max_drawdown_pct: 15.0,
            max_open_positions: 10,
            default_stop_loss_pct: 20.0,
            default_take_profit_pct: 100.0,
            cooldown_secs: 300,
            warning_threshold_pct: 70.0,
            halt_threshold_pct: 90.0,
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
}
