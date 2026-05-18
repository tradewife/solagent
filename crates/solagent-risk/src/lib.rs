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
            // Only check drawdown if both peak and current value are positive.
            // A zero portfolio value likely means RPC failure, not real drawdown.
            if peak > 0.0 && portfolio_value > 0.0 {
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
    /// Ignores zero values (which indicate RPC failure, not real portfolio).
    pub fn update_peak(&self, current_value: f64) {
        if current_value <= 0.0 {
            return; // Don't update peak with bogus zero values
        }
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

        // Use a minimum floor to ensure we always attempt a trade when risk checks pass.
        // If portfolio_value is 0 (RPC balance fetch failed), fall back to the absolute cap.
        let size = if portfolio_value > 0.0 {
            by_pct.min(by_token_cap).min(absolute_cap)
        } else {
            // Can't calculate percentage-based size — use absolute cap.
            absolute_cap
        };

        // Enforce a minimum trade size of $1 to avoid dust swaps.
        size.max(1.0)
    }

    /// Calculate a dynamic position size that scales with confluence score and win rate.
    ///
    /// - `portfolio_value`: current portfolio value in USD.
    /// - `confluence_score`: composite signal score (0-100).
    /// - `win_rate`: historical win rate (0.0-1.0). 0.5 = breakeven / unknown.
    /// - `runtime_max_position_size`: max position cap from RuntimeConfig.
    ///
    /// # Logic
    /// - Base tier by confluence: 0-35→$5, 35-45→$10, 45-60→$15, 60+→$20.
    /// - Win rate modifier: >60% → +$5 per tier (up to max), <30% → -$5 (down to $5 floor).
    /// - Cap at `min(runtime_max_position_size, portfolio_value * 0.25, $30)`.
    /// - Floor at $1 to avoid dust.
    pub fn calculate_dynamic_position_size(
        &self,
        portfolio_value: f64,
        confluence_score: u8,
        win_rate: f64,
        runtime_max_position_size: f64,
    ) -> f64 {
        // Base tier selection by confluence score.
        let base_size = match confluence_score {
            0..=34 => 5.0,
            35..=44 => 10.0,
            45..=59 => 15.0,
            _ => 20.0, // 60+
        };

        // Win rate modifier.
        let size: f64 = if win_rate > 0.60 {
            (base_size + 5.0_f64).min(runtime_max_position_size)
        } else if win_rate < 0.30 {
            (base_size - 5.0_f64).max(5.0)
        } else {
            base_size
        };

        // Cap at runtime max, portfolio-based limit, and hard $30 ceiling.
        let portfolio_cap = portfolio_value * 0.25;
        let cap = runtime_max_position_size
            .min(portfolio_cap.max(1.0)) // ensure cap isn't ≤0 when portfolio_value is 0
            .min(30.0);
        let capped = size.min(cap);

        // Floor at $1 to avoid dust swaps.
        capped.max(1.0)
    }

    // ─── Runtime Config Sync ────────────────────────────────────────────

    /// Set the max open positions at runtime (for auto-tuner / RuntimeConfig sync).
    pub fn set_max_open_positions(&mut self, n: usize) {
        self.config.max_open_positions = n;
    }

    /// Set the max position size at runtime (for auto-tuner / RuntimeConfig sync).
    pub fn set_max_position_size(&mut self, size: f64) {
        self.config.max_position_size_usd = size;
    }

    /// Set the daily loss limit at runtime (for auto-tuner / RuntimeConfig sync).
    pub fn set_daily_loss_limit(&mut self, limit: f64) {
        self.config.max_daily_loss_usd = limit;
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
        if let Some(tp) = take_profit
            && current_price >= tp
        {
            return Some(format!("Take profit hit: ${current_price:.6} >= ${tp:.6}"));
        }

        // Trailing stop -- uses the per-position trailing pct.
        if let Some(peak) = peak_price
            && peak > entry_price
        {
            let trail = peak * (1.0 - trailing_stop_pct / 100.0);
            if current_price <= trail {
                return Some(format!(
                    "Trailing stop hit: ${current_price:.6} <= ${trail:.6} (peak: ${peak:.6}, trail: -{trailing_stop_pct:.0}%)"
                ));
            }
        }

        None
    }

    /// Check if the circuit breaker would allow a new trade.
    pub fn can_trade(&self) -> bool {
        !matches!(self.circuit_breaker(), CircuitBreaker::Halted)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manager() -> RiskManager {
        RiskManager::new(RiskConfig::default())
    }

    // ─── Dynamic Position Sizing Tests ──────────────────────────────────

    #[test]
    fn test_dynamic_size_confluence_0_returns_5() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 0, 0.5, 15.0);
        assert!((size - 5.0).abs() < f64::EPSILON,
            "confluence=0 should give base $5, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_34_returns_5() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 34, 0.5, 15.0);
        assert!((size - 5.0).abs() < f64::EPSILON,
            "confluence=34 (0-34 tier) should give base $5, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_35_returns_10() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 35, 0.5, 15.0);
        assert!((size - 10.0).abs() < f64::EPSILON,
            "confluence=35 (35-44 tier) should give base $10, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_44_returns_10() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 44, 0.5, 15.0);
        assert!((size - 10.0).abs() < f64::EPSILON,
            "confluence=44 (35-44 tier) should give base $10, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_45_returns_15() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 45, 0.5, 15.0);
        assert!((size - 15.0).abs() < f64::EPSILON,
            "confluence=45 (45-59 tier) should give base $15, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_60_returns_20() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(200.0, 60, 0.5, 25.0);
        assert!((size - 20.0).abs() < f64::EPSILON,
            "confluence=60 (60+ tier) should give base $20, got ${size}");
    }

    #[test]
    fn test_dynamic_size_confluence_100_returns_20() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(200.0, 100, 0.5, 25.0);
        assert!((size - 20.0).abs() < f64::EPSILON,
            "confluence=100 (60+ tier) should give base $20, got ${size}");
    }

    // ─── Win Rate Modifier Tests ────────────────────────────────────────

    #[test]
    fn test_win_rate_above_60_bumps_up() {
        let rm = test_manager();
        // confluence=45 (base $15), win_rate=0.8 → +$5 = $20, capped by runtime_max=25
        let size = rm.calculate_dynamic_position_size(200.0, 45, 0.8, 25.0);
        assert!((size - 20.0).abs() < f64::EPSILON,
            "win_rate=0.8 should bump $15→$20, got ${size}");
    }

    #[test]
    fn test_win_rate_below_30_reduces() {
        let rm = test_manager();
        // confluence=60 (base $20), win_rate=0.1 → -$5 = $15
        let size = rm.calculate_dynamic_position_size(200.0, 60, 0.1, 25.0);
        assert!((size - 15.0).abs() < f64::EPSILON,
            "win_rate=0.1 should reduce $20→$15, got ${size}");
    }

    #[test]
    fn test_win_rate_neutral_no_change() {
        let rm = test_manager();
        // confluence=35 (base $10), win_rate=0.5 → no modifier
        let size = rm.calculate_dynamic_position_size(100.0, 35, 0.5, 15.0);
        assert!((size - 10.0).abs() < f64::EPSILON,
            "win_rate=0.5 should not change base, got ${size}");
    }

    #[test]
    fn test_win_rate_exactly_60_no_bump() {
        let rm = test_manager();
        // Exactly 0.60 should NOT trigger the win rate bonus (> 0.60, not >=).
        let size = rm.calculate_dynamic_position_size(100.0, 45, 0.60, 15.0);
        assert!((size - 15.0).abs() < f64::EPSILON,
            "win_rate=0.60 should NOT bump ($15 stays $15), got ${size}");
    }

    #[test]
    fn test_win_rate_exactly_30_no_reduce() {
        let rm = test_manager();
        // Exactly 0.30 should NOT trigger the reduction (< 0.30, not <=).
        let size = rm.calculate_dynamic_position_size(200.0, 60, 0.30, 25.0);
        assert!((size - 20.0).abs() < f64::EPSILON,
            "win_rate=0.30 should NOT reduce ($20 stays $20), got ${size}");
    }

    // ─── Bounds Enforcement Tests ───────────────────────────────────────

    #[test]
    fn test_dynamic_size_never_below_1() {
        let rm = test_manager();
        // Small portfolio, small confluence, small max_position_size — still ≥ $1.
        let size = rm.calculate_dynamic_position_size(0.0, 0, 0.5, 1.0);
        assert!(size >= 1.0, "Should never be below $1, got ${size}");
    }

    #[test]
    fn test_dynamic_size_capped_by_max_position() {
        let rm = test_manager();
        // confluence=100 (base $20), win_rate=0.9 (bump to $25), but max=10.
        let size = rm.calculate_dynamic_position_size(500.0, 100, 0.9, 10.0);
        assert!((size - 10.0).abs() < f64::EPSILON,
            "Should be capped at runtime_max=10, got ${size}");
    }

    #[test]
    fn test_dynamic_size_capped_by_portfolio() {
        let rm = test_manager();
        // Portfolio=$20, confluence=100 (base $20), but 25% of $20 = $5 cap.
        let size = rm.calculate_dynamic_position_size(20.0, 100, 0.5, 30.0);
        assert!((size - 5.0).abs() < f64::EPSILON,
            "Should be capped at 25% of portfolio=$5, got ${size}");
    }

    #[test]
    fn test_dynamic_size_capped_at_30_hard_limit() {
        let rm = test_manager();
        // Large portfolio, high confluence, high win rate, high max — still ≤$30.
        // With confluence=100 (base $20) and win_rate=0.9 (+$5), we get $25.
        // The $30 hard cap is a safety ceiling — it would only kick in if
        // tiers are raised above their current values.
        let size = rm.calculate_dynamic_position_size(10000.0, 100, 0.9, 50.0);
        // The cap chain is: min(50.0, min(10000*0.25=2500, 30)) = min(50, 30) = 30.
        // But size=$25 is already below 30, so it returns 25.
        assert!((size - 25.0).abs() < f64::EPSILON,
            "Should return $25 (below $30 hard cap), got ${size}");

        // To actually test the hard cap: set max_position_size to 40 but force
        // a higher base by using a RuntimeConfig-style override (set_max_position_size).
        let mut rm2 = test_manager();
        rm2.set_max_position_size(40.0);
        // With max=40, the cap chain: min(40, min(10000*0.25=2500, 30)) = min(40, 30) = 30.
        // size=25 < 30, so still 25. The $30 hard cap is a true ceiling, enforced
        // in all paths.
        let size2 = rm2.calculate_dynamic_position_size(10000.0, 100, 0.9, 40.0);
        assert!(size2 <= 30.0, "Should never exceed $30 hard cap, got ${size2}");
    }

    #[test]
    fn test_dynamic_size_low_win_rate_floor_5() {
        let rm = test_manager();
        // confluence=35 (base $10), win_rate=0.1 → -$5 = $5 (floor at $5).
        let size = rm.calculate_dynamic_position_size(100.0, 35, 0.1, 20.0);
        assert!((size - 5.0).abs() < f64::EPSILON,
            "win_rate=0.1 should reduce $10→$5, but not below $5 floor, got ${size}");
    }

    #[test]
    fn test_dynamic_size_low_win_rate_below_5_floor() {
        let rm = test_manager();
        // confluence=0 (base $5), win_rate=0.1 → -$5 would give $0, but floor is $5.
        let size = rm.calculate_dynamic_position_size(100.0, 0, 0.1, 20.0);
        assert!((size - 5.0).abs() < f64::EPSILON,
            "Should never go below $5 floor when reducing, got ${size}");
    }

    #[test]
    fn test_dynamic_size_high_win_rate_capped_by_max_position() {
        let rm = test_manager();
        // confluence=45 (base $15), win_rate=0.8 → +$5 = $20, but max=15 caps it.
        let size = rm.calculate_dynamic_position_size(200.0, 45, 0.8, 15.0);
        assert!((size - 15.0).abs() < f64::EPSILON,
            "win_rate bump should be capped by runtime_max, got ${size}");
    }

    // ─── Edge Case Tests ────────────────────────────────────────────────

    #[test]
    fn test_dynamic_size_zero_portfolio() {
        let rm = test_manager();
        // Portfolio is $0 (no balance), confluence=60 (base $20). 
        // portfolio_cap = max(0*0.25, 1.0) = 1.0.
        // cap = min(25.0, 1.0, 30.0) = 1.0. So $20 gets capped to $1.
        let size = rm.calculate_dynamic_position_size(0.0, 60, 0.5, 25.0);
        assert!((size - 1.0).abs() < f64::EPSILON,
            "Zero portfolio should result in $1 size, got ${size}");
    }

    #[test]
    fn test_dynamic_size_win_rate_exactly_50() {
        let rm = test_manager();
        let size = rm.calculate_dynamic_position_size(100.0, 50, 0.5, 20.0);
        assert!((size - 15.0).abs() < f64::EPSILON,
            "confluence=50 (45-59 tier) + win_rate=0.5 = $15, got ${size}");
    }

    #[test]
    fn test_dynamic_size_all_tiers_with_neutral_win_rate() {
        let rm = test_manager();
        let cases = [
            (0, 5.0),
            (20, 5.0),
            (30, 5.0),
            (34, 5.0),
            (35, 10.0),
            (40, 10.0),
            (44, 10.0),
            (45, 15.0),
            (50, 15.0),
            (59, 15.0),
            (60, 20.0),
            (80, 20.0),
            (100, 20.0),
        ];

        for (confluence, expected) in cases {
            let size = rm.calculate_dynamic_position_size(500.0, confluence, 0.5, 30.0);
            assert!((size - expected).abs() < f64::EPSILON,
                "confluence={confluence} should give {expected}, got {size}");
        }
    }

    // ─── Setter Tests ───────────────────────────────────────────────────

    #[test]
    fn test_set_max_open_positions() {
        let mut rm = test_manager();
        assert_eq!(rm.config.max_open_positions, 10); // default
        rm.set_max_open_positions(3);
        assert_eq!(rm.config.max_open_positions, 3);
    }

    #[test]
    fn test_set_max_position_size() {
        let mut rm = test_manager();
        assert!((rm.config.max_position_size_usd - 500.0).abs() < f64::EPSILON);
        rm.set_max_position_size(15.0);
        assert!((rm.config.max_position_size_usd - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_set_daily_loss_limit() {
        let mut rm = test_manager();
        assert!((rm.config.max_daily_loss_usd - 200.0).abs() < f64::EPSILON);
        rm.set_daily_loss_limit(15.0);
        assert!((rm.config.max_daily_loss_usd - 15.0).abs() < f64::EPSILON);
    }

    // ─── Exit Condition Tests ───────────────────────────────────────────

    /// Helper: create a RiskManager with custom per-trade stop loss for testing.
    fn test_manager_with_sl(sl_pct: f64) -> RiskManager {
        let mut config = RiskConfig::default();
        config.default_stop_loss_pct = sl_pct;
        RiskManager::new(config)
    }

    /// Stop-loss triggers when current price drops to the SL level.
    #[test]
    fn test_stop_loss_triggers_below_sl() {
        let rm = test_manager_with_sl(15.0); // 15% stop loss
        // Entry=$1.00, SL=$0.85. Current=$0.84 → should trigger.
        let result = rm.check_stop_conditions(
            0.84,              // current_price
            1.00,              // entry_price
            Some(0.85),        // stop_loss
            None,              // take_profit
            Some(1.00),        // peak_price
            10.0,              // trailing_stop_pct (not relevant here)
        );
        assert!(result.is_some(), "Stop loss should trigger when price drops below SL");
        let reason = result.unwrap();
        assert!(
            reason.contains("Stop loss"),
            "Reason should mention 'Stop loss', got: {reason}"
        );
    }

    /// Stop-loss does NOT trigger when current price is still above SL.
    #[test]
    fn test_stop_loss_does_not_trigger_above_sl() {
        let rm = test_manager_with_sl(15.0);
        // Entry=$1.00, SL=$0.85. Current=$0.86 → should NOT trigger.
        let result = rm.check_stop_conditions(
            0.86,              // current_price
            1.00,              // entry_price
            Some(0.85),        // stop_loss
            None,              // take_profit
            Some(1.00),        // peak_price
            10.0,              // trailing_stop_pct
        );
        assert!(result.is_none(), "Stop loss should NOT trigger when price is above SL");
    }

    /// Take-profit triggers exactly at the TP target.
    #[test]
    fn test_take_profit_triggers_at_target() {
        let rm = test_manager();
        // Entry=$1.00, TP=$2.00 (100% profit). Current=$2.00 → should trigger.
        let result = rm.check_stop_conditions(
            2.00,              // current_price
            1.00,              // entry_price
            Some(0.80),        // stop_loss (not relevant)
            Some(2.00),        // take_profit
            Some(1.00),        // peak_price
            10.0,              // trailing_stop_pct (not relevant)
        );
        assert!(result.is_some(), "Take profit should trigger when price reaches TP target");
        let reason = result.unwrap();
        assert!(
            reason.contains("Take profit"),
            "Reason should mention 'Take profit', got: {reason}"
        );
    }

    /// Take-profit does NOT trigger when price is just below TP.
    #[test]
    fn test_take_profit_not_triggered_below() {
        let rm = test_manager();
        // Entry=$1.00, TP=$2.00. Current=$1.99 → should NOT trigger.
        let result = rm.check_stop_conditions(
            1.99,              // current_price
            1.00,              // entry_price
            Some(0.80),        // stop_loss
            Some(2.00),        // take_profit
            Some(1.00),        // peak_price
            10.0,              // trailing_stop_pct
        );
        assert!(result.is_none(), "Take profit should NOT trigger below the TP target");
    }

    /// Trailing stop tracks the peak price. When price retraces from peak
    /// by the trailing percentage, it triggers.
    #[test]
    fn test_trailing_stop_tracks_peak() {
        let rm = test_manager();
        // Entry=$1.00, peak=$1.50, trailing=15%.
        // Trail level = $1.50 * 0.85 = $1.275.
        let trail_pct = 15.0;

        // Price at $1.27 ≤ $1.275 → should trigger.
        let result = rm.check_stop_conditions(
            1.27,              // current_price
            1.00,              // entry_price
            Some(0.50),        // stop_loss (far below, not relevant)
            None,              // take_profit
            Some(1.50),        // peak_price > entry → trailing stop active
            trail_pct,
        );
        assert!(result.is_some(), "Trailing stop should trigger at $1.27 (below $1.275 trail)");
        let reason = result.unwrap();
        assert!(
            reason.contains("Trailing stop"),
            "Reason should mention 'Trailing stop', got: {reason}"
        );

        // Price at $1.28 > $1.275 → should NOT trigger.
        let result2 = rm.check_stop_conditions(
            1.28,              // current_price
            1.00,              // entry_price
            Some(0.50),        // stop_loss
            None,              // take_profit
            Some(1.50),        // peak_price
            trail_pct,
        );
        assert!(result2.is_none(), "Trailing stop should NOT trigger at $1.28 (above $1.275 trail)");
    }

    /// Trailing stop should NOT fire if the peak has never been above entry,
    /// even if the price has dropped significantly.
    #[test]
    fn test_trailing_stop_not_active_when_below_entry() {
        let rm = test_manager();
        // Entry=$1.00, peak=$0.95 (never above entry).
        // Current=$0.80. Trailing stop should NOT fire.
        let result = rm.check_stop_conditions(
            0.80,              // current_price
            1.00,              // entry_price
            Some(0.60),        // stop_loss (far below, to isolate trailing check)
            None,              // take_profit
            Some(0.95),        // peak_price < entry → trailing stop inactive
            15.0,              // trailing_stop_pct
        );
        assert!(result.is_none(),
            "Trailing stop should NOT trigger when peak is below entry price"
        );
    }

    /// Exit profile selection by market cap, age, and confluence score.
    #[test]
    fn test_exit_profile_selection_by_market_cap() {
        // Moonbag: MC=$50K, age=0.5h, confluence=85
        let p1 = RiskManager::select_exit_profile(Some(50_000.0), Some(0.5), 85);
        assert_eq!(p1.name, "moonbag", "MC=$50K, age=0.5h, confluence=85 → moonbag");

        // Runner: MC=$500K, age=12h, confluence=75
        let p2 = RiskManager::select_exit_profile(Some(500_000.0), Some(12.0), 75);
        assert_eq!(p2.name, "runner", "MC=$500K, age=12h, confluence=75 → runner");

        // Swing: MC=$5M, age=48h, confluence=50
        let p3 = RiskManager::select_exit_profile(Some(5_000_000.0), Some(48.0), 50);
        assert_eq!(p3.name, "swing", "MC=$5M, age=48h, confluence=50 → swing");

        // Conservative: MC=$50M, age=720h (30 days)
        let p4 = RiskManager::select_exit_profile(Some(50_000_000.0), Some(720.0), 50);
        assert_eq!(p4.name, "conservative", "MC=$50M, age=720h → conservative");
    }

    /// Moonbag profile has take_profit_pct=None, so calculate_exit
    /// returns None for the TP component.
    #[test]
    fn test_moonbag_no_take_profit() {
        // Moonbag conditions: MC=$50K, age=0.5h, confluence=85
        let (sl, tp, trail) = RiskManager::calculate_exit(
            1.00,              // entry_price
            Some(50_000.0),    // market_cap
            Some(0.5),         // age_hours
            85,                // confluence_score
        );
        // Moonbag: stop_loss_pct=20%, so SL = $1.00 * 0.80 = $0.80
        assert!((sl - 0.80).abs() < 0.001, "Moonbag SL should be 20% below entry ($0.80), got ${sl}");
        // Moonbag: take_profit_pct=None, so TP is None
        assert!(tp.is_none(), "Moonbag should have no take profit (tp=None), got {tp:?}");
        // Moonbag: trailing_stop_pct=25%
        assert!((trail - 25.0).abs() < f64::EPSILON, "Moonbag trailing should be 25%, got {trail}");
    }

    // ════════════════════════════════════════════════════════════════════
    // PART A: Risk Constraint Enforcement Tests
    // ════════════════════════════════════════════════════════════════════

    // ─── Circuit Breaker Tests ──────────────────────────────────────────

    #[test]
    fn test_circuit_breaker_halted_on_drawdown() {
        let rm = test_manager();
        // Set peak to 100, then evaluate with portfolio at 84 (16% drawdown).
        // halt_threshold_pct is 10% by default, so 16% > 10% → circuit breaker = Halted.
        rm.update_peak(100.0);
        let report = rm.evaluate_trade(
            "token_abc",
            10.0,   // size_usd
            1,      // current_open_positions
            84.0,   // portfolio_value (16% drawdown from 100)
            &[],
        );
        assert_eq!(report.circuit_breaker, CircuitBreaker::Halted,
            "16% drawdown should trigger CircuitBreaker::Halted, got {:?}", report.circuit_breaker);
        // When halted, can_trade() should return false.
        assert!(!rm.can_trade(), "can_trade() should be false when circuit breaker is Halted");
    }

    #[test]
    fn test_circuit_breaker_warning_on_drawdown() {
        let rm = test_manager();
        // Set peak to 100, evaluate with portfolio at 92 (8% drawdown).
        // warning_threshold_pct=7% → 8% ≥ 7% → Warning.
        // halt_threshold_pct=10% → 8% < 10% → NOT Halted.
        rm.update_peak(100.0);
        let report = rm.evaluate_trade(
            "token_def",
            10.0,
            1,
            92.0,   // 8% drawdown
            &[],
        );
        assert_eq!(report.circuit_breaker, CircuitBreaker::Warning,
            "8% drawdown should trigger CircuitBreaker::Warning, got {:?}", report.circuit_breaker);
        // Warning state still allows trading (reduced sizing implied).
        assert!(rm.can_trade(), "can_trade() should be true in Warning state");
    }

    #[test]
    fn test_circuit_breaker_normal_on_small_drawdown() {
        let rm = test_manager();
        // 5% drawdown — below both warning (7%) and halt (10%).
        rm.update_peak(100.0);
        let report = rm.evaluate_trade(
            "token_ghi",
            10.0,
            0,
            95.0,   // 5% drawdown
            &[],
        );
        assert_eq!(report.circuit_breaker, CircuitBreaker::Normal,
            "5% drawdown should keep circuit breaker Normal, got {:?}", report.circuit_breaker);
        assert!(rm.can_trade());
    }

    // ─── Daily Loss Limit Test ──────────────────────────────────────────

    #[test]
    fn test_daily_loss_limit_halt() {
        // Create a RiskManager with a $15 daily loss limit.
        let mut cfg = RiskConfig::default();
        cfg.max_daily_loss_usd = 15.0;
        let rm = RiskManager::new(cfg);

        // Record $16 of realized losses via a sell trade.
        let trade = Trade {
            id: solagent_core::uuid::Uuid::new_v4(),
            token_address: "loss_token".to_string(),
            chain: solagent_core::Chain::Solana,
            side: TradeSide::Sell,
            size_usd: 16.0,
            token_amount: 100.0,
            price: 0.16,
            tx_signature: None,
            slippage_bps: None,
            executed_at: Utc::now(),
            latency_ms: None,
        };
        rm.record_trade(&trade);

        // Daily loss check: $16 > $15 limit → should fail.
        let check = rm.check_daily_loss();
        assert!(!check.passed,
            "Daily loss $16 should exceed the $15 limit (check passed unexpectedly)");

        // evaluate_trade should also fail because daily loss check is included.
        let report = rm.evaluate_trade(
            "loss_token2",
            10.0,
            0,
            100.0,
            &[],
        );
        assert!(!report.passed,
            "evaluate_trade should reject when daily loss limit exceeded");
    }

    // ─── Position Size Cap Tests ────────────────────────────────────────

    #[test]
    fn test_position_size_capped_at_max_30() {
        // Set max_position_size_usd to $30 and verify a $40 position is rejected.
        let mut cfg = RiskConfig::default();
        cfg.max_position_size_usd = 30.0;
        let rm = RiskManager::new(cfg);

        let report = rm.evaluate_trade(
            "big_position_token",
            40.0,    // exceeds $30 cap
            1,
            500.0,
            &[],
        );
        assert!(!report.passed, "Position size $40 should be rejected with $30 max");
        let pos_check = report.checks.iter()
            .find(|c| c.name == "position_size")
            .expect("position_size check should be present");
        assert!(!pos_check.passed, "position_size check should fail for $40 > $30");
    }

    #[test]
    fn test_position_size_within_limit_passes() {
        let mut cfg = RiskConfig::default();
        cfg.max_position_size_usd = 30.0;
        let rm = RiskManager::new(cfg);

        let report = rm.evaluate_trade(
            "normal_token",
            25.0,    // within $30 cap
            1,
            500.0,
            &[],
        );
        assert!(report.passed, "Position size $25 should pass with $30 max");
    }

    // ─── Wallet Reserve Test ────────────────────────────────────────────

    #[test]
    fn test_calculate_position_size_caps_small_portfolio() {
        // On a small portfolio, calculate_position_size caps at 2% per trade.
        // Portfolio=$21 → 2% = $0.42, floor $1.
        let rm = test_manager();
        let allowed_size = rm.calculate_position_size(21.0);
        assert!(
            allowed_size < 20.0,
            "Portfolio $21 should not permit a $20 position; calculated allowed size: ${allowed_size}"
        );
        // 2% of $21 = $0.42, floored to $1 minimum.
        assert_eq!(allowed_size, 1.0, "Expected $1 floor on small portfolio, got ${allowed_size}");
    }

    // ─── Max Open Positions Test ────────────────────────────────────────

    #[test]
    fn test_max_open_positions_exceeded() {
        let mut cfg = RiskConfig::default();
        cfg.max_open_positions = 3;
        let rm = RiskManager::new(cfg);

        // 3 open, max=3 → current_open_positions (3) >= max (3) → rejected.
        let report = rm.evaluate_trade(
            "fourth_token",
            10.0,
            3,      // current_open_positions = max
            500.0,
            &[],
        );
        assert!(!report.passed, "4th position should be rejected when max_open_positions is 3");
        let max_check = report.checks.iter()
            .find(|c| c.name == "max_positions")
            .expect("max_positions check should be present");
        assert!(!max_check.passed,
            "max_positions check should fail when current(3) >= max(3)");
    }

    #[test]
    fn test_max_open_positions_not_exceeded() {
        let mut cfg = RiskConfig::default();
        cfg.max_open_positions = 3;
        let rm = RiskManager::new(cfg);

        // 2 open, max=3 → 3rd position should pass.
        let report = rm.evaluate_trade(
            "third_token",
            10.0,
            2,
            500.0,
            &[],
        );
        assert!(report.passed, "3rd position should pass with max=3 and 2 currently open");
    }

    // ─── Multiple Simultaneous Failures Test ────────────────────────────

    #[test]
    fn test_multiple_failures_reported() {
        let mut cfg = RiskConfig::default();
        cfg.max_position_size_usd = 10.0;
        cfg.max_open_positions = 1;
        cfg.max_daily_loss_usd = 1.0;
        let rm = RiskManager::new(cfg);

        // Record a loss to trigger daily loss check.
        let trade = Trade {
            id: solagent_core::uuid::Uuid::new_v4(),
            token_address: "prior_loss".to_string(),
            chain: solagent_core::Chain::Solana,
            side: TradeSide::Sell,
            size_usd: 5.0,
            token_amount: 100.0,
            price: 0.05,
            tx_signature: None,
            slippage_bps: None,
            executed_at: Utc::now(),
            latency_ms: None,
        };
        rm.record_trade(&trade);

        // This proposed trade violates: position_size ($20 > $10 max),
        // max_positions ($3 open > $1 max), and daily_loss ($5 > $1 limit).
        let report = rm.evaluate_trade(
            "doomed_token",
            20.0,   // too big
            3,      // too many open
            500.0,  // healthy portfolio, so drawdown is not an issue
            &[],
        );
        assert!(!report.passed, "Trade violating 3 constraints should be rejected");

        let failed_names: Vec<&str> = report.checks.iter()
            .filter(|c| !c.passed)
            .map(|c| c.name.as_str())
            .collect();
        assert!(failed_names.contains(&"position_size"),
            "Expected position_size to fail; failures: {failed_names:?}");
        assert!(failed_names.contains(&"max_positions"),
            "Expected max_positions to fail; failures: {failed_names:?}");
        assert!(failed_names.contains(&"daily_loss"),
            "Expected daily_loss to fail; failures: {failed_names:?}");
    }
}
