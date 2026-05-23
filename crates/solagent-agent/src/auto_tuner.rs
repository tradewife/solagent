//! Auto-tuner module for adapting signal weights, confluence threshold, and
//! position sizing at runtime based on trade outcomes.
//!
//! The tuner runs periodically (default every 6 hours) and only when enough
//! completed trades exist (≥5). It uses aggregate win rate to decide whether
//! to tighten or loosen parameters, with small random exploratory perturbations
//! to avoid getting stuck in local optima.

use anyhow::Result;
use chrono::{DateTime, Utc};
use rand::Rng;
use solagent_portfolio::PortfolioManager;
use solagent_signals::RuntimeConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Minimum number of completed trades before the auto-tuner activates.
const DEFAULT_MIN_TRADES: usize = 5;

/// Maximum absolute change in any single weight during tuning.
const DEFAULT_MAX_WEIGHT_CHANGE: f64 = 0.10;

/// Default tuning interval in hours.
const DEFAULT_TUNE_INTERVAL_HOURS: i64 = 6;

/// Bounds for individual signal weights.
const WEIGHT_MIN: f64 = 0.05;
const WEIGHT_MAX: f64 = 0.40;

/// Bounds for confluence threshold.
/// Minimum 25.0 matches the ABSOLUTE_FLOOR in lib.rs — the agent must never
/// accept signals below this quality level.
const THRESHOLD_MIN: f64 = 25.0;
const THRESHOLD_MAX: f64 = 80.0;

/// Bounds for max position size (USD).
const POSITION_SIZE_MIN: f64 = 5.0;
const POSITION_SIZE_MAX: f64 = 30.0;

/// The auto-tuner adjusts signal weights, confluence threshold, and risk
/// parameters based on historical trade performance.
pub struct AutoTuner {
    /// Shared runtime configuration (signal weights, threshold, position size, etc.).
    runtime_config: RuntimeConfig,
    /// Portfolio manager for querying trade history and win rate.
    portfolio: Arc<PortfolioManager>,
    /// Optional Zerion client for cross-validating PnL.
    zerion: Option<solagent_data::ZerionClient>,
    /// Wallet address for Zerion PnL queries.
    wallet_address: Option<String>,
    /// Minimum number of completed trades before tuning activates.
    min_trades_before_tuning: usize,
    /// Maximum absolute change in any single weight during a tune cycle.
    max_weight_change: f64,
    /// Timestamp of the last tune cycle.
    last_tune_time: Arc<RwLock<DateTime<Utc>>>,
    /// Minimum interval between tune cycles (hours).
    tune_interval_hours: i64,
}

impl AutoTuner {
    /// Create a new AutoTuner with default parameters.
    pub fn new(runtime_config: RuntimeConfig, portfolio: Arc<PortfolioManager>) -> Self {
        Self {
            runtime_config,
            portfolio,
            zerion: None,
            wallet_address: None,
            min_trades_before_tuning: DEFAULT_MIN_TRADES,
            max_weight_change: DEFAULT_MAX_WEIGHT_CHANGE,
            last_tune_time: Arc::new(RwLock::new(Utc::now())),
            tune_interval_hours: DEFAULT_TUNE_INTERVAL_HOURS,
        }
    }

    /// Create an AutoTuner with custom tuning parameters.
    pub fn with_params(
        runtime_config: RuntimeConfig,
        portfolio: Arc<PortfolioManager>,
        min_trades: usize,
        max_weight_change: f64,
        tune_interval_hours: i64,
    ) -> Self {
        Self {
            runtime_config,
            portfolio,
            zerion: None,
            wallet_address: None,
            min_trades_before_tuning: min_trades,
            max_weight_change,
            last_tune_time: Arc::new(RwLock::new(Utc::now())),
            tune_interval_hours,
        }
    }

    /// Set the Zerion client for PnL cross-validation during tuning.
    pub fn set_zerion(&mut self, client: solagent_data::ZerionClient, wallet_address: String) {
        self.zerion = Some(client);
        self.wallet_address = Some(wallet_address);
    }

    /// Check whether enough time has passed and enough trades exist.
    /// If so, run the tuning cycle.
    pub async fn maybe_tune(&self) -> Result<()> {
        // Check if enough time has elapsed since the last tune.
        {
            let last = *self.last_tune_time.read().await;
            let elapsed = Utc::now() - last;
            if elapsed.num_hours() < self.tune_interval_hours {
                tracing::debug!(
                    elapsed_hours = elapsed.num_hours(),
                    tune_interval_hours = self.tune_interval_hours,
                    "Auto-tuner: not enough time since last tune"
                );
                return Ok(());
            }
        }

        // Check how many completed trades exist.
        let pnl = self.portfolio.get_pnl().await?;
        let total_trades = pnl.total_trades as usize;

        if total_trades < self.min_trades_before_tuning {
            tracing::debug!(
                total_trades,
                min_required = self.min_trades_before_tuning,
                "Auto-tuner: not enough completed trades to tune"
            );
            return Ok(());
        }

        tracing::info!(
            total_trades,
            "Auto-tuner: running tuning cycle with {} completed trades",
            total_trades,
        );

        self.tune().await?;

        // Update last tune time.
        *self.last_tune_time.write().await = Utc::now();

        Ok(())
    }

    /// Execute a full tuning cycle: adjust confluence threshold, signal weights,
    /// and position sizing based on win rate.
    pub async fn tune(&self) -> Result<()> {
        let win_rate = self.portfolio.get_win_rate().await?;

        tracing::info!(
            win_rate = format!("{:.1}%", win_rate * 100.0),
            "Auto-tuner: tuning parameters based on win rate"
        );

        // Optional Zerion PnL cross-check.
        if let (Some(zerion), Some(wallet)) = (&self.zerion, &self.wallet_address) {
            match zerion.get_pnl(wallet, Some("solana")).await {
                Ok(zerion_pnl) => {
                    tracing::info!(
                        zerion_total_gain = format!("${:.2}", zerion_pnl.total_gain),
                        zerion_realized = format!("${:.2}", zerion_pnl.realized_gain),
                        zerion_unrealized = format!("${:.2}", zerion_pnl.unrealized_gain),
                        zerion_roi = format!("{:.2}%", zerion_pnl.relative_total_gain_pct),
                        "Auto-tuner: Zerion PnL cross-check"
                    );
                }
                Err(e) => {
                    tracing::debug!(error = %e, "Auto-tuner: Zerion PnL unavailable (non-fatal)");
                }
            }
        }

        // ─── 1. Adjust confluence threshold ──────────────────────────────
        self.tune_threshold(win_rate).await;

        // ─── 2. Adjust signal weights with exploratory perturbations ─────
        self.tune_weights().await;

        // ─── 3. Adjust position sizing ───────────────────────────────────
        self.tune_position_size(win_rate).await;

        // ─── 4. Record performance snapshot ──────────────────────────────
        if let Err(e) = self.record_snapshot(win_rate).await {
            tracing::warn!(error = %e, "Auto-tuner: failed to record performance snapshot (non-fatal)");
        }

        Ok(())
    }

    /// Record a performance snapshot after tuning completes.
    async fn record_snapshot(&self, win_rate: f64) -> Result<()> {
        let pnl = self.portfolio.get_pnl().await?;
        let open_positions = self.portfolio.get_open_positions().await?;

        let avg_trade_return = if pnl.total_trades > 0 {
            Some(pnl.total_pnl / pnl.total_trades as f64)
        } else {
            None
        };

        let weights = self.runtime_config.weights.read().await;
        let signal_weights = serde_json::json!({
            "whale_consensus": weights.whale_consensus,
            "accumulation": weights.accumulation,
            "launch_momentum": weights.launch_momentum,
            "volume_spike": weights.volume_spike,
            "social": weights.social,
        })
        .to_string();

        let threshold = *self.runtime_config.confluence_threshold.read().await;
        let position_size = *self.runtime_config.max_position_size_usd.read().await;

        let snapshot = solagent_portfolio::PerformanceSnapshot {
            timestamp: Utc::now().to_rfc3339(),
            win_rate,
            total_trades: pnl.total_trades as usize,
            total_pnl: pnl.total_pnl,
            avg_trade_return,
            signal_weights,
            confluence_threshold: threshold,
            position_size,
            open_positions: open_positions.len(),
        };

        self.portfolio.insert_performance_snapshot(&snapshot).await?;

        tracing::info!(
            win_rate = format!("{:.1}%", win_rate * 100.0),
            total_trades = snapshot.total_trades,
            total_pnl = format!("${:.2}", snapshot.total_pnl),
            "Auto-tuner: performance snapshot recorded"
        );

        Ok(())
    }

    #[allow(clippy::doc_lazy_continuation)]
    /// Adjust the confluence threshold based on win rate.
    ///
    /// - win_rate > 60% → raise threshold by 5 (tighten — we're doing well, be pickier)
    /// - win_rate < 30% → lower threshold by 5 (loosen — we're struggling, cast wider net)
    /// - Otherwise keep current threshold.
    /// Clamped to [THRESHOLD_MIN, THRESHOLD_MAX].
    async fn tune_threshold(&self, win_rate: f64) {
        let old_threshold = *self.runtime_config.confluence_threshold.read().await;
        let direction = if win_rate > 0.60 {
            5.0
        } else if win_rate < 0.30 {
            -5.0
        } else {
            0.0
        };

        if direction != 0.0 {
            let new = (old_threshold + direction).clamp(THRESHOLD_MIN, THRESHOLD_MAX);

            *self.runtime_config.confluence_threshold.write().await = new;
            let direction_label = if direction > 0.0 { "↑" } else { "↓" };
            tracing::info!(
                before = old_threshold,
                after = new,
                win_rate = format!("{:.1}%", win_rate * 100.0),
                "Auto-tuner: confluence threshold {direction_label} ({old_threshold:.0} → {new:.0})"
            );
        } else {
            tracing::debug!(
                threshold = old_threshold,
                win_rate = format!("{:.1}%", win_rate * 100.0),
                "Auto-tuner: confluence threshold unchanged (win rate in neutral zone)"
            );
        }
    }

    /// Adjust signal weights with small random exploratory perturbations (±5%).
    ///
    /// Each weight gets a random perturbation in [-max_weight_change, +max_weight_change],
    /// clamped to [WEIGHT_MIN, WEIGHT_MAX], then all weights are L1-normalized
    /// so they sum to 1.0.
    async fn tune_weights(&self) {
        let old_weights = self.runtime_config.weights.read().await.clone();
        let mut rng = rand::rng();

        // Apply random perturbations.
        let mut new: Vec<f64> = vec![
            old_weights.whale_consensus,
            old_weights.accumulation,
            old_weights.launch_momentum,
            old_weights.volume_spike,
            old_weights.social,
        ]
        .into_iter()
        .map(|w| {
            let delta = rng.random_range(-self.max_weight_change..self.max_weight_change);
            (w + delta).clamp(WEIGHT_MIN, WEIGHT_MAX)
        })
        .collect();

        // L1 normalize so all weights sum to 1.0.
        let sum: f64 = new.iter().sum();
        if sum > 0.0 {
            for w in &mut new {
                *w /= sum;
            }
        }

        // Write back.
        let mut weights = self.runtime_config.weights.write().await;
        weights.whale_consensus = new[0];
        weights.accumulation = new[1];
        weights.launch_momentum = new[2];
        weights.volume_spike = new[3];
        weights.social = new[4];

        let total = weights.whale_consensus
            + weights.accumulation
            + weights.launch_momentum
            + weights.volume_spike
            + weights.social;

        tracing::info!(
            before_wc = format!("{:.3}", old_weights.whale_consensus),
            after_wc = format!("{:.3}", weights.whale_consensus),
            before_acc = format!("{:.3}", old_weights.accumulation),
            after_acc = format!("{:.3}", weights.accumulation),
            before_lm = format!("{:.3}", old_weights.launch_momentum),
            after_lm = format!("{:.3}", weights.launch_momentum),
            before_vs = format!("{:.3}", old_weights.volume_spike),
            after_vs = format!("{:.3}", weights.volume_spike),
            before_soc = format!("{:.3}", old_weights.social),
            after_soc = format!("{:.3}", weights.social),
            sum = format!("{:.4}", total),
            "Auto-tuner: signal weights perturbed (before → after, sum={total:.4})"
        );
    }

    #[allow(clippy::doc_lazy_continuation)]
    /// Adjust max position size based on win rate.
    ///
    /// - win_rate > 60% → increase by $2 (up to $30) — confidence is high
    /// - win_rate < 30% → decrease by $2 (down to $5) — reduce exposure
    /// - Otherwise keep current size.
    /// Clamped to [POSITION_SIZE_MIN, POSITION_SIZE_MAX].
    async fn tune_position_size(&self, win_rate: f64) {
        let old_size = *self.runtime_config.max_position_size_usd.read().await;
        let delta = if win_rate > 0.60 {
            2.0
        } else if win_rate < 0.30 {
            -2.0
        } else {
            0.0
        };

        if delta != 0.0 {
            let new = (old_size + delta).clamp(POSITION_SIZE_MIN, POSITION_SIZE_MAX);
            *self.runtime_config.max_position_size_usd.write().await = new;
            let direction_label = if delta > 0.0 { "↑" } else { "↓" };
            tracing::info!(
                before = format!("${:.0}", old_size),
                after = format!("${:.0}", new),
                win_rate = format!("{:.1}%", win_rate * 100.0),
                "Auto-tuner: max position size {direction_label} (${old_size:.0} → ${new:.0})"
            );
        } else {
            tracing::debug!(
                size = format!("${:.0}", old_size),
                win_rate = format!("{:.1}%", win_rate * 100.0),
                "Auto-tuner: position size unchanged (win rate in neutral zone)"
            );
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use solagent_signals::SignalWeights;
    use super::*;
    use solagent_portfolio::db;

    /// Helper: create an in-memory PortfolioManager with migrations applied.
    async fn test_portfolio() -> Arc<PortfolioManager> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(db::MIGRATION_SQL).execute(&pool).await.unwrap();
        Arc::new(PortfolioManager::new(pool))
    }

    /// Helper: create a RuntimeConfig with weights summing to 1.0.
    fn test_runtime_config() -> RuntimeConfig {
        RuntimeConfig::new(
            SignalWeights::default(),
            65.0,
            15.0,
            3,
            15.0,
        )
    }

    /// Helper: open and close a position with a specific PnL outcome.
    async fn record_completed_trade(
        pm: &PortfolioManager,
        token: &str,
        entry_price: f64,
        close_price: f64,
        size_usd: f64,
        token_amount: f64,
    ) {
        let pos = pm
            .open_position(
                token,
                solagent_core::Chain::Solana,
                entry_price,
                size_usd,
                token_amount,
                None,
                None,
            )
            .await
            .unwrap();
        pm.close_position(&pos.id, close_price).await.unwrap();
    }

    // ─── Test: No tune with fewer than 5 trades ──────────────────────

    #[tokio::test]
    async fn test_auto_tuner_no_tune_with_few_trades() {
        let portfolio = test_portfolio().await;

        // Only 2 completed trades.
        record_completed_trade(&portfolio, "token_a", 1.0, 1.5, 10.0, 10.0).await;
        record_completed_trade(&portfolio, "token_b", 1.0, 0.8, 10.0, 10.0).await;

        let config = test_runtime_config();
        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,   // min_trades_before_tuning
            0.10, // max_weight_change
            0,    // tune_interval_hours = 0 so tune is due immediately
        );

        // Force last_tune_time to be old enough.
        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        let old_threshold = *config.confluence_threshold.read().await;
        let old_size = *config.max_position_size_usd.read().await;

        // maybe_tune should NOT run because only 2 trades (less than 5).
        tuner.maybe_tune().await.unwrap();

        // Threshold and position size should be unchanged.
        assert!(
            (*config.confluence_threshold.read().await - old_threshold).abs() < f64::EPSILON,
            "Threshold should not change with <5 trades"
        );
        assert!(
            (*config.max_position_size_usd.read().await - old_size).abs() < f64::EPSILON,
            "Position size should not change with <5 trades"
        );
    }

    // ─── Test: Raise threshold on high win rate ────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_raises_threshold_on_high_winrate() {
        let portfolio = test_portfolio().await;

        // 5 completed trades, all wins (win_rate = 1.0 > 0.60).
        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("win_token_{i}"),
                1.0,
                2.0, // profitable
                10.0,
                10.0,
            )
            .await;
        }

        let config = test_runtime_config();
        let initial_threshold = *config.confluence_threshold.read().await;

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let new_threshold = *config.confluence_threshold.read().await;
        assert!(
            new_threshold > initial_threshold,
            "Threshold should increase on high win rate: {initial_threshold} → {new_threshold}"
        );
        assert!(
            new_threshold <= THRESHOLD_MAX,
            "Threshold should not exceed max: {new_threshold} > {THRESHOLD_MAX}"
        );
    }

    // ─── Test: Lower threshold on low win rate ─────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_lowers_threshold_on_low_winrate() {
        let portfolio = test_portfolio().await;

        // 5 completed trades, all losses (win_rate = 0.0 < 0.30).
        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("lose_token_{i}"),
                2.0,
                1.0, // unprofitable
                10.0,
                10.0,
            )
            .await;
        }

        let config = test_runtime_config();
        let initial_threshold = *config.confluence_threshold.read().await;

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let new_threshold = *config.confluence_threshold.read().await;
        assert!(
            new_threshold < initial_threshold,
            "Threshold should decrease on low win rate: {initial_threshold} → {new_threshold}"
        );
        assert!(
            new_threshold >= THRESHOLD_MIN,
            "Threshold should not go below min: {new_threshold} < {THRESHOLD_MIN}"
        );
    }

    // ─── Test: Bounds enforcement for threshold ────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_bounds_enforcement() {
        let portfolio = test_portfolio().await;

        // ─── Lower bound: start at 27, win_rate < 0.30 → try -5 to get 22, clamp to 25.
        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("bad_token_{i}"),
                2.0,
                1.0,
                10.0,
                10.0,
            )
            .await;
        }

        let config = RuntimeConfig::new(
            SignalWeights::default(),
            27.0,  // start near lower bound
            15.0,
            3,
            15.0,
        );

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let threshold = *config.confluence_threshold.read().await;
        assert!(
            threshold >= THRESHOLD_MIN,
            "Threshold should be clamped to min: {threshold} < {THRESHOLD_MIN}"
        );
        assert!((threshold - THRESHOLD_MIN).abs() < f64::EPSILON,
            "Threshold should be at min={THRESHOLD_MIN} after lowering from 27, got {threshold}");

        // ─── Upper bound: start at 78, win_rate > 0.60 → try +5 to get 83, clamp to 80.
        // Need a new portfolio and tuner for this direction.
        let portfolio2 = test_portfolio().await;
        for i in 0..5 {
            record_completed_trade(
                &portfolio2,
                &format!("good_token_{i}"),
                1.0,
                2.0, // all wins
                10.0,
                10.0,
            )
            .await;
        }

        let config2 = RuntimeConfig::new(
            SignalWeights::default(),
            78.0, // start near upper bound
            15.0,
            3,
            15.0,
        );

        let tuner2 = AutoTuner::with_params(
            config2.clone(),
            portfolio2.clone(),
            5,
            0.10,
            0,
        );

        *tuner2.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner2.maybe_tune().await.unwrap();

        let threshold2 = *config2.confluence_threshold.read().await;
        assert!(
            threshold2 <= THRESHOLD_MAX,
            "Threshold should be clamped to max: {threshold2} > {THRESHOLD_MAX}"
        );
    }

    // ─── Test: Weight normalization ────────────────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_weight_normalization() {
        let portfolio = test_portfolio().await;

        // Need 5 trades to trigger tuning.
        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("weight_test_token_{i}"),
                1.0,
                1.5, // wins
                10.0,
                10.0,
            )
            .await;
        }

        let config = test_runtime_config();

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let weights = config.weights.read().await;
        let sum = weights.whale_consensus
            + weights.accumulation
            + weights.launch_momentum
            + weights.volume_spike
            + weights.social;

        assert!(
            (sum - 1.0).abs() < 1e-6,
            "Weights should sum to 1.0 after normalization, got {sum:.8}"
        );

        // Each weight should be within [WEIGHT_MIN, WEIGHT_MAX].
        for w in [
            weights.whale_consensus,
            weights.accumulation,
            weights.launch_momentum,
            weights.volume_spike,
            weights.social,
        ] {
            assert!(
                w >= WEIGHT_MIN && w <= WEIGHT_MAX,
                "Weight {w:.4} should be in [{WEIGHT_MIN}, {WEIGHT_MAX}]"
            );
        }
    }

    // ─── Test: Position size adjustment ────────────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_position_size_increase_on_high_winrate() {
        let portfolio = test_portfolio().await;

        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("size_test_win_{i}"),
                1.0,
                2.0,
                10.0,
                10.0,
            )
            .await;
        }

        let config = RuntimeConfig::new(
            SignalWeights::default(),
            65.0,
            15.0, // starting size
            3,
            15.0,
        );

        let old_size = *config.max_position_size_usd.read().await;

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let new_size = *config.max_position_size_usd.read().await;
        assert!(
            new_size > old_size,
            "Position size should increase on high win rate: {old_size} → {new_size}"
        );
        assert!(
            new_size <= POSITION_SIZE_MAX,
            "Position size should not exceed max: {new_size} > {POSITION_SIZE_MAX}"
        );
    }

    // ─── Test: Neutral win rate doesn't change anything ────────────────

    #[tokio::test]
    async fn test_auto_tuner_neutral_winrate_no_change() {
        let portfolio = test_portfolio().await;

        // 5 trades with mixed results → win_rate should be around 0.4-0.6.
        // wins: token_a, token_c, token_d (3 wins), losses: token_b, token_e (2 losses).
        record_completed_trade(&portfolio, "token_a", 1.0, 1.5, 10.0, 10.0).await;
        record_completed_trade(&portfolio, "token_b", 1.0, 0.8, 10.0, 10.0).await;
        record_completed_trade(&portfolio, "token_c", 1.0, 1.3, 10.0, 10.0).await;
        record_completed_trade(&portfolio, "token_d", 1.0, 1.2, 10.0, 10.0).await;
        record_completed_trade(&portfolio, "token_e", 1.0, 0.9, 10.0, 10.0).await;

        let config = test_runtime_config();
        let old_threshold = *config.confluence_threshold.read().await;
        let old_size = *config.max_position_size_usd.read().await;

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let wr = portfolio.get_win_rate().await.unwrap();
        // win_rate should be between 0.30 and 0.60 -> neutral zone, no changes.
        assert!(
            wr >= 0.30 && wr <= 0.60,
            "Expected neutral win rate, got {wr:.2}"
        );

        // Threshold and position size should stay the same.
        assert!(
            (*config.confluence_threshold.read().await - old_threshold).abs() < f64::EPSILON,
            "Threshold should not change in neutral zone"
        );
        assert!(
            (*config.max_position_size_usd.read().await - old_size).abs() < f64::EPSILON,
            "Position size should not change in neutral zone"
        );
    }

    // ─── Test: Interval-based gating ───────────────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_respects_tune_interval() {
        let portfolio = test_portfolio().await;

        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("interval_test_{i}"),
                1.0,
                1.5,
                10.0,
                10.0,
            )
            .await;
        }

        let config = test_runtime_config();
        let old_threshold = *config.confluence_threshold.read().await;

        // tune_interval_hours = 24 — even with enough trades, not enough time has passed.
        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            24, // 24 hours — won't be due
        );

        // last_tune_time is set to now, so no time has elapsed.
        tuner.maybe_tune().await.unwrap();

        assert!(
            (*config.confluence_threshold.read().await - old_threshold).abs() < f64::EPSILON,
            "Threshold should not change when tune interval hasn't elapsed"
        );
    }

    // ─── Test: Position size lower bound ───────────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_position_size_lower_bound() {
        let portfolio = test_portfolio().await;

        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("size_lb_{i}"),
                2.0,
                1.0, // all losses
                10.0,
                10.0,
            )
            .await;
        }

        let config = RuntimeConfig::new(
            SignalWeights::default(),
            65.0,
            6.0, // start just above min → -$2 = $4, clamped to $5
            3,
            15.0,
        );

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let size = *config.max_position_size_usd.read().await;
        assert!(
            size >= POSITION_SIZE_MIN,
            "Position size should not go below min: {size} < {POSITION_SIZE_MIN}"
        );
    }

    // ─── Test: Position size upper bound ───────────────────────────────

    #[tokio::test]
    async fn test_auto_tuner_position_size_upper_bound() {
        let portfolio = test_portfolio().await;

        for i in 0..5 {
            record_completed_trade(
                &portfolio,
                &format!("size_ub_{i}"),
                1.0,
                2.0, // all wins
                10.0,
                10.0,
            )
            .await;
        }

        let config = RuntimeConfig::new(
            SignalWeights::default(),
            65.0,
            29.0, // start just below max → +$2 = $31, clamped to $30
            3,
            15.0,
        );

        let tuner = AutoTuner::with_params(
            config.clone(),
            portfolio.clone(),
            5,
            0.10,
            0,
        );

        *tuner.last_tune_time.write().await =
            Utc::now() - chrono::Duration::hours(1);

        tuner.maybe_tune().await.unwrap();

        let size = *config.max_position_size_usd.read().await;
        assert!(
            size <= POSITION_SIZE_MAX,
            "Position size should not exceed max: {size} > {POSITION_SIZE_MAX}"
        );
    }
}
