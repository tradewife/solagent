//! Helius API credit budget tracker.
//!
//! Estimates credit consumption from Helius API calls and provides threshold
//! warnings at 50% and 90% consumption. Tracks usage in-memory with thread-safe
//! atomics and supports persistence to SQLite (via `agent_state` table) so that
//! budget tracking survives agent restarts.
//!
//! ## Credit estimation model
//!
//! Helius free tier provides 1M credits/month. Different endpoints have
//! different per-call costs. We use conservative estimates:
//!
//! | Endpoint                | Estimated credits |
//! |------------------------|-------------------|
//! | Enhanced transaction history | 2           |
//! | Parse transaction           | 2           |
//! | DAS API getAssetsByOwner    | 2           |
//! | Priority fee estimate       | 1           |
//! | Smart Transaction Sender    | 2           |
//! | RPC calls (getHealth, etc.) | 1           |
//! | WebSocket events            | 0 (streaming) |
//!
//! The tracker logs estimated usage periodically and emits structured WARN
//! messages at 50% and 90% consumption thresholds.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Default total monthly credit budget (Helius free tier: 1M credits).
pub const DEFAULT_CREDIT_BUDGET: u64 = 1_000_000;

/// WARN threshold at 50% consumption.
const WARN_50_PCT: f64 = 0.50;

/// WARN threshold at 90% consumption.
const WARN_90_PCT: f64 = 0.90;

/// Minimum interval between periodic log messages.
const LOG_INTERVAL_SECS: u64 = 300; // 5 minutes

/// Estimated credit cost per call type.
#[derive(Debug, Clone, Copy)]
pub enum ApiCallType {
    /// Enhanced transaction history query (~2 credits).
    TransactionHistory,
    /// Parse a single transaction (~2 credits).
    ParseTransaction,
    /// DAS API getAssetsByOwner (~2 credits).
    DasGetAssets,
    /// Priority fee estimation (~1 credit).
    PriorityFee,
    /// Smart Transaction Sender (~2 credits).
    SmartTransaction,
    /// Basic RPC call: getHealth, getBalance, etc. (~1 credit).
    RpcCall,
}

impl ApiCallType {
    /// Return the estimated credit cost for this call type.
    pub fn estimated_credits(&self) -> u64 {
        match self {
            ApiCallType::TransactionHistory => 2,
            ApiCallType::ParseTransaction => 2,
            ApiCallType::DasGetAssets => 2,
            ApiCallType::PriorityFee => 1,
            ApiCallType::SmartTransaction => 2,
            ApiCallType::RpcCall => 1,
        }
    }
}

/// Snapshot of the current credit budget state.
#[derive(Debug, Clone)]
pub struct CreditSnapshot {
    /// Total credits used in this tracking period.
    pub credits_used: u64,
    /// Total credit budget.
    pub credits_total: u64,
    /// Percentage consumed (0.0 – 100.0).
    pub pct: f64,
    /// Remaining credits.
    pub credits_remaining: u64,
    /// Whether the 50% threshold has been crossed.
    pub warn_50_crossed: bool,
    /// Whether the 90% threshold has been crossed.
    pub warn_90_crossed: bool,
}

/// Thread-safe Helius credit budget tracker.
///
/// Maintains an atomic counter of estimated credits used. Each component making
/// Helius API calls records the call via `record_call()`. The tracker checks
/// thresholds and logs warnings as appropriate.
///
/// Persistence is handled externally — call `snapshot()` to get the current
/// state and persist via `PortfolioManager::set_agent_state()`. On startup,
/// call `restore()` to resume from a persisted value.
#[derive(Debug)]
pub struct HeliusCreditTracker {
    /// Running total of estimated credits consumed.
    credits_used: AtomicU64,
    /// Total monthly credit budget.
    credits_total: u64,
    /// Whether the 50% WARN has already been emitted (reset on restore).
    warn_50_triggered: AtomicBool,
    /// Whether the 90% WARN has already been emitted (reset on restore).
    warn_90_triggered: AtomicBool,
    /// Timestamp of the last periodic log message.
    last_log: RwLock<Instant>,
    /// Minimum interval between periodic log messages.
    log_interval: Duration,
}

impl HeliusCreditTracker {
    /// Create a new credit tracker with the specified monthly budget.
    pub fn new(credits_total: u64) -> Self {
        Self {
            credits_used: AtomicU64::new(0),
            credits_total,
            warn_50_triggered: AtomicBool::new(false),
            warn_90_triggered: AtomicBool::new(false),
            last_log: RwLock::new(Instant::now()),
            log_interval: Duration::from_secs(LOG_INTERVAL_SECS),
        }
    }

    /// Create a tracker with the default Helius free tier budget (1M credits).
    pub fn default_budget() -> Self {
        Self::new(DEFAULT_CREDIT_BUDGET)
    }

    /// Create a new credit tracker wrapped in `Arc` for sharing across components.
    pub fn new_shared(credits_total: u64) -> Arc<Self> {
        Arc::new(Self::new(credits_total))
    }

    /// Restore credits_used from a persisted value (e.g., from SQLite).
    ///
    /// This should be called once on startup before any API calls are made.
    /// Resets threshold warning flags so they can re-trigger if appropriate.
    pub fn restore(&self, persisted_used: u64) {
        self.credits_used.store(persisted_used, Ordering::SeqCst);

        // Re-evaluate thresholds after restore so warnings can re-trigger.
        // Flags are `true` when the threshold has ALREADY been crossed (don't re-trigger).
        // Flags are `false` when below the threshold (allow triggering when crossed).
        let pct = self.percentage();
        self.warn_50_triggered.store(pct >= WARN_50_PCT, Ordering::SeqCst);
        self.warn_90_triggered.store(pct >= WARN_90_PCT, Ordering::SeqCst);

        tracing::info!(
            credits_used = persisted_used,
            credits_total = self.credits_total,
            pct = format!("{:.1}%", pct * 100.0),
            "Helius credit tracker restored from persisted state"
        );
    }

    /// Record an API call and check thresholds.
    ///
    /// This should be called after every successful Helius API call.
    /// Logs estimated credit usage and emits WARN messages at thresholds.
    pub fn record_call(&self, call_type: ApiCallType) {
        let cost = call_type.estimated_credits();
        let new_total = self.credits_used.fetch_add(cost, Ordering::SeqCst) + cost;

        let pct = new_total as f64 / self.credits_total as f64;

        // Check 90% threshold first (higher severity).
        if pct >= WARN_90_PCT && !self.warn_90_triggered.load(Ordering::SeqCst) {
            self.warn_90_triggered.store(true, Ordering::SeqCst);
            let remaining = self.credits_total.saturating_sub(new_total);

            // Estimate time to exhaustion based on average daily usage.
            // We don't have a reliable daily rate yet, so just report remaining.
            tracing::warn!(
                helius_credit_pct = format!("{:.1}%", pct * 100.0),
                helius_credits_used = new_total,
                helius_credits_total = self.credits_total,
                helius_credits_remaining = remaining,
                suggested_action = "Reduce polling frequency, upgrade Helius plan, or disable wallet watcher to conserve credits",
                "Helius credit budget 90% consumed — take action to avoid service interruption"
            );
        } else if pct >= WARN_50_PCT && !self.warn_50_triggered.load(Ordering::SeqCst) {
            self.warn_50_triggered.store(true, Ordering::SeqCst);
            let remaining = self.credits_total.saturating_sub(new_total);

            tracing::warn!(
                helius_credit_pct = format!("{:.1}%", pct * 100.0),
                helius_credits_used = new_total,
                helius_credits_total = self.credits_total,
                helius_credits_remaining = remaining,
                "Helius credit budget 50% consumed"
            );
        }

        // Periodic log (every LOG_INTERVAL_SECS at most).
        // Non-blocking: try to acquire a write lock, skip if contention.
        if let Ok(mut last) = self.last_log.try_write()
            && last.elapsed() >= self.log_interval
        {
            *last = Instant::now();
            tracing::info!(
                helius_credits_used = new_total,
                helius_credits_total = self.credits_total,
                helius_credit_pct = format!("{:.1}%", pct * 100.0),
                call_type = format!("{:?}", call_type),
                estimated_cost = cost,
                "Helius credit budget usage"
            );
        }
    }

    /// Take a snapshot of the current credit tracking state.
    ///
    /// Use this for persisting to SQLite via `set_agent_state()`.
    pub fn snapshot(&self) -> CreditSnapshot {
        let used = self.credits_used.load(Ordering::SeqCst);
        let pct = if self.credits_total > 0 {
            used as f64 / self.credits_total as f64
        } else {
            0.0
        };
        CreditSnapshot {
            credits_used: used,
            credits_total: self.credits_total,
            pct: pct * 100.0,
            credits_remaining: self.credits_total.saturating_sub(used),
            warn_50_crossed: self.warn_50_triggered.load(Ordering::SeqCst),
            warn_90_crossed: self.warn_90_triggered.load(Ordering::SeqCst),
        }
    }

    /// Get the current credits used (atomic load).
    pub fn credits_used(&self) -> u64 {
        self.credits_used.load(Ordering::SeqCst)
    }

    /// Get the total credit budget.
    pub fn credits_total(&self) -> u64 {
        self.credits_total
    }

    /// Get the current consumption as a fraction (0.0 – 1.0).
    pub fn percentage(&self) -> f64 {
        let used = self.credits_used.load(Ordering::SeqCst);
        if self.credits_total > 0 {
            used as f64 / self.credits_total as f64
        } else {
            0.0
        }
    }

    /// Check whether the 50% threshold has been crossed.
    pub fn is_warn_50(&self) -> bool {
        self.percentage() >= WARN_50_PCT
    }

    /// Check whether the 90% threshold has been crossed.
    pub fn is_warn_90(&self) -> bool {
        self.percentage() >= WARN_90_PCT
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credit_tracker_starts_at_zero() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        assert_eq!(tracker.credits_used(), 0);
        assert_eq!(tracker.credits_total(), 1_000_000);
        assert!((tracker.percentage() - 0.0).abs() < f64::EPSILON);
        assert!(!tracker.is_warn_50());
        assert!(!tracker.is_warn_90());
    }

    #[test]
    fn test_credit_tracker_record_call_accumulates() {
        let tracker = HeliusCreditTracker::new(1_000_000);

        tracker.record_call(ApiCallType::TransactionHistory); // 2 credits
        assert_eq!(tracker.credits_used(), 2);

        tracker.record_call(ApiCallType::RpcCall); // 1 credit
        assert_eq!(tracker.credits_used(), 3);

        tracker.record_call(ApiCallType::DasGetAssets); // 2 credits
        assert_eq!(tracker.credits_used(), 5);
    }

    #[test]
    fn test_credit_tracker_snapshot_accurate() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        tracker.record_call(ApiCallType::TransactionHistory); // 2
        tracker.record_call(ApiCallType::RpcCall); // 1

        let snap = tracker.snapshot();
        assert_eq!(snap.credits_used, 3);
        assert_eq!(snap.credits_total, 1_000_000);
        assert!((snap.pct - 0.0003).abs() < 0.0001);
        assert_eq!(snap.credits_remaining, 999_997);
        assert!(!snap.warn_50_crossed);
        assert!(!snap.warn_90_crossed);
    }

    #[test]
    fn test_credit_tracker_50_pct_threshold() {
        let tracker = HeliusCreditTracker::new(100);

        // Use exactly 50 credits (50%).
        for _ in 0..25 {
            tracker.record_call(ApiCallType::TransactionHistory); // 2 each = 50 total
        }

        assert_eq!(tracker.credits_used(), 50);
        assert!(tracker.is_warn_50());
        assert!(!tracker.is_warn_90());

        let snap = tracker.snapshot();
        assert!(snap.warn_50_crossed);
        assert!(!snap.warn_90_crossed);
    }

    #[test]
    fn test_credit_tracker_90_pct_threshold() {
        let tracker = HeliusCreditTracker::new(100);

        // Use 90 credits (90%).
        for _ in 0..45 {
            tracker.record_call(ApiCallType::TransactionHistory); // 2 each = 90 total
        }

        assert_eq!(tracker.credits_used(), 90);
        assert!(tracker.is_warn_50());
        assert!(tracker.is_warn_90());

        let snap = tracker.snapshot();
        assert!(snap.warn_50_crossed);
        assert!(snap.warn_90_crossed);
    }

    #[test]
    fn test_credit_tracker_restore_from_persisted() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        tracker.restore(500_000);

        assert_eq!(tracker.credits_used(), 500_000);
        assert!(tracker.is_warn_50());
    }

    #[test]
    fn test_credit_tracker_restore_at_90_pct() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        tracker.restore(900_000);

        assert_eq!(tracker.credits_used(), 900_000);
        assert!(tracker.is_warn_90());
    }

    #[test]
    fn test_credit_tracker_warn_flags_reset_on_restore_below_threshold() {
        let tracker = HeliusCreditTracker::new(100);

        // Trigger 90% warning.
        for _ in 0..45 {
            tracker.record_call(ApiCallType::TransactionHistory);
        }
        assert!(tracker.snapshot().warn_90_crossed);

        // Restore to below 50% — flags should be reset.
        tracker.restore(10);
        let snap = tracker.snapshot();
        assert!(!snap.warn_50_crossed);
        assert!(!snap.warn_90_crossed);
    }

    #[test]
    fn test_credit_tracker_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(HeliusCreditTracker::new(1_000_000));
        let mut handles = vec![];

        for _ in 0..10 {
            let t = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    t.record_call(ApiCallType::RpcCall); // 1 credit each
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // 10 threads × 100 calls × 1 credit = 1000.
        assert_eq!(tracker.credits_used(), 1000);
    }

    #[test]
    fn test_api_call_type_estimated_credits() {
        assert_eq!(ApiCallType::TransactionHistory.estimated_credits(), 2);
        assert_eq!(ApiCallType::ParseTransaction.estimated_credits(), 2);
        assert_eq!(ApiCallType::DasGetAssets.estimated_credits(), 2);
        assert_eq!(ApiCallType::PriorityFee.estimated_credits(), 1);
        assert_eq!(ApiCallType::SmartTransaction.estimated_credits(), 2);
        assert_eq!(ApiCallType::RpcCall.estimated_credits(), 1);
    }

    #[test]
    fn test_default_budget_is_1m() {
        let tracker = HeliusCreditTracker::default_budget();
        assert_eq!(tracker.credits_total(), 1_000_000);
    }

    #[test]
    fn test_shared_new() {
        let tracker = HeliusCreditTracker::new_shared(500_000);
        tracker.record_call(ApiCallType::RpcCall);
        assert_eq!(tracker.credits_used(), 1);
        assert_eq!(tracker.credits_total(), 500_000);
    }

    // ─── Tests matching validation assertions ─────────────────────────────

    /// VAL-HARD-007: Credit usage estimated from API call tracking.
    #[test]
    fn test_helius_credit_tracking_estimation_from_calls() {
        let tracker = HeliusCreditTracker::new(1_000_000);

        // Simulate a sequence of API calls.
        tracker.record_call(ApiCallType::TransactionHistory); // 2
        tracker.record_call(ApiCallType::DasGetAssets);        // 2
        tracker.record_call(ApiCallType::PriorityFee);          // 1
        tracker.record_call(ApiCallType::RpcCall);              // 1
        tracker.record_call(ApiCallType::ParseTransaction);     // 2

        // Total estimated credits: 2+2+1+1+2 = 8.
        assert_eq!(tracker.credits_used(), 8);

        let snap = tracker.snapshot();
        assert_eq!(snap.credits_used, 8);
        assert_eq!(snap.credits_total, 1_000_000);
        assert!((snap.pct - 0.0008).abs() < 0.0001);
    }

    /// VAL-HARD-008: WARN at 50% consumption with remaining credits.
    #[test]
    fn test_helius_credit_tracking_warn_at_50_pct() {
        let tracker = HeliusCreditTracker::new(1_000_000);

        // Use 500,000 credits (50%).
        for _ in 0..250_000 {
            tracker.record_call(ApiCallType::TransactionHistory); // 2 each
        }

        assert!(tracker.is_warn_50());
        let snap = tracker.snapshot();
        assert!(snap.warn_50_crossed);
        assert_eq!(snap.credits_used, 500_000);
        assert_eq!(snap.credits_remaining, 500_000);
        assert!((snap.pct - 50.0).abs() < 0.01);
    }

    /// VAL-HARD-009: WARN at 90% consumption with remediation suggestion.
    #[test]
    fn test_helius_credit_tracking_warn_at_90_pct() {
        let tracker = HeliusCreditTracker::new(1_000_000);

        // Use 900,000 credits (90%).
        for _ in 0..450_000 {
            tracker.record_call(ApiCallType::TransactionHistory); // 2 each
        }

        assert!(tracker.is_warn_90());
        let snap = tracker.snapshot();
        assert!(snap.warn_90_crossed);
        assert_eq!(snap.credits_used, 900_000);
        assert_eq!(snap.credits_remaining, 100_000);
        assert!((snap.pct - 90.0).abs() < 0.01);
    }

    /// VAL-HARD-010: Credit tracking persists across restarts (restore test).
    #[test]
    fn test_helius_credit_tracking_persists_across_restart() {
        // Simulate first run.
        let tracker = HeliusCreditTracker::new(1_000_000);
        for _ in 0..100 {
            tracker.record_call(ApiCallType::TransactionHistory);
        }
        assert_eq!(tracker.credits_used(), 200);

        // Simulate persisting the snapshot.
        let snap = tracker.snapshot();
        let persisted_used = snap.credits_used.to_string();
        let persisted_total = snap.credits_total.to_string();

        // Simulate restart — new tracker loads from persisted state.
        let tracker2 = HeliusCreditTracker::new(
            persisted_total.parse::<u64>().unwrap(),
        );
        tracker2.restore(persisted_used.parse::<u64>().unwrap());

        // Credit usage must not reset to 0.
        assert_eq!(tracker2.credits_used(), 200);
        assert!(tracker2.credits_used() >= 200);
    }

    /// VAL-HARD-010: Post-restart credit usage ≥ pre-stop usage.
    #[test]
    fn test_helius_credit_tracking_post_restart_not_less() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        for _ in 0..500 {
            tracker.record_call(ApiCallType::DasGetAssets);
        }
        let pre_stop = tracker.credits_used(); // 1000

        // Persist and restart.
        let tracker2 = HeliusCreditTracker::new(1_000_000);
        tracker2.restore(pre_stop);

        assert!(tracker2.credits_used() >= pre_stop);

        // Additional calls after restart should accumulate.
        tracker2.record_call(ApiCallType::RpcCall);
        assert!(tracker2.credits_used() > pre_stop);
    }

    /// Snapshot can be serialized for persistence.
    #[test]
    fn test_snapshot_persistence_format() {
        let tracker = HeliusCreditTracker::new(1_000_000);
        tracker.record_call(ApiCallType::TransactionHistory);
        let snap = tracker.snapshot();

        // The snapshot values should be serializable as strings for agent_state.
        let used_str = snap.credits_used.to_string();
        let total_str = snap.credits_total.to_string();

        assert_eq!(used_str.parse::<u64>().unwrap(), 2);
        assert_eq!(total_str.parse::<u64>().unwrap(), 1_000_000);
    }
}
