//! # solagent-safety
//!
//! Safety scoring system with individual checks for token risk assessment.

use serde::{Deserialize, Serialize};
use solagent_core::Chain;

// ─── Safety Report ───────────────────────────────────────────────────────────

/// Per-check score and details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub name: String,
    pub score: u8,       // 0-100 (100 = safest)
    pub weight: f64,
    pub passed: bool,
    pub details: String,
}

/// Full safety report for a token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyReport {
    pub token_address: String,
    pub chain: Chain,
    pub checks: Vec<CheckResult>,
    pub total_score: u8,
    pub passed: bool,
    pub threshold: u8,
}

impl SafetyReport {
    /// Compute the weighted total score from individual checks.
    pub fn compute_total(&mut self) {
        let weight_sum: f64 = self.checks.iter().map(|c| c.weight).sum();
        if weight_sum > 0.0 {
            let weighted: f64 = self
                .checks
                .iter()
                .map(|c| c.score as f64 * c.weight)
                .sum();
            self.total_score = (weighted / weight_sum).round() as u8;
        }
        self.passed = self.total_score >= self.threshold;
    }
}

// ─── Individual Safety Checks ────────────────────────────────────────────────

/// Check if mint authority is revoked (renounced).
pub fn check_mint_authority(mint_authority: Option<&str>) -> CheckResult {
    match mint_authority {
        None | Some("") => CheckResult {
            name: "mint_authority".to_string(),
            score: 100,
            weight: 1.5,
            passed: true,
            details: "Mint authority revoked".to_string(),
        },
        Some(addr) => CheckResult {
            name: "mint_authority".to_string(),
            score: 0,
            weight: 1.5,
            passed: false,
            details: format!("Mint authority held by {addr} — can mint infinite tokens"),
        },
    }
}

/// Check if freeze authority is revoked.
pub fn check_freeze_authority(freeze_authority: Option<&str>) -> CheckResult {
    match freeze_authority {
        None | Some("") => CheckResult {
            name: "freeze_authority".to_string(),
            score: 100,
            weight: 1.5,
            passed: true,
            details: "Freeze authority revoked".to_string(),
        },
        Some(addr) => CheckResult {
            name: "freeze_authority".to_string(),
            score: 0,
            weight: 1.5,
            passed: false,
            details: format!("Freeze authority held by {addr} — can freeze wallets"),
        },
    }
}

/// Check LP lock status.
pub fn check_lp_lock(lp_locked: Option<bool>, lp_lock_pct: Option<f64>) -> CheckResult {
    match (lp_locked, lp_lock_pct) {
        (Some(true), Some(pct)) if pct >= 80.0 => CheckResult {
            name: "lp_lock".to_string(),
            score: 100,
            weight: 2.0,
            passed: true,
            details: format!("LP locked: {pct:.1}%"),
        },
        (Some(true), Some(pct)) => CheckResult {
            name: "lp_lock".to_string(),
            score: 50,
            weight: 2.0,
            passed: false,
            details: format!("LP partially locked: {pct:.1}% (need ≥80%)"),
        },
        _ => CheckResult {
            name: "lp_lock".to_string(),
            score: 0,
            weight: 2.0,
            passed: false,
            details: "LP not locked — rug pull risk".to_string(),
        },
    }
}

/// Check holder concentration (top 10 holders).
pub fn check_holder_concentration(top_holders_pct: Vec<f64>) -> CheckResult {
    let top10_total: f64 = top_holders_pct.iter().take(10).sum();
    let (score, passed) = if top10_total < 20.0 {
        (100, true)
    } else if top10_total < 40.0 {
        (60, true)
    } else if top10_total < 60.0 {
        (30, false)
    } else {
        (0, false)
    };
    CheckResult {
        name: "holder_concentration".to_string(),
        score,
        weight: 1.5,
        passed,
        details: format!("Top 10 holders: {top10_total:.1}% of supply"),
    }
}

/// Check if dev wallet still holds significant tokens.
pub fn check_dev_wallet(dev_holdings_pct: f64) -> CheckResult {
    let (score, passed) = if dev_holdings_pct < 1.0 {
        (100, true)
    } else if dev_holdings_pct < 5.0 {
        (70, true)
    } else if dev_holdings_pct < 10.0 {
        (30, false)
    } else {
        (0, false)
    };
    CheckResult {
        name: "dev_wallet".to_string(),
        score,
        weight: 1.5,
        passed,
        details: format!("Dev holdings: {dev_holdings_pct:.1}%"),
    }
}

/// Check for honeypot (can sell?).
pub fn check_honeypot(is_honeypot: Option<bool>) -> CheckResult {
    match is_honeypot {
        Some(false) => CheckResult {
            name: "honeypot".to_string(),
            score: 100,
            weight: 3.0,
            passed: true,
            details: "Not a honeypot".to_string(),
        },
        Some(true) => CheckResult {
            name: "honeypot".to_string(),
            score: 0,
            weight: 3.0,
            passed: false,
            details: "HONEYPOT DETECTED — cannot sell".to_string(),
        },
        None => CheckResult {
            name: "honeypot".to_string(),
            score: 50,
            weight: 3.0,
            passed: false,
            details: "Honeypot status unknown".to_string(),
        },
    }
}

/// Check buy/sell tax.
pub fn check_tax(buy_tax: Option<f64>, sell_tax: Option<f64>) -> CheckResult {
    let buy = buy_tax.unwrap_or(0.0);
    let sell = sell_tax.unwrap_or(0.0);
    let total = buy + sell;
    let (score, passed) = if total < 3.0 {
        (100, true)
    } else if total < 5.0 {
        (70, true)
    } else if total < 10.0 {
        (40, false)
    } else {
        (0, false)
    };
    CheckResult {
        name: "tax".to_string(),
        score,
        weight: 2.0,
        passed,
        details: format!("Buy tax: {buy:.1}%, Sell tax: {sell:.1}% (total: {total:.1}%)"),
    }
}

// ─── Dev Wallet Registry ─────────────────────────────────────────────────────

/// Blacklist management for known dev wallets / bad actors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevWalletRegistry {
    blacklisted: Vec<String>,
}

impl DevWalletRegistry {
    pub fn new() -> Self {
        Self {
            blacklisted: Vec::new(),
        }
    }

    /// Add a wallet to the blacklist.
    pub fn add(&mut self, address: String) {
        if !self.blacklisted.contains(&address) {
            self.blacklisted.push(address);
        }
    }

    /// Remove a wallet from the blacklist.
    pub fn remove(&mut self, address: &str) {
        self.blacklisted.retain(|a| a != address);
    }

    /// Check if a wallet is blacklisted.
    pub fn is_blacklisted(&self, address: &str) -> bool {
        self.blacklisted.iter().any(|a| a == address)
    }

    /// Get all blacklisted wallets.
    pub fn all(&self) -> &[String] {
        &self.blacklisted
    }
}

impl Default for DevWalletRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Safety Scorer ───────────────────────────────────────────────────────────

/// Orchestrates all safety checks and produces a final report.
pub struct SafetyScorer {
    pub threshold: u8,
    pub dev_wallet_registry: DevWalletRegistry,
}

impl SafetyScorer {
    pub fn new(threshold: u8) -> Self {
        Self {
            threshold,
            dev_wallet_registry: DevWalletRegistry::new(),
        }
    }

    /// Compute the full safety report for a token.
    /// The caller should supply all available data; missing fields will produce
    /// moderate scores (not pass/fail on their own unless critical).
    pub fn compute_score(
        &self,
        token_address: &str,
        chain: Chain,
        mint_authority: Option<&str>,
        freeze_authority: Option<&str>,
        lp_locked: Option<bool>,
        lp_lock_pct: Option<f64>,
        top_holders_pct: Vec<f64>,
        dev_holdings_pct: f64,
        is_honeypot: Option<bool>,
        buy_tax: Option<f64>,
        sell_tax: Option<f64>,
    ) -> SafetyReport {
        let checks = vec![
            check_mint_authority(mint_authority),
            check_freeze_authority(freeze_authority),
            check_lp_lock(lp_locked, lp_lock_pct),
            check_holder_concentration(top_holders_pct),
            check_dev_wallet(dev_holdings_pct),
            check_honeypot(is_honeypot),
            check_tax(buy_tax, sell_tax),
        ];

        let mut report = SafetyReport {
            token_address: token_address.to_string(),
            chain,
            checks,
            total_score: 0,
            passed: false,
            threshold: self.threshold,
        };
        report.compute_total();
        report
    }
}

impl Default for SafetyScorer {
    fn default() -> Self {
        Self::new(60)
    }
}
