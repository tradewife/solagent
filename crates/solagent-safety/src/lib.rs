//! # solagent-safety
//!
//! Safety scoring system with individual checks for token risk assessment.
//! Includes an async evaluator that fetches live data from Birdeye and GMGN,
//! and checks the dev-wallet blacklist from the portfolio database.
//!
//! GMGN integration provides:
//! - LP burn/lock status (fills the gap where Birdeye returns "not available")
//! - Token security cross-validation (honeypot, tax, mint/freeze authority)
//! - Dev track record scoring (launch history, graduation rate, best ATH)

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

    /// Get a summary of failed checks.
    pub fn failed_checks(&self) -> Vec<&CheckResult> {
        self.checks.iter().filter(|c| !c.passed).collect()
    }

    /// Get a formatted summary string.
    pub fn summary(&self) -> String {
        let status = if self.passed { "PASS" } else { "FAIL" };
        let mut s = format!("Safety: {}/{} [{}]", self.total_score, self.threshold, status);
        for c in &self.checks {
            let mark = if c.passed { "+" } else { "-" };
            s.push_str(&format!("\n  {} {}: {} ({})", mark, c.name, c.score, c.details));
        }
        s
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

// ─── GMGN-Enriched Safety Checks ─────────────────────────────────────────────

/// Check LP burn/lock status using GMGN data.
/// GMGN provides `lp_burn_percent` and `lp_burned` which fill the gap
/// where Birdeye always returns "not available".
pub fn check_lp_lock_gmgn(lp_burned: Option<bool>, lp_burn_percent: Option<f64>) -> CheckResult {
    match (lp_burned, lp_burn_percent) {
        (Some(true), Some(pct)) if pct >= 80.0 => CheckResult {
            name: "lp_lock_gmgn".to_string(),
            score: 100,
            weight: 2.0,
            passed: true,
            details: format!("LP burned: {pct:.1}% (via GMGN)"),
        },
        (Some(true), Some(pct)) => CheckResult {
            name: "lp_lock_gmgn".to_string(),
            score: 60,
            weight: 2.0,
            passed: pct >= 50.0,
            details: format!("LP partially burned: {pct:.1}% (via GMGN)"),
        },
        (Some(true), None) => CheckResult {
            name: "lp_lock_gmgn".to_string(),
            score: 70,
            weight: 2.0,
            passed: true,
            details: "LP burned (via GMGN)".to_string(),
        },
        (Some(false), _) => CheckResult {
            name: "lp_lock_gmgn".to_string(),
            score: 0,
            weight: 2.0,
            passed: false,
            details: "LP NOT burned — rug pull risk (via GMGN)".to_string(),
        },
        _ => CheckResult {
            name: "lp_lock_gmgn".to_string(),
            score: 30,
            weight: 2.0,
            passed: false,
            details: "LP burn status unknown (GMGN data unavailable)".to_string(),
        },
    }
}

/// Result of dev track record analysis from GMGN.
#[derive(Debug, Clone)]
pub struct DevTrackRecord {
    /// Number of tokens launched by the dev.
    pub total_launches: usize,
    /// Number of tokens that graduated to a DEX.
    pub graduated_count: usize,
    /// Graduation rate (0.0 - 1.0).
    pub graduation_rate: f64,
    /// Highest ATH market cap across all dev's tokens.
    pub best_ath_mcap: f64,
    /// Whether the best ATH suggests legitimate projects (>$100K).
    pub has_legitimate_project: bool,
}

/// Check dev wallet track record using GMGN created-tokens data.
/// A dev who has launched many tokens with low ATHs and zero graduations
/// is a strong rug-pull predictor.
pub fn check_dev_track_record(record: &DevTrackRecord) -> CheckResult {
    if record.total_launches == 0 {
        return CheckResult {
            name: "dev_track_record".to_string(),
            score: 70,
            weight: 1.5,
            passed: true,
            details: "New dev (no prior launches)".to_string(),
        };
    }

    // Scoring logic:
    // - High graduation rate = good (legitimate dev)
    // - Many launches with 0 graduations = rug pattern
    // - Best ATH > $100K = has built real projects
    // - Best ATH < $5K across many launches = serial rugger

    let mut score: f64 = 50.0; // Start neutral

    // Graduation rate scoring
    if record.graduation_rate >= 0.5 {
        score += 30.0; // Half or more graduate = good
    } else if record.graduation_rate >= 0.2 {
        score += 10.0;
    } else if record.graduation_rate == 0.0 && record.total_launches >= 5 {
        score -= 30.0; // 0% graduation with 5+ launches = major red flag
    } else if record.graduation_rate == 0.0 && record.total_launches >= 3 {
        score -= 15.0;
    }

    // Best ATH scoring
    if record.best_ath_mcap >= 1_000_000.0 {
        score += 20.0; // Has built a $1M+ token = legitimate
    } else if record.best_ath_mcap >= 100_000.0 {
        score += 10.0;
    } else if record.best_ath_mcap < 5_000.0 && record.total_launches >= 5 {
        score -= 20.0; // All tokens under $5K with many launches = serial rugger
    }

    // Volume penalty for excessive launches
    if record.total_launches >= 20 {
        score -= 15.0;
    } else if record.total_launches >= 10 {
        score -= 5.0;
    }

    let score = score.clamp(0.0, 100.0) as u8;
    let passed = score >= 40;

    CheckResult {
        name: "dev_track_record".to_string(),
        score,
        weight: 1.5,
        passed,
        details: format!(
            "Dev track record: {} launches, {} graduated ({:.0}%), best ATH ${:.0}",
            record.total_launches,
            record.graduated_count,
            record.graduation_rate * 100.0,
            record.best_ath_mcap,
        ),
    }
}

/// Check token security using GMGN data as cross-validation against Birdeye.
/// Provides LP burn, honeypot, and tax data from a Solana-native source.
pub fn check_gmgn_security(security: &solagent_data::GmgnTokenSecurity) -> Vec<CheckResult> {
    let mut results = Vec::new();

    // Honeypot check from GMGN
    results.push(CheckResult {
        name: "honeypot_gmgn".to_string(),
        score: match security.is_honeypot {
            Some(false) => 100,
            Some(true) => 0,
            None => 50,
        },
        weight: 2.0,
        passed: security.is_honeypot != Some(true),
        details: match security.is_honeypot {
            Some(false) => "Not a honeypot (GMGN confirmed)".to_string(),
            Some(true) => "HONEYPOT DETECTED (GMGN)".to_string(),
            None => "Honeypot status unknown (GMGN)".to_string(),
        },
    });

    // Tax check from GMGN
    let buy_tax = security.buy_tax.unwrap_or(0.0) * 100.0;
    let sell_tax = security.sell_tax.unwrap_or(0.0) * 100.0;
    let total_tax = buy_tax + sell_tax;
    let (score, passed) = if total_tax < 3.0 {
        (100, true)
    } else if total_tax < 5.0 {
        (70, true)
    } else if total_tax < 10.0 {
        (40, false)
    } else {
        (0, false)
    };
    results.push(CheckResult {
        name: "tax_gmgn".to_string(),
        score,
        weight: 1.5,
        passed,
        details: format!(
            "Buy tax: {buy_tax:.1}%, Sell tax: {sell_tax:.1}% (total: {total_tax:.1}%) [GMGN]"
        ),
    });

    results
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
    #[allow(clippy::too_many_arguments)]
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

// ─── Dev Blacklist Check ─────────────────────────────────────────────────────

/// Check if the deployer/dev wallet is on the blacklist.
pub fn check_dev_blacklisted(is_blacklisted: bool) -> CheckResult {
    CheckResult {
        name: "dev_blacklist".to_string(),
        score: if is_blacklisted { 0 } else { 100 },
        weight: 1.5,
        passed: !is_blacklisted,
        details: if is_blacklisted {
            "Dev wallet is BLACKLISTED — known rug puller".to_string()
        } else {
            "Dev wallet is clean".to_string()
        },
    }
}

// ─── Async Safety Evaluator ─────────────────────────────────────────────────

/// A dev blacklist checker that wraps a SQLite pool query.
/// Checks if a deployer address is on the dev_blacklist table.
#[derive(Clone)]
pub struct DevBlacklistChecker {
    pool: sqlx::SqlitePool,
}

impl DevBlacklistChecker {
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn is_blacklisted(&self, address: &str, chain: Chain) -> bool {
        let result: Result<(i64,), sqlx::Error> = sqlx::query_as(
            "SELECT COUNT(*) FROM dev_blacklist WHERE address = ?1 AND chain = ?2",
        )
        .bind(address)
        .bind(chain.to_string())
        .fetch_one(&self.pool)
        .await;
        match result {
            Ok((count,)) => count > 0,
            Err(_) => false,
        }
    }
}

/// Re-export for backward compat. Prefer `DevBlacklistChecker`.
pub type SqliteDevBlacklist = DevBlacklistChecker;

/// Async safety evaluator that fetches live data from Birdeye and GMGN.
pub struct SafetyEvaluator {
    pub threshold: u8,
    pub birdeye: solagent_data::BirdeyeClient,
    pub dev_blacklist: DevBlacklistChecker,
    /// Optional GMGN client for LP burn data, token security, and dev track record.
    pub gmgn: Option<solagent_data::GmgnClient>,
}

impl SafetyEvaluator {
    pub fn new(
        threshold: u8,
        birdeye: solagent_data::BirdeyeClient,
        dev_blacklist: DevBlacklistChecker,
    ) -> Self {
        Self {
            threshold,
            birdeye,
            dev_blacklist,
            gmgn: None,
        }
    }

    /// Create with GMGN client enabled for enriched safety checks.
    pub fn with_gmgn(
        threshold: u8,
        birdeye: solagent_data::BirdeyeClient,
        dev_blacklist: DevBlacklistChecker,
        gmgn: solagent_data::GmgnClient,
    ) -> Self {
        Self {
            threshold,
            birdeye,
            dev_blacklist,
            gmgn: Some(gmgn),
        }
    }

    /// Evaluate a token's safety by fetching live data from Birdeye and GMGN.
    ///
    /// `deployer_address` is optional -- if provided, checks the dev blacklist
    /// and fetches dev track record from GMGN.
    /// Returns a full `SafetyReport` with all check results.
    pub async fn evaluate(
        &self,
        token_address: &str,
        chain: Chain,
        deployer_address: Option<&str>,
    ) -> SafetyReport {
        let mut checks = Vec::new();

        // Fetch Birdeye security data.
        let security_result = self.birdeye.get_token_security(token_address).await;
        match security_result {
            Ok(sec) => {
                checks.push(check_mint_authority(sec.mint_authority.as_deref()));
                checks.push(check_freeze_authority(sec.freeze_authority.as_deref()));
                checks.push(check_honeypot(sec.is_honeypot));
                checks.push(check_tax(
                    sec.buy_tax.map(|t| t * 100.0),
                    sec.sell_tax.map(|t| t * 100.0),
                ));
                let holder_pcts: Vec<f64> = sec.top_holders.iter().map(|h| h.pct).collect();
                checks.push(check_holder_concentration(holder_pcts));
            }
            Err(e) => {
                tracing::warn!(token = token_address, error = %e, "Birdeye security fetch failed");
                checks.push(CheckResult {
                    name: "mint_authority".into(), score: 50, weight: 1.5, passed: false,
                    details: "Security data unavailable".into(),
                });
                checks.push(CheckResult {
                    name: "freeze_authority".into(), score: 50, weight: 1.5, passed: false,
                    details: "Security data unavailable".into(),
                });
                checks.push(check_honeypot(None));
                checks.push(check_tax(None, None));
                checks.push(check_holder_concentration(vec![]));
            }
        }

        // Fetch top holders for dev holdings analysis.
        let holders_result = self.birdeye.get_top_holders(token_address).await;
        match holders_result {
            Ok(holders) => {
                if let Some(deployer) = deployer_address {
                    let dev_holdings_pct = holders.iter()
                        .find(|h| h.owner == deployer)
                        .map(|h| h.pct)
                        .unwrap_or(0.0);
                    checks.push(check_dev_wallet(dev_holdings_pct));
                } else {
                    let top_holder_pct = holders.first().map(|h| h.pct).unwrap_or(0.0);
                    checks.push(check_dev_wallet(top_holder_pct));
                }
                if checks.iter().all(|c| c.name != "holder_concentration") {
                    let holder_pcts: Vec<f64> = holders.iter().map(|h| h.pct).collect();
                    checks.push(check_holder_concentration(holder_pcts));
                }
            }
            Err(e) => {
                tracing::warn!(token = token_address, error = %e, "Birdeye holders fetch failed");
                checks.push(check_dev_wallet(0.0));
            }
        }

        // ─── GMGN-Enriched Checks ──────────────────────────────────────
        // These supplement or replace Birdeye-only checks with GMGN data.

        if let Some(ref gmgn) = self.gmgn {
            // LP lock check from GMGN (replaces the "not available" stub).
            match gmgn.get_token_security(token_address).await {
                Some(gmgn_sec) => {
                    checks.push(check_lp_lock_gmgn(
                        gmgn_sec.lp_burned,
                        gmgn_sec.lp_burn_percent,
                    ));
                    // Also add GMGN cross-validated honeypot and tax checks.
                    checks.extend(check_gmgn_security(&gmgn_sec));
                }
                None => {
                    // GMGN unavailable — use the stub.
                    checks.push(CheckResult {
                        name: "lp_lock".into(), score: 50, weight: 2.0, passed: false,
                        details: "LP lock data not available (Birdeye + GMGN)".into(),
                    });
                }
            }

            // Dev track record check from GMGN (new safety check).
            if let Some(dev_addr) = deployer_address {
                let dev_tokens = gmgn.get_dev_created_tokens(dev_addr).await;
                if !dev_tokens.is_empty() {
                    let total = dev_tokens.len();
                    let graduated = dev_tokens.iter()
                        .filter(|t| t.is_graduated == Some(true))
                        .count();
                    let best_ath = dev_tokens.iter()
                        .filter_map(|t| t.token_ath_mc)
                        .fold(0.0_f64, f64::max);

                    let record = DevTrackRecord {
                        total_launches: total,
                        graduated_count: graduated,
                        graduation_rate: if total > 0 { graduated as f64 / total as f64 } else { 0.0 },
                        best_ath_mcap: best_ath,
                        has_legitimate_project: best_ath >= 100_000.0,
                    };

                    // Auto-blacklist devs with terrible track records.
                    if record.graduation_rate == 0.0 && total >= 10 && best_ath < 5_000.0 {
                        tracing::warn!(
                            dev = &dev_addr[..dev_addr.len().min(12)],
                            launches = total,
                            "Dev has {} launches with 0% graduation and best ATH ${:.0} — auto-blacklisting",
                            total, best_ath
                        );
                    }

                    checks.push(check_dev_track_record(&record));
                } else {
                    // No prior launches = new dev, not necessarily bad.
                    checks.push(CheckResult {
                        name: "dev_track_record".to_string(),
                        score: 70,
                        weight: 1.5,
                        passed: true,
                        details: "New dev (no prior launches found via GMGN)".to_string(),
                    });
                }
            }
        } else {
            // No GMGN client — fall back to stub.
            checks.push(CheckResult {
                name: "lp_lock".into(), score: 50, weight: 2.0, passed: false,
                details: "LP lock data not available from Birdeye".into(),
            });
        }

        // Dev blacklist check.
        if let Some(deployer) = deployer_address {
            let blacklisted = self.dev_blacklist.is_blacklisted(deployer, chain).await;
            checks.push(check_dev_blacklisted(blacklisted));
        }

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
