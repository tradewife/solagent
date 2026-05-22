//! Report types for the behavioral intelligence scanner.

use serde::{Deserialize, Serialize};

/// Tier classification for scored wallets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    Precognitive,
    Sovereign,
    Emerging,
    Noise,
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tier::Precognitive => write!(f, "PRECOGNITIVE"),
            Tier::Sovereign => write!(f, "SOVEREIGN"),
            Tier::Emerging => write!(f, "EMERGING"),
            Tier::Noise => write!(f, "NOISE"),
        }
    }
}

/// Confidence level for a wallet's score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

impl std::fmt::Display for Confidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Confidence::High => write!(f, "HIGH"),
            Confidence::Medium => write!(f, "MEDIUM"),
            Confidence::Low => write!(f, "LOW"),
        }
    }
}

/// Per-layer scores for a wallet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerScores {
    /// L1: Inverse Loss Archaeology (weight 0.20)
    pub inverse_loss: f64,
    /// L2: Liquidity Ghost Detection (weight 0.25)
    pub ghost_detect: f64,
    /// L3: Irrational Conviction Scoring (weight 0.20)
    pub conviction: f64,
    /// L4: CTO Meta-Reader Accuracy (weight 0.20)
    pub cto_reader: f64,
    /// L5: Consensus Deviation (weight 0.15)
    pub deviation: f64,
}

/// A scored wallet from the behavioral scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletScore {
    pub address: String,
    pub tier: Tier,
    pub composite_score: f64,
    pub layer_scores: LayerScores,
    pub primary_edge: String,
    pub red_flags: Vec<String>,
    pub confidence: Confidence,
    /// Source token addresses that led to discovering this wallet.
    pub discovered_from_tokens: Vec<String>,
    /// PnL on the discovery tokens (from Birdeye trader data).
    pub token_pnl: f64,
    /// Number of tokens used in discovery.
    pub token_count: usize,
    /// Notes about behavioral characterization.
    pub notes: String,
}

/// Full behavioral scan report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehavioralReport {
    pub scan_timestamp: String,
    pub chain: String,
    pub tokens_scanned: usize,
    pub wallets_discovered: usize,
    pub wallets_scored: usize,
    /// Tokens used for Layer 1/2 (crashed tokens).
    pub crash_tokens: Vec<String>,
    /// Tokens used for Layer 3 (10x+ tokens).
    pub moon_tokens: Vec<String>,
    /// All scored wallets, sorted by composite score descending.
    pub wallets: Vec<WalletScore>,
}

impl BehavioralReport {
    /// Get wallets in a specific tier.
    pub fn by_tier(&self, tier: Tier) -> Vec<&WalletScore> {
        self.wallets.iter().filter(|w| w.tier == tier).collect()
    }

    /// Get clean wallets (no red flags) sorted by score.
    pub fn clean_wallets(&self) -> Vec<&WalletScore> {
        self.wallets.iter().filter(|w| w.red_flags.is_empty()).collect()
    }

    /// Format as a human-readable summary.
    pub fn summary(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("Behavioral Intelligence Report — {}\n", self.scan_timestamp));
        out.push_str(&format!("Tokens scanned: {} | Wallets discovered: {} | Scored: {}\n\n",
            self.tokens_scanned, self.wallets_discovered, self.wallets_scored));

        let precognitive = self.by_tier(Tier::Precognitive);
        let sovereign = self.by_tier(Tier::Sovereign);
        let emerging = self.by_tier(Tier::Emerging);
        let noise = self.by_tier(Tier::Noise);

        out.push_str(&format!("Tiers: PRECOGNITIVE={} SOVEREIGN={} EMERGING={} NOISE={}\n\n",
            precognitive.len(), sovereign.len(), emerging.len(), noise.len()));

        if !sovereign.is_empty() {
            out.push_str("=== SOVEREIGN (track every position) ===\n");
            for w in sovereign {
                out.push_str(&format!("  {} | Score: {:.0} | Edge: {} | {}\n",
                    &w.address[..w.address.len().min(20)], w.composite_score, w.primary_edge, w.notes));
            }
            out.push('\n');
        }

        if !emerging.is_empty() {
            out.push_str("=== EMERGING (watch 7 more days) ===\n");
            for w in emerging {
                out.push_str(&format!("  {} | Score: {:.0} | Edge: {} | {}\n",
                    &w.address[..w.address.len().min(20)], w.composite_score, w.primary_edge, w.notes));
            }
            out.push('\n');
        }

        out
    }

    /// Format the full detailed report.
    pub fn detailed(&self) -> String {
        let mut out = self.summary();

        out.push_str(&format!("=== FULL RANKINGS ===\n"));
        out.push_str(&format!("{:<40} {:<14} {:>5} {:>8} {:>5} {}\n",
            "Address", "Tier", "Score", "PnL", "Toks", "Primary Edge"));
        out.push_str(&"-".repeat(90));
        out.push('\n');

        for w in &self.wallets {
            let rf = if w.red_flags.is_empty() { "" } else { " [RF]" };
            out.push_str(&format!("{:<40} {:<14} {:>5.0} {:>8.0} {:>5} {}{}\n",
                &w.address[..w.address.len().min(39)],
                w.tier,
                w.composite_score,
                w.token_pnl,
                w.token_count,
                w.primary_edge,
                rf));
        }

        out
    }
}
