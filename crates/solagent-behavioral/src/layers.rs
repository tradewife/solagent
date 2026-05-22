//! 5-layer scoring logic for behavioral wallet analysis.

use crate::report::{LayerScores, Tier, Confidence};

/// Weight configuration for the 5-layer composite score.
#[derive(Debug, Clone)]
pub struct LayerWeights {
    pub inverse_loss: f64,   // 0.20
    pub ghost_detect: f64,   // 0.25
    pub conviction: f64,     // 0.20
    pub cto_reader: f64,     // 0.20
    pub deviation: f64,      // 0.15
}

impl Default for LayerWeights {
    fn default() -> Self {
        Self {
            inverse_loss: 0.20,
            ghost_detect: 0.25,
            conviction: 0.20,
            cto_reader: 0.20,
            deviation: 0.15,
        }
    }
}

/// Input data for scoring a wallet discovered from Birdeye trader analysis.
#[derive(Debug, Clone)]
pub struct WalletSignals {
    pub address: String,
    /// PnL aggregated across all discovery tokens.
    pub total_pnl: f64,
    /// Number of distinct tokens this wallet traded among discovery set.
    pub token_count: usize,
    /// Number of tokens where wallet was profitable.
    pub profitable_tokens: usize,
    /// Number of tokens where wallet lost money.
    pub losing_tokens: usize,
    /// Number of tokens where wallet exited before a crash (>40% drop after exit).
    pub ghost_exits: usize,
    /// Number of tokens where wallet entered early (mcap < $100k at entry) and token went 10x+.
    pub conviction_entries: usize,
    /// Number of tokens where wallet was profitable on a CTO token.
    pub cto_profits: usize,
    /// Whether wallet uses bot platform tags (from GMGN if available).
    pub is_bot: bool,
    /// Whether wallet is MEV/sandwich.
    pub is_mev: bool,
    /// Average hold time in hours (0 if unknown).
    pub avg_hold_hours: f64,
    /// Source token addresses.
    pub source_tokens: Vec<String>,
}

/// Score a wallet across all 5 layers and return composite score + tier.
pub fn score_wallet(signals: &WalletSignals, weights: &LayerWeights) -> (LayerScores, f64, Tier, Confidence, Vec<String>, String) {
    let mut red_flags = Vec::new();

    if signals.is_mev {
        red_flags.push("MEV/Sandwich bot".to_string());
    }
    if signals.is_bot {
        red_flags.push("Bot platform user".to_string());
    }

    let l1 = score_inverse_loss(signals);
    let l2 = score_ghost_detect(signals);
    let l3 = score_conviction(signals);
    let l4 = score_cto_reader(signals);
    let l5 = score_deviation(signals);

    let layers = LayerScores {
        inverse_loss: l1,
        ghost_detect: l2,
        conviction: l3,
        cto_reader: l4,
        deviation: l5,
    };

    let composite = l1 * weights.inverse_loss
        + l2 * weights.ghost_detect
        + l3 * weights.conviction
        + l4 * weights.cto_reader
        + l5 * weights.deviation;

    // Downgrade tier if red flags present
    let tier = if !red_flags.is_empty() && composite < 85.0 {
        Tier::Noise
    } else if composite >= 90.0 {
        Tier::Precognitive
    } else if composite >= 75.0 {
        Tier::Sovereign
    } else if composite >= 55.0 {
        Tier::Emerging
    } else {
        Tier::Noise
    };

    let confidence = if !red_flags.is_empty() {
        Confidence::Low
    } else if composite >= 85.0 {
        Confidence::High
    } else if composite >= 70.0 {
        Confidence::Medium
    } else {
        Confidence::Low
    };

    let _primary_edge = identify_primary_edge(&layers);
    let notes = build_notes(signals);

    (layers, composite, tier, confidence, red_flags, notes)}

/// L1: Inverse Loss Archaeology — high win rate + low loss ratio.
fn score_inverse_loss(s: &WalletSignals) -> f64 {
    if s.token_count == 0 { return 0.0; }

    let win_rate = s.profitable_tokens as f64 / s.token_count as f64;
    let loss_ratio = s.losing_tokens as f64 / s.token_count as f64;

    let mut score: f64 = 0.0;

    // Win rate contribution (0-50 pts)
    if win_rate >= 0.70 { score += 50.0; }
    else if win_rate >= 0.55 { score += 35.0; }
    else if win_rate >= 0.40 { score += 20.0; }
    else if win_rate >= 0.25 { score += 10.0; }

    // Low loss ratio (0-30 pts)
    if loss_ratio < 0.10 { score += 30.0; }
    else if loss_ratio < 0.20 { score += 20.0; }
    else if loss_ratio < 0.35 { score += 10.0; }

    // Positive PnL (0-20 pts)
    if s.total_pnl > 10000.0 { score += 20.0; }
    else if s.total_pnl > 1000.0 { score += 15.0; }
    else if s.total_pnl > 0.0 { score += 5.0; }

    score.min(100.0_f64)
}

/// L2: Liquidity Ghost Detection — exits before crashes.
fn score_ghost_detect(s: &WalletSignals) -> f64 {
    let mut score: f64 = 0.0;

    // Ghost exits (0-50 pts) — core signal
    if s.ghost_exits >= 3 { score += 50.0; }
    else if s.ghost_exits >= 2 { score += 35.0; }
    else if s.ghost_exits >= 1 { score += 20.0; }

    // Selective trading + profitable = ghost-like behavior (0-30 pts)
    if s.token_count < 20 && s.total_pnl > 0.0 {
        let wr = s.profitable_tokens as f64 / s.token_count.max(1) as f64;
        if wr > 0.60 { score += 30.0; }
        else if wr > 0.40 { score += 15.0; }
    }

    // No big losses = exits early from losers (0-20 pts)
    if s.losing_tokens == 0 && s.token_count > 3 { score += 20.0; }
    else if s.losing_tokens > 0 {
        let loss_ratio = s.losing_tokens as f64 / s.token_count.max(1) as f64;
        if loss_ratio < 0.15 { score += 10.0; }
    }

    score.min(100.0_f64)
}

/// L3: Irrational Conviction Scoring — early entry in 10x+ tokens.
fn score_conviction(s: &WalletSignals) -> f64 {
    let mut score: f64 = 0.0;

    // Conviction entries (0-60 pts)
    if s.conviction_entries >= 5 { score += 60.0; }
    else if s.conviction_entries >= 3 { score += 45.0; }
    else if s.conviction_entries >= 2 { score += 30.0; }
    else if s.conviction_entries >= 1 { score += 15.0; }

    // Long hold times = conviction (0-20 pts)
    if s.avg_hold_hours > 100.0 { score += 20.0; }
    else if s.avg_hold_hours > 24.0 { score += 15.0; }
    else if s.avg_hold_hours > 6.0 { score += 10.0; }

    // High PnL with moderate token count (0-20 pts)
    if s.token_count > 0 && s.token_count <= 50 && s.total_pnl > 5000.0 { score += 20.0; }
    else if s.token_count > 0 && s.token_count <= 100 && s.total_pnl > 2000.0 { score += 10.0; }

    score.min(100.0_f64)
}

/// L4: CTO Meta-Reader Accuracy — profitable on CTO tokens.
fn score_cto_reader(s: &WalletSignals) -> f64 {
    let mut score: f64 = 0.0;

    // CTO profits (0-60 pts)
    if s.cto_profits >= 3 { score += 60.0; }
    else if s.cto_profits >= 2 { score += 40.0; }
    else if s.cto_profits >= 1 { score += 20.0; }

    // High token count = experienced with many situations (0-20 pts)
    if s.token_count >= 20 { score += 20.0; }
    else if s.token_count >= 10 { score += 10.0; }

    // Long holds = willingness to hold through CTO (0-20 pts)
    if s.avg_hold_hours > 24.0 { score += 20.0; }
    else if s.avg_hold_hours > 6.0 { score += 10.0; }

    score.min(100.0_f64)
}

/// L5: Consensus Deviation — different methodology while profitable.
fn score_deviation(s: &WalletSignals) -> f64 {
    if s.total_pnl <= 0.0 { return 0.0; }

    let mut score: f64 = 0.0;
    let win_rate = s.profitable_tokens as f64 / s.token_count.max(1) as f64;

    // High WR deviation from consensus (~40%) (0-30 pts)
    if win_rate > 0.65 { score += 30.0; }
    else if win_rate < 0.30 && s.total_pnl > 5000.0 { score += 25.0; } // Low WR but profitable

    // Very selective (0-35 pts)
    if s.token_count < 10 { score += 35.0; }
    else if s.token_count < 20 { score += 25.0; }
    else if s.token_count < 50 { score += 15.0; }

    // Hold time deviation from consensus (~5hr) (0-35 pts)
    if s.avg_hold_hours > 50.0 { score += 35.0; }
    else if s.avg_hold_hours < 0.5 && s.total_pnl > 3000.0 { score += 25.0; }

    score.min(100.0_f64)
}

pub fn identify_primary_edge(layers: &LayerScores) -> String {
    let scores = [
        ("L1_InverseLoss", layers.inverse_loss),
        ("L2_GhostDetect", layers.ghost_detect),
        ("L3_Conviction", layers.conviction),
        ("L4_CTOReader", layers.cto_reader),
        ("L5_Deviation", layers.deviation),
    ];
    scores.into_iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(name, _)| name.to_string())
        .unwrap_or_else(|| "Unknown".to_string())
}

fn build_notes(s: &WalletSignals) -> String {
    let wr = if s.token_count > 0 {
        format!("{:.0}%", s.profitable_tokens as f64 / s.token_count as f64 * 100.0)
    } else {
        "N/A".to_string()
    };
    format!(
        "PnL: ${:.0} | WR: {} | Tokens: {} | Ghost exits: {} | Conviction: {} | CTO wins: {} | Hold: {:.1}h",
        s.total_pnl, wr, s.token_count, s.ghost_exits, s.conviction_entries, s.cto_profits, s.avg_hold_hours,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signals(
        total_pnl: f64,
        token_count: usize,
        profitable_tokens: usize,
        losing_tokens: usize,
        ghost_exits: usize,
        conviction_entries: usize,
        cto_profits: usize,
        avg_hold_hours: f64,
    ) -> WalletSignals {
        WalletSignals {
            address: "test".to_string(),
            total_pnl,
            token_count,
            profitable_tokens,
            losing_tokens,
            ghost_exits,
            conviction_entries,
            cto_profits,
            is_bot: false,
            is_mev: false,
            avg_hold_hours,
            source_tokens: vec![],
        }
    }

    #[test]
    fn test_l1_inverse_loss_high_win_rate() {
        // 80/100=80% wr, 5/100=5% loss, $50K pnl
        let s = make_signals(50000.0, 100, 80, 5, 0, 0, 0, 5.0);
        let score = score_inverse_loss(&s);
        // 50 (wr>=70) + 30 (loss<10%) + 20 (pnl>10K) = 100
        assert_eq!(score, 100.0);
    }

    #[test]
    fn test_l1_inverse_loss_medium() {
        // 28/50=56% wr, 15/50=30% loss, $2K pnl
        let s = make_signals(2000.0, 50, 28, 15, 0, 0, 0, 5.0);
        let score = score_inverse_loss(&s);
        // 35 (wr>=55) + 10 (loss<35%) + 15 (pnl>1K) = 60
        assert_eq!(score, 60.0);
    }

    #[test]
    fn test_l1_inverse_loss_poor() {
        // 5/30=17% wr → no wr bonus. 20/30=67% loss → no loss bonus. pnl=-500 → no pnl bonus
        let s = make_signals(-500.0, 30, 5, 20, 0, 0, 0, 1.0);
        let score = score_inverse_loss(&s);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_l2_ghost_detect_multiple_exits() {
        // ghost_exits=3→50, selective+wr>60%→30, losses=2/15=13%<15%→10 = 90
        let s = make_signals(10000.0, 15, 12, 2, 3, 0, 0, 3.0);
        let score = score_ghost_detect(&s);
        assert_eq!(score, 90.0);
    }

    #[test]
    fn test_l2_ghost_detect_no_signals() {
        let s = make_signals(100.0, 100, 40, 50, 0, 0, 0, 1.0);
        let score = score_ghost_detect(&s);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_l3_conviction_high() {
        let s = make_signals(20000.0, 30, 25, 5, 0, 5, 0, 120.0);
        let score = score_conviction(&s);
        assert_eq!(score, 100.0); // 60 (5+ conviction) + 20 (hold>100) + 20 (pnl>5K, tokens<=50)
    }

    #[test]
    fn test_l3_conviction_none() {
        let s = make_signals(100.0, 200, 50, 100, 0, 0, 0, 1.0);
        let score = score_conviction(&s);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_l4_cto_reader_multiple() {
        let s = make_signals(10000.0, 25, 20, 5, 0, 0, 3, 30.0);
        let score = score_cto_reader(&s);
        assert_eq!(score, 100.0); // 60 (3+ cto) + 20 (tokens>=20) + 20 (hold>24)
    }

    #[test]
    fn test_l5_deviation_selective_profitable() {
        let s = make_signals(8000.0, 8, 6, 2, 0, 0, 0, 80.0);
        let score = score_deviation(&s);
        assert_eq!(score, 100.0); // 30 (wr>65%) + 35 (tokens<10) + 35 (hold>50)
    }

    #[test]
    fn test_l5_deviation_negative_pnl() {
        let s = make_signals(-500.0, 20, 5, 15, 0, 0, 0, 2.0);
        let score = score_deviation(&s);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_composite_score_sovereign() {
        // L1: 13/15=87%wr→50, 1/15=7%loss→30, pnl>10K→20 = 100
        // L2: 3 ghost→50, selective+wr>60%→30, loss<15%→10 = 90
        // L3: 4 conviction→45, hold>24→15, selective+pnl>5K→20 = 80
        // L4: 2 cto→40, tokens>=10→10, hold>24→20 = 70
        // L5: wr>65%→30, tokens<20→25, hold>50→35 = 90
        // composite: 100*0.20 + 90*0.25 + 80*0.20 + 70*0.20 + 90*0.15 = 86
        let s = make_signals(30000.0, 15, 13, 1, 3, 4, 2, 80.0);
        let (_layers, composite, tier, confidence, flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(composite >= 75.0, "composite should be >= 75, got {composite}");
        assert_eq!(tier, Tier::Sovereign);
        assert_eq!(confidence, Confidence::High); // 86 >= 85
        assert!(flags.is_empty());
    }

    #[test]
    fn test_composite_score_precognitive() {
        // Perfect wallet: 90% wr, $50K pnl, 4 ghost, 6 conviction, very selective
        let s = make_signals(50000.0, 10, 9, 1, 4, 6, 3, 100.0);
        let (layers, composite, tier, confidence, flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(composite >= 90.0, "composite should be >= 90, got {composite}");
        assert_eq!(tier, Tier::Precognitive);
        assert_eq!(confidence, Confidence::High);
        assert!(flags.is_empty());
        let edge = identify_primary_edge(&layers);
        assert!(!edge.is_empty());
    }

    #[test]
    fn test_mev_wallet_downgraded_to_noise() {
        // Use a wallet with composite < 85 so the red flag triggers Noise tier
        let mut s = make_signals(3000.0, 20, 14, 4, 1, 1, 0, 8.0);
        s.is_mev = true;
        let (_layers, composite, tier, confidence, flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(flags.contains(&"MEV/Sandwich bot".to_string()));
        assert!(composite < 85.0, "composite should be < 85 for downgrade, got {composite}");
        assert_eq!(tier, Tier::Noise);
        assert_eq!(confidence, Confidence::Low);
    }

    #[test]
    fn test_bot_wallet_flagged() {
        // Use a wallet with composite < 85 so the red flag triggers Noise tier
        let mut s = make_signals(3000.0, 20, 14, 4, 1, 1, 0, 8.0);
        s.is_bot = true;
        let (_layers, _composite, tier, _confidence, flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(flags.contains(&"Bot platform user".to_string()));
        assert_eq!(tier, Tier::Noise);
    }

    #[test]
    fn test_noise_tier_low_score() {
        let s = make_signals(-100.0, 5, 1, 4, 0, 0, 0, 0.5);
        let (_layers, composite, tier, _confidence, _flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(composite < 55.0, "composite should be < 55, got {composite}");
        assert_eq!(tier, Tier::Noise);
    }

    #[test]
    fn test_weights_are_honored() {
        let s = make_signals(20000.0, 20, 16, 3, 2, 2, 1, 30.0);
        let default_weights = LayerWeights::default();
        let (_layers_default, comp_default, _, _, _, _) =
            score_wallet(&s, &default_weights);

        let ghost_heavy = LayerWeights { ghost_detect: 0.50, ..Default::default() };
        let (_, comp_ghost, _, _, _, _) = score_wallet(&s, &ghost_heavy);

        assert_ne!(comp_default, comp_ghost);

        let sum = default_weights.inverse_loss + default_weights.ghost_detect
            + default_weights.conviction + default_weights.cto_reader + default_weights.deviation;
        assert!((sum - 1.0).abs() < 0.001, "weights should sum to 1.0, got {sum}");
    }

    #[test]
    fn test_zero_token_count() {
        let s = make_signals(0.0, 0, 0, 0, 0, 0, 0, 0.0);
        let l1 = score_inverse_loss(&s);
        let l5 = score_deviation(&s);
        assert_eq!(l1, 0.0);
        assert_eq!(l5, 0.0);
    }

    #[test]
    fn test_emerging_tier() {
        // Need signals that produce composite in [55, 75) range
        // L1: 10 tokens, 7 prof → wr=70% → 50, loss=2/10=20% → no loss<20 → 0? wait <0.20 is false (2/10=0.20 not <0.20)
        // Let me use: 15 tokens, 9 prof, 4 loss, $4K pnl, 1 ghost, 1 conviction, hold=10h
        // L1: 9/15=60% → 35, 4/15=27%<35% → 10, pnl>1K → 15 = 60
        // L2: 1 ghost → 20, tokens<20, wr=60%>60%? 9/15=60% yes → 30, loss=4/15=27% → no = 50
        // L3: 1 conviction → 15, hold=10>6 → 10, tokens=15,pnl=4K>2K → 10 = 35
        // L4: cto=0 → 0, tokens=15>=10 → 10, hold=10>6 → 10 = 20
        // L5: pnl>0, wr=60%<65% → 0, tokens=15<20 → 25, hold=10<50, hold>0.5 but pnl<3K → 0 = 25
        // composite: 60*0.20 + 50*0.25 + 35*0.20 + 20*0.20 + 25*0.15 = 12+12.5+7+4+3.75 = 39.25 → still too low

        // Try stronger: 12 tokens, 10 prof, 1 loss, $8K pnl, 2 ghost, 2 conviction, hold=30h, cto=1
        // L1: 10/12=83% → 50, 1/12=8%<10% → 30, pnl>10K? 8K<10K → 15 = 95
        // L2: 2 ghost → 35, tokens<20,wr>60% → 30, 1 loss → loss_ratio<15% → 10 = 75
        // L3: 2 conviction → 30, hold=30>24 → 15, tokens=12,pnl=8K>5K → 20 = 65
        // L4: cto=1 → 20, tokens=12>=10 → 10, hold=30>24 → 20 = 50
        // L5: wr=83%>65% → 30, tokens=12<20 → 25, hold=30<50 → 0 = 55
        // composite: 95*0.20 + 75*0.25 + 65*0.20 + 50*0.20 + 55*0.15 = 19+18.75+13+10+8.25 = 69 → Emerging!
        let s = make_signals(8000.0, 12, 10, 1, 2, 2, 1, 30.0);
        let (_layers, composite, tier, _confidence, _flags, _notes) =
            score_wallet(&s, &LayerWeights::default());
        assert!(composite >= 55.0, "composite should be >= 55, got {composite}");
        assert!(composite < 75.0, "composite should be < 75, got {composite}");
        assert_eq!(tier, Tier::Emerging);
    }
}
