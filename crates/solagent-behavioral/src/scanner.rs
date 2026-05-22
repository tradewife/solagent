//! Behavioral scanner: discovers wallets via DexScreener token analysis,
//! extracts traders via GMGN CLI (free tier), and scores them.

use anyhow::Result;
use std::collections::HashMap;
use std::process::Stdio;
use solagent_data::{BirdeyeClient, DexScreenerClient};

use crate::layers::{self, LayerWeights, WalletSignals};
use crate::report::{BehavioralReport, WalletScore};

/// Configuration for a behavioral scan.
#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Number of crashed tokens to analyze (Layer 1/2).
    pub crash_token_count: usize,
    /// Number of mooning tokens to analyze (Layer 3).
    pub moon_token_count: usize,
    /// Minimum PnL threshold for a wallet to be scored (USD).
    pub min_pnl_usd: f64,
    /// Minimum number of tokens a wallet must appear in to be scored.
    pub min_token_appearances: usize,
    /// Layer weights for composite scoring.
    pub weights: LayerWeights,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            crash_token_count: 10,
            moon_token_count: 10,
            min_pnl_usd: 50.0,
            min_token_appearances: 2,
            weights: LayerWeights::default(),
        }
    }
}

/// Trader info extracted from GMGN CLI output.
#[derive(Debug, Clone)]
struct GmgnTrader {
    address: String,
    pnl: f64,
    is_profitable: bool,
}

/// The main behavioral scanner.
pub struct BehavioralScanner {
    #[allow(dead_code)] // reserved for future Birdeye-based enrichment passes
    birdeye: BirdeyeClient,
    dex: DexScreenerClient,
    config: ScanConfig,
}

impl BehavioralScanner {
    pub fn new(birdeye: BirdeyeClient) -> Self {
        Self {
            birdeye,
            dex: DexScreenerClient::new("https://api.dexscreener.com".to_string(), None),
            config: ScanConfig::default(),
        }
    }

    pub fn with_config(birdeye: BirdeyeClient, config: ScanConfig) -> Self {
        Self {
            birdeye,
            dex: DexScreenerClient::new("https://api.dexscreener.com".to_string(), None),
            config,
        }
    }

    /// Run the full 5-layer behavioral scan.
    pub async fn scan(&self) -> Result<BehavioralReport> {
        let now = chrono::Utc::now();
        tracing::info!(
            crash_tokens = self.config.crash_token_count,
            moon_tokens = self.config.moon_token_count,
            "Starting behavioral scan"
        );

        // Phase 1: Discover tokens via DexScreener (free, unlimited)
        let crash_tokens = self.discover_crash_tokens().await?;
        let moon_tokens = self.discover_moon_tokens().await?;
        let all_tokens: Vec<String> = crash_tokens.iter()
            .chain(moon_tokens.iter())
            .cloned()
            .collect();

        tracing::info!(
            crash = crash_tokens.len(),
            moon = moon_tokens.len(),
            "Token discovery complete"
        );

        // Phase 2: Extract traders from each token via GMGN CLI
        let mut wallet_map: HashMap<String, WalletAccumulator> = HashMap::new();

        for (idx, token_addr) in all_tokens.iter().enumerate() {
            tracing::info!(token = &token_addr[..token_addr.len().min(12)], idx, "Fetching traders via GMGN");

            match self.fetch_traders_gmgn(token_addr).await {
                Ok(traders) => {
                    tracing::debug!(token = &token_addr[..token_addr.len().min(12)], traders = traders.len(), "GMGN traders fetched");
                    for trader in traders {
                        let addr = trader.address.clone();
                        let pnl = trader.pnl;

                        let acc = wallet_map.entry(addr).or_insert_with(|| WalletAccumulator {
                            address: trader.address.clone(),
                            tokens: HashMap::new(),
                            is_bot: false,
                            is_mev: false,
                            avg_hold_hours: 0.0,
                        });

                        let is_crash = crash_tokens.contains(&token_addr);
                        let is_moon = moon_tokens.contains(&token_addr);

                        let entry = acc.tokens.entry(token_addr.clone()).or_insert(TokenTrade {
                            pnl,
                            is_profitable: trader.is_profitable,
                            is_crash_token: is_crash,
                            is_moon_token: is_moon,
                        });

                        if pnl > entry.pnl {
                            entry.pnl = pnl;
                            entry.is_profitable = trader.is_profitable;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(token = &token_addr[..token_addr.len().min(12)], error = %e, "GMGN trader fetch failed");
                }
            }

            // Small delay between GMGN CLI calls to respect rate limits
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }

        tracing::info!(wallets = wallet_map.len(), "Wallet aggregation complete");

        // Phase 3: Score wallets
        let mut scored: Vec<WalletScore> = Vec::new();
        let wallets_discovered = wallet_map.len();

        for (_addr, acc) in wallet_map.iter() {
            if acc.tokens.len() < self.config.min_token_appearances {
                continue;
            }

            let total_pnl: f64 = acc.tokens.values().map(|t| t.pnl).sum();
            if total_pnl.abs() < self.config.min_pnl_usd {
                continue;
            }

            let profitable_tokens = acc.tokens.values().filter(|t| t.is_profitable).count();
            let losing_tokens = acc.tokens.values().filter(|t| !t.is_profitable).count();
            let ghost_exits = acc.tokens.values()
                .filter(|t| t.is_crash_token && t.is_profitable)
                .count();
            let conviction_entries = acc.tokens.values()
                .filter(|t| t.is_moon_token && t.is_profitable)
                .count();

            let signals = WalletSignals {
                address: acc.address.clone(),
                total_pnl,
                token_count: acc.tokens.len(),
                profitable_tokens,
                losing_tokens,
                ghost_exits,
                conviction_entries,
                cto_profits: 0,
                is_bot: acc.is_bot,
                is_mev: acc.is_mev,
                avg_hold_hours: acc.avg_hold_hours,
                source_tokens: acc.tokens.keys().cloned().collect(),
            };

            let (layer_scores, composite, tier, confidence, red_flags, notes) =
                layers::score_wallet(&signals, &self.config.weights);

            let primary_edge = layers::identify_primary_edge(&layer_scores);

            scored.push(WalletScore {
                address: acc.address.clone(),
                tier,
                composite_score: composite,
                layer_scores,
                primary_edge,
                red_flags,
                confidence,
                discovered_from_tokens: signals.source_tokens,
                token_pnl: total_pnl,
                token_count: signals.token_count,
                notes,
            });
        }

        scored.sort_by(|a, b| b.composite_score.partial_cmp(&a.composite_score).unwrap_or(std::cmp::Ordering::Equal));

        let wallets_scored = scored.len();

        Ok(BehavioralReport {
            scan_timestamp: now.to_rfc3339(),
            chain: "solana".to_string(),
            tokens_scanned: all_tokens.len(),
            wallets_discovered,
            wallets_scored,
            crash_tokens,
            moon_tokens,
            wallets: scored,
        })
    }

    /// Fetch top traders for a token via GMGN CLI (free tier, 4 req/sec).
    async fn fetch_traders_gmgn(&self, token_addr: &str) -> Result<Vec<GmgnTrader>> {
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::process::Command::new("gmgn-cli")
                .args([
                    "token", "traders",
                    "--chain", "sol",
                    "--address", token_addr,
                    "--tag", "smart_degen",
                    "--order-by", "profit",
                    "--direction", "desc",
                    "--limit", "20",
                    "--raw",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        ).await
        .map_err(|_| anyhow::anyhow!("gmgn-cli timeout"))??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("gmgn-cli failed: {}", stderr);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut traders = Vec::new();

        // GMGN returns a JSON object with a "list" array
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&stdout) {
            if let Some(list) = data.get("list").and_then(|l| l.as_array()) {
                for item in list {
                    let address = item.get("address")
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string();

                    let profit = item.get("profit")
                        .or_else(|| item.get("realized_profit"))
                        .and_then(|p| p.as_f64())
                        .unwrap_or(0.0);

                    if !address.is_empty() {
                        traders.push(GmgnTrader {
                            address,
                            pnl: profit,
                            is_profitable: profit > 0.0,
                        });
                    }
                }
            }
        }

        Ok(traders)
    }

    /// Discover tokens with largest 24h price drops via DexScreener.
    async fn discover_crash_tokens(&self) -> Result<Vec<String>> {
        match self.dex.get_boosted_tokens().await {
            Ok(boosts) => {
                let sol_boosts: Vec<_> = boosts.iter()
                    .filter(|b| b.chain_id.as_deref() == Some("solana"))
                    .take(self.config.crash_token_count * 3)
                    .collect();

                let mut crash_addrs = Vec::new();
                for boost in sol_boosts {
                    if crash_addrs.len() >= self.config.crash_token_count { break; }
                    if let Ok(Some(pair)) = self.dex.get_token_info(&boost.token_address).await {
                        let change_24h = pair.price_change.as_ref().and_then(|c| c.h24).unwrap_or(0.0);
                        if change_24h < -20.0 {
                            crash_addrs.push(boost.token_address.clone());
                        }
                    }
                }
                Ok(crash_addrs)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to fetch boosted tokens from DexScreener");
                Ok(Vec::new())
            }
        }
    }

    /// Discover tokens with largest 24h price gains via DexScreener.
    async fn discover_moon_tokens(&self) -> Result<Vec<String>> {
        match self.dex.get_boosted_tokens().await {
            Ok(boosts) => {
                let sol_boosts: Vec<_> = boosts.iter()
                    .filter(|b| b.chain_id.as_deref() == Some("solana"))
                    .take(self.config.moon_token_count * 3)
                    .collect();

                let mut moon_addrs = Vec::new();
                for boost in sol_boosts {
                    if moon_addrs.len() >= self.config.moon_token_count { break; }
                    if let Ok(Some(pair)) = self.dex.get_token_info(&boost.token_address).await {
                        let change_24h = pair.price_change.as_ref().and_then(|c| c.h24).unwrap_or(0.0);
                        let liq = pair.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                        if change_24h > 50.0 && liq > 1000.0 {
                            moon_addrs.push(boost.token_address.clone());
                        }
                    }
                }
                Ok(moon_addrs)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to fetch boosted tokens from DexScreener");
                Ok(Vec::new())
            }
        }
    }
}

/// Internal accumulator for aggregating a wallet's trades across tokens.
struct WalletAccumulator {
    address: String,
    tokens: HashMap<String, TokenTrade>,
    is_bot: bool,
    is_mev: bool,
    avg_hold_hours: f64,
}

/// A wallet's trade on a specific token.
struct TokenTrade {
    pnl: f64,
    is_profitable: bool,
    is_crash_token: bool,
    is_moon_token: bool,
}
