//! # solagent-cli
//!
//! Command-line interface for the SolAgent trading system.

use anyhow::Result;
use clap::{Parser, Subcommand};
use solagent_core::Config;

const DEFAULT_DB_PATH: &str = "sqlite:solagent.db?mode=rwc";

#[derive(Parser)]
#[command(name = "solagent")]
#[command(about = "Autonomous Solana/Base trading agent")]
#[command(version)]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config/default.toml")]
    config: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan for new token opportunities
    Scan {
        /// Chain to scan (solana, base, all)
        #[arg(short, long, default_value = "solana")]
        chain: String,
        /// Minimum liquidity filter (USD)
        #[arg(long, default_value = "1000")]
        min_liquidity: f64,
        /// Maximum age filter (minutes)
        #[arg(long, default_value = "60")]
        max_age_mins: u64,
    },

    /// Track a specific wallet or token
    Track {
        /// What to track (wallet, token)
        #[arg(value_enum)]
        target: TrackTarget,
        /// Address to track
        address: String,
    },

    /// Analyze a token (signals + safety + risk)
    Analyze {
        /// Token address to analyze
        address: String,
        /// Chain (solana, base)
        #[arg(short, long, default_value = "solana")]
        chain: String,
        /// Show detailed breakdown
        #[arg(short, long)]
        verbose: bool,
    },

    /// Execute a trade
    Trade {
        /// Trade action (buy, sell)
        #[arg(value_enum)]
        action: TradeAction,
        /// Token address
        address: String,
        /// Amount in USD (for buy) or token amount (for sell)
        #[arg(short, long)]
        amount: f64,
        /// Chain (solana, base)
        #[arg(short, long, default_value = "solana")]
        chain: String,
        /// Slippage tolerance in basis points
        #[arg(short, long, default_value = "100")]
        slippage_bps: u32,
    },

    /// Run a safety check on a token
    Safety {
        /// Token address to check
        address: String,
        /// Deployer address (optional, for dev blacklist check)
        #[arg(short, long)]
        deployer: Option<String>,
    },

    /// Portfolio management
    Portfolio {
        #[command(subcommand)]
        action: PortfolioAction,
    },

    /// Wallet registry operations
    Wallet {
        #[command(subcommand)]
        action: WalletAction,
    },

    /// Run the autonomous agent
    Agent {
        /// Strategy to use (whale_consensus, accumulation, momentum, all)
        #[arg(short, long, default_value = "all")]
        strategy: String,
        /// Dry run mode (no real trades)
        #[arg(long, default_value = "false")]
        dry_run: bool,
    },

    /// Configuration management
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Database operations
    Db {
        #[command(subcommand)]
        action: DbAction,
    },
}

#[derive(Clone, clap::ValueEnum)]
enum TrackTarget {
    Wallet,
    Token,
}

#[derive(Clone, clap::ValueEnum)]
enum TradeAction {
    Buy,
    Sell,
}

#[derive(Subcommand)]
enum PortfolioAction {
    /// Show portfolio summary
    Summary,
    /// List open positions
    Positions,
    /// Show PnL history
    Pnl {
        /// Number of days to show
        #[arg(short, long, default_value = "30")]
        days: u64,
    },
    /// Close a position
    Close {
        /// Position ID
        id: String,
    },
}

#[derive(Subcommand)]
enum WalletAction {
    /// List registered wallets
    List {
        /// Filter by label (smart_money, sniper, whale, insider, mev_bot, dev, unknown)
        #[arg(short, long)]
        label: Option<String>,
        /// Chain filter (solana, base)
        #[arg(short, long)]
        chain: Option<String>,
        /// Max results
        #[arg(long, default_value = "20")]
        limit: u32,
    },
    /// Show wallet balances (requires Helius key)
    Balance {
        /// Chain (solana, base, all)
        #[arg(short, long, default_value = "all")]
        chain: String,
    },
    /// Show transaction history (requires Helius key)
    History {
        /// Number of transactions to show
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },
    /// Add a wallet to the registry
    Add {
        /// Wallet address
        address: String,
        /// Label
        #[arg(short, long, default_value = "unknown")]
        label: String,
        /// Source
        #[arg(short, long, default_value = "manual")]
        source: String,
        /// Chain
        #[arg(long, default_value = "solana")]
        chain: String,
    },
    /// Remove a wallet from the registry
    Remove {
        /// Wallet address
        address: String,
        /// Chain
        #[arg(long, default_value = "solana")]
        chain: String,
    },
    /// List blacklisted dev wallets
    Blacklist {
        /// Chain filter
        #[arg(short, long)]
        chain: Option<String>,
    },
    /// Add dev to blacklist
    BlacklistAdd {
        /// Dev wallet address
        address: String,
        /// Reason
        #[arg(short, long, default_value = "")]
        reason: String,
        /// Source
        #[arg(long, default_value = "manual")]
        source: String,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Show current configuration
    Show,
    /// Validate configuration file
    Validate,
    /// Generate a default config file
    Init {
        /// Output path
        #[arg(short, long, default_value = "config/default.toml")]
        output: String,
    },
}

#[derive(Subcommand)]
enum DbAction {
    /// Run database migrations
    Migrate,
    /// Reset the database (dangerous!)
    Reset,
    /// Show database stats
    Stats,
    /// Show evaluation statistics
    EvalStats,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing.
    let log_level = cli.log_level.as_deref().unwrap_or("info");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    // Load config.
    let config_path = std::path::Path::new(&cli.config);
    let config = if config_path.exists() {
        Some(Config::load_from_file(config_path).await?)
    } else {
        tracing::warn!(path = %cli.config, "Config file not found, using defaults");
        None
    };

    match cli.command {
        // ─── Scan ────────────────────────────────────────────────────────
        Commands::Scan { chain, min_liquidity, max_age_mins } => {
            tracing::info!(chain, min_liquidity, max_age_mins, "Scanning for new tokens");

            let dex = solagent_data::DexScreenerClient::new(
                "https://api.dexscreener.com".to_string(), None,
            );

            let pairs = dex.get_new_pairs(&chain).await?;
            let cutoff_ms = (chrono::Utc::now() - chrono::Duration::minutes(max_age_mins as i64)).timestamp_millis();

            let filtered: Vec<_> = pairs.into_iter().filter(|p| {
                let liq = p.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                let age_ok = p.pair_created_at.map(|t| t > cutoff_ms).unwrap_or(true);
                liq >= min_liquidity && age_ok
            }).collect();

            if filtered.is_empty() {
                println!("No new tokens found matching criteria.");
                return Ok(());
            }

            println!("Found {} new token pairs on {}:\n", filtered.len(), chain);
            for (i, pair) in filtered.iter().enumerate() {
                let symbol = &pair.base_token.symbol;
                let addr = &pair.base_token.address;
                let liq = pair.liquidity.as_ref().and_then(|l| l.usd).map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let mc = pair.market_cap.map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let vol = pair.volume.as_ref().and_then(|v| v.h24).map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let boosts = pair.boosts.as_ref().and_then(|b| b.active).unwrap_or(0);
                let age = pair.pair_created_at.map(|t| {
                    let created = chrono::DateTime::from_timestamp_millis(t).unwrap_or_default();
                    let mins = (chrono::Utc::now() - created).num_minutes();
                    format!("{mins}m ago")
                }).unwrap_or("-".into());

                println!("{:>3}. {} ({}) | MC: {} | Liq: {} | Vol24h: {} | Age: {} | Boosts: {}",
                    i + 1, symbol, &addr[..addr.len().min(12)], mc, liq, vol, age, boosts);
                println!("     Name: {} | DEX: {} | Address: {}",
                    pair.base_token.name, pair.dex_id, addr);
                if boosts > 0 { println!("     *** BOOSTED ({}) ***", boosts); }
            }
        }

        // ─── Track ───────────────────────────────────────────────────────
        Commands::Track { target, address } => {
            let target_str = match target {
                TrackTarget::Wallet => "wallet",
                TrackTarget::Token => "token",
            };
            println!("Tracking {target_str}: {address}");
            println!("(Wallet watcher runs as part of the agent loop. Use `solagent agent --start` to begin tracking.)");
        }

        // ─── Analyze ─────────────────────────────────────────────────────
        Commands::Analyze { address, chain, verbose } => {
            tracing::info!(%address, %chain, "Analyzing token");

            let dex = solagent_data::DexScreenerClient::new(
                "https://api.dexscreener.com".to_string(), None,
            );
            let birdeye = solagent_data::BirdeyeClient::new(
                "https://public-api.birdeye.so".to_string(),
                std::env::var("BIRDEYE_API_KEY").ok(),
            );

            let pair = match dex.get_token_info(&address).await {
                Ok(Some(p)) => p,
                Ok(None) => { println!("Token not found on DexScreener."); return Ok(()); }
                Err(e) => { println!("Error fetching token: {e}"); return Ok(()); }
            };

            println!("=== {} ({}) ===", pair.base_token.symbol, pair.base_token.name);
            println!("Address:  {}", address);
            println!("DEX:      {}", pair.dex_id);
            if let Some(usd) = pair.price_usd.as_ref() { println!("Price:    ${usd}"); }
            if let Some(mc) = pair.market_cap { println!("MC:       ${mc:.0}"); }
            if let Some(fdv) = pair.fdv { println!("FDV:      ${fdv:.0}"); }
            if let Some(liq) = pair.liquidity.as_ref().and_then(|l| l.usd) { println!("Liquidity: ${liq:.0}"); }
            if let Some(vol) = pair.volume.as_ref().and_then(|v| v.h24) { println!("Vol 24h:  ${vol:.0}"); }
            if let Some(change) = pair.price_change.as_ref().and_then(|c| c.h24) {
                let arrow = if change >= 0.0 { "+" } else { "" };
                println!("Change 24h: {arrow}{change:.2}%");
            }
            if let Some(txns) = pair.txns.as_ref() {
                println!("Txns 24h: {} buys / {} sells", txns.h24.buys, txns.h24.sells);
            }
            if let Some(age_ms) = pair.pair_created_at {
                let created = chrono::DateTime::from_timestamp_millis(age_ms).unwrap_or_default();
                let age_mins = (chrono::Utc::now() - created).num_minutes();
                println!("Age:      {}h {}m", age_mins / 60, age_mins % 60);
            }

            // Birdeye security data
            println!("\n--- Security ---");
            match birdeye.get_token_security(&address).await {
                Ok(sec) => {
                    let ma = sec.mint_authority.as_deref().unwrap_or("REVOKED");
                    let fa = sec.freeze_authority.as_deref().unwrap_or("REVOKED");
                    println!("Mint Authority:   {}", if ma.is_empty() { "REVOKED".into() } else { ma.to_string() });
                    println!("Freeze Authority: {}", if fa.is_empty() { "REVOKED".into() } else { fa.to_string() });
                    println!("Renounced:        {}", sec.renounced.unwrap_or(false));
                    if let Some(hp) = sec.is_honeypot {
                        println!("Honeypot:         {}", if hp { "YES - DANGER" } else { "No" });
                    }
                    if let Some(bt) = sec.buy_tax { println!("Buy Tax:          {:.2}%", bt * 100.0); }
                    if let Some(st) = sec.sell_tax { println!("Sell Tax:         {:.2}%", st * 100.0); }
                }
                Err(e) => {
                    println!("Security data unavailable: {e}");
                    println!("(Set BIRDEYE_API_KEY env var for full security data)");
                }
            }

            // Verbose holders and traders
            if verbose {
                println!("\n--- Top Holders ---");
                match birdeye.get_top_holders(&address).await {
                    Ok(holders) => {
                        for (i, h) in holders.iter().take(10).enumerate() {
                            let owner = &h.owner;
                            println!("{:>3}. {} ... | {:.2}%", i + 1, &owner[..owner.len().min(20)], h.pct);
                        }
                    }
                    Err(_) => println!("Holder data unavailable"),
                }
                println!("\n--- Top Traders ---");
                match birdeye.get_top_traders(&address).await {
                    Ok(traders) => {
                        for (i, t) in traders.iter().take(10).enumerate() {
                            let owner = &t.owner;
                            let pnl = t.pnl.map(|p| format!("${p:.0}")).unwrap_or("-".into());
                            println!("{:>3}. {} ... | PnL: {}", i + 1, &owner[..owner.len().min(20)], pnl);
                        }
                    }
                    Err(_) => println!("Trader data unavailable"),
                }
            }

            // Safety score evaluation
            println!("\n--- Safety Score ---");
            let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
            let dev_blacklist = solagent_safety::SqliteDevBlacklist::new(pool);
            let safety_birdeye = solagent_data::BirdeyeClient::with_api_key(
                std::env::var("BIRDEYE_API_KEY").ok(),
            );
            let safety = solagent_safety::SafetyEvaluator::new(
                60, safety_birdeye, dev_blacklist,
            );
            let report = safety.evaluate(
                &address,
                match chain.as_str() {
                    "base" => solagent_core::Chain::Base,
                    _ => solagent_core::Chain::Solana,
                },
                None,
            ).await;
            println!("{}", report.summary());
        }

        // ─── Trade ───────────────────────────────────────────────────────
        Commands::Trade { action, address, amount, chain, slippage_bps } => {
            let action_str = match action { TradeAction::Buy => "BUY", TradeAction::Sell => "SELL" };
            println!("{action_str} {address}");
            println!("   Amount: {amount}");
            println!("   Chain: {chain}");
            println!("   Slippage: {slippage_bps} bps");

            let birdeye = solagent_data::BirdeyeClient::with_api_key(std::env::var("BIRDEYE_API_KEY").ok());
            match birdeye.get_token_price(&address, &chain).await {
                Ok(price) => println!("   Current price: ${:.8}", price.price_usd),
                Err(e) => println!("   Price lookup failed: {e}"),
            }
            println!("\nTo execute trades, configure the agent:");
            println!("  1. Set BIRDEYE_API_KEY env var");
            println!("  2. Set SOLANA_PRIVATE_KEY env var (base58 encoded)");
            println!("  3. Run `solagent agent --start` for autonomous execution");
        }

        // ─── Safety ──────────────────────────────────────────────────────
        Commands::Safety { address, deployer } => {
            tracing::info!(%address, "Running safety check");

            let birdeye = solagent_data::BirdeyeClient::with_api_key(
                std::env::var("BIRDEYE_API_KEY").ok(),
            );
            let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
            let dev_blacklist = solagent_safety::SqliteDevBlacklist::new(pool);
            let safety = solagent_safety::SafetyEvaluator::new(60, birdeye, dev_blacklist);

            let report = safety.evaluate(
                &address,
                solagent_core::Chain::Solana,
                deployer.as_deref(),
            ).await;
            println!("{}", report.summary());
        }

        // ─── Portfolio ───────────────────────────────────────────────────
        Commands::Portfolio { action } => {
            let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
            let pm = solagent_portfolio::PortfolioManager::new(pool);

            match action {
                PortfolioAction::Summary => {
                    let pnl = pm.get_pnl().await?;
                    println!("Portfolio Summary:");
                    println!("  Realized PnL:   ${:.2}", pnl.realized_pnl);
                    println!("  Unrealized PnL: ${:.2}", pnl.unrealized_pnl);
                    println!("  Total PnL:      ${:.2}", pnl.total_pnl);
                    println!("  Total Trades:   {}", pnl.total_trades);
                    println!("  Win Rate:       {:.1}%", pnl.win_rate * 100.0);
                }
                PortfolioAction::Positions => {
                    let positions = pm.get_open_positions().await?;
                    if positions.is_empty() {
                        println!("No open positions.");
                    } else {
                        println!("Open Positions ({}):\n", positions.len());
                        for p in &positions {
                            let sign = if p.unrealized_pnl >= 0.0 { "+" } else { "" };
                            println!("  {} | {} | {} @ ${:.4} | Size: ${:.2} | PnL: {sign}${:.2}",
                                &p.id[..p.id.len().min(8)], p.token_address, p.chain,
                                p.current_price, p.size_usd, p.unrealized_pnl);
                        }
                    }
                }
                PortfolioAction::Pnl { days: _ } => {
                    let pnl = pm.get_pnl().await?;
                    println!("PnL Summary:");
                    println!("  Realized:   ${:.2}", pnl.realized_pnl);
                    println!("  Unrealized: ${:.2}", pnl.unrealized_pnl);
                    println!("  Win Rate:   {:.1}%", pnl.win_rate * 100.0);
                    println!("  Best:       ${:.2}", pnl.largest_win_usd);
                    println!("  Worst:      ${:.2}", pnl.largest_loss_usd);
                }
                PortfolioAction::Close { id } => {
                    println!("Closing position: {id}");
                    println!("Use `solagent agent --start` for automated position management.");
                }
            }
        }

        // ─── Wallet ──────────────────────────────────────────────────────
        Commands::Wallet { action } => {
            let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
            let registry = solagent_portfolio::WalletRegistry::new(pool);

            match action {
                WalletAction::List { label, chain, limit } => {
                    let label_filter = label.as_deref().map(solagent_portfolio::WalletLabel::from_str_lossy);
                    let chain_filter = chain.as_deref().and_then(|c| match c {
                        "solana" => Some(solagent_core::Chain::Solana),
                        "base" => Some(solagent_core::Chain::Base),
                        _ => None,
                    });
                    let wallets = registry.list_wallets(label_filter, chain_filter, limit).await?;
                    if wallets.is_empty() {
                        println!("No wallets in registry.");
                    } else {
                        println!("Wallets ({}):\n", wallets.len());
                        println!("{:<40} {:<10} {:<14} {:>6} {:>10} {:>6}",
                            "Address", "Chain", "Label", "Score", "PnL", "Win%");
                        println!("{}", "-".repeat(90));
                        for w in &wallets {
                            println!("{:<40} {:<10} {:<14} {:>5.1} {:>9.0} {:>5.0}%",
                                &w.address[..w.address.len().min(39)],
                                w.chain, w.label, w.score, w.total_pnl, w.win_rate * 100.0);
                        }
                    }
                }
                WalletAction::Balance { chain: _ } => {
                    println!("Wallet balance requires Helius API key. Use `solagent analyze <TOKEN>` for token info.");
                }
                WalletAction::History { limit: _ } => {
                    println!("Transaction history requires Helius API key.");
                }
                WalletAction::Add { address, label, source, chain } => {
                    let chain = match chain.as_str() {
                        "base" => solagent_core::Chain::Base,
                        _ => solagent_core::Chain::Solana,
                    };
                    let entry = solagent_portfolio::WalletEntry {
                        address: address.clone(), chain,
                        label: solagent_portfolio::WalletLabel::from_str_lossy(&label),
                        source, win_rate: 0.0, total_pnl: 0.0, total_trades: 0,
                        avg_hold_time_mins: 0.0, score: 0.0, tags: vec![],
                        last_seen_at: None, created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
                    };
                    registry.upsert_wallet(&entry).await?;
                    println!("Added wallet: {} (label={})", address, label);
                }
                WalletAction::Remove { address, chain } => {
                    let chain = match chain.as_str() {
                        "base" => solagent_core::Chain::Base,
                        _ => solagent_core::Chain::Solana,
                    };
                    let removed = registry.remove_wallet(&address, chain).await?;
                    if removed { println!("Removed wallet: {address}"); }
                    else { println!("Wallet not found: {address}"); }
                }
                WalletAction::Blacklist { chain } => {
                    let chain_filter = chain.as_deref().and_then(|c| match c {
                        "solana" => Some(solagent_core::Chain::Solana),
                        "base" => Some(solagent_core::Chain::Base),
                        _ => None,
                    });
                    let devs = registry.list_blacklisted(chain_filter).await?;
                    if devs.is_empty() { println!("No blacklisted dev wallets."); }
                    else {
                        println!("Blacklisted Devs ({}):\n", devs.len());
                        for d in &devs {
                            println!("  {} | {} | {} | {}", &d.address[..d.address.len().min(20)], d.chain, d.source, d.reason);
                        }
                    }
                }
                WalletAction::BlacklistAdd { address, reason, source } => {
                    let entry = solagent_portfolio::DevBlacklistEntry {
                        address: address.clone(), chain: solagent_core::Chain::Solana,
                        reason, source, token_address: None, added_at: chrono::Utc::now(),
                    };
                    registry.blacklist_dev(&entry).await?;
                    println!("Blacklisted dev: {address}");
                }
            }
        }

        // ─── Agent ───────────────────────────────────────────────────────
        Commands::Agent { strategy, dry_run } => {
            println!("Starting SolAgent");
            println!("   Strategy: {strategy}");
            println!("   Dry run: {dry_run}");
            if dry_run { println!("   DRY RUN MODE -- no real trades will execute"); }

            let event_bus = solagent_core::EventBus::new(1024);
            let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;

            let dex = solagent_data::DexScreenerClient::new("https://api.dexscreener.com".to_string(), None);
            let birdeye = solagent_data::BirdeyeClient::with_api_key(std::env::var("BIRDEYE_API_KEY").ok());
            let dev_blacklist = solagent_safety::SqliteDevBlacklist::new(pool.clone());

            let safety_threshold = config.as_ref().map(|c| c.risk.safety_score_threshold).unwrap_or(60);
            let safety = solagent_safety::SafetyEvaluator::new(safety_threshold, birdeye, dev_blacklist);

            let risk_config = match &config {
                Some(c) => solagent_risk::RiskConfig::from(c.risk.clone()),
                None => solagent_risk::RiskConfig::default(),
            };

            // Wire execution engine with Solana provider + Jupiter if private key is available.
            let exec = if !dry_run {
                let pk = config.as_ref()
                    .map(|c| c.chains.solana.private_key_bs58.trim().to_string())
                    .unwrap_or_default();
                if pk.is_empty() {
                    tracing::warn!("No SOLANA_PRIVATE_KEY in config -- trades will fail. Set chains.solana.private_key_bs58 or use --dry-run.");
                    solagent_exec::ExecutionEngine::new(solagent_exec::ExecutionConfig::default())
                } else {
                    let mut rpc_urls = config.as_ref()
                        .map(|c| c.chains.solana.rpc_urls.clone())
                        .unwrap_or_else(|| vec!["https://api.mainnet-beta.solana.com".to_string()]);
                    // Prepend Helius RPC if key is available (faster, higher limits).
                    let helius_key = config.as_ref()
                        .map(|c| c.chains.solana.helius_api_key.trim().to_string())
                        .unwrap_or_default();
                    if !helius_key.is_empty() {
                        let helius_rpc = format!("https://mainnet.helius-rpc.com/?api-key={helius_key}");
                        rpc_urls.insert(0, helius_rpc);
                    }
                    let jupiter_url = config.as_ref()
                        .map(|c| c.data.jupiter_api_url.clone())
                        .unwrap_or_else(|| "https://quote-api.jup.ag/v6".to_string());

                    match solagent_chain_solana::SolanaProvider::new(
                        rpc_urls,
                        &pk,
                        solana_sdk::commitment_config::CommitmentConfig::confirmed(),
                    ) {
                        Ok(provider) => {
                            tracing::info!(pubkey = %provider.pubkeys(), "Solana provider configured");
                            solagent_exec::ExecutionEngine::new_solana(
                                solagent_exec::ExecutionConfig::default(),
                                solagent_data::JupiterClient::new(jupiter_url),
                                std::sync::Arc::new(provider),
                            )
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to create Solana provider -- falling back to dry-run exec");
                            solagent_exec::ExecutionEngine::new(solagent_exec::ExecutionConfig::default())
                        }
                    }
                }
            } else {
                solagent_exec::ExecutionEngine::new(solagent_exec::ExecutionConfig::default())
            };

            let confluence_threshold = config.as_ref().map(|c| c.strategies.confluence_threshold).unwrap_or(35.0);

            // Create wallet score cache from SQLite registry so whale consensus
            // can match watched-wallet buys against known smart money scores.
            let score_cache = solagent_signals::RegistryScoreCache::new(pool.clone());
            if let Err(e) = score_cache.refresh().await {
                tracing::warn!(error = %e, "Failed to refresh wallet score cache");
            } else {
                tracing::info!(count = score_cache.len(), "Wallet score cache loaded");
            }

            // Create signal detectors.
            let whale_signal = std::sync::Arc::new(solagent_signals::WhaleConsensusSignal::new(
                solagent_core::Chain::Solana,
                2,    // min_wallets
                30,   // window_minutes
                50.0, // min_buy_usd
                Box::new(score_cache),
            ));
            // Subscribe whale signal to WalletBuy events on the event bus.
            whale_signal.subscribe_to_events(&event_bus);
            tracing::info!("Whale consensus signal subscribed to event bus");

            let accumulation_signal = solagent_signals::AccumulationSignal::new(
                solagent_core::Chain::Solana, 20,
            );
            let launch_signal = solagent_signals::LaunchMomentumSignal::new(
                solagent_core::Chain::Solana, 20,
            );
            let volume_signal = solagent_signals::VolumeSpikeSignal::new(
                solagent_core::Chain::Solana, 3.0, 20,
            );
            let social_signal = solagent_signals::SocialSignal::with_config(
                solagent_core::Chain::Solana,
                "/home/kt/solagent/scripts/twitter-wrapper.sh".to_string(),
                vec![
                    "pump.fun".to_string(),
                    "solana memecoin".to_string(),
                    "ca:".to_string(),
                ],
                60,  // window_minutes
                1,   // min_mentions (1 tweet about a specific CA is enough)
            );

            let mut confluence = solagent_signals::ConfluenceScorer::new(confluence_threshold);
            let weights = solagent_signals::SignalWeights::default();
            // We need owned values for the scorer, but whale is Arc for event subscription.
            // Clone the Arc inner into a new owned instance for scoring.
            // Actually, the scorer just needs StrategyKind. We give it the Arc-dewrapped one.
            // The Arc-wrapped one continues listening to events and recording buys.
            // But they share the same DashMap state through interior mutability!
            // Wait -- WhaleConsensusSignal uses Arc<DashMap> internally, so cloning the struct
            // gives a new struct pointing to the SAME DashMap. Perfect.
            confluence.add_strategy(
                solagent_signals::StrategyKind::WhaleConsensus((*whale_signal).clone()),
                weights.whale_consensus,
            );
            confluence.add_strategy(
                solagent_signals::StrategyKind::Accumulation(accumulation_signal),
                weights.accumulation,
            );
            confluence.add_strategy(
                solagent_signals::StrategyKind::LaunchMomentum(launch_signal),
                weights.launch_momentum,
            );
            confluence.add_strategy(
                solagent_signals::StrategyKind::VolumeSpike(volume_signal),
                weights.volume_spike,
            );
            confluence.add_strategy(
                solagent_signals::StrategyKind::Social(social_signal),
                weights.social,
            );

            // Create wallet watcher if Helius key is available.
            let helius_key = config.as_ref()
                .map(|c| c.chains.solana.helius_api_key.trim().to_string())
                .unwrap_or_default();
            let (watcher, wallet_registry) = if !helius_key.is_empty() {
                let helius = std::sync::Arc::new(solagent_data::HeliusClient::new_with_key(helius_key));
                let watcher_config = solagent_data::WatcherConfig::default();
                let watcher = solagent_data::WalletWatcher::new(helius, event_bus.clone(), watcher_config);
                tracing::info!("Helius wallet watcher configured");
                (Some(watcher), true)
            } else {
                tracing::info!("No Helius API key -- wallet watcher disabled");
                (None, false)
            };

            let subsystems = {
                // Extract risk values for RuntimeConfig before risk_config is
                // moved into RiskManager::new in the struct literal below.
                let max_pos_size = risk_config.max_position_size_usd;
                let max_open_pos = risk_config.max_open_positions;
                let daily_loss = risk_config.max_daily_loss_usd;

                solagent_agent::AgentSubsystems {
                dex,
                safety,
                risk: std::sync::Mutex::new(solagent_risk::RiskManager::new(risk_config)),
                exec,
                portfolio: solagent_portfolio::PortfolioManager::new(pool.clone()),
                event_bus: event_bus.clone(),
                confluence: std::sync::Mutex::new(confluence),
                confluence_threshold,
                progressive_threshold_failures: config.as_ref().map(|c| c.strategies.progressive_threshold_failures).unwrap_or(10),
                progressive_threshold_step: config.as_ref().map(|c| c.strategies.progressive_threshold_step).unwrap_or(5.0),
                progressive_threshold_floor: config.as_ref().map(|c| c.strategies.progressive_threshold_floor).unwrap_or(10.0),
                watcher,
                gmgn: solagent_data::GmgnClient::new(),
                runtime_config: solagent_signals::RuntimeConfig::new(
                    solagent_signals::SignalWeights::default(),
                    confluence_threshold,
                    max_pos_size,
                    max_open_pos,
                    daily_loss,
                ),
                auto_tuner: None,
            }
            };

            // Load watched wallets from registry into the watcher.
            if wallet_registry {
                let registry = solagent_portfolio::WalletRegistry::new(pool);
                let wallets = registry.list_wallets(
                    Some(solagent_portfolio::WalletLabel::SmartMoney),
                    None,
                    50,
                ).await.unwrap_or_default();
                if let Some(ref _w) = subsystems.watcher {
                    let watched: Vec<solagent_data::WatchedWallet> = wallets.iter().map(|w| {
                        solagent_data::WatchedWallet {
                            address: w.address.clone(),
                            chain: w.chain,
                            score: w.score,
                        }
                    }).collect();
                    let count = watched.len();
                    if let Err(e) = subsystems.watcher.as_ref().unwrap().set_watched_wallets(watched).await {
                        tracing::warn!(error = %e, "Failed to load wallets into watcher");
                    } else {
                        tracing::info!(count, "Loaded smart money wallets into watcher");
                    }
                }
            }

            let agent_config = solagent_agent::AgentConfig::default();
            let agent = solagent_agent::Agent::new(agent_config, subsystems);

            println!("\nAgent initialized. Press Ctrl+C to stop.\n");

            // Graceful shutdown on Ctrl+C.
            let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let r = running.clone();
            ctrlc::set_handler(move || {
                println!("\nShutdown requested...");
                r.store(false, std::sync::atomic::Ordering::SeqCst);
            })?;

            if let Err(e) = agent.run().await {
                tracing::error!(error = %e, "Agent loop failed");
            }
        }

        // ─── Config ──────────────────────────────────────────────────────
        Commands::Config { action } => match action {
            ConfigAction::Show => {
                if let Some(ref cfg) = config {
                    println!("{}", toml::to_string_pretty(cfg)?);
                } else {
                    println!("No configuration loaded.");
                }
            }
            ConfigAction::Validate => {
                if config.is_some() { println!("Configuration is valid."); }
                else { println!("Configuration is invalid or missing."); }
            }
            ConfigAction::Init { output } => {
                let default_config = generate_default_config();
                let output_path = std::path::Path::new(&output);
                if let Some(parent) = output_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(output_path, &default_config)?;
                println!("Generated default config at: {output}");
            }
        },

        // ─── DB ──────────────────────────────────────────────────────────
        Commands::Db { action } => match action {
            DbAction::Migrate => {
                println!("Running database migrations...");
                let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
                drop(pool);
                println!("Migrations complete.");
            }
            DbAction::Reset => {
                println!("Database reset requested. To reset, delete the file: solagent.db");
            }
            DbAction::Stats => {
                let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
                let registry = solagent_portfolio::WalletRegistry::new(pool.clone());
                let pm = solagent_portfolio::PortfolioManager::new(pool);

                let wallet_count = registry.count().await?;
                let blacklist_count = registry.blacklist_count().await?;
                let positions = pm.get_open_positions().await?;
                let pnl = pm.get_pnl().await?;

                println!("Database Statistics:");
                println!("  Registered wallets:  {}", wallet_count);
                println!("  Blacklisted devs:    {}", blacklist_count);
                println!("  Open positions:      {}", positions.len());
                println!("  Total trades:        {}", pnl.total_trades);
                println!("  Total PnL:           ${:.2}", pnl.total_pnl);
            }
            DbAction::EvalStats => {
                let pool = solagent_portfolio::db::init_pool(DEFAULT_DB_PATH).await?;
                let pm = solagent_portfolio::PortfolioManager::new(pool);

                let stats = pm.get_eval_stats().await?;

                if stats.total_evaluations == 0 {
                    println!("No evaluations recorded yet.");
                    return Ok(());
                }

                println!("Evaluation Statistics:");
                println!("  Total evaluations:   {}", stats.total_evaluations);
                println!("  Passed:              {} ({:.1}%)", stats.passed_evaluations, stats.pass_rate * 100.0);
                println!("  Failed:              {}", stats.failed_evaluations);
                println!("  Avg confluence:      {:.1}", stats.avg_confluence_score);
                println!("  Avg safety:          {:.1}", stats.avg_safety_score);

                if !stats.top_tokens.is_empty() {
                    println!("\n  Top-scoring tokens:");
                    println!("  {:<45} {:>10} {:>8}", "Token", "Avg Conf", "Evals");
                    println!("  {}", "-".repeat(65));
                    for (addr, avg, count) in &stats.top_tokens {
                        let display_addr = if addr.len() > 44 {
                            format!("{}...{}", &addr[..20], &addr[addr.len()-20..])
                        } else {
                            addr.clone()
                        };
                        println!("  {:<45} {:>10.1} {:>8}", display_addr, avg, count);
                    }
                }
            }
        },
    }

    Ok(())
}

fn generate_default_config() -> String {
    r#"[agent]
name = "solagent"
poll_interval_secs = 30
log_level = "info"

[chains]
[chains.solana]
rpc_urls = ["https://api.mainnet-beta.solana.com"]
ws_url = "wss://api.mainnet-beta.solana.com"
helius_api_key = ""
private_key_bs58 = ""

[risk]
max_position_size_usd = 500.0
max_portfolio_risk_pct = 10.0
max_daily_loss_usd = 200.0
max_drawdown_pct = 10.0
max_open_positions = 10
default_stop_loss_pct = 20.0
default_take_profit_pct = 50.0
cooldown_secs = 300
safety_score_threshold = 60

[strategies]
active_strategies = ["whale_consensus", "accumulation", "launch_momentum", "volume_spike", "social"]
confluence_threshold = 65.0
min_signal_count = 2

[data]
jupiter_api_url = "https://quote-api.jup.ag/v6"
dexscreener_base_url = "https://api.dexscreener.com"
birdeye_base_url = "https://public-api.birdeye.so"
"#.to_string()
}
