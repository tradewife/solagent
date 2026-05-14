//! # solagent-cli
//!
//! Command-line interface for the SolAgent trading system.

use anyhow::Result;
use clap::{Parser, Subcommand};
use solagent_core::Config;

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

    /// Portfolio management
    Portfolio {
        #[command(subcommand)]
        action: PortfolioAction,
    },

    /// Wallet operations
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
    /// Show wallet balances
    Balance {
        /// Chain (solana, base, all)
        #[arg(short, long, default_value = "all")]
        chain: String,
    },
    /// Show transaction history
    History {
        /// Number of transactions to show
        #[arg(short, long, default_value = "20")]
        limit: usize,
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
        Commands::Scan {
            chain,
            min_liquidity,
            max_age_mins,
        } => {
            tracing::info!(chain, min_liquidity, max_age_mins, "Scanning for new tokens");

            let dex = solagent_data::DexScreenerClient::new(
                "https://api.dexscreener.com".to_string(),
                None,
            );

            let pairs = dex.get_new_pairs(&chain).await?;
            let cutoff_ms = (chrono::Utc::now() - chrono::Duration::minutes(max_age_mins as i64)).timestamp_millis();

            let filtered: Vec<_> = pairs
                .into_iter()
                .filter(|p| {
                    let liq = p.liquidity.as_ref().and_then(|l| l.usd).unwrap_or(0.0);
                    let age_ok = p.pair_created_at
                        .map(|t| t > cutoff_ms)
                        .unwrap_or(true);
                    liq >= min_liquidity && age_ok
                })
                .collect();

            if filtered.is_empty() {
                println!("No new tokens found matching criteria.");
                return Ok(());
            }

            println!("Found {} new token pairs on {}:\n", filtered.len(), chain);
            for (i, pair) in filtered.iter().enumerate() {
                let name = &pair.base_token.name;
                let symbol = &pair.base_token.symbol;
                let addr = &pair.base_token.address;
                let liq = pair.liquidity.as_ref().and_then(|l| l.usd).map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let mc = pair.market_cap.map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let vol = pair.volume.as_ref().and_then(|v| v.h24).map(|v| format!("${v:.0}")).unwrap_or("-".into());
                let boosts = pair.boosts.as_ref().and_then(|b| b.active).unwrap_or(0);
                let age = pair.pair_created_at
                    .map(|t| {
                        let created = chrono::DateTime::from_timestamp_millis(t).unwrap_or_default();
                        let mins = (chrono::Utc::now() - created).num_minutes();
                        format!("{mins}m ago")
                    })
                    .unwrap_or("-".into());

                println!("{:>3}. {} ({}) | MC: {} | Liq: {} | Vol24h: {} | Age: {} | Boosts: {}",
                    i + 1, symbol, &addr[..12], mc, liq, vol, age, boosts);
                println!("     Name: {} | DEX: {} | Address: {}", name, pair.dex_id, addr);
                if boosts > 0 {
                    println!("     *** BOOSTED ({}) ***", boosts);
                }
            }
        }

        Commands::Track { target, address } => {
            let target_str = match target {
                TrackTarget::Wallet => "wallet",
                TrackTarget::Token => "token",
            };
            println!("👁️  Tracking {target_str}: {address}");
            todo!("solagent-data: set up tracking for wallet/token")
        }

        Commands::Analyze {
            address,
            chain,
            verbose,
        } => {
            tracing::info!(%address, %chain, "Analyzing token");

            let dex = solagent_data::DexScreenerClient::new(
                "https://api.dexscreener.com".to_string(),
                None,
            );
            let birdeye = solagent_data::BirdeyeClient::new(
                "https://public-api.birdeye.so".to_string(),
                std::env::var("BIRDEYE_API_KEY").ok(),
            );

            // DexScreener pair data
            let pairs_result = dex.get_token_info(&address).await;
            let pair = match pairs_result {
                Ok(Some(p)) => p,
                Ok(None) => {
                    println!("Token not found on DexScreener.");
                    return Ok(());
                }
                Err(e) => {
                    println!("Error fetching token: {e}");
                    return Ok(());
                }
            };

            let symbol = &pair.base_token.symbol;
            let name = &pair.base_token.name;

            println!("=== {} ({}) ===", symbol, name);
            println!("Address:  {}", address);
            println!("DEX:      {}", pair.dex_id);
            if let Some(usd) = pair.price_usd.as_ref() {
                println!("Price:    ${usd}");
            }
            if let Some(mc) = pair.market_cap {
                println!("MC:       ${mc:.0}");
            }
            if let Some(fdv) = pair.fdv {
                println!("FDV:      ${fdv:.0}");
            }
            if let Some(liq) = pair.liquidity.as_ref().and_then(|l| l.usd) {
                println!("Liquidity: ${liq:.0}");
            }
            if let Some(vol) = pair.volume.as_ref().and_then(|v| v.h24) {
                println!("Vol 24h:  ${vol:.0}");
            }
            if let Some(change) = pair.price_change.as_ref().and_then(|c| c.h24) {
                let arrow = if change >= 0.0 { "+" } else { "" };
                println!("Change 24h: {arrow}{change:.2}%");
            }
            if let Some(txns) = pair.txns.as_ref() {
                println!("Txns 24h: {} buys / {} sells", txns.h24.buys, txns.h24.sells);
            }
            if let Some(boosts) = pair.boosts.as_ref().and_then(|b| b.active) {
                println!("Boosts:   {}", boosts);
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
                    let mint_auth = sec.mint_authority.as_deref().unwrap_or("REVOKED");
                    let freeze_auth = sec.freeze_authority.as_deref().unwrap_or("REVOKED");
                    let renounced = sec.renounced.unwrap_or(false);
                    println!("Mint Authority:   {}", if mint_auth.is_empty() { "REVOKED".to_string() } else { mint_auth.to_string() });
                    println!("Freeze Authority: {}", if freeze_auth.is_empty() { "REVOKED".to_string() } else { freeze_auth.to_string() });
                    println!("Renounced:        {}", renounced);
                    if let Some(honeypot) = sec.is_honeypot {
                        println!("Honeypot:         {}", if honeypot { "YES - DANGER" } else { "No" });
                    }
                    if let Some(buy_tax) = sec.buy_tax {
                        println!("Buy Tax:          {:.2}%", buy_tax * 100.0);
                    }
                    if let Some(sell_tax) = sec.sell_tax {
                        println!("Sell Tax:         {:.2}%", sell_tax * 100.0);
                    }
                }
                Err(e) => {
                    println!("Security data unavailable: {e}");
                    println!("(Set BIRDEYE_API_KEY env var for full security data)");
                }
            }

            // Birdeye holders
            if verbose {
                println!("\n--- Top Holders ---");
                match birdeye.get_top_holders(&address).await {
                    Ok(holders) => {
                        for (i, h) in holders.iter().take(10).enumerate() {
                            let owner = &h.owner;
                            let pct_display = format!("{:.2}%", h.pct);
                            println!("{:>3}. {} ... | {}", i + 1, &owner[..owner.len().min(20)], pct_display);
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
        }

        Commands::Trade {
            action,
            address,
            amount,
            chain,
            slippage_bps,
        } => {
            let action_str = match action {
                TradeAction::Buy => "BUY",
                TradeAction::Sell => "SELL",
            };
            println!("💰 {action_str} {address}");
            println!("   Amount: {amount}");
            println!("   Chain: {chain}");
            println!("   Slippage: {slippage_bps} bps");
            todo!("solagent-exec: execute trade")
        }

        Commands::Portfolio { action } => match action {
            PortfolioAction::Summary => {
                println!("📈 Portfolio Summary");
                todo!("solagent-portfolio: get portfolio summary")
            }
            PortfolioAction::Positions => {
                println!("📋 Open Positions");
                todo!("solagent-portfolio: list open positions")
            }
            PortfolioAction::Pnl { days } => {
                println!("💹 PnL History ({days} days)");
                todo!("solagent-portfolio: show PnL")
            }
            PortfolioAction::Close { id } => {
                println!("❌ Closing position: {id}");
                todo!("solagent-portfolio + solagent-exec: close position")
            }
        },

        Commands::Wallet { action } => match action {
            WalletAction::Balance { chain } => {
                println!("💳 Wallet Balance ({chain})");
                todo!("solagent-chain-*: get wallet balances")
            }
            WalletAction::History { limit } => {
                println!("📜 Transaction History (last {limit})");
                todo!("solagent-chain-*: get transaction history")
            }
        },

        Commands::Agent {
            strategy,
            dry_run,
        } => {
            println!("🤖 Starting SolAgent");
            println!("   Strategy: {strategy}");
            println!("   Dry run: {dry_run}");
            if dry_run {
                println!("   ⚠️  DRY RUN MODE — no real trades will execute");
            }
            todo!("solagent-agent: create and run agent")
        }

        Commands::Config { action } => match action {
            ConfigAction::Show => {
                if let Some(ref cfg) = config {
                    println!("{}", toml::to_string_pretty(cfg)?);
                } else {
                    println!("No configuration loaded.");
                }
            }
            ConfigAction::Validate => {
                if config.is_some() {
                    println!("✅ Configuration is valid.");
                } else {
                    println!("❌ Configuration is invalid or missing.");
                }
            }
            ConfigAction::Init { output } => {
                println!("📝 Generating default config at: {output}");
                todo!("Generate and write default config TOML")
            }
        },

        Commands::Db { action } => match action {
            DbAction::Migrate => {
                println!("🗄️  Running database migrations...");
                todo!("solagent-portfolio: run migrations")
            }
            DbAction::Reset => {
                println!("⚠️  Database reset requested. This will delete all data!");
                todo!("solagent-portfolio: confirm and reset database")
            }
            DbAction::Stats => {
                println!("📊 Database Statistics");
                todo!("solagent-portfolio: show DB stats")
            }
        },
    }

    Ok(())
}
