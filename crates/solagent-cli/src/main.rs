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
            println!("📊 Analyzing token: {address}");
            println!("   Chain: {chain}");
            println!("   Verbose: {verbose}");
            todo!("solagent-signals + solagent-safety: full analysis")
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
