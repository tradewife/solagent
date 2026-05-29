# SolAgent

Autonomous Solana trading agent built in Rust. Scans for opportunities via DexScreener, evaluates through a 6-signal confluence engine with hot-token tracker for multi-cycle data and per-signal reasoning, runs 8-point safety checks against live Birdeye data, executes via Jupiter V6 with Helius Smart Transaction Sender, and manages risk with institutional-grade controls. Smart money wallets sourced from GMGN. WebSocket-first wallet monitoring. Runs 24/7 with offline resilience.

## Key Features

- **6-signal confluence engine**: Whale Consensus, Behavioral, Accumulation, Launch Momentum, Volume Spike, Social — with weighted scoring and per-signal diagnostic reasoning
- **Hot-token tracker**: Persists signal data across scan cycles for longitudinal accumulation/volume analysis
- **WebSocket-first wallet monitoring**: Real-time transaction streaming via Helius SDK with automatic polling fallback (~80% fewer API calls than polling)
- **Helius Smart Transaction Sender**: Priority fee estimation for reliable swap execution
- **Helius credit budget tracking**: Persistent SQLite tracking of API credit usage
- **8-point safety scoring**: Mint/freeze authority, LP lock, holder concentration, dev blacklist, honeypot, tax checks
- **Behavioral intelligence scanner**: 5-layer algorithm classifying wallets into PRECOGNITIVE/SOVEREIGN/EMERGING/NOISE tiers
- **Dynamic risk management**: Position sizing by confluence + win rate, circuit breaker, trailing stops, exponential backoff
- **`solagent status` CLI**: Structured health snapshot of agent, positions, and API budgets
- **300 tests passing**: Comprehensive coverage across all crates

## Quick Start

### Prerequisites
- Rust 1.85+ (`rustup default stable`)
- Node.js 18+ (for gmgn-cli)
- Birdeye API key (free: https://bds.birdeye.so/)
- Helius API key (free: https://dev.helius.xyz/)
- GMGN API key (free: https://gmgn.ai/ai)
- Funded Solana wallet (private key in base58)

### Setup

```bash
git clone <repo> && cd solagent

# 1. Configure environment
cp .env.example .env
# Edit .env: add your BIRDEYE_API_KEY, HELIUS_API_KEY

# 2. Configure wallet
cp config/local.toml.example config/local.toml
# Edit config/local.toml: add your private_key_bs58

# 3. Build release binary
cargo build --release -p solagent-cli

# 4. Initialize database
./target/release/solagent-cli --config config/local.toml db migrate

# 5. Seed smart money wallets from GMGN
./scripts/seed-wallets.sh

# 6. Test with dry run
./scripts/run-agent.sh --dry-run
```

### Run Live

```bash
# Direct
./target/release/solagent-cli --config config/local.toml agent

# With auto-restart and logging
./scripts/run-agent.sh

# Check agent health
./target/release/solagent-cli --config config/local.toml status
```

## CLI Commands

```
solagent status                                       # Health snapshot
solagent scan [--chain solana] [--min-liquidity 1000] # Scan for tokens
solagent analyze <TOKEN_CA> [-v]                      # Deep analyze a token
solagent safety <TOKEN_CA> [--deployer <ADDR>]        # Safety check only
solagent trade buy <TOKEN_CA> --amount <USD>          # Buy (info only)
solagent trade sell <TOKEN_CA> --amount <USD>         # Sell (info only)
solagent portfolio summary|positions|pnl|performance|signal-report
solagent portfolio sync --address <WALLET>            # Sync from Zerion
solagent wallet list|add|remove|blacklist-add
solagent agent [--dry-run]                            # Run autonomous agent
solagent config show|init|validate
solagent db migrate|stats|eval-stats
solagent behavioral-scan                              # Run behavioral scanner
```

## Architecture

```
solagent/
├── crates/
│   ├── solagent-core/         # Config, errors, events, EventBus
│   ├── solagent-data/         # DexScreener, Birdeye, Helius SDK v1.x, Jupiter, Zerion, WalletWatcher (WebSocket)
│   ├── solagent-chain-solana/ # Solana RPC (solana-sdk 3.0), keypair mgmt, pump.fun parsing
│   ├── solagent-signals/      # 6 signals + ConfluenceScorer + HotTokenTracker + BehavioralWalletCache
│   ├── solagent-behavioral/   # 5-layer behavioral intelligence scanner
│   ├── solagent-safety/       # 8-point safety scoring + dev blacklist
│   ├── solagent-risk/         # Position sizing, drawdown, circuit breaker, exit profiles
│   ├── solagent-exec/         # Jupiter V6 + Helius Smart Transaction Sender with retry
│   ├── solagent-portfolio/    # SQLite positions, trades, PnL, wallet registry, credit tracking
│   ├── solagent-agent/        # Autonomous agent loop + state machine + auto-tuner
│   └── solagent-cli/          # CLI binary with all commands
├── .agents/skills/            # GMGN skills (6 total)
├── config/
│   ├── default.toml           # Default risk/signal params
│   └── local.toml             # Your keys (gitignored)
├── scripts/
│   ├── run-agent.sh           # Agent runner with auto-restart
│   ├── seed-wallets.sh        # Pull GMGN smart money + seed registry
│   ├── monitor.sh             # Position health check (cron)
│   └── daily-report.sh        # Daily PnL report (cron)
└── solagent.db                # SQLite database
```

### Data Flow

```
DexScreener ──┐
Birdeye ──────┤──> Signal Engine (6) + HotTokenTracker ──> Confluence Scorer ──> Safety Check
Helius SDK ───┤       ↑                                                        │
GMGN ─────────┘       │                                                        ▼
                      ├── BehavioralWalletCache <── Behavioral Scanner    Risk Manager
                      └── WhaleConsensus quality weighting                     │
                                                                                   ▼
                                                        Execution Engine (Jupiter + Smart TX Sender)
                                                                                   │
                                                                                   ▼
                                                                 Portfolio Manager (SQLite)
```

### Agent State Machine

```
Scanning → Evaluating → RiskCheck → Executing → Monitoring
    ↑          ↓            ↓           ↓           │
    └──────────┴────────────┴───────────┴───────────┘
```

## Signal Weights

| Signal | Weight | Trigger |
|--------|--------|---------|
| Whale Consensus | 0.25 | 2+ smart money buy same token in 1 hour + GMGN fallback |
| Behavioral | 0.25 | SOVEREIGN/PRECOGNITIVE wallet in GMGN top traders |
| Accumulation | 0.15 | Holder growth + price flat (multi-cycle) |
| Launch Momentum | 0.15 | New token with rapid holder + volume growth |
| Volume Spike | 0.10 | 3x+ average volume (multi-cycle) |
| Social | 0.10 | Twitter mention velocity |

Confluence threshold: 35/100 (progressive floor: 25, absolute floor: 25.0).

## Risk Management

| Parameter | Value | Description |
|-----------|-------|-------------|
| Max position size | $5-$20 | Dynamic by confluence + win rate |
| Max per token | 25% | Portfolio concentration |
| Max open positions | 3 | Concurrent trades |
| Daily loss limit | $15 | Halts trading |
| Drawdown breaker | 15% | Halts agent |
| Stop loss | -15% | Per position |
| Take profit | +300% | Per position |
| Trailing stop | -15% | From peak |
| Safety threshold | 60/100 | Min to trade |
| Confluence threshold | 35/100 | Min to evaluate (floor: 25.0) |
| Cooldown | 5 min | After any loss |

## GMGN Integration

Smart money wallets are sourced from [GMGN](https://gmgn.ai) via their API:

```bash
# Discover active smart money wallets
gmgn-cli track smartmoney --chain sol --limit 100 --raw

# Discover KOL wallets
gmgn-cli track kol --chain sol --limit 100 --raw

# Profile a specific wallet (win rate, PnL, trade stats)
gmgn-cli portfolio stats --chain sol --wallet <ADDR> --raw

# Research a token (price, safety, holders, smart money exposure)
gmgn-cli token info --chain sol --address <TOKEN_CA>

# Seed SolAgent registry from GMGN
./scripts/seed-wallets.sh
```

6 GMGN skills installed at `.agents/skills/`: gmgn-track, gmgn-portfolio, gmgn-token, gmgn-market, gmgn-swap, gmgn-cooking.

## API Budget

| API | Free Limit | Usage |
|-----|-----------|-------|
| DexScreener | 300 req/min | New pair scanning |
| Birdeye | ~1 req/sec | Safety + prices |
| Jupiter | Unlimited | Swap execution |
| Helius | 1M credits/mo | WebSocket monitoring + Smart TX Sender + RPC (credit usage tracked) |
| GMGN | 20 req/sec | Smart money discovery + profiling |
| Zerion | 60K calls/mo | Portfolio, PnL, positions |

## Testing

```bash
# Run all 300 tests
cargo test --workspace

# Run with output
cargo test --workspace -- --nocapture

# Run specific crate
cargo test -p solagent-signals
```

300 tests passing across all crates with 0 failures.

## License

MIT
