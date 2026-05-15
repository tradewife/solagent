# SolAgent

Autonomous Solana trading agent built in Rust. Scans for opportunities via DexScreener, evaluates through a 5-signal confluence engine with 8-point safety checks, executes via Jupiter V6, and manages risk with institutional-grade controls. Smart money wallets sourced from GMGN. Runs 24/7 with offline resilience.

## Quick Start

### Prerequisites
- Rust 1.85+ (`rustup default stable`)
- Node.js 18+ (for gmgn-cli)
- Birdeye API key (free: https://bds.birdeye.so/)
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
```

## CLI Commands

```
solagent scan [--chain solana] [--min-liquidity 1000] [--max-age-mins 60]
solagent analyze <TOKEN_CA> [--chain solana] [-v]
solagent safety <TOKEN_CA> [--deployer <ADDR>]
solagent trade buy <TOKEN_CA> --amount <USD>
solagent trade sell <TOKEN_CA> --amount <USD>
solagent portfolio summary|positions|pnl
solagent wallet list|add|remove|blacklist|blacklist-add
solagent agent [--dry-run]
solagent config show|init|validate
solagent db migrate|stats
```

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
gmgn-cli token security --chain sol --address <TOKEN_CA>

# Seed SolAgent registry from GMGN
./scripts/seed-wallets.sh
```

6 GMGN skills installed at `.agents/skills/`: gmgn-track, gmgn-portfolio, gmgn-token, gmgn-market, gmgn-swap, gmgn-cooking.

## Architecture

```
solagent/
├── crates/
│   ├── solagent-core/        # Config, errors, events, EventBus
│   ├── solagent-data/        # DexScreener, Birdeye, Helius, Jupiter clients
│   ├── solagent-chain-solana/ # Solana RPC, keypair mgmt, pump.fun parsing
│   ├── solagent-signals/     # 5 signal detectors + ConfluenceScorer
│   ├── solagent-safety/      # 8-point safety scoring + dev blacklist
│   ├── solagent-risk/        # Position sizing, drawdown, circuit breaker
│   ├── solagent-exec/        # Jupiter V6 execution with retry
│   ├── solagent-portfolio/   # SQLite positions, trades, PnL, wallet registry
│   ├── solagent-agent/       # Autonomous agent loop + state machine
│   └── solagent-cli/         # CLI binary
├── .agents/skills/           # GMGN skills (6 total)
├── config/
│   ├── default.toml          # Default risk/signal params
│   └── local.toml            # Your keys (gitignored)
├── scripts/
│   ├── run-agent.sh          # Agent runner with auto-restart
│   ├── seed-wallets.sh       # Pull GMGN smart money + seed registry
│   ├── monitor.sh            # Position health check (cron)
│   └── daily-report.sh       # Daily PnL report (cron)
└── solagent.db               # SQLite database
```

### Data Flow

```
DexScreener ──┐
Birdeye ──────┤──> Signal Engine ──> Confluence Scorer ──> Safety Check
Helius ───────┤                                              │
GMGN ─────────┘                                              ▼
                                                     Risk Manager
                                                          │
                                                          ▼
                                              Execution Engine (Jupiter)
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

## Risk Management

| Parameter | Value | Description |
|-----------|-------|-------------|
| Max position size | $15 | Per-trade cap |
| Max per token | 25% | Portfolio concentration |
| Max open positions | 3 | Concurrent trades |
| Daily loss limit | $15 | Halts trading |
| Drawdown breaker | 15% | Halts agent |
| Stop loss | -15% | Per position |
| Take profit | +40% | Per position |
| Trailing stop | -8% | From peak |
| Safety threshold | 70/100 | Min to trade |
| Cooldown | 5 min | After any loss |

## Signal Weights

| Signal | Weight | Trigger |
|--------|--------|---------|
| Whale Consensus | 0.30 | 2+ smart money buy same token in 30 min |
| Accumulation | 0.20 | Holder growth + price flat |
| Launch Momentum | 0.20 | New token with rapid growth |
| Volume Spike | 0.15 | 3x+ average volume |
| Social | 0.15 | Twitter mention velocity |

Confluence threshold: 65/100 required to evaluate a trade.

## API Budget

| API | Free Limit | Usage |
|-----|-----------|-------|
| DexScreener | 300 req/min | New pair scanning |
| Birdeye | ~1 req/sec | Safety + prices |
| Jupiter | Unlimited | Swap execution |
| Helius | 1M credits/mo | Wallet monitoring |
| GMGN | 20 req/sec | Smart money discovery + profiling |

## License

MIT
