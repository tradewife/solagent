<coding_guidelines>
# SolAgent — Autonomous Solana Trading Agent

## What This Is
A Rust-based autonomous trading agent that scans Solana for token opportunities, evaluates them through a multi-signal confluence engine, runs safety checks against live Birdeye data, manages risk with institutional-grade controls, and executes trades via Jupiter V6. Runs 24/7 under Hermes cron with offline resilience (SL/TP/trailing stops baked in). Smart money wallets sourced from GMGN API. Full context in `SESSION-CONTEXT.md`.

## Architecture
```
DexScreener/Birdeye/Helius/GMGN --> Data Pipeline --> Signal Engine (5 signals)
                                                    --> Confluence Scorer
                                                    --> Safety Evaluator (8 checks)
                                                    --> Risk Manager (position sizing, drawdown, circuit breaker)
                                                    --> Execution Engine (Jupiter V6 swap)
                                                    --> Portfolio Manager (SQLite positions + PnL)
```

## Key Commands
```bash
# Scan for new tokens
solagent --config config/local.toml scan --chain solana

# Analyze a specific token (DexScreener + Birdeye + safety score)
solagent --config config/local.toml analyze <TOKEN_CA> --chain solana -v

# Run safety check only
solagent --config config/local.toml safety <TOKEN_CA> [--deployer <ADDR>]

# Portfolio management
solagent --config config/local.toml portfolio summary
solagent --config config/local.toml portfolio positions
solagent --config config/local.toml portfolio pnl --days 7
solagent --config config/local.toml portfolio performance
solagent --config config/local.toml portfolio signal-report
solagent --config config/local.toml portfolio sync --address <WALLET>

# Wallet registry (smart money tracking)
solagent --config config/local.toml wallet add <ADDR> --label smart_money
solagent --config config/local.toml wallet list
solagent --config config/local.toml wallet blacklist-add <DEV_ADDR> --reason "known rugger"

# Database
solagent --config config/local.toml db migrate
solagent --config config/local.toml db stats
solagent --config config/local.toml db eval-stats

# Start autonomous agent
./scripts/run-agent.sh              # live mode
./scripts/run-agent.sh --dry-run    # dry run (no real trades)

# Seed smart money wallets from GMGN
./scripts/seed-wallets.sh           # pull fresh SM + KOL wallets
./scripts/seed-wallets.sh --dry-run # preview only

# GMGN CLI (smart money research)
gmgn-cli track smartmoney --chain sol --limit 100 --raw
gmgn-cli track kol --chain sol --limit 100 --raw
gmgn-cli portfolio stats --chain sol --wallet <ADDR> --raw
gmgn-cli token info --chain sol --address <TOKEN_CA>

# Config
solagent --config config/local.toml config show
```

## Current Risk Parameters (tuned for < 1 SOL wallet)
- Position size: $15 max per trade
- Max per token: 25% of portfolio
- Max open positions: 3
- Daily loss limit: $15 (halts trading)
- Drawdown circuit breaker: 15% from peak (halts agent)
- Stop loss: -15% per position
- Take profit: +300% per position
- Trailing stop: -15% from peak
- Safety score threshold: 60/100 minimum to trade
- Cooldown: 5 min after any loss
- Confluence progressive floor: 25 (minimum threshold after progressive lowering)
- Position sizing: dynamic by confluence score + win rate, capped by available SOL cash (not total portfolio)

## Offline Resilience
Every position opened by the agent gets automatic SL/TP/trailing stop levels persisted to SQLite. The monitor loop checks all open positions every 60 seconds against current prices (fetched from Birdeye). If the agent crashes or the machine reboots:
1. Hermes cron watchdog restarts the agent within 5 minutes
2. On restart, the agent resumes monitoring all open positions
3. Stop losses execute automatically — no manual intervention needed
4. Circuit breaker state persists across restarts via daily PnL tracking

## Emergency Procedures
**Halt the agent immediately:**
```bash
pkill -f solagent-cli  # Kill the running agent
```

**Close all positions manually:**
```bash
# List positions, then close each one
solagent --config config/local.toml portfolio positions
solagent --config config/local.toml trade sell <TOKEN_CA> --amount 100
```

**Reset circuit breaker:**
The circuit breaker resets automatically on agent restart (daily PnL counter resets at midnight UTC).

## API Keys
- `BIRDEYE_API_KEY` — Required. Free at birdeye.so. Used for safety scoring, token prices, holder analysis.
- `HELIUS_API_KEY` — Optional. Free at dev.helius.xyz. Used for wallet transaction parsing and faster RPC.
- `GMGN_API_KEY` — Required for wallet seeding. Free at gmgn.ai/ai. Used for smart money/KOL wallet discovery and profiling. Stored in `~/.config/gmgn/.env`.
- `ZERION_API_KEY` — Optional. Free at dashboard.zerion.io (60K calls/mo). Used for wallet portfolio, positions, and FIFO PnL data.
- DexScreener and Jupiter need no keys.

## Configuration
- `config/default.toml` — Default params (checked in to git)
- `config/local.toml` — Your keys and overrides (gitignored)
- `.env` — API keys (gitignored)
- `~/.config/gmgn/.env` — GMGN API key + Ed25519 private key for auth

## GMGN Integration
GMGN provides the smart money wallet data that feeds the Whale Consensus signal.
- **6 skills installed** at `.agents/skills/`: gmgn-track, gmgn-portfolio, gmgn-token, gmgn-market, gmgn-swap, gmgn-cooking
- **gmgn-cli v1.3.0** installed globally — used for wallet discovery, profiling, and token research
- **Wallet seeding**: `scripts/seed-wallets.sh` pulls active smart money + KOL wallets, profiles their stats, and upserts into SolAgent's SQLite registry
- **Registry**: 55 wallets seeded (26 smart_money + 15 KOL + extras), scored by composite formula (win_rate*0.3 + pnl*0.3 + consistency*0.2 + recency*0.2)

## Wallet Watcher
- Polls Helius for watched wallet transactions every 60 seconds
- Staggered at 1.5s per wallet to stay within Helius free tier (~1 req/sec)
- Max 20 wallets polled concurrently (top 20 by composite score from registry)
- Emits `WalletBuy`/`WalletSell` events on the EventBus
- These events feed the WhaleConsensusSignal

## Signal Engine
1. **Whale Consensus** (weight 0.30) — Multiple smart money wallets buy same token within 1 hour
2. **Accumulation** (weight 0.20) — Holder growth vs flat price = accumulation phase
3. **Launch Momentum** (weight 0.20) — New token with rapid holder + volume growth
4. **Volume Spike** (weight 0.15) — 3x+ average volume in rolling window
5. **Social** (weight 0.15) — Twitter mention velocity + engagement

Confluence threshold: 35/100 weighted composite required to trigger evaluation (progressive floor: 25).

## Safety Checks (8 per token)
1. Mint authority revoked (15 pts)
2. Freeze authority revoked (10 pts)
3. LP lock status (0-20 pts)
4. Top 10 holder concentration < 20% (0-15 pts)
5. Dev wallet clean / not blacklisted (0-15 pts)
6. Dev holdings < 5% (0-10 pts)
7. Not a honeypot (0-15 pts)
8. Buy/sell tax < 5% (0-10 pts)
Threshold: 70/100 to proceed.
</coding_guidelines>
