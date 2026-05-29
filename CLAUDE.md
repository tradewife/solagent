<coding_guidelines>
<coding_guidelines>
# SolAgent — Autonomous Solana Trading Agent

## What This Is
A Rust-based autonomous trading agent that scans Solana for token opportunities, evaluates them through a 6-signal confluence engine with hot-token tracker for multi-cycle data, runs safety checks against live Birdeye data, manages risk with institutional-grade controls, and executes trades via Jupiter V6 with Helius Smart Transaction Sender. Runs 24/7 under Hermes cron with offline resilience (SL/TP/trailing stops baked in). Smart money wallets sourced from GMGN API. WebSocket-first wallet monitoring with polling fallback. Full context in `SESSION-CONTEXT.md`.

## Architecture
```
DexScreener/Birdeye/Helius SDK/GMGN --> Data Pipeline --> Signal Engine (6 signals + HotTokenTracker)
                                                            --> Confluence Scorer (per-signal reasoning)
                                                            --> Safety Evaluator (8 checks)
                                                            --> Risk Manager (position sizing, drawdown, circuit breaker)
                                                            --> Execution Engine (Jupiter V6 + Helius Smart Transaction Sender)
                                                            --> Portfolio Manager (SQLite positions + PnL)

Helius SDK (WebSocket) --> WalletWatcher --> WalletBuy/WalletSell events
                                            --> WhaleConsensusSignal
Behavioral Scanner (4h cycle) --> BehavioralWalletCache --> BehavioralSignal
                                                       --> WhaleConsensusSignal (quality boost)
```

## Key Dependencies
- **solana-sdk 3.0** — Solana core SDK (upgraded from 2.2)
- **helius SDK v1.x** — Official Rust SDK replacing custom HTTP client; provides WebSocket wallet monitoring, parsed transactions, Smart Transaction Sender
- **spl-token v9** — SPL token operations (upgraded from v7)
- **Jupiter V6** — Swap execution with priority fee estimation
- **tokio** — Async runtime
- **sqlx + SQLite** — Persistent storage (positions, wallets, PnL, credit tracking)
- **reqwest + governor** — Rate-limited HTTP clients
- **clap v4** — CLI framework

## Key Commands
```bash
# Agent health snapshot
solagent --config config/local.toml status

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
- Position size: $15 max per trade (dynamic: $5-$20 by confluence + win rate)
- Max per token: 25% of portfolio
- Max open positions: 3
- Daily loss limit: $15 (halts trading)
- Drawdown circuit breaker: 15% from peak (halts agent)
- Stop loss: -15% per position
- Take profit: +300% per position
- Trailing stop: -15% from peak
- Safety score threshold: 60/100 minimum to trade
- Cooldown: 5 min after any loss
- Confluence threshold: 35/100 (progressive floor: 25)
- Effective threshold floor: 25.0 (absolute minimum, enforced)
- Position sizing: dynamic by confluence score + win rate, capped by available SOL cash (not total portfolio)

## Offline Resilience
Every position opened by the agent gets automatic SL/TP/trailing stop levels persisted to SQLite. The monitor loop checks all open positions every 60 seconds against current prices (fetched from Birdeye). If the agent crashes or the machine reboots:
1. Hermes cron watchdog restarts the agent within 5 minutes
2. On restart, the agent resumes monitoring all open positions
3. Stop losses execute automatically — no manual intervention needed
4. Circuit breaker state persists across restarts via daily PnL tracking

## Scan Loop
- Exponential backoff on errors (prevents API hammering during outages)
- SOL balance caching to avoid redundant RPC calls
- Hot-token tracker persists signal data across scan cycles for longitudinal analysis

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
- `HELIUS_API_KEY` — Required. Free at dev.helius.xyz. Used for WebSocket wallet monitoring, parsed transactions, Smart Transaction Sender, RPC. Credit usage tracked with persistence.
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

## Wallet Watcher (WebSocket-first)
- WebSocket connection to Helius for real-time wallet transaction streaming (replaced polling)
- Automatic fallback to HTTP polling if WebSocket disconnects
- Emits `WalletBuy`/`WalletSell` events on the EventBus
- These events feed the WhaleConsensusSignal
- Reduced Helius API credit consumption by ~80% vs polling

## Signal Engine (6 signals + HotTokenTracker)
1. **Whale Consensus** (weight 0.25) — Multiple smart money wallets buy same token within 1 hour. GMGN top-trader fallback when Helius is rate-limited.
2. **Behavioral** (weight 0.25) — SOVEREIGN/PRECOGNITIVE wallets from behavioral intelligence scanner detected in GMGN top traders per token.
3. **Accumulation** (weight 0.15) — Holder growth vs flat price = accumulation phase
4. **Launch Momentum** (weight 0.15) — New token with rapid holder + volume growth
5. **Volume Spike** (weight 0.10) — 3x+ average volume in rolling window
6. **Social** (weight 0.10) — Twitter mention velocity + engagement

**Hot-token tracker**: Persists token signal data across scan cycles, enabling longitudinal accumulation/volume analysis that was previously lost between cycles.

Confluence threshold: 35/100 weighted composite required to trigger evaluation (progressive floor: 25, absolute floor: 25.0).

Per-signal reasoning and diagnostic detail logged for each evaluation.

## Behavioral Intelligence Scanner
Runs as a background task every 4 hours. Discovers wallets with genuine edge across 5 detection layers:
1. **Inverse Loss Archaeology** (weight 0.20) — Statistical inverse of losing wallet patterns
2. **Liquidity Ghost Detection** (weight 0.25) — Exits before crashes across multiple tokens
3. **Irrational Conviction Scoring** (weight 0.20) — Early entry in 10x+ tokens before social amplification
4. **CTO Meta-Reader** (weight 0.20) — Profitable re-entry post-community takeover
5. **Consensus Deviation** (weight 0.15) — Profitable but methodologically unlike top 500

Wallets classified into tiers: PRECOGNITIVE (90-100), SOVEREIGN (75-89), EMERGING (55-74), NOISE (<55).
Red flags auto-disqualify: MEV/bot patterns, copy-trade clustering, KOL correlation >40%.

CLI command: `solagent --config config/local.toml behavioral-scan`

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

## Testing
- **300 tests passing** across all crates (0 failures)
- Unit tests for signal scoring, risk management, safety checks, behavioral layers, execution, portfolio
- Cross-area validation tests for weight normalization and signal interplay
- `cargo test --workspace` to run all

## Helius Credit Budget Tracking
- Persistent tracking of Helius API credit usage in SQLite
- Status reporting via `solagent status` CLI command
- Prevents unexpected credit exhaustion
</coding_guidelines>
</coding_guidelines>
