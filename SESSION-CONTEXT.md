# SolAgent — Session Context

## What This Is

Autonomous Solana trading agent in Rust. Scans DexScreener for opportunities, evaluates through 5-signal confluence engine with 8-point Birdeye safety checks, executes via Jupiter V6, manages risk with institutional-grade controls. Smart money wallets sourced from GMGN. Runs 24/7 with offline resilience.

**All 9 phases + mission complete. Zero `todo!()` in codebase. Release build passes clean with 0 warnings. 144 tests pass. 55 smart money wallets seeded. Zerion API integrated.**

## Build Status

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Repo Audit & Recon | DONE |
| 1 | Core Infrastructure | DONE |
| 2 | Data Pipeline & Wallet Intelligence | DONE |
| 3 | Signal Engine (all 5 signals) | DONE |
| 4 | Safety & Risk Layer | DONE |
| 5 | Execution Engine | DONE |
| 6 | Agent Loop, CLI & Skills | DONE |
| 7 | Deployment & Hardening (local) | DONE |
| 8 | GMGN Integration & Wallet Seeding | DONE |

## Build & Run

```bash
cd /home/kt/solagent
cargo build --release                              # zero warnings, zero errors
./target/release/solagent-cli --config config/local.toml db migrate
./target/release/solagent-cli --config config/local.toml scan --chain solana
./target/release/solagent-cli --config config/local.toml analyze <CA> --chain solana -v
./scripts/seed-wallets.sh                          # refresh GMGN smart money wallets
./scripts/run-agent.sh --dry-run                   # test without real trades
./scripts/run-agent.sh                             # live trading
```

## Key Config

| File | Purpose |
|------|---------|
| `config/default.toml` | Default risk/signal/safety params (checked in) |
| `config/local.toml` | Private key + API key overrides (gitignored) |
| `.env` | `BIRDEYE_API_KEY`, `HELIUS_API_KEY` (gitignored) |
| `~/.config/gmgn/.env` | GMGN API key + Ed25519 private key |

### API Keys

| Key | Required | Source | Used For |
|-----|----------|--------|----------|
| `BIRDEYE_API_KEY` | Yes | birdeye.so | Safety scoring, token prices, holder analysis |
| `HELIUS_API_KEY` | Yes | dev.helius.xyz | Wallet transaction parsing, RPC |
| `GMGN_API_KEY` | Yes | gmgn.ai/ai | Smart money/KOL wallet discovery and profiling |
| `ZERION_API_KEY` | Optional | dashboard.zerion.io | Wallet portfolio, positions, PnL, prices (free: 60K calls/mo) |
| DexScreener | No key | — | New pair scanning |
| Jupiter | No key | — | Swap execution |

## Architecture

```
crates/
├── solagent-core/           # Config, error types, Event enum, Chain enum, EventBus
├── solagent-data/           # API clients + WalletWatcher
│   ├── http.rs              # Rate-limited HTTP client (reqwest + governor)
│   ├── dexscreener.rs       # DexScreener client
│   ├── birdeye.rs           # Birdeye client
│   ├── helius.rs            # Helius client (typed SwapEvent, TokenTransfer, etc.)
│   ├── jupiter.rs           # Jupiter V6 client (quote + swap transaction)
│   ├── zerion.rs            # Zerion API client (portfolio, positions, PnL, prices)
│   └── watcher.rs           # WalletWatcher (Helius polling, 1.5s stagger, EventBus events)
├── solagent-chain-solana/   # Solana RPC pool, keypair mgmt, pump.fun parsing
├── solagent-chain-base/     # Base/alloy provider (stubs — lower priority)
├── solagent-signals/        # All 5 signals + ConfluenceScorer + RegistryScoreCache
├── solagent-safety/         # 8-point safety scoring with live Birdeye data + dev blacklist
├── solagent-risk/           # Position sizing, dynamic exit profiles, drawdown, circuit breaker
├── solagent-exec/           # Jupiter V6 execution with retry + pre-flight checks
├── solagent-portfolio/      # SQLite wallet registry + portfolio manager
├── solagent-agent/          # Autonomous agent loop + state machine + auto-tuner
└── solagent-cli/            # CLI binary with all commands
```

### Data Flow

```
DexScreener ──┐
Birdeye ──────┤──> Signal Engine ──> Confluence Scorer ──> Safety Check ──> Risk Manager ──> Execution (Jupiter)
Helius ───────┤                                                                              │
GMGN ─────────┘                                                                              ▼
                                                                               Portfolio Manager (SQLite)
```

### Agent State Machine

```
Scanning → Evaluating → RiskCheck → Executing → Monitoring
    ↑          ↓            ↓           ↓           │
    └──────────┴────────────┴───────────┴───────────┘
```

## Key Technical Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | Rust | Max speed, memory safety, single binary |
| Async runtime | tokio | Standard Rust async |
| Database | SQLite | sqlx, file-based, zero ops |
| API clients | reqwest + governor | Battle-tested HTTP + rate limiting |
| CLI framework | clap v4 | Standard Rust CLI |
| Logging | tracing | Structured, async-aware |
| Chain abstraction | Trait-based | `ChainProvider` trait with Solana and Base impls |
| Strategy pattern | Trait-based | `Strategy` trait with `evaluate() -> Signal` |

---

## What's Done (Phases 0-8)

### Phase 0: Audit
Repos in `audit/`. Key grades:
- GMGN Skills: A-/A — USE
- 1fge/pump-fun-sniper (Go): B+/A- — REFERENCE (pump.fun IDL, Jito MEV)
- Arkham skill: A-/A — INTEGRATE
- TreeCityWes: REJECTED (sends keys to 3rd party)
- SolanaWhaleAlert: REJECTED (non-functional)

### Phase 1: Core Infrastructure
- TOML config system: `config/default.toml` + `config/local.toml` (gitignored)
- Unified `SolAgentError` with chain-specific variants
- Event bus: `tokio::sync::broadcast` based
- SQLite via sqlx: wallets, dev_blacklist, positions, trades, snapshots tables

### Phase 2: Data Pipeline
- **DexScreener**: new pairs, token search, pair data (no key)
- **Birdeye**: token price, security, top holders, top traders (needs key)
- **Helius**: parsed transactions, balances (needs key)
- **Jupiter V6**: quote + swap construction (no key)
- **GMGN CLI**: smart money tracking, wallet profiling, token research (needs key)
- **WalletWatcher**: polls Helius for watched wallets, emits WalletBuy/WalletSell events

### Phase 3: Signal Engine (all 5)
1. **WhaleConsensusSignal** (w=0.30): sliding window, wallet-quality-weighted, event-driven
2. **AccumulationSignal** (w=0.20): holder growth vs flat price
3. **LaunchMomentumSignal** (w=0.20): new launch volume/holder spike
4. **VolumeSpikeSignal** (w=0.15): 3x+ average over rolling window
5. **SocialSignal** (w=0.15): twitter-cli mention velocity

Confluence threshold: 65/100 weighted composite required to trigger evaluation.

### Phase 4: Safety & Risk
- **8-point safety scoring** with live Birdeye data: mint authority (15pts), freeze authority (15pts), LP lock (20pts), holder concentration (15pts), dev blacklist (15pts), dev holdings (10pts), honeypot (15pts), tax (10pts). Threshold: 70/100.
- **Risk Manager**: position sizing, max positions, daily loss limit, drawdown circuit breaker, cooldown, dynamic exit profiles (moonbag/runner/swing/conservative by mcap/age/confluence)

### Phase 5: Execution Engine
- Jupiter V6: quote → swap tx → sign → send
- Solana RPC pool with round-robin failover
- 3 retries with +50bps slippage increase per retry
- Pre-flight checks: balance, provider availability
- Execution quality tracking: slippage, latency, success rate

### Phase 6: Agent Loop & CLI
- State machine: Scanning → Evaluating → RiskCheck → Executing → Monitoring
- Full wiring: scan (DexScreener) → evaluate (Birdeye safety) → risk check → execute (Jupiter) → monitor (trailing stops)
- CLI: `scan`, `analyze`, `safety`, `portfolio`, `wallet`, `db`, `agent [--dry-run]`

### Phase 7: Deployment (local)
- `scripts/run-agent.sh`: auto-restart with backoff, log rotation, dry-run support
- `scripts/monitor.sh`: position health check (for cron)
- `scripts/daily-report.sh`: daily PnL report (for cron)
- Offline resilience: SL/TP/trailing stops persisted, survive restarts

### Phase 8: GMGN Integration & Wallet Seeding
- **6 GMGN skills** at `.agents/skills/`: track, portfolio, token, market, swap, cooking
- **gmgn-cli v1.3.0** installed globally, Ed25519 keypair auth at `~/.config/gmgn/.env`
- **55 wallets seeded** from live GMGN smart money + KOL feeds
  - 26 `smart_degen` + 15 KOL (`renowned`) + 14 overlap
  - Composite scoring: `win_rate*0.3 + pnl_norm*0.3 + consistency*0.2 + recency*0.2` (0-100)
  - Top: RaVenxw8... (score=81.9, wr=0.79, pnl=$60K), 4BdKaxN8... (score=70.4, wr=0.71, pnl=$30K)
- **Wallet watcher tuned** for Helius free tier: 20 wallets max, 1.5s stagger, 60s poll interval
- **Seed script**: `scripts/seed-wallets.sh` for periodic refresh
- **Dry run verified**: agent starts, loads 20 wallets, scans 43 tokens, evaluates safety+confluence

---

## Signal Engine Detail

| Signal | Weight | Trigger | Data Source |
|--------|--------|---------|-------------|
| Whale Consensus | 0.30 | 2+ smart money buy same token in 30 min | Helius watcher + GMGN wallet registry |
| Accumulation | 0.20 | Holder growth + price flat | Birdeye holder API |
| Launch Momentum | 0.20 | New token with rapid holder + volume growth | DexScreener new pairs |
| Volume Spike | 0.15 | 3x+ average volume in rolling window | DexScreener pair data |
| Social | 0.15 | Twitter mention velocity | twitter-cli |

Confluence = weighted sum. Threshold: 65/100.

## Safety Checks Detail

| Check | Points | Method |
|-------|--------|--------|
| Mint authority revoked | 15 | Birdeye security API |
| Freeze authority revoked | 15 | Birdeye security API |
| LP lock status | 0-20 | Birdeye security API |
| Top 10 holder concentration < 20% | 0-15 | Birdeye holder API |
| Dev wallet not blacklisted | 0-15 | SQLite dev_blacklist |
| Dev holdings < 5% | 0-10 | Birdeye security API |
| Not a honeypot | 0-15 | Birdeye security API |
| Buy/sell tax < 5% | 0-10 | Birdeye security API |

Threshold: 70/100 to proceed.

## Risk Parameters (tuned for < 1 SOL wallet)

| Parameter | Value |
|-----------|-------|
| Max position size | $15 per trade |
| Max per token | 25% of portfolio |
| Max open positions | 3 |
| Daily loss limit | $15 (halts trading) |
| Drawdown breaker | 15% from peak (halts agent) |
| Stop loss | -15% per position |
| Take profit | +40% per position |
| Trailing stop | -8% from peak |
| Safety threshold | 70/100 |
| Confluence threshold | 65/100 |
| Cooldown | 5 min after any loss |
| Exit profiles | Dynamic: moonbag/runner/swing/conservative by mcap/age/confluence |

## API Budget

| API | Free Limit | Usage | Rate Strategy |
|-----|-----------|-------|---------------|
| DexScreener | 300 req/min | New pair scanning | Poll every 30s, cache |
| Birdeye | ~1 req/sec | Safety + prices | Cache 30s |
| Jupiter | Unlimited | Swap execution | Use freely |
| Helius | 1M credits/mo | Wallet monitoring | 20 wallets × 1.5s stagger × 60s cycle |
| GMGN | 20 req/sec | Wallet discovery/profiling | Used during seeding only |
| Zerion | 60K calls/mo (2K/day) | Portfolio, PnL, positions | 8 RPS, ~20 calls/day for sync + tune |

## External Tools

- **gmgn-cli v1.3.0**: globally installed — `gmgn-cli track smartmoney --chain sol`
- **twitter-cli**: `/home/kt/twitter-cli/` — `cd /home/kt/twitter-cli && uv run twitter <cmd>`

## Git

- Repo at `/home/kt/solagent/`, on `main` branch
- Latest: `fix: circuit breaker stuck HALTED from phantom DRPY PnL` + Zerion integration

## Remaining Gaps (nice-to-have, not blocking)

- `solagent-chain-base`: all stubs (Base/Uniswap not a priority)
- CLI `trade buy/sell`: prints info only (agent handles execution)
- Telegram alerts not implemented
- Railway/Docker deployment not set up (local-only)

## Mission Completion (May 2025)

All 15 mission features completed and verified:
- **Milestone 1 (First Blood)**: All 8 features done — dead signals fixed, first live trades executed
- **Milestone 2 (Self-Tuning)**: All 7 features done — runtime-mutable config, auto-tuner (11 tests), dynamic sizing (30+ tests), graceful degradation, monitor loop validation
- **37/37 validation assertions passed** with unit test + log analysis evidence
- **144 tests pass, 0 failures, 0 clippy warnings**

## Zerion API Integration

- **Client**: `crates/solagent-data/src/zerion.rs` — HTTP Basic Auth, 8 RPS rate limiting
- **Endpoints**: portfolio overview, token positions, FIFO PnL (realized/unrealized/ROI)
- **Auto-tuner**: logs Zerion PnL cross-check during each tuning cycle (optional)
- **CLI**: `solagent portfolio sync --address <WALLET>` — fetch portfolio + positions + PnL
- **Config**: `ZERION_API_KEY` env var or `[data] zerion_api_key` in config/local.toml
- **Free tier**: 60K calls/mo, 10 RPS — budget ~20 calls/day for daily sync + tune cycles
