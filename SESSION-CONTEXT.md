# SolAgent — Session Context

## What This Is

Autonomous Solana trading agent in Rust. Scans DexScreener for opportunities, evaluates through 6-signal confluence engine with GMGN signal enrichment (+SM buy/KOL call/price surge) and hot-token tracker for multi-cycle data, runs 11-point Birdeye + GMGN safety checks (LP burn, dev track record, cross-validated security) with per-signal reasoning, executes via Jupiter V6 with Helius Smart Transaction Sender, manages risk with institutional-grade controls including smart money exit detection. Smart money wallets sourced from GMGN. WebSocket-first wallet monitoring with polling fallback. Runs 24/7 with offline resilience.

**All 4 mission milestones complete (stabilize, helius-integration, signal-revival, hardening) + GMGN deep integration. 25 features implemented. 300 tests passing (0 failures). Release build passes clean with 0 warnings. solana-sdk 3.0 + helius SDK v1.x integrated. WebSocket wallet monitoring operational. Smart Transaction Sender for execution. Hot-token tracker for multi-cycle signal data. GMGN-enriched safety, signals, and exit detection.**

## Build Status

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Repo Audit & Recon | DONE |
| 1 | Core Infrastructure | DONE |
| 2 | Data Pipeline & Wallet Intelligence | DONE |
| 3 | Signal Engine (all 6 signals + behavioral + HotTokenTracker) | DONE |
| 4 | Safety & Risk Layer | DONE |
| 5 | Execution Engine (Jupiter V6 + Smart Transaction Sender) | DONE |
| 6 | Agent Loop, CLI & Skills | DONE |
| 7 | Deployment & Hardening (local) | DONE |
| 8 | GMGN Integration & Wallet Seeding | DONE |
| 9 | GMGN Deep Integration (Safety + Signals + Exit Detection) | DONE |
| — | Mission: Stabilize | DONE |
| — | Mission: Helius Integration | DONE |
| — | Mission: Signal Revival | DONE |
| — | Mission: Hardening | DONE |

## Build & Run

```bash
cd /home/kt/solagent
cargo build --release                              # zero warnings, zero errors
./target/release/solagent-cli --config config/local.toml status        # health snapshot
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
| `HELIUS_API_KEY` | Yes | dev.helius.xyz | WebSocket wallet monitoring, parsed transactions, Smart Transaction Sender, RPC, credit tracking |
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
│   ├── gmgn.rs              # GMGN client (9 methods: token info, security, dev tokens, market signals, SM trades, wallet holdings, etc.)
│   ├── helius.rs            # Helius SDK v1.x (WebSocket monitoring, parsed tx, Smart Transaction Sender)
│   ├── jupiter.rs           # Jupiter V6 client (quote + swap transaction)
│   ├── zerion.rs            # Zerion API client (portfolio, positions, PnL, prices)
│   └── watcher.rs           # WalletWatcher (WebSocket-first, polling fallback, EventBus events)
├── solagent-chain-solana/   # Solana RPC pool, keypair mgmt, pump.fun parsing (solana-sdk 3.0)
├── solagent-chain-base/     # Base/alloy provider (stubs — lower priority)
├── solagent-signals/        # All 6 signals + ConfluenceScorer + RegistryScoreCache + BehavioralWalletCache + HotTokenTracker + GmgnSignalEnrichment
├── solagent-behavioral/     # 5-layer behavioral intelligence scanner (DexScreener + GMGN)
├── solagent-safety/         # 11-point safety scoring with Birdeye + GMGN data + dev blacklist + dev track record
├── solagent-risk/           # Position sizing, dynamic exit profiles, drawdown, circuit breaker
├── solagent-exec/           # Jupiter V6 execution with Helius Smart Transaction Sender + retry + pre-flight checks
├── solagent-portfolio/      # SQLite wallet registry + portfolio manager + Helius credit tracking
├── solagent-agent/          # Autonomous agent loop + state machine + auto-tuner + exponential backoff
└── solagent-cli/            # CLI binary with all commands including `status`
```

### Data Flow

```
DexScreener ──┐
Birdeye ──────┤──> Signal Engine (6) + HotTokenTracker ──> Confluence Scorer ──> Safety Check ──> Risk Manager ──> Execution (Jupiter + Smart TX Sender)
Helius SDK ───┤       ↑     ↑                                ↑ + GMGN boost         │ + GMGN checks         │                   │
GMGN ─────────┤       │     │                                │  (SM/KOL/surge)       │  (LP, dev, honeypot)   │                   │
              │       │     └── GmgnSignalEnrichment ────────┘                        │                       ▼                   ▼
              │       │          (refreshed each scan cycle)                          └── Dev Track Record    Portfolio Manager
              │       └── BehavioralWalletCache <── Behavioral Scanner (4h cycle)         (graduation rate,       (SQLite)
              │            WhaleConsensus quality weighting                                  best ATH)            + Helius credit tracking
              │                         │
              │                         ▼
              └── SM Exit Detection (monitor loop: tightens stops, forces close)
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
| Database | SQLite (sqlx) | File-based, zero ops, persists across restarts |
| API clients | reqwest + governor | Battle-tested HTTP + rate limiting |
| Helius integration | Official helius SDK v1.x | WebSocket support, Smart Transaction Sender, typed APIs |
| Solana SDK | solana-sdk 3.0 | Latest stable with full feature set |
| SPL token | spl-token v9 | Latest SPL token library |
| CLI framework | clap v4 | Standard Rust CLI |
| Logging | tracing | Structured, async-aware |
| Chain abstraction | Trait-based | `ChainProvider` trait with Solana and Base impls |
| Strategy pattern | Trait-based | `Strategy` trait with `evaluate() -> Signal` |
| Wallet monitoring | WebSocket-first | Real-time streaming, ~80% fewer API calls than polling |

---

## What's Done (Phases 0-8 + Mission)

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
- SQLite via sqlx: wallets, dev_blacklist, positions, trades, snapshots, helius_credits tables

### Phase 2: Data Pipeline
- **DexScreener**: new pairs, token search, pair data (no key)
- **Birdeye**: token price, security, top holders, top traders (needs key)
- **Helius SDK v1.x**: WebSocket wallet monitoring, parsed transactions, Smart Transaction Sender (needs key)
- **Jupiter V6**: quote + swap construction (no key)
- **GMGN CLI**: smart money tracking, wallet profiling, token research (needs key)
- **WalletWatcher**: WebSocket-first with polling fallback, emits WalletBuy/WalletSell events

### Phase 3: Signal Engine (all 6 + HotTokenTracker)
1. **WhaleConsensusSignal** (w=0.25): sliding window (1hr), wallet-quality-weighted, event-driven, GMGN fallback
2. **BehavioralSignal** (w=0.25): SOVEREIGN/PRECOGNITIVE wallets from behavioral scanner detected in GMGN top traders
3. **AccumulationSignal** (w=0.15): holder growth vs flat price (enhanced by HotTokenTracker multi-cycle data)
4. **LaunchMomentumSignal** (w=0.15): new launch volume/holder spike
5. **VolumeSpikeSignal** (w=0.10): 3x+ average over rolling window (enhanced by HotTokenTracker)
6. **SocialSignal** (w=0.10): twitter-cli mention velocity
- **HotTokenTracker**: persists signal data across scan cycles for longitudinal analysis

Confluence threshold: 35/100 weighted composite (progressive floor: 25, absolute floor: 25.0).
Per-signal reasoning and diagnostic detail logged for each evaluation.

### Phase 4: Safety & Risk
- **8-point safety scoring** with live Birdeye data: mint authority (15pts), freeze authority (10pts), LP lock (20pts), holder concentration (15pts), dev blacklist (15pts), dev holdings (10pts), honeypot (15pts), tax (10pts). Threshold: 70/100.
- **Risk Manager**: position sizing, max positions, daily loss limit, drawdown circuit breaker, cooldown, dynamic exit profiles (moonbag/runner/swing/conservative by mcap/age/confluence)

### Phase 5: Execution Engine
- Jupiter V6: quote → swap tx → sign → send
- **Helius Smart Transaction Sender**: priority fee estimation, reliable transaction delivery
- Solana RPC pool with round-robin failover
- 3 retries with +50bps slippage increase per retry
- Pre-flight checks: balance, provider availability
- Execution quality tracking: slippage, latency, success rate

### Phase 6: Agent Loop & CLI
- State machine: Scanning → Evaluating → RiskCheck → Executing → Monitoring
- Full wiring: scan (DexScreener) → evaluate (Birdeye safety) → risk check → execute (Jupiter + Smart TX Sender) → monitor (trailing stops)
- **Exponential backoff** in scan loop on errors
- SOL balance caching
- CLI: `status`, `scan`, `analyze`, `safety`, `portfolio`, `wallet`, `db`, `agent [--dry-run]`

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
- **Seed script**: `scripts/seed-wallets.sh` for periodic refresh
- **Dry run verified**: agent starts, loads wallets, scans tokens, evaluates safety+confluence

### Phase 9: GMGN Deep Integration (Safety + Signals + Exit Detection)
- **GmgnClient extended** with 6 methods (token security, full token info, dev created tokens, market signals, smart money trades, wallet holdings)
- **3 new safety checks**: LP burn/lock from GMGN (fills Birdeye "not available" gap), dev track record scoring (launch count/graduation rate/best ATH), GMGN cross-validated honeypot + tax
- **GmgnSignalEnrichment**: Pre-computed market signals (SM cluster-buy, KOL call, price surge) refreshed each scan cycle; boosts confluence by up to +60 pts
- **Smart money exit detection**: Monitor loop queries GMGN for SM sell activity on held positions; 2+ sellers tightens trailing stop to 50%, 3+ sellers + >$500 sells forces position close
- **GMGN skills directory**: 46 skills indexed from `https://gmgn.ai/static/opstatic/skills.json` across 5 categories; search on demand, install on demand

### Mission Completion (May 2025)

All 4 milestones completed with 18 features implemented:

**Milestone 1 — Stabilize:**
- Enforce absolute floor of 25.0 on effective confluence threshold
- Scan loop exponential backoff + SOL balance caching

**Milestone 2 — Helius Integration:**
- Replace custom HeliusClient with official Helius Rust SDK v1.x
- Upgrade solana-sdk/solana-client to 3.0, bump spl-token to v9
- Add WebSocket-first wallet monitoring with polling fallback

**Milestone 3 — Signal Revival:**
- Add HotTokenTracker for multi-cycle signal data persistence
- Revive WhaleConsensusSignal with GMGN fallback and hold scoring
- Widen behavioral scanner for more high-tier wallet discovery
- Add per-signal reasoning and diagnostic detail to evaluation logs

**Milestone 4 — Hardening:**
- Upgrade Jupiter swap execution to Helius Smart Transaction Sender with priority fee estimation
- Add Helius API credit budget tracking with persistence and status reporting
- Add `solagent status` CLI command for structured health snapshot
- Add cross-area validation tests (weight normalization, signal interplay)

---

## Signal Engine Detail

| Signal | Weight | Trigger | Data Source |
|--------|--------|---------|-------------|
| Whale Consensus | 0.25 | 2+ smart money buy same token in 1 hour + GMGN trader fallback | Helius WebSocket + GMGN wallet registry |
| Behavioral | 0.25 | SOVEREIGN/PRECOGNITIVE wallet detected in GMGN top traders | Behavioral intelligence scanner (4h cycle) |
| Accumulation | 0.15 | Holder growth + price flat (multi-cycle via HotTokenTracker) | Birdeye holder API |
| Launch Momentum | 0.15 | New token with rapid holder + volume growth | DexScreener new pairs |
| Volume Spike | 0.10 | 3x+ average volume in rolling window (multi-cycle) | DexScreener pair data |
| Social | 0.10 | Twitter mention velocity | twitter-cli |

Confluence = weighted sum. Threshold: 35/100 (progressive floor: 25, absolute floor: 25.0).

## Behavioral Intelligence Scanner

Background task runs every 4 hours. Uses DexScreener (crash/moon token discovery) + GMGN CLI (top trader extraction) + 5-layer scoring algorithm. Updates a shared `BehavioralWalletCache` that feeds both `BehavioralSignal` and `WhaleConsensusSignal`.

| Layer | Weight | Description |
|-------|--------|-------------|
| Inverse Loss Archaeology | 0.20 | Statistical inverse of losing wallet patterns |
| Liquidity Ghost Detection | 0.25 | Exits before crashes across 3+ tokens |
| Irrational Conviction Scoring | 0.20 | Early entry in 10x+ tokens before social amplification |
| CTO Meta-Reader | 0.20 | Profitable re-entry post-community takeover |
| Consensus Deviation | 0.15 | Profitable but methodologically unlike top 500 |

Tier output: PRECOGNITIVE (90-100), SOVEREIGN (75-89), EMERGING (55-74), NOISE (<55).

## Safety Checks Detail (11: 8 Birdeye + 3 GMGN)

### Birdeye (original)
| Check | Points | Method |
|-------|--------|--------|
| Mint authority revoked | 15 | Birdeye security API |
| Freeze authority revoked | 10 | Birdeye security API |
| LP lock status | 0-20 | Birdeye security API (often "not available") |
| Top 10 holder concentration < 20% | 0-15 | Birdeye holder API |
| Dev wallet not blacklisted | 0-15 | SQLite dev_blacklist |
| Dev holdings < 5% | 0-10 | Birdeye security API |
| Not a honeypot | 0-15 | Birdeye security API |
| Buy/sell tax < 5% | 0-10 | Birdeye security API |

### GMGN-Enriched (new)
| Check | Points | Method |
|-------|--------|--------|
| LP burn/lock from GMGN | 0-100 | `gmgn-cli token security` → lp_burned, lp_burn_percent |
| Dev track record | 0-100 | `gmgn-cli portfolio created-tokens` → launch count, graduation rate, best ATH |
| GMGN cross-validated honeypot + tax | 0-100 | `gmgn-cli token security` → is_honeypot, buy_tax, sell_tax |

Threshold: 60/100 weighted composite to proceed.

## Risk Parameters (tuned for < 1 SOL wallet)

| Parameter | Value |
|-----------|-------|
| Max position size | $15 per trade (dynamic: $5-$20 by confluence + win rate) |
| Max per token | 25% of portfolio |
| Max open positions | 3 |
| Daily loss limit | $15 (halts trading) |
| Drawdown breaker | 15% from peak (halts agent) |
| Stop loss | -15% per position |
| Take profit | +300% per position |
| Trailing stop | -15% from peak |
| Safety threshold | 60/100 |
| Confluence threshold | 35/100 (progressive floor: 25, absolute floor: 25.0) |
| Effective threshold floor | 25.0 (enforced absolute minimum) |
| Cooldown | 5 min after any loss |
| Exit profiles | Dynamic: moonbag/runner/swing/conservative by mcap/age/confluence |
| Position sizing | Capped by available SOL cash (not total portfolio) to avoid $1 dust trades |

## API Budget

| API | Free Limit | Usage | Rate Strategy |
|-----|-----------|-------|---------------|
| DexScreener | 300 req/min | New pair scanning | Poll every 30s, cache |
| Birdeye | ~1 req/sec | Safety + prices | Cache 30s |
| Jupiter | Unlimited | Swap execution | Use freely |
| Helius | 1M credits/mo | WebSocket wallet monitoring + RPC + Smart TX Sender | WebSocket ~80% fewer calls than polling; credit usage tracked in SQLite |
| GMGN | 20 req/sec | Wallet discovery/profiling + token security + dev track record + market signals + SM exit detection + wallet holdings | Rate-limited (0.5s); used throughout agent lifecycle |
| Zerion | 60K calls/mo (2K/day) | Portfolio, PnL, positions (primary balance source) | 8 RPS, ~20 calls/day for sync + tune |

## External Tools

- **gmgn-cli v1.3.0**: globally installed — `gmgn-cli track smartmoney --chain sol`
- **twitter-cli**: `/home/kt/twitter-cli/` — `cd /home/kt/twitter-cli && uv run twitter <cmd>`

## Git

- Repo at `/home/kt/solagent/`, on `main` branch
- Recent commits:
```
090c600 test: add cross-area validation tests (VAL-CROSS-001/002/003/005) and fix weight normalization
7200992 feat: add Helius API credit budget tracking with persistence and status reporting
8b443f9 feat: add solagent status CLI command for structured health snapshot
a287abc feat: add per-signal reasoning and diagnostic detail to evaluation logs
5fec432 feat: widen behavioral scanner for more high-tier wallet discovery
50c5589 feat: revive WhaleConsensusSignal with GMGN fallback and hold scoring
e154ae6 feat: add HotTokenTracker for multi-cycle signal data persistence
9079e84 feat: upgrade Jupiter swap execution to Helius Smart Transaction Sender with priority fee estimation
dcd0c5b feat: add WebSocket-first wallet monitoring with polling fallback
4461394 feat: replace custom HeliusClient with official Helius Rust SDK
cf50762 feat: upgrade solana-sdk/solana-client to 3.0, add helius SDK, bump spl-token to v9
```

## Known Issues & Recent Fixes

- **LP lock always "not available"** — Fixed: GMGN token security provides LP burn/lock data that Birdeye doesn't offer.
- **No dev track record visibility** — Fixed: GMGN dev created-tokens endpoint provides launch history, graduation rate, and best ATH for rug-pull prediction.
- **No sell-side intelligence** — Fixed: SM exit detection in monitor loop queries GMGN for smart money selling on held positions; tightens stops or forces close.
- **Whale consensus was dead** — Fixed: BehavioralSignal as 6th strategy with GMGN top-trader detection, WebSocket-first monitoring, behavioral tier data boosts whale consensus quality weighting.
- **Accumulation/Volume spike stuck at baseline** — Fixed: HotTokenTracker now persists signal data across scan cycles for longitudinal analysis.
- **Confluence threshold cratered to floor** — Fixed: absolute floor enforced at 25.0 (was allowing lower).
- **Dynamic sizer returned $1 for all trades** — Fixed: `get_available_cash()` caps against spendable SOL minus 0.01 SOL fee reserve.
- **Wallet monitoring burning Helius credits** — Fixed: WebSocket-first approach reduced Helius API calls by ~80%.
- **Execution reliability** — Fixed: Helius Smart Transaction Sender with priority fee estimation replaces manual send.
- **Win rate**: Was 11.1% (3W/24L) — expected to improve significantly with behavioral signal providing genuine edge detection, HotTokenTracker enabling longitudinal signals, and higher confluence threshold floor filtering out low-quality trades.

## Remaining Gaps (nice-to-have, not blocking)

- `solagent-chain-base`: all stubs (Base/Uniswap not a priority)
- CLI `trade buy/sell`: prints info only (agent handles execution)
- Telegram alerts not implemented
- Railway/Docker deployment not set up (local-only)

## Testing

- **300 tests passing** across all crates (0 failures)
  - 76 portfolio tests, 65 behavioral tests, 52 signal tests, 45 risk tests, 23 core tests, 19 execution tests, 15 safety tests, 5 chain-solana tests
- Cross-area validation tests (VAL-CROSS-001/002/003/005) for weight normalization
- `cargo test --workspace` runs all

## Zerion API Integration

- **Client**: `crates/solagent-data/src/zerion.rs` — HTTP Basic Auth, 8 RPS rate limiting
- **Endpoints**: portfolio overview, token positions, FIFO PnL (realized/unrealized/ROI)
- **Auto-tuner**: logs Zerion PnL cross-check during each tuning cycle (optional)
- **Agent enrichment**: every ~4h, refreshes top 10 wallet scores from Zerion PnL and emits `WalletHold` events for positions held by watched wallets
- **Multi-provider fallback**: Zerion → GMGN → Helius RPC → cached (balance and reconciliation tolerate individual API outages)
- **CLI**: `solagent portfolio sync --address <WALLET>` — fetch portfolio + positions + PnL
- **Config**: `ZERION_API_KEY` env var or `[data] zerion_api_key` in config/local.toml
- **Free tier**: 60K calls/mo, 10 RPS — budget ~20 calls/day for daily sync + tune cycles
