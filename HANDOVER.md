# SolAgent Session Handover

## What This Is
An autonomous multi-chain (Solana + Base) trading agent built in Rust. The spec is at `SPEC.md`.

## Current State (End of Session)

### What's Working
- **11-crate Rust workspace** compiles clean at `/home/kt/solagent/`
- **DexScreener client**: live API, fetches boosted token pairs with MC/liq/vol/age/boosts
- **Birdeye client**: token price, security (mint/freeze/honeypot/tax), top holders, top traders (needs `BIRDEYE_API_KEY` env var)
- **Helius client**: parsed transactions, balances, webhook management (needs `HELIUS_API_KEY`)
- **Jupiter V6 client**: quote + swap transaction construction
- **CLI `scan` command**: `solagent-cli scan --chain solana` returns live boosted pairs
- **CLI `analyze` command**: `solagent-cli analyze <CA> --chain solana -v` returns full token analysis with security data
- **Phase 0 audit complete**: See `phase0-audit-report.md`. Key finding: TreeCityWes/pump-bot sends private keys to 3rd party -- DO NOT USE.

### Audit Repos (in `/home/kt/solagent/audit/`)
Cloned and audited repos are in the `audit/` directory. Key grades:
- GMGN Skills: A-/A (use via CLI integration, has AI agent skill pack)
- 1fge/pump-fun-sniper (Go): B+/A- (best reference for pump.fun IDL + Jito MEV)
- Arkham skill: A-/A (wallet investigation, has Claude skill)
- TreeCityWes: REJECTED (sends private keys to third-party API)
- SolanaWhaleAlert: REJECTED (non-functional, sends keys over Telegram)

### Architecture
```
crates/
├── solagent-core/       # Config, error types, events, Chain enum
├── solagent-data/       # API clients (DexScreener, Birdeye, Helius, Jupiter)
├── solagent-chain-solana/  # Solana RPC, keypair mgmt, pump.fun parsing
├── solagent-chain-base/    # Base/alloy provider (stubs)
├── solagent-signals/       # Signal engine (structs defined, logic is todo!())
├── solagent-safety/        # Safety scorer (structs defined, logic is todo!())
├── solagent-risk/          # Risk manager (structs defined, logic is todo!())
├── solagent-exec/          # Execution engine (stubs)
├── solagent-portfolio/     # Portfolio + SQLite (stubs)
├── solagent-agent/         # Autonomous agent loop (stubs)
└── solagent-cli/           # CLI binary (scan + analyze working, rest todo!())
```

## Where to Pick Up

### Next Priority (Phase 2 continuation)
1. **SQLite schema + wallet registry** (`solagent-portfolio` crate)
   - Create migration SQL for: wallets table, positions table, trades table, snapshots table
   - Wallet registry: store known smart money wallets with labels, win_rate, pnl, last_updated
   - Seed from: GMGN smart money lists, 0x_Discover Twitter feed
   - Dev wallet blacklist: seed from Wallet Master data

2. **Wallet watcher** (`solagent-chain-solana` + `solagent-data`)
   - Use Helius webhooks to monitor top N wallets in real-time
   - Fire `WalletBuy`/`WalletSell` events when watched wallets move
   - Integrate with the event bus in `solagent-core`

3. **Signal engine** (`solagent-signals` crate)
   - Implement `WhaleConsensusSignal`: sliding window of wallet buys per token, flag when 2+ smart money wallets buy same token within 30 min
   - Implement `AccumulationSignal`: holder count increasing + price flat
   - Implement `LaunchMomentumSignal`: new launch with volume/holder rate spike
   - Implement `VolumeSpikeSignal`: 3x+ average volume in 5-min window
   - Implement `ConfluenceScorer`: weighted composite of all signals, threshold 65/100

### After That (Phase 3-4)
4. **Safety scorer** (`solagent-safety`): implement the 7 check functions against real on-chain data
5. **Risk manager** (`solagent-risk`): implement position sizing, drawdown circuit breaker, daily loss limit
6. **Execution engine** (`solagent-exec`): wire Jupiter swap to actual Solana transaction signing/sending
7. **Agent loop** (`solagent-agent`): wire the state machine (Scanning -> Evaluating -> RiskCheck -> Executing -> Monitoring)

### Key Config
- Default config at `config/default.toml` with all risk/safety/signal parameters
- API keys via env vars: `BIRDEYE_API_KEY`, `HELIUS_API_KEY`
- No keys needed for DexScreener or Jupiter (free, no auth)

### Git
- Repo at `/home/kt/solagent/`, 2 commits on `main` branch
- Git config set locally to `kt <kt@solagent.dev>`

### Build & Run
```bash
cd /home/kt/solagent
cargo build -p solagent-cli
./target/debug/solagent-cli scan --chain solana
./target/debug/solagent-cli analyze <TOKEN_ADDRESS> --chain solana -v
```
