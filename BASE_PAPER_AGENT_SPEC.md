# BASE_PAPER_AGENT_SPEC.md

**Spec: Parallel Base Paper Trading Agent (BaseAgent) — Resilient v1**

**Version:** 1.1 (Refined with SolAgent Diagnostic Learnings)  
**Date:** 2026-06-05  
**Status:** Ready for implementation  
**Owner:** tradewife

---

## 1. Goal & Key Learnings from SolAgent Diagnostic

**Primary goal:** Run a clean, observable **Basechain paper trading agent** in true parallel with the existing Solana `solagent` paper trader. Enable fair A/B comparison of edge, signal quality, and regime behavior between chains.

**Critical lessons from current SolAgent paper trader (June 2026 diagnostic):**

- **Dust positions are lethal.** 3 tiny positions ($0.00–$0.68) completely blocked `max_open_positions=3`, preventing all new entries even when tokens passed evaluation.
- **Infrastructure fragility kills performance.** Behavioral disabled, Whale dead (Helius WS exhausted + GMGN fallback failing), Social broken (missing `twitter-cli` binary), GMGN enrichment returning zero every cycle.
- **Signals at baseline or disabled** → confluence rarely clears threshold. The agent was correctly conservative, but 4 of 6 signals contributed almost nothing.
- **Over-reliance on external binaries/APIs** (twitter-cli, Helius WS, GMGN) created single points of failure.
- **Same tokens re-evaluated endlessly** but couldn't trade due to position quota.

**Design principles for Base v1 (non-negotiable):**

1. **Graceful degradation first** — Never let one missing data source (GMGN, Twitter, etc.) paralyze the whole agent.
2. **Dust position auto-cleanup built in** from day one for paper mode.
3. **Start simpler on signals** — Do not port all 6 Solana signals immediately. Begin with robust core + strong safety, then layer.
4. **Separate paper DB by default** — Isolation between Solana paper and Base paper.
5. **Excellent observability** — Status must explain *why* signals are weak or disabled.
6. **Parallel execution must be trivial and safe** — Two independent processes preferred for v1.

---

## 2. Overview

- New binary / mode: `baseagent agent --paper` (or `solagent agent --chain base --paper`).
- Pure **paper trading** (simulation). No real Base transactions in v1.
- Runs alongside Solana paper trader with zero interference.
- Produces comparable metrics (PnL, win rate, drawdown, signal contribution) for easy diffing.
- Designed to remain useful even when GMGN, social data, or on-chain enrichment is degraded or missing.

**Success criteria (v1):**
- Both agents run 24/7 in paper mode.
- Dust positions never block new entries for > 1 hour.
- Clear daily comparison report between Solana paper vs Base paper.
- Status output explains signal health and data source status.
- At least 2–3 signals meaningfully contributing on Base (not just one dominant broken signal).

---

## 3. Architecture

**Recommended for v1: Two independent processes**

```bash
# Terminal 1
solagent agent --paper --config config/solana-paper.toml

# Terminal 2  (or via script)
baseagent agent --paper --config config/base-paper.toml
```

**Why separate processes?**
- Fault isolation (one chain's data issues don't affect the other).
- Easier to give different resource/credit budgets.
- Matches the pattern already used in `sol-porpoise` (parallel deterministic + AI paper accounts).

**Crate / Module reuse**
- `solagent-core`: Move `ChainProvider` trait + `SimulateResult` + `Chain` enum here.
- `solagent-chain-base`: Fully implement (simulation-first `BaseProvider`).
- `solagent-data`: Add Base-aware clients with fallbacks.
- `solagent-portfolio`: Add `chain` column support + separate DB path for paper.
- `solagent-risk`: Reuse as-is (add dust cleanup hooks for paper mode).
- `solagent-signals` / `solagent-safety`: Start with minimal viable set + graceful degradation.
- `solagent-agent`: Thin wrapper or dedicated `BasePaperAgent` loop.
- `solagent-cli`: Support `--chain base` or new `baseagent` sub-binary.

**New / modified files**
- `config/base-paper.toml` (example config)
- `scripts/run-parallel-paper.sh` (starts both agents with logging)
- `BASE_PAPER_AGENT_SPEC.md` (this file)

---

## 4. Data Layer — Resilience & Fallbacks (Critical)

**Core principle:** The Base paper agent must continue functioning (even if degraded) when enrichment sources are missing or rate-limited.

**Primary data sources (v1):**

| Source          | Purpose                     | Fallback / Degraded Mode                  | Notes |
|-----------------|-----------------------------|-------------------------------------------|-------|
| DexScreener     | Scanning + basic token data | None (primary)                            | Multi-chain native |
| GoPlus Security | Honeypot, tax, approval risk| Conservative safety score                 | Free tier sufficient for paper |
| GMGN            | SM buys, KOL, surges        | Disable enrichment boosts; log warning    | Probe for Base support; do not hard fail |
| alloy (Base RPC)| On-chain verification       | Skip deep checks; rely on DexScreener     | Only for high-conviction candidates |
| Twitter / Social| Mention velocity            | Disable social signal or use very low weight | Do **not** require `twitter-cli` binary |

**Implementation rules:**
- Every external call must have timeout + error handling.
- Missing optional enrichment (GMGN, social) must **not** zero out the entire confluence score.
- Status output must clearly state: "GMGN enrichment: disabled (no Base support or zero results)" or "Social: disabled (twitter-cli not found)".
- Helius-style WebSocket over-use must be avoided. Prefer polling with smart caching for paper mode.

---

## 5. Signals Strategy for Base v1 (Start Simple)

**Do not** attempt full parity with Solana's 6-signal engine in v1.

**Recommended minimal viable set for Base paper (v1):**

1. **VolumeSpikeSignal** (core)
2. **AccumulationSignal** (core)
3. **SafetyScore** (GoPlus + basic DexScreener filters) — hard gate
4. Optional / low weight:
   - Simplified whale / smart money (only if GMGN Base support confirmed)
   - Basic launch / momentum (adapted for Base launch patterns)

**Rules:**
- Social signal is **optional and low weight** (or disabled by default) because of the `twitter-cli` fragility observed on Solana.
- Behavioral / Whale signals: Only enable if GMGN reliably returns Base data. Otherwise log and continue with reduced weight.
- Every signal evaluation must produce **non-empty diagnostic reasoning** (even when score = 0).
- Confluence scorer must support dynamic weight adjustment and per-signal status (active / degraded / disabled).

**Later phases** can add more signals once the core loop + comparison tooling is proven.

---

## 6. Paper Trading Mechanics (Must-Haves)

**Dust position handling (non-negotiable):**
- In paper mode, automatically close any position where:
  - Current value < $1.00 **AND**
  - Unrealized PnL has been flat (< $0.05 change) for > 4 hours, **OR**
  - Position has been open > 48h with negligible movement.
- This must run in the monitor loop so the `max_open_positions` quota is never permanently consumed by dust.

**Virtual execution:**
- Use live DexScreener prices.
- Apply configurable slippage model (e.g. 0.3–1.5% based on liquidity/volume).
- Simulate realistic gas cost for PnL fairness (even though no real tx).
- Record virtual positions with `chain = "base"` in the portfolio DB.

**Risk & position management:**
- Reuse existing risk crate.
- `max_open_positions` respected per chain (separate counters if using shared DB, or use separate DBs).
- Circuit breaker and daily loss limits apply per paper instance.

**Reconciliation:**
- Periodic full portfolio sync from on-chain (via alloy) even in paper mode for sanity checking.

---

## 7. Configuration

**Recommended structure:**

```toml
# config/base-paper.toml
[agent]
name = "baseagent-paper"
poll_interval_secs = 45
log_level = "info"
chain = "base"

[chains.base]
rpc_url = "https://mainnet.base.org"        # or Alchemy/QuickNode
paper_initial_usd = 150.0

[risk]
max_open_positions = 4                       # Slightly higher than Solana to start
max_position_size_usd = 20.0
# ... other risk params ...

[paper]
db_path = "baseagent_paper.db"             # Separate DB
enable_dust_cleanup = true
dust_cleanup_threshold_usd = 1.0
dust_cleanup_flat_hours = 4

[strategies]
active_strategies = ["volume_spike", "accumulation", "safety"]
confluence_threshold = 28.0                  # Start slightly lower while tuning
```

Use separate config files for Solana paper vs Base paper for clarity.

---

## 8. CLI & Observability

```bash
baseagent status                          # Must show signal health + data source status
baseagent portfolio summary --chain base
baseagent portfolio performance
baseagent compare --against solana        # or standalone script
```

**Status output requirements:**
- Show per-signal status: `active | degraded | disabled` + short reason.
- Show data source health (GMGN, GoPlus, DexScreener last success, etc.).
- Show open positions + any dust flagged for auto-cleanup.
- Show current confluence distribution.

**Comparison reporting:**
- Daily/periodic diff between the two paper DBs (PnL, win rate, best signals per chain, equity curves).

---

## 9. Parallel Execution

Create `scripts/run-parallel-paper.sh`:

```bash
#!/bin/bash
# Starts both paper traders with separate logs

LOG_DIR="./logs"
mkdir -p $LOG_DIR

nohup cargo run --release -p solagent-cli -- agent --paper --config config/solana-paper.toml > $LOG_DIR/solana-paper.log 2>&1 &
nohup cargo run --release -p baseagent-cli -- agent --paper --config config/base-paper.toml > $LOG_DIR/base-paper.log 2>&1 &

echo "Both paper agents started. Use 'tail -f logs/*.log' to monitor."
```

Optional: Add tmux session or systemd user services for always-on operation.

---

## 10. Implementation Phases (Updated)

**Phase 0 — Foundations (small refactor)**
- Move `ChainProvider` trait + `SimulateResult` to `solagent-core`.
- Ensure `Chain` enum supports `"base"`.
- Create example `config/base-paper.toml`.

**Phase 1 — BaseProvider + Data Resilience (highest priority)**
- Implement simulation-mode `BaseProvider` in `solagent-chain-base` (DexScreener prices + virtual execution).
- Add GoPlus client + basic alloy read helpers with timeouts/fallbacks.
- Wire graceful degradation for GMGN / social sources.

**Phase 2 — Core Paper Loop + Dust Handling**
- Implement `BasePaperAgent` loop or unified chain-aware loop.
- Add automatic dust position detection and forced close in paper mode.
- Virtual position tracking with `chain = "base"`.
- Basic Volume + Accumulation + Safety signals working.

**Phase 3 — CLI, Status & Comparison**
- Extend CLI for `--chain base` or new `baseagent` binary.
- Rich `status` output showing signal + data source health.
- Initial comparison script or subcommand.

**Phase 4 — Parallel Runner + Hardening**
- `scripts/run-parallel-paper.sh`.
- Full test coverage for paper path + dust cleanup.
- Documentation + example daily comparison report.

**Phase 5 (future)** — Expand signals, add real execution path, deeper on-chain verification, etc.

---

## 11. Out of Scope for v1

- Full 6-signal parity with Solana version.
- Real Base transaction execution (swap building, signing, sending).
- Complex smart money / behavioral scanner on Base (until GMGN Base support is confirmed reliable).
- Cross-chain features or shared capital between the two paper accounts.
- Advanced backtesting harness.

---

## 12. Risks & Mitigations

| Risk                              | Mitigation in this spec                          |
|-----------------------------------|--------------------------------------------------|
| Dust positions blocking quota     | Auto dust cleanup in paper mode (Phase 2)        |
| Missing GMGN / social data        | Graceful degradation + explicit status messaging |
| Over-reliance on one data source  | DexScreener primary + multiple fallbacks         |
| Comparison unfairness             | Separate DBs + same risk rules + gas simulation  |
| Helius-style credit blowout       | Avoid heavy WS in paper mode; use smart polling  |

---

## 13. Next Steps After v1

- Once Base paper is running cleanly, compare signal contribution and regime behavior between chains.
- Decide whether to invest in deeper Base signal work or keep Base as a diversified / lower-frequency paper book.
- Use learnings to harden the Solana agent (dust cleanup, better fallbacks, reduced WS dependency).

---

**This spec is ready to hand to your coding agent (Hermes, Claude, etc.).**

Key improvements in v1.1:
- Explicit dust auto-cleanup requirement
- Strong emphasis on graceful degradation and observability
- Simplified initial signal set recommendation
- Separate paper DBs as default
- Updated phases and risk table based on real diagnostic data

Run with: `cargo run --release -p baseagent-cli ...` or equivalent after implementation.
