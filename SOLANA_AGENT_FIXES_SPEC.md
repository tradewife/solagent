# SOLANA_AGENT_FIXES_SPEC.md

**Spec: Critical Fixes for SolAgent Paper Trader Stability & Signal Health**

**Version:** 1.0  
**Date:** 2026-06-05  
**Status:** Ready for implementation  
**Owner:** tradewife

---

## 1. Context & Goal

The Solana paper trader is currently underperforming primarily due to infrastructure fragility and a small number of blocking issues rather than fundamental signal logic problems. The recent diagnostic showed:

- 3 dust positions completely blocking `max_open_positions=3`
- Behavioral signal disabled
- Whale consensus contributing almost nothing (0.1 avg score)
- Social signal producing fake/stale scores (twitter-cli binary missing)
- GMGN enrichment returning zero every cycle
- Helius free tier credits exhausted 13× (WebSocket + monitor loop)
- Accumulation, volume_spike, and launch_momentum stuck at baseline

**Goal:** Rapidly restore signal health, unblock trading, improve observability, and make the Solana paper trader robust enough to run fairly in parallel with the newly deployed Base paper agent.

These fixes should also serve as a template to prevent the same classes of fragility from appearing in the Base agent.

**Success criteria:**
- Dust positions no longer block new entries for more than 1 hour.
- At least 4 of 6 signals contributing meaningfully (not just Social).
- `status` command clearly explains why any signal is disabled or scoring low.
- Helius credit usage brought back under control or has safe fallbacks.
- Both Solana and Base paper agents can run side-by-side with comparable observability.

---

## 2. Prioritized Fix List

### P0 — Immediate Unblockers (Do First)

#### Fix 1: Dust Position Auto-Cleanup + Force Close (Highest Impact)

**Problem:** 3 tiny positions ($0.00–$0.68) are consuming the entire position quota and preventing any new trades.

**Solution:**
- Add automatic dust detection and forced closure in paper mode.
- New CLI command: `solagent portfolio close-dust [--max-value 2.0] [--force]`
- In the monitor loop (paper mode only): periodically scan open positions and auto-close any where:
  - Current value < $1.00 **AND**
  - Unrealized PnL change < $0.05 for the last 4+ hours, **OR**
  - Position open > 48 hours with negligible movement.
- Log every auto-close with reason.
- Add safety: never auto-close if it would realize > $X loss (configurable).

**Files to modify:**
- `crates/solagent-portfolio/src/lib.rs` (or new `dust.rs` module)
- `crates/solagent-cli/src/commands/portfolio.rs`
- `crates/solagent-agent/src/monitor.rs` (paper-mode hook)

**Acceptance:** Running `solagent portfolio close-dust --force` clears the 3 current dust positions and frees the quota. Auto-cleanup prevents future blocking.

---

### P1 — High-Impact Infrastructure & Observability Fixes

#### Fix 2: Helius Credit Management & Fallbacks

**Problem:** WebSocket + monitor loop burned through 13M credits vs 1M free tier.

**Solution:**
- Make WebSocket usage configurable and aggressive only when credits allow.
- Add credit-aware client in `solagent-data`:
  - Track remaining credits (persistent in SQLite alongside existing Helius credit tracking).
  - Automatically downgrade to polling-only mode when credits < threshold.
  - Expose warning in `status` output: "Helius: 87% credits used — WS disabled, using polling fallback".
- Reduce default poll frequency in monitor loop for paper mode.
- Add CLI flag: `--helius-mode ws|poll|auto`

**Files:** `crates/solagent-data/src/helius.rs`, `crates/solagent-agent`, config, CLI.

#### Fix 3: Rich Status Output & Signal Health Diagnostics

**Problem:** Current `status` does not explain *why* Behavioral is disabled, why Whale is scoring 0.1, or why GMGN enrichment is empty.

**Solution:**
- Extend `solagent status` to show per-signal status with reason:
  ```
  Signal Health:
    whale_consensus     active     (42 events in window, 3 GMGN fallback matches)
    behavioral          disabled   (scanner last successful run: 19h ago — check GMGN CLI)
    social              degraded   (twitter-cli not found at /home/kt/twitter-cli/)
    accumulation        active     (baseline 22.9)
  ```
- Add data source health section:
  ```
  Data Sources:
    GMGN enrichment     zero results last 12 cycles
    Helius WS           degraded (credit limited)
    DexScreener         healthy (last poll 14s ago)
  ```
- Make this information easily queryable by scripts for the parallel comparison reports.

**Files:** `crates/solagent-cli/src/commands/status.rs`, core event/logging system.

#### Fix 4: twitter-cli Path & Social Signal Robustness

**Problem:** Social signal relies on missing binary and is producing misleading high scores.

**Solution:**
- Make twitter-cli path fully configurable in `config/local.toml` + environment variable.
- Add graceful degradation: if binary not found or returns error, disable Social signal with clear reason in status (do not return fake baseline scores).
- Optionally add a lightweight fallback using direct Twitter API or cached data (low priority).

**Files:** `crates/solagent-signals/src/social.rs`, config loading.

---

### P2 — Signal Recovery & Tuning

#### Fix 5: Re-enable & Debug Behavioral Signal

**Problem:** Behavioral is disabled; only rare EMERGING tier hits.

**Actions:**
- Investigate why the periodic behavioral scanner task is not running or not populating the cache.
- Ensure `BehavioralWalletCache` is being refreshed from GMGN + registry.
- Add explicit logging + status visibility for the scanner's last successful run time and number of wallets discovered.
- If GMGN calls are failing, fall back to registry-only mode temporarily.

#### Fix 6: GMGN Enrichment & Fallback Reliability

**Problem:** `get_market_signals()` returning zero every cycle (no SM buy / KOL / price surge).

**Actions:**
- Add detailed logging around the GMGN market signals calls (request, response, errors).
- Implement retry with exponential backoff.
- If enrichment consistently returns zero, log a clear warning and continue without the +60 boost (do not silently degrade confluence).
- Expose current enrichment status in the new rich `status` output.

#### Fix 7: Signal Threshold & Weight Tuning (Short Experiment)

While infrastructure is being restored, run a short tuning pass:
- Temporarily lower `volume_spike` threshold from 3.0x to 2.0–2.5x.
- Review `launch_momentum` max_age_hours (currently 1h — may be too strict).
- Consider small weight rebalancing once Behavioral and Whale are contributing again.
- Add runtime weight adjustment via the existing auto-tuner if not already exposed in CLI.

**Note:** These are experiments. Revert or lock in after 48–72h of data with the Base agent running in parallel.

---

## 3. Implementation Phases

**Phase 0 — Quick Wins (Today / Tomorrow)**
- Implement Fix 1 (dust auto-cleanup + `close-dust` command). Deploy immediately.
- Fix twitter-cli path / graceful degradation (Fix 4).
- Add basic rich status output for signal health (Fix 3, minimal version).

**Phase 1 — Infrastructure Stability (Next 2–3 days)**
- Helius credit-aware client + fallback to polling (Fix 2).
- Full rich `status` command with data source health (Fix 3).
- Debug and re-enable Behavioral signal (Fix 5).

**Phase 2 — Signal Recovery & Tuning (This week)**
- GMGN enrichment reliability + logging (Fix 6).
- Short threshold/weight tuning experiment (Fix 7).
- Validate that 4+ signals are now contributing meaningfully.

**Phase 3 — Parallel Comparison Hardening**
- Ensure both Solana and Base paper agents produce comparable metrics and status output.
- Create or enhance the daily comparison report script that diffs the two paper DBs.
- Add alerts or simple health checks for the parallel setup.

---

## 4. Observability & Comparison Requirements

Because the Base paper agent is now live, the Solana agent must reach similar observability standards:

- Both agents should support `status`, `portfolio performance`, and `signal-report` with consistent fields.
- The comparison tooling should clearly show per-chain signal contribution, win rate by signal, and equity curves.
- Any new status fields added here should be mirrored (or easily adaptable) in the Base agent.

---

## 5. Risks & Mitigations

| Risk                              | Mitigation                              |
|-----------------------------------|-----------------------------------------|
| Auto dust cleanup closes winners    | Conservative thresholds + manual override flag |
| Helius credit changes             | Credit-aware auto-downgrade + clear status |
| Tuning experiments hurt performance | Short duration + easy rollback         |
| Behavioral/GMGN fixes take time   | Graceful degradation so agent still functions |

---

## 6. Out of Scope (for this fix sprint)

- Major signal logic rewrites
- Adding new data sources beyond current stack
- Full real-execution hardening on Solana
- Cross-chain features between Solana and Base paper books

---

## 7. How These Fixes Help the Parallel Setup

Once these changes are deployed:
- The Solana paper trader will stop being artificially blocked by dust and missing infrastructure.
- Both agents will have comparable visibility into why they are (or aren't) taking trades.
- You will be able to make a data-driven decision on whether Base has a genuine edge or whether the current Solana issues were mostly operational.
- The resilience patterns (dust handling, graceful degradation, credit awareness) can be back-ported to the Base agent if similar issues appear there.

---

**This spec is ready to hand to your coding agent.**

Prioritize Phase 0 fixes first — they will have the largest immediate effect on your current paper trading results.
