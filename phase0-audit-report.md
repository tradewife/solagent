# Phase 0 Audit Report

## Summary Grades

| Repo | Safety | Alpha | Recommendation |
|---|---|---|---|
| GMGN Skills | A- | A | **USE** -- API client + SKILL.md integration |
| 1fge/pump-fun-sniper (Go) | B+ | A- | **REFERENCE** -- pump.fun IDL, Jito MEV, execution flow |
| Arkham Claude Skill | A- | A | **INTEGRATE** -- wallet investigation skill |
| TreeCityWes/pump-fun-bot | F | D | **REJECT** -- sends private keys to 3rd party |
| cutupdev/pumpfun-sniper | D | C | **REJECT** -- incomplete source, untrusted binary |
| SolanaWhaleAlert | D | D- | **REJECT** -- non-functional, key exposure |
| Zentryx | N/A | A (claimed) | **NO PUBLIC REPO** -- reimplement from description |
| SLIP | N/A | A (claimed) | **NO PUBLIC REPO** -- reimplement from description |
| Aegis | N/A | B+ (claimed) | **NO PUBLIC REPO** -- reimplement from description |
| RugRadar | N/A | B (claimed) | **NO PUBLIC REPO** -- reimplement from description |
| Solana Indexer (techie-ghost) | N/A | A (claimed) | **NO PUBLIC REPO** -- use senzenn/gRPC_indexer as reference |

---

## Detailed Findings

### 1. GMGN Skills (USE)
- **24+ API endpoints** for token data, wallet analytics, smart money, trading, token creation
- **Ed25519/RSA-SHA256 request signing** -- non-trivial to reimplement
- **18 signal types** (price spikes, smart money buys, large buys, CTO events, DEX listings)
- **50+ filter fields** for new token screening (rug_ratio, bundler_rate, insider_ratio, smart_degen_count)
- **Multi-chain**: Solana, BSC, Base, Ethereum
- **Rate limit**: 20 req/sec, auto-retry on 429
- **Key insight**: Smart money labels (`smart_degen`, `renowned`) are GMGN-proprietary -- can't reproduce without their API
- **Integration path**: Use `gmgn-cli` as subprocess from Rust agent, parse JSON output

### 2. 1fge/pump-fun-sniper-bot (REFERENCE -- Go)
- **Complete, auditable Go source** -- no security issues found
- **Full pump.fun IDL** with all instruction discriminators
- **Jito bundle integration** for MEV protection
- **Creator vetting pipeline**: checks dev wallet balance, created tokens, token history
- **Fast-sell spam strategy** for quick exits
- **Key insight**: pump.fun program ID is `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P8p`
- **Reimplementation value**: The pump.fun instruction parsing and Jito bundle construction are valuable reference code

### 3. Arkham Intelligence Claude Skill (INTEGRATE)
- **50+ API endpoints** across 10+ chains
- **Entity labeling**: exchanges, market makers, funds, whales -- thousands of labeled addresses
- **WebSocket support** for real-time transfer monitoring
- **Declarative skill** -- purely prompt-based, no executable code, safe
- **Integration path**: Use as SKILL.md template, call Arkham API directly from Rust

### 4. TreeCityWes/Pump-Fun-Trading-Bot-Solana (REJECT)
- **CRITICAL**: Sends wallet private key to `https://pumpapi.fun/api/transaction` (third-party)
- **CRITICAL**: `rejectUnauthorized: false` disables TLS verification
- The third-party API could steal keys and drain wallets
- Only useful as a negative example of what NOT to do

### 5. cutupdev/Solana-Pumpfun-Sniper-Bot (REJECT)
- Source code is incomplete -- 6+ modules referenced but not present
- Contains a 54MB precompiled binary (`target/release/pumpfun-sniper`)
- Cannot verify what the binary does
- Architecture description is sound (Rust, async, gRPC) but unverifiable

### 6. SolanaWhaleAlert (REJECT)
- Core webhook controller is entirely commented out -- non-functional
- `/createwallet` command sends private keys in plain text over Telegram
- Architecture pattern (Helius webhook -> BullMQ queue -> Telegram) is worth noting for reimplementation

---

## Algorithms to Reimplement (No Public Repo)

### Zentryx -- Whale Consensus Detection
From Twitter descriptions:
- Track top N profitable wallets continuously (Birdeye API for wallet PnL)
- When 2+ wallets buy the same token within X minutes, flag as consensus signal
- Score based on: wallet quality, buy amounts, time between buys (faster = stronger)
- Paper-trade tracking with TP/SL and live PnL
- Implementation: maintain a sliding window of wallet buys per token, check for overlap

### SLIP -- Trade Autopsy (VERDICT/MIRROR/PAYSLIP)
From Twitter descriptions:
- **VERDICT**: Check mint authority, freeze authority, LP lock status, deployer history. All available via on-chain data.
- **MIRROR**: For a given token, query Birdeye top traders, rank by SOL gained. Compare user position vs winners.
- **PAYSLIP**: Full trade cost breakdown including fees, slippage, opportunity cost.

### Aegis -- Token Safety Scoring
From Twitter descriptions:
- Safety score 0-100 based on: honeypot check, rug/freeze detection, buy/sell tax simulation
- Shareable risk reports
- Core data available via: Solana RPC (mint/freeze authority), Birdeye API (security data), simulation (tax check)

### RugRadar -- Real-time Rug Detection
From Twitter descriptions:
- Every new Solana token gets RugScore 0-100
- Telegram alerts when score >= 80 (high rug risk)
- Likely checks: mint authority, freeze authority, LP lock, holder concentration, dev token holdings
- Implementation: subscribe to new token events, run safety checks, compute score, alert

---

## Security Patterns Identified

1. **Key handling**: Private keys must NEVER leave the machine. Sign locally, send only signed transactions.
2. **Third-party APIs**: Never send private keys to any external API. The TreeCityWes bot is a textbook attack.
3. **TLS verification**: Always verify TLS. Disabling it enables MITM attacks.
4. **Precompiled binaries**: Do not trust them. Always build from auditable source.
5. **Rate limiting**: All free-tier APIs need careful rate limit management. Use token bucket algorithms.
