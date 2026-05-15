#!/usr/bin/env bash
# Seed SolAgent wallet registry with GMGN smart money + KOL wallets.
# Requires: gmgn-cli installed, GMGN_API_KEY configured in ~/.config/gmgn/.env
#
# Usage:
#   ./scripts/seed-wallets.sh           # Pull and seed fresh wallets
#   ./scripts/seed-wallets.sh --dry-run # Show what would be seeded

set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

echo "=== SolAgent Wallet Seeding ==="
echo "Pulling smart money wallets from GMGN..."

# Pull smart money trades (limit 200) and extract unique wallet addresses
SM_WALLETS=$(gmgn-cli track smartmoney --chain sol --limit 200 --raw 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
seen = set()
for item in data.get('list', []):
    addr = item.get('maker', '')
    if addr and addr not in seen:
        seen.add(addr)
        print(addr)
" 2>/dev/null)

SM_COUNT=$(echo "$SM_WALLETS" | wc -l)
echo "Found $SM_COUNT unique smart money wallets"

# Pull KOL trades (limit 200) and extract unique wallet addresses
echo "Pulling KOL wallets from GMGN..."
KOL_WALLETS=$(gmgn-cli track kol --chain sol --limit 200 --raw 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
seen = set()
for item in data.get('list', []):
    addr = item.get('maker', '')
    if addr and addr not in seen:
        seen.add(addr)
        print(addr)
" 2>/dev/null)

KOL_COUNT=$(echo "$KOL_WALLETS" | wc -l)
echo "Found $KOL_COUNT unique KOL wallets"

# Profile and seed each wallet
echo ""
echo "Profiling wallets and seeding into SolAgent DB..."

python3 << 'PYEOF'
import subprocess, json, sqlite3, time, sys

dry_run = "--dry-run" in sys.argv or "DRYRUN" in os.environ if (import os := None) else False

sm_addrs = """SM_WALLETS_PLACEHOLDER""".strip().split("\n")
kol_addrs = """KOL_WALLETS_PLACEHOLDER""".strip().split("\n")
PYEOF

# Use inline python to do the actual profiling + seeding
ALL_WALLETS=$(echo -e "$SM_WALLETS\n$KOL_WALLETS" | sort -u)
TOTAL=$(echo "$ALL_WALLETS" | wc -l)

echo "Total unique wallets to profile: $TOTAL"

if $DRY_RUN; then
    echo "[DRY RUN] Would profile and seed $TOTAL wallets"
    exit 0
fi

python3 - << PYTHON_SCRIPT
import subprocess, json, sqlite3, time, datetime, os

sm_addrs = """$(echo "$SM_WALLETS")""".strip().split("\n")
kol_addrs = """$(echo "$KOL_WALLETS")""".strip().split("\n")
kol_set = set(kol_addrs)
all_addrs = list(dict.fromkeys(sm_addrs + kol_addrs))  # dedupe, preserve order

db = sqlite3.connect("solagent.db")
cur = db.cursor()
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
seeded = 0
errors = 0

for i, addr in enumerate(all_addrs):
    if not addr.strip():
        continue
    cmd = f"gmgn-cli portfolio stats --chain sol --wallet {addr.strip()} --raw"
    try:
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if out.returncode != 0 or not out.stdout.strip():
            errors += 1
            continue
        data = json.loads(out.stdout.strip())
        stat = data.get("pnl_stat", {})
        common = data.get("common", {})
        
        win_rate = stat.get("winrate", 0)
        total_pnl = float(data.get("realized_profit", 0))
        total_trades = data.get("buy", 0) + data.get("sell", 0)
        avg_hold_mins = stat.get("avg_holding_period", 0) or 0
        tags_json = json.dumps(common.get("tags", []))
        label = "smart_money"  # Both SM and KOL go as smart_money for signal engine
        
        # Score matching SolAgent formula
        score = min(max(
            win_rate * 30.0 +
            min(max(total_pnl / 1000.0, 0.0), 100.0) * 0.3 +
            min(total_trades / 100.0, 1.0) * 20.0 +
            20.0,  # recency bonus (just seen)
            0.0), 100.0)
        
        cur.execute("""
            INSERT INTO wallets (address, chain, label, source, win_rate, total_pnl,
               total_trades, avg_hold_time_mins, score, tags, last_seen_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(address, chain) DO UPDATE SET
              win_rate=excluded.win_rate, total_pnl=excluded.total_pnl,
              total_trades=excluded.total_trades, avg_hold_time_mins=excluded.avg_hold_time_mins,
              score=excluded.score, tags=excluded.tags, last_seen_at=excluded.last_seen_at,
              updated_at=excluded.updated_at
        """, (addr.strip(), "solana", label, "gmgn_api", win_rate, total_pnl, total_trades,
              avg_hold_mins, score, tags_json, now, now, now))
        seeded += 1
        twitter = common.get("twitter_username", "")
        tag_str = "+".join(common.get("tags", [])[:2])
        print(f"  [{i+1}/{len(all_addrs)}] {addr.strip()[:16]}... wr={win_rate:.2f} pnl=${total_pnl:,.0f} score={score:.1f} @{twitter} [{tag_str}]")
    except Exception as e:
        errors += 1
        print(f"  [{i+1}/{len(all_addrs)}] {addr.strip()[:16]}... ERROR: {e}")
    time.sleep(0.5)

db.commit()
count = cur.execute("SELECT COUNT(*) FROM wallets WHERE label='smart_money'").fetchone()[0]
db.close()
print(f"\nSeeded: {seeded} | Errors: {errors} | Total in registry: {count}")
PYTHON_SCRIPT

echo ""
echo "=== Done ==="
