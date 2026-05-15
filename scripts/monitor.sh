#!/usr/bin/env bash
# SolAgent position health monitor.
# Runs every 2 minutes via hermes cron.
# Outputs current position status and alerts.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; source .env; set +a
fi

CONFIG="config/default.toml"
if [ -f config/local.toml ]; then
    CONFIG="config/local.toml"
fi

BINARY="./target/release/solagent-cli"
if [ ! -f "$BINARY" ]; then
    BINARY="./target/debug/solagent-cli"
fi

echo "=== SolAgent Monitor $(date -Iseconds) ==="

# Portfolio positions.
echo ""
echo "--- Open Positions ---"
"$BINARY" --config "$CONFIG" portfolio positions 2>/dev/null || echo "No positions or DB not initialized"

# Portfolio PnL.
echo ""
echo "--- PnL Summary ---"
"$BINARY" --config "$CONFIG" portfolio pnl 2>/dev/null || echo "PnL unavailable"

# DB stats.
echo ""
echo "--- DB Stats ---"
"$BINARY" --config "$CONFIG" db stats 2>/dev/null || echo "Stats unavailable"

echo ""
echo "=== Monitor Complete ==="
