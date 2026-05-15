#!/usr/bin/env bash
# SolAgent daily report.
# Runs at midnight via hermes cron.
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

echo "=== SolAgent Daily Report ==="
echo "Date: $(date +%Y-%m-%d)"
echo ""

echo "--- Portfolio Summary ---"
"$BINARY" --config "$CONFIG" portfolio summary 2>/dev/null || echo "Summary unavailable"

echo ""
echo "--- PnL (30 day) ---"
"$BINARY" --config "$CONFIG" portfolio pnl --days 30 2>/dev/null || echo "PnL unavailable"

echo ""
echo "--- Positions ---"
"$BINARY" --config "$CONFIG" portfolio positions 2>/dev/null || echo "No positions"

echo ""
echo "--- Database Stats ---"
"$BINARY" --config "$CONFIG" db stats 2>/dev/null || echo "Stats unavailable"

echo ""
echo "=== End of Daily Report ==="
