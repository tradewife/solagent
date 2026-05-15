#!/usr/bin/env bash
# SolAgent runner script with auto-restart and logging.
# Usage: ./scripts/run-agent.sh [--dry-run]
set -euo pipefail

cd "$(dirname "$0")/.."
PROJECT_DIR="$(pwd)"

# Load environment.
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Config file: prefer local.toml, fall back to default.
CONFIG="${SOLAGENT_CONFIG:-}"
if [ -z "$CONFIG" ]; then
    if [ -f config/local.toml ]; then
        CONFIG="config/local.toml"
    else
        CONFIG="config/default.toml"
    fi
fi

BINARY="${PROJECT_DIR}/target/release/solagent-cli"
if [ ! -f "$BINARY" ]; then
    BINARY="${PROJECT_DIR}/target/debug/solagent-cli"
fi

DRY_RUN="${1:-}"
LOG_DIR="${PROJECT_DIR}/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="${LOG_DIR}/solagent-$(date +%Y%m%d).log"

echo "[$(date -Iseconds)] SolAgent starting" | tee -a "$LOG_FILE"
echo "[$(date -Iseconds)] Config: $CONFIG" | tee -a "$LOG_FILE"
echo "[$(date -Iseconds)] Binary: $BINARY" | tee -a "$LOG_FILE"

# Auto-restart loop with backoff.
BACKOFF=5
MAX_BACKOFF=120

while true; do
    START=$(date +%s)

    if [ "$DRY_RUN" = "--dry-run" ]; then
        "$BINARY" --config "$CONFIG" agent --dry-run 2>&1 | tee -a "$LOG_FILE"
    else
        "$BINARY" --config "$CONFIG" agent 2>&1 | tee -a "$LOG_FILE"
    fi

    EXIT_CODE=${PIPESTATUS[0]}
    END=$(date +%s)
    UPTIME=$((END - START))

    echo "[$(date -Iseconds)] Agent exited (code=$EXIT_CODE, uptime=${UPTIME}s)" | tee -a "$LOG_FILE"

    # If it ran for more than 60s, reset backoff.
    if [ "$UPTIME" -gt 60 ]; then
        BACKOFF=5
    fi

    echo "[$(date -Iseconds)] Restarting in ${BACKOFF}s..." | tee -a "$LOG_FILE"
    sleep "$BACKOFF"

    BACKOFF=$((BACKOFF * 2))
    if [ "$BACKOFF" -gt "$MAX_BACKOFF" ]; then
        BACKOFF=$MAX_BACKOFF
    fi
done
