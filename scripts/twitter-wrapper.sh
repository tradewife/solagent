#!/usr/bin/env bash
# Wrapper for twitter-cli -- invoked by solagent's SocialSignal detector.
set -euo pipefail
cd /home/kt/twitter-cli
exec uv run twitter "$@"
