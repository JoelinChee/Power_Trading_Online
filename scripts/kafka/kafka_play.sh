#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Keep this wrapper deliberately thin: all replay behavior lives in Python, and
# PYTHON_BIN lets callers pin the same interpreter used by the running services.
exec "$PYTHON_BIN" "$SCRIPT_DIR/kafka_play.py" "$@"