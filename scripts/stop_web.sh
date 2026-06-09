#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$REPO_ROOT/generated/pids/web_frontend.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "web_frontend is not running"
    exit 0
fi

pid="$(cat "$PID_FILE")"
if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in {1..10}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid"
    fi
    echo "Stopped web_frontend with PID $pid"
else
    echo "web_frontend PID $pid is not active"
fi

rm -f "$PID_FILE"

stale_pids="$(pgrep -f "streamlit run $REPO_ROOT/web/app.py" || true)"
if [[ -n "$stale_pids" ]]; then
    echo "Stopping stale web_frontend process(es): $stale_pids"
    echo "$stale_pids" | xargs kill -9
fi
