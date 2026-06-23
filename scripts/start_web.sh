#!/usr/bin/env bash

set -euo pipefail

# Resolve repository and artifact paths once for consistent runtime outputs.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
PID_DIR="$ARTIFACTS_ROOT/pids"
LOG_DIR="$ARTIFACTS_ROOT/logs"

# Allow overrides while keeping sensible defaults.
PYTHON_BIN="${PYTHON_BIN:-/home/joelin/miniconda3/envs/test_RL/bin/python}"
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8088}"
WEB_APP_PATH="${WEB_APP_PATH:-$REPO_ROOT/web/app.py}"
WEB_URL="${WEB_URL:-http://127.0.0.1:${WEB_PORT}}"
PID_FILE="$PID_DIR/web_frontend.pid"
LOG_FILE="$LOG_DIR/web_frontend.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

if [[ ! -f "$WEB_APP_PATH" ]]; then
    echo "Web app not found: $WEB_APP_PATH"
    exit 1
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "web_frontend is already running with PID $(cat "$PID_FILE")"
    echo "URL: $WEB_URL"
    exit 0
fi

stale_pids="$(pgrep -f "streamlit run $WEB_APP_PATH" || true)"
if [[ -n "$stale_pids" ]]; then
    echo "Stopping stale web_frontend process(es): $stale_pids"
    echo "$stale_pids" | xargs kill -9
fi

# Start Streamlit in background and persist PID/log for later inspection.
nohup "$PYTHON_BIN" -m streamlit run "$WEB_APP_PATH" --server.address "$WEB_HOST" --server.port "$WEB_PORT" --server.headless true --browser.gatherUsageStats false >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

echo "Started web_frontend with PID $(cat "$PID_FILE")"
echo "Log: $LOG_FILE"
echo "URL: $WEB_URL"

open_browser() {
    local url="$1"

    if command -v xdg-open >/dev/null 2>&1; then
        nohup xdg-open "$url" >/dev/null 2>&1 &
        return 0
    fi

    if command -v gio >/dev/null 2>&1; then
        nohup gio open "$url" >/dev/null 2>&1 &
        return 0
    fi

    if command -v powershell.exe >/dev/null 2>&1; then
        nohup powershell.exe -NoProfile -Command "Start-Process '$url'" >/dev/null 2>&1 &
        return 0
    fi

    if command -v cmd.exe >/dev/null 2>&1; then
        nohup cmd.exe /C start "" "$url" >/dev/null 2>&1 &
        return 0
    fi

    return 1
}

if open_browser "$WEB_URL"; then
    echo "Browser launch command sent."
else
    echo "Could not auto-open browser. Please open manually: $WEB_URL"
fi
