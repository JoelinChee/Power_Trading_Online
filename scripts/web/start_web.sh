#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

# Allow overrides while keeping sensible defaults.
PYTHON_BIN="${PYTHON_BIN:-/home/joelin/miniconda3/envs/test_RL/bin/python}"
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8088}"
WEB_APP_PATH="${WEB_APP_PATH:-$PTO_REPO_ROOT/web/app.py}"
WEB_URL="${WEB_URL:-http://127.0.0.1:${WEB_PORT}}"
PID_FILE="$PTO_PID_DIR/web_frontend.pid"
LOG_FILE="$PTO_LOG_DIR/web_frontend.log"

pto_prepare_runtime_dirs

if [[ ! -f "$WEB_APP_PATH" ]]; then
    echo "Web app not found: $WEB_APP_PATH"
    exit 1
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "web_frontend is already running with PID $(cat "$PID_FILE")"
    echo "URL: $WEB_URL"
    exit 0
fi

pto_kill_matching_processes "web_frontend" "streamlit run $WEB_APP_PATH"

# Start Streamlit in the background and persist PID/log paths under generated/.
# Streamlit's onboarding prompt is disabled so non-interactive starts do not hang.
nohup "$PYTHON_BIN" -m streamlit run "$WEB_APP_PATH" --server.address "$WEB_HOST" --server.port "$WEB_PORT" --server.headless true --browser.gatherUsageStats false >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

echo "Started web_frontend with PID $(cat "$PID_FILE")"
echo "Log: $LOG_FILE"
echo "URL: $WEB_URL"

open_browser() {
    local url="$1"
    local browser_commands=(
        firefox
        google-chrome
        chrome
        chromium
        chromium-browser
        microsoft-edge
        brave-browser
    )

    for browser_command in "${browser_commands[@]}"; do
        if command -v "$browser_command" >/dev/null 2>&1; then
            nohup "$browser_command" "$url" >/dev/null 2>&1 &
            return 0
        fi
    done

    # Try common Linux, GNOME, WSL/Windows routes in order. Browser launch is a
    # convenience only; failing to open one must not fail the web service start.
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
