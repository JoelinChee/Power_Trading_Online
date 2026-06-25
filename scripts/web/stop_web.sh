#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

PID_FILE="$PTO_PID_DIR/web_frontend.pid"
WEB_APP_PATH="${WEB_APP_PATH:-$PTO_REPO_ROOT/web/app.py}"

# Stop the tracked Streamlit process first, then remove any orphan left behind
# by manual interruption or a missing PID file.
pto_stop_pid_file "web_frontend" "$PID_FILE" 10
pto_kill_matching_processes "web_frontend" "streamlit run $WEB_APP_PATH"
