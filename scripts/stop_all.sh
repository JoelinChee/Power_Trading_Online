#!/usr/bin/env bash

set -euo pipefail

# Resolve repository and artifact locations so shutdown uses the same PID files
# and Kafka helper scripts as startup.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
PID_DIR="$ARTIFACTS_ROOT/pids"

stop_service() {
    # Stop one tracked Uvicorn process, waiting briefly before forcing it down.
    local name="$1"
    local pid_file="$PID_DIR/${name}.pid"

    if [[ ! -f "$pid_file" ]]; then
        echo "$name is not running"
        return 0
    fi

    local pid
    pid="$(cat "$pid_file")"

    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        for _ in {1..15}; do
            if ! kill -0 "$pid" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid"
        fi
        echo "Stopped $name with PID $pid"
    else
        echo "$name PID $pid is not active"
    fi

    rm -f "$pid_file"
}

# Stop all application services before stopping Kafka.
stop_service "execution_boot"
stop_service "forecast_boot"
stop_service "data_boot"

stale_service_pids="$(pgrep -f 'uvicorn boots\.(data_boot|forecast_boot|execution_boot)\.main:app' || true)"
if [[ -n "$stale_service_pids" ]]; then
    echo "Stopping stale service process(es): $stale_service_pids"
    echo "$stale_service_pids" | xargs kill -9
fi

bash "$SCRIPT_DIR/stop_local_kafka.sh"

echo "All services stopped"