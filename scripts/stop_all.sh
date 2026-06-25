#!/usr/bin/env bash

set -euo pipefail

# Resolve repository and artifact locations so shutdown uses the same PID files
# and Kafka helper scripts as startup.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
PID_DIR="$ARTIFACTS_ROOT/pids"
STOP_KAFKA=0
STOP_DATA_BOOT=0
STOP_FORECAST_BOOT=0
STOP_EXECUTION_BOOT=0

usage() {
    echo "Usage: $0 [all|kafka|data|forecast|execution|data_boot|forecast_boot|execution_boot]..."
}

select_all_targets() {
    STOP_KAFKA=1
    STOP_DATA_BOOT=1
    STOP_FORECAST_BOOT=1
    STOP_EXECUTION_BOOT=1
}

if [[ "$#" -eq 0 ]]; then
    select_all_targets
fi

for target in "$@"; do
    case "$target" in
        all)
            select_all_targets
            ;;
        kafka)
            STOP_KAFKA=1
            ;;
        data|data_boot)
            STOP_DATA_BOOT=1
            ;;
        forecast|forecast_boot)
            STOP_FORECAST_BOOT=1
            ;;
        execution|execution_boot)
            STOP_EXECUTION_BOOT=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            exit 1
            ;;
    esac
done

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

stop_stale_service_processes() {
    local pattern_parts=()
    local pattern
    local stale_service_pids

    if [[ "$STOP_DATA_BOOT" -eq 1 ]]; then
        pattern_parts+=(data_boot)
    fi
    if [[ "$STOP_FORECAST_BOOT" -eq 1 ]]; then
        pattern_parts+=(forecast_boot)
    fi
    if [[ "$STOP_EXECUTION_BOOT" -eq 1 ]]; then
        pattern_parts+=(execution_boot)
    fi

    if [[ "${#pattern_parts[@]}" -eq 0 ]]; then
        return 0
    fi

    pattern="$(IFS='|'; echo "${pattern_parts[*]}")"
    stale_service_pids="$(pgrep -f "uvicorn boots\.(${pattern})\.main:app" || true)"
    if [[ -n "$stale_service_pids" ]]; then
        echo "Stopping stale service process(es): $stale_service_pids"
        echo "$stale_service_pids" | xargs kill -9
    fi
}

# Stop application services in reverse dependency order before stopping Kafka.
if [[ "$STOP_EXECUTION_BOOT" -eq 1 ]]; then
    stop_service "execution_boot"
fi

if [[ "$STOP_FORECAST_BOOT" -eq 1 ]]; then
    stop_service "forecast_boot"
fi

if [[ "$STOP_DATA_BOOT" -eq 1 ]]; then
    stop_service "data_boot"
fi

stop_stale_service_processes

if [[ "$STOP_KAFKA" -eq 1 ]]; then
    bash "$SCRIPT_DIR/stop_local_kafka.sh"
fi

echo "Requested shutdown complete"
