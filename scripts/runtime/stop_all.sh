#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

STOP_KAFKA=0
STOP_DATA_BOOT=0
STOP_FORECAST_BOOT=0
STOP_EXECUTION_BOOT=0

usage() {
    echo "Usage: $0 [all|kafka|data|forecast|execution|data_boot|forecast_boot|execution_boot]..."
}

select_all_targets() {
    # Expand `all` into explicit flags so shutdown later runs in one fixed
    # reverse dependency order, independent of argument order.
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
    local name="$1"
    # The helper handles live, stale, missing, and malformed PID files so stop
    # commands are safe to run repeatedly during local development.
    pto_stop_pid_file "$name" "$PTO_PID_DIR/${name}.pid" 15
}

stop_stale_service_processes() {
    # PID files are the source of truth, but developers sometimes interrupt
    # scripts manually. This second pass only targets the services requested by
    # the current command so stopping one boot will not kill the others.
    local pattern_parts=()
    local pattern

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
    pto_kill_matching_processes "service" "uvicorn boots\.(${pattern})\.main:app"
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
    bash "$SCRIPT_DIR/../kafka/stop_local_kafka.sh"
fi

echo "Requested shutdown complete"
