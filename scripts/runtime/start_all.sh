#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHONPATH_PREFIX="$PTO_ARTIFACTS_ROOT:$PTO_REPO_ROOT"
START_KAFKA=0
START_DATA_BOOT=0
START_FORECAST_BOOT=0
START_EXECUTION_BOOT=0

usage() {
    echo "Usage: $0 [all|kafka|data|forecast|execution|data_boot|forecast_boot|execution_boot]..."
}

select_all_targets() {
    # `all` is expanded into explicit flags so later startup always follows the
    # dependency order below, regardless of the order of user-provided targets.
    START_KAFKA=1
    START_DATA_BOOT=1
    START_FORECAST_BOOT=1
    START_EXECUTION_BOOT=1
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
            START_KAFKA=1
            ;;
        data|data_boot)
            START_DATA_BOOT=1
            ;;
        forecast|forecast_boot)
            START_FORECAST_BOOT=1
            ;;
        execution|execution_boot)
            START_EXECUTION_BOOT=1
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

pto_prepare_runtime_dirs
pto_bootstrap_env_file

wait_for_kafka() {
    # Services connect to Kafka during startup. When this script starts Kafka in
    # the same invocation, wait for the socket before launching those services.
    if ! pto_wait_for_tcp 127.0.0.1 9092 40 1 "$PYTHON_BIN" "$PTO_PYCACHE_DIR"; then
        pto_die "Kafka did not become reachable on 127.0.0.1:9092"
    fi
}

start_service() {
    # Start one Uvicorn process and track both PID and logs under generated/.
    # If a previous PID file points to a live process, leave it alone; if a
    # matching orphan process exists without a valid PID file, clean it up first.
    local name="$1"
    local module="$2"
    local port="$3"
    local pid_file="$PTO_PID_DIR/${name}.pid"
    local log_file="$PTO_LOG_DIR/${name}.log"
    local stale_pids

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "$name is already running with PID $(cat "$pid_file")"
        return 0
    fi

    stale_pids="$(pgrep -f "uvicorn ${module} --host 0.0.0.0 --port ${port}" || true)"
    if [[ -n "$stale_pids" ]]; then
        echo "Stopping stale $name process(es): $stale_pids"
        echo "$stale_pids" | xargs kill -9
    fi

    nohup env PYTHONPATH="$PYTHONPATH_PREFIX:${PYTHONPATH:-}" PYTHONPYCACHEPREFIX="$PTO_PYCACHE_DIR" "$PYTHON_BIN" -m uvicorn "$module" --host 0.0.0.0 --port "$port" >"$log_file" 2>&1 &
    echo $! >"$pid_file"
    echo "Started $name on port $port with PID $(cat "$pid_file")"
}

cd "$PTO_REPO_ROOT"

# Compile protobuf definitions first so every service can import the generated module.
if [[ "$START_DATA_BOOT" -eq 1 || "$START_FORECAST_BOOT" -eq 1 || "$START_EXECUTION_BOOT" -eq 1 ]]; then
    bash "$SCRIPT_DIR/../proto/compile_protos.sh"
fi

if [[ "$START_KAFKA" -eq 1 ]]; then
    bash "$SCRIPT_DIR/../kafka/start_local_kafka.sh"
fi

if [[ "$START_KAFKA" -eq 1 && ( "$START_DATA_BOOT" -eq 1 || "$START_FORECAST_BOOT" -eq 1 || "$START_EXECUTION_BOOT" -eq 1 ) ]]; then
    wait_for_kafka
fi

if [[ "$START_DATA_BOOT" -eq 1 ]]; then
    start_service "data_boot" "boots.data_boot.main:app" "${DATA_BOOT_PORT:-8001}"
fi

if [[ "$START_FORECAST_BOOT" -eq 1 ]]; then
    start_service "forecast_boot" "boots.forecast_boot.main:app" "${FORECAST_BOOT_PORT:-8002}"
fi

if [[ "$START_EXECUTION_BOOT" -eq 1 ]]; then
    start_service "execution_boot" "boots.execution_boot.main:app" "${EXECUTION_BOOT_PORT:-8003}"
fi

echo "Requested startup complete. Logs are under $PTO_LOG_DIR"

