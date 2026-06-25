#!/usr/bin/env bash

set -euo pipefail

# Resolve repository and artifact locations once so every subprocess uses the
# same directory layout and the repository stays free of runtime artifacts.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
PID_DIR="$ARTIFACTS_ROOT/pids"
LOG_DIR="$ARTIFACTS_ROOT/logs"
PYCACHE_DIR="$ARTIFACTS_ROOT/pycache"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHONPATH_PREFIX="$ARTIFACTS_ROOT:$REPO_ROOT"
START_KAFKA=0
START_DATA_BOOT=0
START_FORECAST_BOOT=0
START_EXECUTION_BOOT=0

usage() {
    echo "Usage: $0 [all|kafka|data|forecast|execution|data_boot|forecast_boot|execution_boot]..."
}

select_all_targets() {
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

# Ensure the external artifact directories exist before starting any process.
mkdir -p "$PID_DIR" "$LOG_DIR" "$PYCACHE_DIR"

# Bootstrap a local environment file automatically when the repository is fresh.
if [[ ! -f "$REPO_ROOT/.env" ]]; then
    cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
fi

wait_for_kafka() {
    # Wait until the Kafka broker socket is reachable before starting any service.
    for _ in {1..40}; do
        if PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$PYTHON_BIN" - <<'PY'
import socket
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect(("127.0.0.1", 9092))
    raise SystemExit(0)
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
        then
            return 0
        fi
        sleep 1
    done

    echo "Kafka did not become reachable on 127.0.0.1:9092" >&2
    return 1
}

start_service() {
    # Start one Uvicorn process and track both its PID and log file externally.
    local name="$1"
    local module="$2"
    local port="$3"
    local pid_file="$PID_DIR/${name}.pid"
    local log_file="$LOG_DIR/${name}.log"
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

    nohup env PYTHONPATH="$PYTHONPATH_PREFIX:${PYTHONPATH:-}" PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$PYTHON_BIN" -m uvicorn "$module" --host 0.0.0.0 --port "$port" >"$log_file" 2>&1 &
    echo $! >"$pid_file"
    echo "Started $name on port $port with PID $(cat "$pid_file")"
}

cd "$REPO_ROOT"

# Compile protobuf definitions first so every service can import the generated module.
if [[ "$START_DATA_BOOT" -eq 1 || "$START_FORECAST_BOOT" -eq 1 || "$START_EXECUTION_BOOT" -eq 1 ]]; then
    bash "$SCRIPT_DIR/compile_protos.sh"
fi

if [[ "$START_KAFKA" -eq 1 ]]; then
    bash "$SCRIPT_DIR/start_local_kafka.sh"
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

echo "Requested startup complete. Logs are under $LOG_DIR"

