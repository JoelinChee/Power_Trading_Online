#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
RELEASE_DIR="$ARTIFACTS_ROOT/releases"
PYCACHE_DIR="$ARTIFACTS_ROOT/pycache"
VERSION_FILE="$REPO_ROOT/VERSION"
PYTHON_BIN="${PYTHON_BIN:-python3}"
KAFKA_RUNTIME_DIR="$ARTIFACTS_ROOT/kafka-local/kafka_2.13-3.7.1"

mkdir -p "$RELEASE_DIR" "$PYCACHE_DIR"

if [[ ! -f "$VERSION_FILE" ]]; then
    echo "VERSION file not found at $VERSION_FILE" >&2
    exit 1
fi

VERSION="$(tr -d '[:space:]' <"$VERSION_FILE")"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid VERSION value: $VERSION" >&2
    exit 1
fi

if ! "$PYTHON_BIN" -m pip show pyinstaller >/dev/null 2>&1; then
    PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$PYTHON_BIN" -m pip install pyinstaller
fi

# Build-time protobuf generation for binary bundling.
PYTHONPYCACHEPREFIX="$PYCACHE_DIR" bash "$SCRIPT_DIR/compile_protos.sh"

# Ensure Kafka runtime dependencies are available locally before packaging.
if [[ ! -d "$KAFKA_RUNTIME_DIR" ]]; then
    bash "$SCRIPT_DIR/start_local_kafka.sh"
    bash "$SCRIPT_DIR/stop_local_kafka.sh"
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo 'nogit')"
PKG_BASENAME="power_trading_online-${VERSION}"
ARCHIVE_NAME="${PKG_BASENAME}.tar.gz"
ARCHIVE_PATH="$RELEASE_DIR/$ARCHIVE_NAME"
BUILDINFO_PATH="$RELEASE_DIR/${PKG_BASENAME}.buildinfo"
STAGING_ROOT="$RELEASE_DIR/_staging/${PKG_BASENAME}"
BUILD_ROOT="$RELEASE_DIR/_build/${PKG_BASENAME}"

rm -rf "$STAGING_ROOT" "$BUILD_ROOT"
mkdir -p "$STAGING_ROOT/bin" "$STAGING_ROOT/lib" "$STAGING_ROOT/scripts" "$STAGING_ROOT/config" "$BUILD_ROOT"

LAUNCHER_FILE="$BUILD_ROOT/power_trading_boot_launcher.py"
cat >"$LAUNCHER_FILE" <<'PY'
#!/usr/bin/env python3

import argparse

import uvicorn


def resolve_app(service: str):
    if service == "data":
        from boots.data_boot.main import app

        return app
    if service == "forecast":
        from boots.forecast_boot.main import app

        return app
    if service == "execution":
        from boots.execution_boot.main import app

        return app
    raise ValueError(f"Unsupported service: {service}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Power Trading Boot Launcher")
    parser.add_argument("--service", required=True, choices=["data", "forecast", "execution"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()

    app = resolve_app(args.service)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
PY

PYTHONPYCACHEPREFIX="$PYCACHE_DIR" env PYTHONPATH="$ARTIFACTS_ROOT:$REPO_ROOT:${PYTHONPATH:-}" \
    "$PYTHON_BIN" -m PyInstaller \
    --noconfirm \
    --clean \
    --onefile \
    --name power_trading_boot \
    --distpath "$STAGING_ROOT/bin" \
    --workpath "$BUILD_ROOT/pyinstaller-work" \
    --specpath "$BUILD_ROOT" \
    --paths "$REPO_ROOT" \
    --paths "$ARTIFACTS_ROOT" \
    --collect-submodules uvicorn \
    --collect-submodules confluent_kafka \
    --collect-submodules apscheduler \
    "$LAUNCHER_FILE"

cp -a "$REPO_ROOT/config/." "$STAGING_ROOT/config/"
mkdir -p "$STAGING_ROOT/lib/kafka-local"
cp -a "$KAFKA_RUNTIME_DIR" "$STAGING_ROOT/lib/kafka-local/"

cat >"$STAGING_ROOT/scripts/start_local_kafka.sh" <<'SH'
#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LIB_KAFKA_DIR="$REPO_ROOT/lib/kafka-local/kafka_2.13-3.7.1"
RUNTIME_DIR="$REPO_ROOT/run/kafka-local"
PID_FILE="$RUNTIME_DIR/kafka.pid"
CLUSTER_ID_FILE="$RUNTIME_DIR/cluster.id"
CONFIG_FILE="$RUNTIME_DIR/server-low-memory.properties"
LOG_DIR="$RUNTIME_DIR/kraft-logs"
SERVER_LOG="$RUNTIME_DIR/server.out"
BOOTSTRAP_SERVER="127.0.0.1:9092"

wait_for_kafka_ready() {
    for _ in {1..30}; do
        if "$LIB_KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
            echo "Kafka is ready on $BOOTSTRAP_SERVER"
            return 0
        fi
        if [[ -f "$PID_FILE" ]] && ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
            echo "Kafka process exited unexpectedly; see $SERVER_LOG" >&2
            return 1
        fi
        sleep 1
    done
    echo "Kafka did not become ready within 30 seconds; see $SERVER_LOG" >&2
    return 1
}

if [[ ! -d "$LIB_KAFKA_DIR" ]]; then
    echo "Bundled Kafka runtime not found: $LIB_KAFKA_DIR" >&2
    exit 1
fi

mkdir -p "$RUNTIME_DIR" "$LOG_DIR"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Kafka already running with PID $(cat "$PID_FILE")"
    wait_for_kafka_ready
    exit $?
fi

cat > "$CONFIG_FILE" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$LOG_DIR
num.partitions=1
log.segment.bytes=1048576
log.index.size.max.bytes=262144
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
offsets.topic.segment.bytes=1048576
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
EOF

if [[ ! -f "$CLUSTER_ID_FILE" ]]; then
    "$LIB_KAFKA_DIR/bin/kafka-storage.sh" random-uuid > "$CLUSTER_ID_FILE"
fi

if [[ ! -f "$LOG_DIR/meta.properties" ]]; then
    "$LIB_KAFKA_DIR/bin/kafka-storage.sh" format -t "$(cat "$CLUSTER_ID_FILE")" -c "$CONFIG_FILE"
fi

KAFKA_HEAP_OPTS='-Xms128M -Xmx256M' nohup "$LIB_KAFKA_DIR/bin/kafka-server-start.sh" "$CONFIG_FILE" > "$SERVER_LOG" 2>&1 &
echo $! > "$PID_FILE"
echo "Started local Kafka with PID $(cat "$PID_FILE")"
wait_for_kafka_ready
SH

cat >"$STAGING_ROOT/scripts/stop_local_kafka.sh" <<'SH'
#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$REPO_ROOT/run/kafka-local/kafka.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "Local Kafka is not running"
    exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
    kill "$PID"
    for _ in {1..15}; do
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID"
    fi
    echo "Stopped local Kafka with PID $PID"
else
    echo "Local Kafka PID $PID is not active"
fi

rm -f "$PID_FILE"
SH

cat >"$STAGING_ROOT/scripts/start_all.sh" <<'SH'
#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_DIR="$REPO_ROOT/run"
PID_DIR="$RUN_DIR/pids"
LOG_DIR="$RUN_DIR/logs"
BOOT_BIN="$REPO_ROOT/bin/power_trading_boot"

mkdir -p "$PID_DIR" "$LOG_DIR"
export POWER_TRADING_HOME="$REPO_ROOT"

bash "$SCRIPT_DIR/start_local_kafka.sh"

for _ in {1..40}; do
    if (echo > /dev/tcp/127.0.0.1/9092) >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

start_service() {
    local name="$1"
    local service="$2"
    local port="$3"
    local pid_file="$PID_DIR/${name}.pid"
    local log_file="$LOG_DIR/${name}.log"

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "$name is already running with PID $(cat "$pid_file")"
        return 0
    fi

    nohup "$BOOT_BIN" --service "$service" --host 0.0.0.0 --port "$port" >"$log_file" 2>&1 &
    echo $! >"$pid_file"
    echo "Started $name on port $port with PID $(cat "$pid_file")"
}

start_service "data_boot" "data" "${DATA_BOOT_PORT:-8001}"
start_service "forecast_boot" "forecast" "${FORECAST_BOOT_PORT:-8002}"
start_service "execution_boot" "execution" "${EXECUTION_BOOT_PORT:-8003}"

echo "All services started. Logs are under $LOG_DIR"
SH

cat >"$STAGING_ROOT/scripts/stop_all.sh" <<'SH'
#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_DIR="$REPO_ROOT/run/pids"

stop_service() {
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

stop_service "execution_boot"
stop_service "forecast_boot"
stop_service "data_boot"

stale_service_pids="$(pgrep -f 'power_trading_boot --service (data|forecast|execution)' || true)"
if [[ -n "$stale_service_pids" ]]; then
    echo "Stopping stale service process(es): $stale_service_pids"
    echo "$stale_service_pids" | xargs kill -9
fi

bash "$SCRIPT_DIR/stop_local_kafka.sh"

echo "All services stopped"
SH

chmod +x "$STAGING_ROOT/bin/power_trading_boot" "$STAGING_ROOT/scripts/"*.sh

# Hard validation: only these top-level directories are allowed in release package.
for required_dir in bin lib scripts config; do
    if [[ ! -d "$STAGING_ROOT/$required_dir" ]]; then
        echo "Missing required top-level directory: $required_dir" >&2
        exit 1
    fi
done

for entry in "$STAGING_ROOT"/*; do
    name="$(basename "$entry")"
    case "$name" in
    bin|lib|scripts|config)
        ;;
    *)
        echo "Forbidden top-level entry in release package: $name" >&2
        exit 1
        ;;
    esac
done

if find "$STAGING_ROOT" -type f -name '*.py' | grep -q .; then
    echo "Release package must not contain any .py files" >&2
    find "$STAGING_ROOT" -type f -name '*.py' >&2
    exit 1
fi

rm -f "$ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" -C "$RELEASE_DIR/_staging" "$PKG_BASENAME"

cat >"$BUILDINFO_PATH" <<EOF
name=${PKG_BASENAME}
version=${VERSION}
build_timestamp=${TIMESTAMP}
git_sha=${GIT_SHA}
archive=${ARCHIVE_NAME}
python_bin=${PYTHON_BIN}
packaging_mode=binary-only
binary=bin/power_trading_boot
allowed_top_level=bin,lib,scripts,config
EOF

echo "Release package created: $ARCHIVE_PATH"
echo "Build metadata created: $BUILDINFO_PATH"
