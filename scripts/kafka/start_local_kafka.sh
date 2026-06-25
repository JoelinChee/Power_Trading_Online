#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

RUNTIME_DIR="$PTO_ARTIFACTS_ROOT/kafka-local"
KAFKA_VERSION="3.7.1"
SCALA_VERSION="2.13"
KAFKA_DIR="$RUNTIME_DIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
ARCHIVE_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
ARCHIVE_PATH="$RUNTIME_DIR/$ARCHIVE_NAME"
MIRROR_URLS=(
    "https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
    "https://mirrors.aliyun.com/apache/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
    "https://mirrors.huaweicloud.com/apache/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
    "https://mirrors.cloud.tencent.com/apache/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
)
PID_FILE="$RUNTIME_DIR/kafka.pid"
CLUSTER_ID_FILE="$RUNTIME_DIR/cluster.id"
CONFIG_FILE="$RUNTIME_DIR/server-low-memory.properties"
LOG_DIR="$RUNTIME_DIR/kraft-logs"
SERVER_LOG="$RUNTIME_DIR/server.out"
BOOTSTRAP_SERVER="127.0.0.1:9092"

wait_for_kafka_ready() {
    # kafka-topics.sh exercises the broker API, which is stronger than a raw
    # socket check: the port can be open before the broker is ready for clients.
    for _ in {1..30}; do
        if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
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

pto_prepare_runtime_dirs

# Keep Kafka downloads, logs, PID files, and KRaft metadata under generated/ so
# local broker runs never modify source-controlled project files.
mkdir -p "$RUNTIME_DIR"

# Remove any stale JVM replay files from the repository if they exist from old runs.
find "$PTO_REPO_ROOT" -maxdepth 1 \( -name 'hs_err_pid*.log' -o -name 'replay_pid*.log' \) -delete

# Exit early when a healthy Kafka process is already registered.
if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Kafka already running with PID $(cat "$PID_FILE")"
    wait_for_kafka_ready
    exit $?
fi

# Download and unpack Kafka only once into the sibling artifact directory.
if [[ ! -d "$KAFKA_DIR" ]]; then
    if [[ -f "$ARCHIVE_PATH" ]] && ! tar -tzf "$ARCHIVE_PATH" >/dev/null 2>&1; then
        echo "检测到损坏的 Kafka 压缩包，正在重新下载: $ARCHIVE_PATH"
        rm -f "$ARCHIVE_PATH"
    fi

    if [[ ! -f "$ARCHIVE_PATH" ]]; then
        if ! command -v aria2c >/dev/null 2>&1; then
            pto_require_command wget "Install wget or aria2c to download Kafka automatically."
        fi

        downloaded=false
        for url in "${MIRROR_URLS[@]}"; do
            echo "正在尝试下载 Kafka: $url"
            if command -v aria2c &>/dev/null; then
                aria2c -x 8 -s 8 -k 1M -d "$RUNTIME_DIR" -o "$ARCHIVE_NAME" "$url" && downloaded=true && break
            else
                wget --show-progress -O "$ARCHIVE_PATH" "$url" && downloaded=true && break
            fi
            echo "下载失败，尝试下一个镜像..."
            rm -f "$ARCHIVE_PATH"
        done
        if [[ "$downloaded" != "true" ]]; then
            echo "所有镜像下载均失败" >&2
            exit 1
        fi
    fi

    if ! tar -tzf "$ARCHIVE_PATH" >/dev/null 2>&1; then
        echo "Kafka 压缩包校验失败: $ARCHIVE_PATH" >&2
        exit 1
    fi

    echo "正在解压 Kafka 到 $RUNTIME_DIR"
    tar -xzf "$ARCHIVE_PATH" -C "$RUNTIME_DIR"
fi

mkdir -p "$LOG_DIR"

# Write a low-resource single-node KRaft configuration that fits this machine.
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

# Generate a stable cluster identifier once and reuse it across restarts.
if [[ ! -f "$CLUSTER_ID_FILE" ]]; then
    "$KAFKA_DIR/bin/kafka-storage.sh" random-uuid > "$CLUSTER_ID_FILE"
fi

# Format the KRaft metadata directory only when it has not been initialized yet.
if [[ ! -f "$LOG_DIR/meta.properties" ]]; then
    "$KAFKA_DIR/bin/kafka-storage.sh" format -t "$(cat "$CLUSTER_ID_FILE")" -c "$CONFIG_FILE"
fi

# Start Kafka with a bounded heap that has been verified to pass the project smoke test.
KAFKA_HEAP_OPTS='-Xms128M -Xmx256M' nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$CONFIG_FILE" > "$SERVER_LOG" 2>&1 &
echo $! > "$PID_FILE"
echo "Started local Kafka with PID $(cat "$PID_FILE")"
wait_for_kafka_ready