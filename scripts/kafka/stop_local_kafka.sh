#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

PID_FILE="$PTO_ARTIFACTS_ROOT/kafka-local/kafka.pid"

# Stop the broker registered by start_local_kafka.sh. Missing or stale PID files
# are treated as an already-stopped broker to keep cleanup scripts idempotent.
pto_stop_pid_file "Local Kafka" "$PID_FILE" 45