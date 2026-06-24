#!/usr/bin/env bash

set -euo pipefail

# Resolve the external artifact directory relative to the repository root.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
PID_FILE="$ARTIFACTS_ROOT/kafka-local/kafka.pid"

# Treat a missing PID file as an already-stopped broker.
if [[ ! -f "$PID_FILE" ]]; then
    echo "Local Kafka is not running"
    exit 0
fi

# Stop the Kafka broker registered by `start_local_kafka.sh`.
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