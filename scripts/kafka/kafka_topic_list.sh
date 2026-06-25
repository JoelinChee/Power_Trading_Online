#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

BROKERS="${BROKERS:-127.0.0.1:9092}"

usage() {
    cat <<EOF
Usage: $(basename "$0") [--brokers HOST:PORT]

List Kafka topics with kcat.

Environment:
  BROKERS   Kafka bootstrap servers. Default: 127.0.0.1:9092
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --brokers|-b)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for $1" >&2
                usage >&2
                exit 1
            fi
            BROKERS="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

pto_require_command kcat "Install kcat/kafkacat before listing topics."
kcat -b "$BROKERS" -L