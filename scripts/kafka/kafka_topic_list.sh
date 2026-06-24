#!/usr/bin/env bash

set -euo pipefail

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

kcat -b "$BROKERS" -L