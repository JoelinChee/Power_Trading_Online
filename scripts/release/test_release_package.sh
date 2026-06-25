#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

ARTIFACTS_ROOT="$PTO_ARTIFACTS_ROOT"
RELEASE_DIR="$ARTIFACTS_ROOT/releases"

usage() {
    cat <<'EOF'
Usage:
    scripts/release/test_release_package.sh [archive_path]

If archive_path is not provided, the script picks the newest
power_trading_online-*.tar.gz package under generated/releases.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$RELEASE_DIR"

if [[ $# -ge 1 ]]; then
    ARCHIVE_PATH="$1"
else
    ARCHIVE_PATH="$(ls -t "$RELEASE_DIR"/power_trading_online-*.tar.gz 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "${ARCHIVE_PATH:-}" || ! -f "$ARCHIVE_PATH" ]]; then
    echo "Release archive not found. Build one first with scripts/release/package_release.sh" >&2
    exit 1
fi

set +o pipefail
PACKAGE_ROOT_NAME="$(tar -tzf "$ARCHIVE_PATH" | head -n 1 | cut -d'/' -f1)"
set -o pipefail
if [[ -z "$PACKAGE_ROOT_NAME" ]]; then
    echo "Unable to determine package root directory from archive." >&2
    exit 1
fi

WORK_DIR="$RELEASE_DIR/_test_work"
EXTRACT_DIR="$WORK_DIR/extracted"
rm -rf "$WORK_DIR"
mkdir -p "$EXTRACT_DIR"

tar -xzf "$ARCHIVE_PATH" -C "$EXTRACT_DIR"
PACKAGE_ROOT="$EXTRACT_DIR/$PACKAGE_ROOT_NAME"

if [[ ! -d "$PACKAGE_ROOT" ]]; then
    echo "Extracted package root not found: $PACKAGE_ROOT" >&2
    exit 1
fi

for required_dir in bin lib scripts config; do
    if [[ ! -d "$PACKAGE_ROOT/$required_dir" ]]; then
        echo "Missing required top-level directory: $required_dir" >&2
        exit 1
    fi
done

for entry in "$PACKAGE_ROOT"/*; do
    name="$(basename "$entry")"
    case "$name" in
    bin|lib|scripts|config)
        ;;
    *)
        echo "Forbidden top-level entry found in release package: $name" >&2
        exit 1
        ;;
    esac
done

if find "$PACKAGE_ROOT" -type f -name '*.py' | grep -q .; then
    echo "Release archive contains .py files, but binary-only package is required." >&2
    find "$PACKAGE_ROOT" -type f -name '*.py' >&2
    exit 1
fi

if [[ ! -x "$PACKAGE_ROOT/bin/power_trading_boot" ]]; then
    echo "Missing executable binary: $PACKAGE_ROOT/bin/power_trading_boot" >&2
    exit 1
fi

if [[ ! -f "$PACKAGE_ROOT/.env" && -f "$PACKAGE_ROOT/.env.example" ]]; then
    cp "$PACKAGE_ROOT/.env.example" "$PACKAGE_ROOT/.env"
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required for release runtime health checks." >&2
    exit 1
fi

pushd "$PACKAGE_ROOT" >/dev/null

cleanup() {
    # Always try to stop the extracted package, even when a health check fails.
    # This keeps failed release tests from leaving brokers or service binaries
    # behind on their test ports.
    bash scripts/stop_all.sh >/dev/null 2>&1 || true
}

trap cleanup EXIT

bash scripts/stop_all.sh >/dev/null 2>&1 || true

DATA_BOOT_PORT=18001 FORECAST_BOOT_PORT=18002 EXECUTION_BOOT_PORT=18003 bash scripts/start_all.sh

check_health() {
    local url="$1"
    local expected_service="$2"
    local response

    for _ in {1..40}; do
        response="$(curl -fsS "$url" 2>/dev/null || true)"
        if [[ -n "$response" ]] && [[ "$response" == *'"status":"UP"'* || "$response" == *'"status": "UP"'* ]] && [[ "$response" == *"\"service\":\"${expected_service}\""* || "$response" == *"\"service\": \"${expected_service}\""* ]]; then
            echo "PASS $url -> $response"
            return 0
        fi
        sleep 1
    done

    echo "Health check failed: $url" >&2
    return 1
}

check_health "http://127.0.0.1:18001/health" "data_boot"
check_health "http://127.0.0.1:18002/health" "forecast_boot"
check_health "http://127.0.0.1:18003/health" "execution_boot"

echo "Release package runtime health checks passed"

bash scripts/stop_all.sh
trap - EXIT

popd >/dev/null

echo "Release package test passed: $ARCHIVE_PATH"
