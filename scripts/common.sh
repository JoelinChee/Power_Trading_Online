#!/usr/bin/env bash

# Common helpers for repository scripts.
#
# This file is intentionally small and dependency-free. The scripts in this
# repository are operational entry points: they should work from any current
# directory, fail loudly when prerequisites are missing, and keep runtime files
# under generated/ instead of polluting source folders.

if [[ -z "${BASH_VERSION:-}" ]]; then
    echo "scripts/common.sh must be sourced from bash" >&2
    return 1 2>/dev/null || exit 1
fi

# Resolve the repository layout from this file instead of from the caller. That
# makes every script that sources this file independent of the user's cwd.
PTO_SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PTO_REPO_ROOT="$(cd "$PTO_SCRIPTS_DIR/.." && pwd)"
PTO_ARTIFACTS_ROOT="${PTO_ARTIFACTS_ROOT:-$PTO_REPO_ROOT/generated}"
PTO_PID_DIR="$PTO_ARTIFACTS_ROOT/pids"
PTO_LOG_DIR="$PTO_ARTIFACTS_ROOT/logs"
PTO_PYCACHE_DIR="$PTO_ARTIFACTS_ROOT/pycache"

pto_info() {
    printf '%s\n' "$*"
}

pto_warn() {
    printf 'WARN: %s\n' "$*" >&2
}

pto_die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

pto_require_command() {
    local command_name="$1"
    local install_hint="${2:-}"

    if command -v "$command_name" >/dev/null 2>&1; then
        return 0
    fi

    if [[ -n "$install_hint" ]]; then
        pto_die "$command_name is required. $install_hint"
    fi
    pto_die "$command_name is required."
}

pto_prepare_runtime_dirs() {
    # Keep PID files, logs, and Python bytecode caches in generated/. This is
    # the main guardrail that keeps repeated local runs from dirtying source.
    mkdir -p "$PTO_PID_DIR" "$PTO_LOG_DIR" "$PTO_PYCACHE_DIR"
}

pto_bootstrap_env_file() {
    # First-run convenience: most scripts need .env, but creating it here keeps
    # startup commands pleasant while still letting users edit the file later.
    if [[ ! -f "$PTO_REPO_ROOT/.env" ]]; then
        cp "$PTO_REPO_ROOT/.env.example" "$PTO_REPO_ROOT/.env"
    fi
}

pto_wait_for_tcp() {
    local host="$1"
    local port="$2"
    local attempts="${3:-40}"
    local delay_seconds="${4:-1}"
    local python_bin="${5:-python3}"
    local pycache_dir="${6:-$PTO_PYCACHE_DIR}"

    # Use Python sockets instead of /dev/tcp so the check behaves consistently
    # across bash builds and Linux distributions.
    for _ in $(seq 1 "$attempts"); do
        if PYTHONPYCACHEPREFIX="$pycache_dir" "$python_bin" - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect((host, port))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
        then
            return 0
        fi
        sleep "$delay_seconds"
    done

    return 1
}

pto_stop_pid_file() {
    local name="$1"
    local pid_file="$2"
    local graceful_wait_seconds="${3:-15}"

    if [[ ! -f "$pid_file" ]]; then
        pto_info "$name is not running"
        return 0
    fi

    local pid
    pid="$(cat "$pid_file")"

    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
        pto_warn "$name PID file is invalid: $pid_file"
        rm -f "$pid_file"
        return 0
    fi

    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        for _ in $(seq 1 "$graceful_wait_seconds"); do
            if ! kill -0 "$pid" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid"
        fi
        pto_info "Stopped $name with PID $pid"
    else
        pto_info "$name PID $pid is not active"
    fi

    rm -f "$pid_file"
}

pto_kill_matching_processes() {
    local description="$1"
    local pattern="$2"
    local pids

    pids="$(pgrep -f "$pattern" || true)"
    if [[ -z "$pids" ]]; then
        return 0
    fi

    pto_info "Stopping stale $description process(es): $pids"
    echo "$pids" | xargs kill -9
}