#!/usr/bin/env bash

set -euo pipefail

# Resolve all important paths from the script location so first-run setup works
# from any current working directory.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
LEGACY_ARTIFACTS_ROOT="$REPO_ROOT/../Power_Trading_Online_artifacts"
PYCACHE_DIR="$ARTIFACTS_ROOT/pycache"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Create external artifact folders up front so setup never writes caches into the repo.
mkdir -p "$ARTIFACTS_ROOT" "$PYCACHE_DIR"

# Remove the old sibling artifact directory left by previous repository layouts.
if [[ -d "$LEGACY_ARTIFACTS_ROOT" ]]; then
    rm -rf "$LEGACY_ARTIFACTS_ROOT"
fi

# Bootstrap the repository-local environment file on the first setup run.
if [[ ! -f "$REPO_ROOT/.env" ]]; then
    cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
fi

# Clear repository-local Python bytecode and stale JVM crash replay files left by older runs.
find "$REPO_ROOT" -type d -name '__pycache__' -prune -exec rm -rf {} +
find "$REPO_ROOT" \( -name '*.pyc' -o -name '*.pyo' -o -name 'hs_err_pid*.log' -o -name 'replay_pid*.log' \) -delete

# Install Python dependencies from the environment manifest using the selected interpreter.
PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$PYTHON_BIN" -m pip install -r "$REPO_ROOT/environment/requirements.txt"

# Compile protobuf modules into the repository-local generated directory.
"$SCRIPT_DIR/compile_protos.sh"

# Download, extract, and verify the local Kafka distribution once so the first
# startup has everything it needs.
"$SCRIPT_DIR/start_local_kafka.sh"

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
        break
    fi
    sleep 1
done

# Stop Kafka after verification so the stack can later be started cleanly with start_all.sh.
"$SCRIPT_DIR/stop_local_kafka.sh"

echo "Environment installation completed. Use ./scripts/start_all.sh to launch the platform."