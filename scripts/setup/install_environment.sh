#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

LEGACY_ARTIFACTS_ROOT="$PTO_REPO_ROOT/../Power_Trading_Online_artifacts"
CONDA_ENV_NAME="${CONDA_ENV_NAME:-power_trading_online}"
CONDA_PYTHON_VERSION="${CONDA_PYTHON_VERSION:-3.12}"

pto_prepare_runtime_dirs

# Remove the old sibling artifact directory left by previous repository layouts.
if [[ -d "$LEGACY_ARTIFACTS_ROOT" ]]; then
    rm -rf "$LEGACY_ARTIFACTS_ROOT"
fi

pto_bootstrap_env_file

# Clear repository-local Python bytecode and stale JVM crash replay files left by older runs.
find "$PTO_REPO_ROOT" -type d -name '__pycache__' -prune -exec rm -rf {} +
find "$PTO_REPO_ROOT" \( -name '*.pyc' -o -name '*.pyo' -o -name 'hs_err_pid*.log' -o -name 'replay_pid*.log' \) -delete

# Create and use a dedicated conda environment for all project dependencies.
if ! command -v conda >/dev/null 2>&1; then
    pto_die "conda is required to install the project environment."
fi

CONDA_BASE="$(conda info --base)"
CONDA_ENV_DIR="$CONDA_BASE/envs/$CONDA_ENV_NAME"

# Enable `conda activate` in this shell.
source "$CONDA_BASE/etc/profile.d/conda.sh"

if [[ ! -x "$CONDA_ENV_DIR/bin/python" ]]; then
    conda create -n "$CONDA_ENV_NAME" "python=$CONDA_PYTHON_VERSION" -y
fi

PYTHON_BIN="$CONDA_ENV_DIR/bin/python"

# Install Kafka CLI recorder dependency.
conda install -n "$CONDA_ENV_NAME" -c conda-forge kafkacat -y

# Install Python dependencies from the environment manifest using the selected interpreter.
PYTHONPYCACHEPREFIX="$PTO_PYCACHE_DIR" "$PYTHON_BIN" -m pip install -r "$PTO_REPO_ROOT/environment/requirements.txt"

# Compile protobuf modules into the repository-local generated directory.
PYTHON_BIN="$PYTHON_BIN" "$SCRIPT_DIR/../proto/compile_protos.sh"

# Download, extract, and verify the local Kafka distribution once so the first
# startup has everything it needs.
cleanup_kafka=0
cleanup_started_kafka() {
    if [[ "$cleanup_kafka" -eq 1 ]]; then
        "$SCRIPT_DIR/../kafka/stop_local_kafka.sh" >/dev/null 2>&1 || true
    fi
}

trap cleanup_started_kafka EXIT
"$SCRIPT_DIR/../kafka/start_local_kafka.sh"
cleanup_kafka=1

if ! pto_wait_for_tcp 127.0.0.1 9092 40 1 "$PYTHON_BIN" "$PTO_PYCACHE_DIR"; then
    pto_die "Kafka did not become reachable on 127.0.0.1:9092 during environment verification"
fi

# Stop Kafka after verification so the stack can later be started cleanly with runtime/start_all.sh.
"$SCRIPT_DIR/../kafka/stop_local_kafka.sh"
cleanup_kafka=0
trap - EXIT

echo "Environment installation completed."
echo "Use ./scripts/runtime/start_all.sh to launch the platform."
printf '\n\033[1;33m============================================================\033[0m\n'
printf '\033[1;32mNEXT STEP: activate the project conda environment\033[0m\n\n'
printf '    \033[1;36mconda activate %s\033[0m\n' "$CONDA_ENV_NAME"
printf '\n\033[1;33m============================================================\033[0m\n\n'