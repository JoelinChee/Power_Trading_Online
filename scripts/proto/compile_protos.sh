#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

PROTO_DIR="$PTO_REPO_ROOT/pub_interfaces"
PYTHON_BIN="${PYTHON_BIN:-python3}"

pto_prepare_runtime_dirs

# Regenerate protobuf modules atomically from the public interface directory.
# Removing only *_pb2.py preserves unrelated generated artifacts such as logs,
# PID files, release packages, and Kafka runtime data.
find "$PTO_ARTIFACTS_ROOT" -maxdepth 1 -name '*_pb2.py' -delete

# Compile every proto definition in the repository into the external generated package.
PYTHONPYCACHEPREFIX="$PTO_PYCACHE_DIR" "$PYTHON_BIN" -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$PTO_ARTIFACTS_ROOT" \
    "$PROTO_DIR"/*.proto

echo "Compiled protobuf modules to $PTO_ARTIFACTS_ROOT"