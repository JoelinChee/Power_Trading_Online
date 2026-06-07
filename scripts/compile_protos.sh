#!/usr/bin/env bash

set -euo pipefail

# Resolve the repository root from the script location so the script works no
# matter which directory the user is currently in when invoking it.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ARTIFACTS_ROOT="$REPO_ROOT/generated"
OUTPUT_DIR="$ARTIFACTS_ROOT/generated"
PROTO_DIR="$REPO_ROOT/pub_interfaces"
PYCACHE_DIR="$ARTIFACTS_ROOT/pycache"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Ensure the external generated package exists before protoc writes into it.
mkdir -p "$ARTIFACTS_ROOT" "$PYCACHE_DIR"
find "$ARTIFACTS_ROOT" -maxdepth 1 -name '*_pb2.py' -delete

# Compile every proto definition in the repository into the external generated package.
PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$PYTHON_BIN" -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$ARTIFACTS_ROOT" \
    "$PROTO_DIR"/*.proto

echo "Compiled protobuf modules to $ARTIFACTS_ROOT"