"""Path helpers for repository-local source files and generated runtime folders.

All generated protobuf files, logs, PID files, Kafka binaries, and other runtime
artifacts are intentionally stored under the repository-local `generated`
directory so they are easy to find while still being ignored by Git.
"""

from __future__ import annotations

from pathlib import Path


# The repository root is the parent directory of the shared `common` package.
REPO_ROOT = Path(__file__).resolve().parents[1]

# All mutable runtime state is kept under the repository-local generated directory.
ARTIFACTS_ROOT = REPO_ROOT / "generated"

# Generated protobuf modules are compiled directly into the artifact root.
GENERATED_PARENT_DIR = ARTIFACTS_ROOT
GENERATED_PACKAGE_DIR = ARTIFACTS_ROOT

# Service logs and PID files are separated for easier operational cleanup.
LOG_DIR = ARTIFACTS_ROOT / "logs"
PID_DIR = ARTIFACTS_ROOT / "pids"

# Kafka-specific files are isolated so the broker can be wiped without affecting other artifacts.
KAFKA_ARTIFACTS_DIR = ARTIFACTS_ROOT / "kafka-local"

# Python bytecode cache is also externalized so runtime imports do not dirty the repo.
PYCACHE_DIR = ARTIFACTS_ROOT / "pycache"

# Repository-local, version-controlled directories.
ENVIRONMENT_DIR = REPO_ROOT / "environment"
PROTO_DIR = REPO_ROOT / "pub_interfaces"
ENV_FILE_PATH = REPO_ROOT / ".env"
ENV_TEMPLATE_PATH = REPO_ROOT / ".env.example"


def ensure_artifact_directories() -> None:
    """Create all generated artifact directories required by the project.

    The function is safe to call repeatedly and is used by both Python code and
    shell-entry scripts to make startup deterministic.
    """

    for directory in (ARTIFACTS_ROOT, GENERATED_PACKAGE_DIR, LOG_DIR, PID_DIR, KAFKA_ARTIFACTS_DIR, PYCACHE_DIR):
        directory.mkdir(parents=True, exist_ok=True)