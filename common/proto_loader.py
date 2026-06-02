"""Load compiled protobuf modules from the repository-local generated directory.

The protobuf compiler writes Python modules directly into the repository's
ignored `generated` directory. This loader inserts that directory into
`sys.path` before importing the compiled module so application code can keep a
stable import surface.
"""

from __future__ import annotations

import sys

from common.paths import GENERATED_PARENT_DIR, ensure_artifact_directories


ensure_artifact_directories()

if str(GENERATED_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATED_PARENT_DIR))

import trading_messages_pb2  # noqa: E402