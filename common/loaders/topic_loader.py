"""Load compiled protobuf modules from the repository-local generated directory.

The module names are driven by the repository configuration so application
code does not need to hardcode generated pb2 module names.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from common.config_loader import load_runtime_config
from common.paths import GENERATED_PARENT_DIR, ensure_artifact_directories


ensure_artifact_directories()

if str(GENERATED_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATED_PARENT_DIR))


def _load_proto_module(from_boot: str, to_boot: str) -> ModuleType:
    runtime_config = load_runtime_config()
    topic_routes = runtime_config.get("topic", {}) if isinstance(runtime_config.get("topic", {}), dict) else {}
    route_key = f"{from_boot}_to_{to_boot}"
    route = topic_routes.get(route_key)
    module_name = route.get("proto_name") if isinstance(route, dict) else None

    if not isinstance(module_name, str) or not module_name:
        raise KeyError(f"Proto route not configured: {from_boot} -> {to_boot}")

    return importlib.import_module(module_name)


weather_pb2 = _load_proto_module("data_boot", "forecast_boot")
trading_messages_pb2 = _load_proto_module("forecast_boot", "execution_boot")


__all__ = ["weather_pb2", "trading_messages_pb2", "_load_proto_module"]