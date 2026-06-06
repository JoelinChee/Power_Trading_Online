"""Load compiled protobuf modules from the repository-local generated directory.

Proto module names are resolved through TopicConfigLoader, which reads only
config/common/topic_config.yaml.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from common.loaders.topic_loader import TopicConfigLoader
from common.paths import GENERATED_PARENT_DIR, ensure_artifact_directories


ensure_artifact_directories()

if str(GENERATED_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATED_PARENT_DIR))


def _load_proto_module(from_boot: str, to_boot: str) -> ModuleType:
    module_name = TopicConfigLoader.proto_name(from_boot, to_boot)
    return importlib.import_module(module_name)


weather_pb2 = _load_proto_module("data_boot", "forecast_boot")
trading_messages_pb2 = _load_proto_module("forecast_boot", "execution_boot")


__all__ = ["weather_pb2", "trading_messages_pb2", "_load_proto_module"]
