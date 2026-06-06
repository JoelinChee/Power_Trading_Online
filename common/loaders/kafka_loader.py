"""Repository-local configuration loader backed entirely by config.yaml.

This module contains no hardcoded runtime parameter values. It only reads the
YAML file and exposes the resulting sections as boot settings objects.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from common.config_loader_base import BaseConfigReader


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "common" / "config.yaml"


class BootSettings:
    """Lightweight settings wrapper around a loaded configuration section."""

    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values
        self._topic_routes = values.get("topic_routes", {}) if isinstance(values.get("topic_routes", {}), dict) else {}
        self._proto_routes = values.get("proto_routes", {}) if isinstance(values.get("proto_routes", {}), dict) else {}

    def __getattr__(self, name: str) -> Any:
        if name in self._values:
            return self._values[name]
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")

    def topic_name(self, from_boot: str, to_boot: str) -> str:
        route_key = self._route_key(from_boot, to_boot)
        route = self._topic_routes.get(route_key)
        if isinstance(route, dict) and isinstance(route.get("topic_name"), str):
            return route["topic_name"]
        raise KeyError(f"Topic route not configured: {from_boot} -> {to_boot}")

    def topic_route(self, from_boot: str, to_boot: str) -> Dict[str, Any]:
        route_key = self._route_key(from_boot, to_boot)
        route = self._topic_routes.get(route_key)
        if isinstance(route, dict):
            return route
        raise KeyError(f"Topic route not configured: {from_boot} -> {to_boot}")

    def proto_name(self, from_boot: str, to_boot: str) -> str:
        route_key = self._route_key(from_boot, to_boot)
        route = self._proto_routes.get(route_key)
        if isinstance(route, dict) and isinstance(route.get("proto_name"), str):
            return route["proto_name"]
        raise KeyError(f"Proto route not configured: {from_boot} -> {to_boot}")

    def proto_module_name(self, from_boot: str, to_boot: str) -> str:
        return self.proto_name(from_boot, to_boot)

    def proto_route(self, from_boot: str, to_boot: str) -> Dict[str, Any]:
        route_key = self._route_key(from_boot, to_boot)
        route = self._proto_routes.get(route_key)
        if isinstance(route, dict):
            return route
        raise KeyError(f"Proto route not configured: {from_boot} -> {to_boot}")

    def consumer_group(self, group_suffix: str) -> str:
        base_group = f"{self.kafka_consumer_group_prefix}.{self.service_name}.{group_suffix}"
        if self.kafka_consumer_group_salt:
            return f"{base_group}.{self.kafka_consumer_group_salt}"
        return base_group

    @staticmethod
    def _route_key(from_boot: str, to_boot: str) -> str:
        return f"{from_boot}_to_{to_boot}"


class BaseBootSettings(BootSettings):
    """Base configuration shared by every boot application."""

    def __init__(self, section_name: str) -> None:
        raw_config = BaseConfigReader.load_config_dict(CONFIG_PATH)
        base_values = raw_config.get("base", {}) if isinstance(raw_config.get("base", {}), dict) else {}
        section_values = raw_config.get(section_name, {}) if isinstance(raw_config.get(section_name, {}), dict) else {}
        topic_values = raw_config.get("topic", {}) if isinstance(raw_config.get("topic", {}), dict) else {}
        proto_values = raw_config.get("proto", {}) if isinstance(raw_config.get("proto", {}), dict) else {}
        merged_values = BaseConfigReader.merge_config_sections(base_values, section_values)
        merged_values["topic_routes"] = topic_values
        merged_values["proto_routes"] = proto_values
        super().__init__(merged_values)


def load_runtime_config() -> dict[str, Any]:
    """Load the raw repository-local runtime config dictionary."""

    return BaseConfigReader.load_config_dict(CONFIG_PATH)


class DataBootSettings(BaseBootSettings):
    """Settings for the data acquisition boot service."""

    def __init__(self) -> None:
        super().__init__("data_boot")


class ForecastBootSettings(BaseBootSettings):
    """Settings for the forecasting boot service."""

    def __init__(self) -> None:
        super().__init__("forecast_boot")


class ExecutionBootSettings(BaseBootSettings):
    """Settings for the execution and risk-control boot service."""

    def __init__(self) -> None:
        super().__init__("execution_boot")


__all__ = ["BootSettings", "BaseBootSettings", "DataBootSettings", "ForecastBootSettings", "ExecutionBootSettings", "load_runtime_config"]