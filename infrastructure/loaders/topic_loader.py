from __future__ import annotations

from typing import Any

from infrastructure.loaders.loader_base import BaseConfigReader


class TopicConfigLoader(BaseConfigReader):
    """Loader for topic and proto route settings from topic_config.yaml."""

    _default_config_path = BaseConfigReader.runtime_config_path("config", "common", "topic_config.yaml")

    @classmethod
    def _config_path(cls):
        return cls.runtime_config_path("config", "common", "topic_config.yaml")

    @classmethod
    def _load_routes(cls) -> dict[str, Any]:
        return cls.load_config_section(cls._config_path(), "topic")

    @staticmethod
    def _route_key(from_boot: str, to_boot: str) -> str:
        return f"{from_boot}_to_{to_boot}"

    @classmethod
    def get_route(cls, from_boot: str, to_boot: str) -> dict[str, Any]:
        routes = cls._load_routes()
        route_key = cls._route_key(from_boot, to_boot)
        route = routes.get(route_key)
        if not isinstance(route, dict):
            raise KeyError(f"Topic route not configured: {from_boot} -> {to_boot}")
        return dict(route)

    @classmethod
    def topic_name(cls, from_boot: str, to_boot: str) -> str:
        route = cls.get_route(from_boot, to_boot)
        topic_name = route.get("topic_name")
        if not isinstance(topic_name, str) or not topic_name:
            raise KeyError(f"Topic name not configured: {from_boot} -> {to_boot}")
        return topic_name

    @classmethod
    def proto_name(cls, from_boot: str, to_boot: str) -> str:
        route = cls.get_route(from_boot, to_boot)
        proto_name = route.get("proto_name")
        if not isinstance(proto_name, str) or not proto_name:
            raise KeyError(f"Proto name not configured: {from_boot} -> {to_boot}")
        return proto_name


__all__ = ["TopicConfigLoader"]