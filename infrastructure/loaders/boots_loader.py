from __future__ import annotations

from typing import Any

from infrastructure.loaders.loader_base import BaseConfigReader


class BootsConfigLoader(BaseConfigReader):
    """Loader for boot runtime settings from boots_config.yaml."""

    _default_config_path = BaseConfigReader.runtime_config_path("config", "common", "boots_config.yaml")

    @classmethod
    def _config_path(cls):
        return cls.runtime_config_path("config", "common", "boots_config.yaml")

    @classmethod
    def _load_root(cls) -> dict[str, Any]:
        return cls.load_config_section(cls._config_path(), "boots")

    @classmethod
    def get_boot(cls, boot_name: str) -> dict[str, Any]:
        root = cls._load_root()
        boot_config = root.get(boot_name)
        if not isinstance(boot_config, dict):
            raise KeyError(f"Boot config not configured: {boot_name}")
        return dict(boot_config)


__all__ = ["BootsConfigLoader"]
