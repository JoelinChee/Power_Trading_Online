from __future__ import annotations

from pathlib import Path
from typing import Any

from common.loaders.loader_base import BaseConfigReader


class BootsConfigLoader(BaseConfigReader):
    """Loader for boot runtime settings from boots_config.yaml."""

    _default_config_path = Path(__file__).resolve().parents[2] / "config" / "common" / "boots_config.yaml"

    @classmethod
    def _load_root(cls) -> dict[str, Any]:
        loaded = cls.load_config_dict(cls._default_config_path)
        return loaded.get("boots", {}) if isinstance(loaded.get("boots", {}), dict) else {}

    @classmethod
    def get_boot(cls, boot_name: str) -> dict[str, Any]:
        root = cls._load_root()
        boot_config = root.get(boot_name)
        if not isinstance(boot_config, dict):
            raise KeyError(f"Boot config not configured: {boot_name}")
        return dict(boot_config)


__all__ = ["BootsConfigLoader"]