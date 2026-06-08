"""Shared configuration loader for runnable modules.

This module keeps the old singleton-style API but removes the dependency on
third-party YAML/logging helpers so it can run in a clean repository checkout.
"""

from __future__ import annotations

from ast import literal_eval
from pathlib import Path
from typing import Any, Optional, Union
import json

from infrastructure.logging.logging import get_logger


logger = get_logger(__name__)

class BaseConfigReader:
    """Reusable singleton configuration reader.

    Subclasses must define `ConfigValues`, `_default_config_path`, and
    `_field_mapping`.
    """

    _instance = None
    _config = None
    config_value = None
    _default_config_path: Optional[Path] = None
    _field_mapping: dict[str, tuple[str, Any, str]] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.config_value = cls.ConfigValues()
            cls._load_config(config_path)
        elif config_path is not None:
            cls._load_config(config_path)
        return cls._instance

    def __getattr__(self, name: str) -> Any:
        config_value = object.__getattribute__(self, "config_value")
        if config_value is not None and hasattr(config_value, name):
            return getattr(config_value, name)
        raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")

    @classmethod
    def load_config_dict(cls, config_path: Optional[Union[Path, str]]) -> dict[str, Any]:
        """Load a config file into a plain dictionary without applying mappings."""

        resolved_path = Path(config_path) if config_path else cls._default_config_path
        if resolved_path is None or not resolved_path.exists():
            cls._log_warn("Config file not found, using empty config: %s", resolved_path)
            return {}
        loaded = cls._read_config_file(resolved_path)
        if not isinstance(loaded, dict):
            raise ValueError(f"Config file format invalid: root must be dict ({resolved_path})")
        return loaded

    @classmethod
    def merge_config_sections(cls, base_config: dict[str, Any], section_config: dict[str, Any]) -> dict[str, Any]:
        """Deep-merge a base section with a specific service section."""

        merged = dict(base_config)
        for key, value in section_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = cls.merge_config_sections(merged[key], value)
            else:
                merged[key] = value
        return merged

    @classmethod
    def _log_error(cls, message: str, *args: Any) -> None:
        logger.error(message, *args)

    @classmethod
    def _log_info(cls, message: str, *args: Any) -> None:
        logger.info(message, *args)

    @classmethod
    def _log_warn(cls, message: str, *args: Any) -> None:
        logger.warning(message, *args)

    @classmethod
    def _parse_int(
        cls,
        raw_value: Any,
        key: str,
        default: Optional[int],
    ) -> Optional[int]:
        if raw_value is None:
            return default
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            cls._log_warn("Invalid int value for '%s': %r, fallback to %r", key, raw_value, default)
            return default

    @classmethod
    def _parse_bool(
        cls,
        raw_value: Any,
        key: str,
        default: bool,
    ) -> bool:
        if isinstance(raw_value, bool):
            return raw_value
        if isinstance(raw_value, str):
            lowered = raw_value.strip().lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "no", "n", "off"}:
                return False
        if raw_value is None:
            return default
        cls._log_warn("Invalid bool value for '%s': %r, fallback to %r", key, raw_value, default)
        return default

    @classmethod
    def _read_value(cls, config: dict[str, Any], key: str, default: Any) -> Any:
        if key in config:
            return config.get(key, default)

        current: Any = config
        for segment in key.split("."):
            if not isinstance(current, dict) or segment not in current:
                return default
            current = current[segment]
        return current

    @classmethod
    def _load_config(cls, config_path: Optional[str] = None) -> bool:
        resolved_path = Path(config_path) if config_path else cls._default_config_path
        loaded: dict[str, Any] = {}

        if resolved_path is not None and resolved_path.exists():
            try:
                loaded = cls._read_config_file(resolved_path)
            except OSError as error:
                cls._log_error("Config file read failed (%s): %s", resolved_path, error)
                loaded = {}
            except ValueError as error:
                cls._log_error("Config parsing failed (%s): %s", resolved_path, error)
                loaded = {}
        else:
            cls._log_warn("Config file not found, using defaults: %s", resolved_path)

        if not isinstance(loaded, dict):
            cls._log_error("Config file format invalid: root must be dict (%s)", resolved_path)
            loaded = {}

        cls._config = loaded

        for field_name, mapping in cls._field_mapping.items():
            config_key, default_value, value_type = mapping
            raw_value = cls._read_value(cls._config, config_key, default_value)

            if value_type == "bool":
                parsed_value = cls._parse_bool(raw_value, config_key, bool(default_value))
            elif value_type == "int":
                parsed_value = cls._parse_int(raw_value, config_key, default_value)
            else:
                parsed_value = raw_value

            setattr(cls._instance.config_value, field_name, parsed_value)

        cls._log_info("Config loaded: %s", resolved_path)
        return True

    @classmethod
    def _read_config_file(cls, resolved_path: Path) -> dict[str, Any]:
        text = resolved_path.read_text(encoding="utf-8")
        stripped = text.lstrip()
        if not stripped:
            return {}

        if stripped.startswith("{") or stripped.startswith("["):
            loaded = json.loads(text)
            if not isinstance(loaded, dict):
                raise ValueError("JSON root must be an object")
            return loaded

        return cls._parse_simple_yaml(text)

    @classmethod
    def _parse_simple_yaml(cls, text: str) -> dict[str, Any]:
        """Parse a small YAML subset with nested dictionaries.

        Supported features:
        - `key: value`
        - nested mappings through indentation
        - scalars: bool, int, float, null, quoted strings, and inline JSON-ish
          literals handled by `ast.literal_eval`
        """

        root: dict[str, Any] = {}
        stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            if "\t" in raw_line:
                raise ValueError(f"Tabs are not supported in YAML indentation (line {line_number})")

            indent = len(raw_line) - len(raw_line.lstrip(" "))
            if ":" not in stripped:
                raise ValueError(f"Invalid config line {line_number}: {raw_line!r}")

            key, raw_value = stripped.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()

            while stack and indent <= stack[-1][0]:
                stack.pop()

            if not stack:
                raise ValueError(f"Invalid indentation at line {line_number}")

            current = stack[-1][1]
            if not raw_value:
                nested: dict[str, Any] = {}
                current[key] = nested
                stack.append((indent, nested))
                continue

            current[key] = cls._parse_scalar(raw_value)

        return root

    @staticmethod
    def _parse_scalar(raw_value: str) -> Any:
        lowered = raw_value.lower()
        if lowered in {"null", "none", "~"}:
            return None
        if lowered in {"true", "yes", "on"}:
            return True
        if lowered in {"false", "no", "off"}:
            return False

        if (raw_value.startswith('"') and raw_value.endswith('"')) or (
            raw_value.startswith("'") and raw_value.endswith("'")
        ):
            try:
                return literal_eval(raw_value)
            except (SyntaxError, ValueError):
                return raw_value[1:-1]

        try:
            return int(raw_value)
        except ValueError:
            pass

        try:
            return float(raw_value)
        except ValueError:
            pass

        if (raw_value.startswith("[") and raw_value.endswith("]")) or (
            raw_value.startswith("{") and raw_value.endswith("}")
        ):
            try:
                return literal_eval(raw_value)
            except (SyntaxError, ValueError):
                return raw_value

        return raw_value


__all__ = ["BaseConfigReader"]