from __future__ import annotations

from pathlib import Path
import os
from typing import Any

from boots.common.loader_base import BaseConfigReader


class ForecastAlgoConfigLoader(BaseConfigReader):
	"""Loader for forecast_boot algorithm parameters."""

	_default_config_path = Path(__file__).resolve().parents[2] / "config" / "forecast_boot" / "config.yaml"

	@classmethod
	def _config_path(cls) -> Path:
		runtime_root = os.getenv("POWER_TRADING_HOME")
		if runtime_root:
			return Path(runtime_root).resolve() / "config" / "forecast_boot" / "config.yaml"
		return cls._default_config_path

	@classmethod
	def get_algo_config(cls) -> dict[str, Any]:
		loaded = cls.load_config_dict(cls._config_path())
		algo_config = loaded.get("algo") if isinstance(loaded.get("algo"), dict) else {}
		return {
			"slots_per_day": int(algo_config.get("slots_per_day", 96)),
			"default_weather_type": str(algo_config.get("default_weather_type", "unknown")),
			"default_enterprise_id": str(algo_config.get("default_enterprise_id", "default-enterprise")),
			"default_base_temperature_celsius": float(algo_config.get("default_base_temperature_celsius", 28.0)),
			"default_base_wind_speed_mps": float(algo_config.get("default_base_wind_speed_mps", 4.0)),
			"base_load_mw": float(algo_config.get("base_load_mw", 60.0)),
			"base_price_yuan_mwh": float(algo_config.get("base_price_yuan_mwh", 420.0)),
			"load_temp_threshold_celsius": float(algo_config.get("load_temp_threshold_celsius", 20.0)),
			"load_temp_factor": float(algo_config.get("load_temp_factor", 1.2)),
			"price_temp_threshold_celsius": float(algo_config.get("price_temp_threshold_celsius", 24.0)),
			"price_temp_factor": float(algo_config.get("price_temp_factor", 2.8)),
			"renewable_wind_factor": float(algo_config.get("renewable_wind_factor", 2.2)),
			"renewable_solar_divisor": float(algo_config.get("renewable_solar_divisor", 40.0)),
			"renewable_fallback_mw": float(algo_config.get("renewable_fallback_mw", 8.0)),
		}


__all__ = ["ForecastAlgoConfigLoader"]

