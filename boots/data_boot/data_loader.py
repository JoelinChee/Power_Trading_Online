from __future__ import annotations

from typing import Any

from infrastructure.loaders.loader_base import BaseConfigReader


class DataAlgoConfigLoader(BaseConfigReader):
	"""Loader for data_boot algorithm parameters."""

	_default_config_path = BaseConfigReader.runtime_config_path("config", "data_boot", "config.yaml")

	@classmethod
	def _config_path(cls):
		return cls.runtime_config_path("config", "data_boot", "config.yaml")

	@classmethod
	def get_algo_config(cls) -> dict[str, Any]:
		algo_config = cls.load_config_section(cls._config_path(), "algo")
		return {
			"region_code": str(algo_config.get("region_code", "CN-SH")),
			"region_name": str(algo_config.get("region_name", "Shanghai")),
			"base_temperature_celsius": float(algo_config.get("base_temperature_celsius", 29.5)),
			"base_humidity_percent": float(algo_config.get("base_humidity_percent", 74.0)),
			"base_pressure_hpa": float(algo_config.get("base_pressure_hpa", 1008.0)),
			"day_visibility_km": float(algo_config.get("day_visibility_km", 8.5)),
			"night_visibility_km": float(algo_config.get("night_visibility_km", 6.2)),
			"day_cloud_cover_percent": float(algo_config.get("day_cloud_cover_percent", 38.0)),
			"night_cloud_cover_percent": float(algo_config.get("night_cloud_cover_percent", 68.0)),
			"rain_start_hour": int(algo_config.get("rain_start_hour", 14)),
			"rain_end_hour": int(algo_config.get("rain_end_hour", 17)),
			"rain_amount_mm": float(algo_config.get("rain_amount_mm", 1.6)),
			"rain_probability_percent": float(algo_config.get("rain_probability_percent", 62.0)),
			"dry_probability_percent": float(algo_config.get("dry_probability_percent", 12.0)),
		}


__all__ = ["DataAlgoConfigLoader"]

