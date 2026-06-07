from __future__ import annotations

from pathlib import Path
import os
from typing import Any

from boots.common.loader_base import BaseConfigReader


class ExecutionAlgoConfigLoader(BaseConfigReader):
	"""Loader for execution_boot algorithm parameters."""

	_default_config_path = Path(__file__).resolve().parents[2] / "config" / "execution_boot" / "config.yaml"

	@classmethod
	def _config_path(cls) -> Path:
		runtime_root = os.getenv("POWER_TRADING_HOME")
		if runtime_root:
			return Path(runtime_root).resolve() / "config" / "execution_boot" / "config.yaml"
		return cls._default_config_path

	@classmethod
	def get_algo_config(cls) -> dict[str, Any]:
		loaded = cls.load_config_dict(cls._config_path())
		algo_config = loaded.get("algo") if isinstance(loaded.get("algo"), dict) else {}
		return {
			"expected_cost_risk_weight": float(algo_config.get("expected_cost_risk_weight", 45.0)),
			"renewable_coverage_risk_weight": float(algo_config.get("renewable_coverage_risk_weight", 30.0)),
			"high_price_risk_weight": float(algo_config.get("high_price_risk_weight", 35.0)),
			"renewable_coverage_threshold_ratio": float(algo_config.get("renewable_coverage_threshold_ratio", 0.2)),
			"high_price_threshold": float(algo_config.get("high_price_threshold", 520.0)),
			"approval_risk_threshold": float(algo_config.get("approval_risk_threshold", 60.0)),
			"max_risk_score": float(algo_config.get("max_risk_score", 100.0)),
			"budget_limit_price_factor": float(algo_config.get("budget_limit_price_factor", 460.0)),
			"order_hours": float(algo_config.get("order_hours", 24.0)),
		}


__all__ = ["ExecutionAlgoConfigLoader"]

