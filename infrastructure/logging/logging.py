"""Centralized logging configuration for all services in this system."""

from __future__ import annotations

import logging
import os


DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DEFAULT_LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "DEBUG").upper()


def configure_logging(level: int | str | None = None, *, force: bool = True) -> None:
	"""Configure root logging once for the whole process.

	Args:
		level: Optional override for log level. Defaults to APP_LOG_LEVEL or DEBUG.
		force: Whether to overwrite existing handlers. True keeps behavior predictable
			under uvicorn and repeated imports.
	"""

	logging.basicConfig(
		level=level or DEFAULT_LOG_LEVEL,
		format=DEFAULT_LOG_FORMAT,
		force=force,
	)

