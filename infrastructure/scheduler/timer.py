"""Reusable scheduler helpers used by timer-driven boot services."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler


logger = logging.getLogger(__name__)

class AsyncFixedRateScheduler:
    """Run a callback with AsyncIOScheduler on a fixed-rate interval."""

    def __init__(self, *, name: str, interval_seconds: float, callback: Callable[[], None]) -> None:
        self.name = name
        self.interval_seconds = interval_seconds
        self.callback = callback
        self._scheduler = AsyncIOScheduler()
        self._started = False

    def start(self) -> None:
        """Start the AsyncIOScheduler once and register a fixed-rate job."""
        if self._started:
            return

        self._scheduler.add_job(
            self.callback,
            trigger="interval",
            seconds=self.interval_seconds,
            id=self.name,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self._scheduler.start()
        self._started = True

    def stop(self) -> None:
        """Stop the scheduler and remove its jobs."""
        if not self._started:
            return

        self._scheduler.shutdown(wait=False)
        self._started = False