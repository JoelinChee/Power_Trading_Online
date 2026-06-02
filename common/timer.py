"""Small reusable periodic worker used by timer-driven boot services."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable


logger = logging.getLogger(__name__)


class PeriodicWorker:
    """Run a callback on a fixed interval in a daemon thread."""

    def __init__(self, *, name: str, interval_seconds: float, callback: Callable[[], None]) -> None:
        self.name = name
        self.interval_seconds = interval_seconds
        self.callback = callback
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the worker thread once."""
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=self.name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the worker thread and wait briefly for completion."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 1.0)
            self._thread = None

    def _run(self) -> None:
        """Wait for the configured interval and invoke the callback repeatedly."""
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self.callback()
            except Exception:  # pragma: no cover
                logger.exception("Periodic worker crashed for task=%s", self.name)