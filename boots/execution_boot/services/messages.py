from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ExecutionInMessages:
    """In-memory holder for inbound payloads consumed by execution boot."""

    forecast_boot_to_execution_boot_queue: list[bytes] = field(default_factory=list)
