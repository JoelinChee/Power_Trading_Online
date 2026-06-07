from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ExecutionInMessages:
    """In-memory holder for inbound payloads consumed by execution boot."""

    forecast_boot_to_execution_boot_queue: list[bytes] = field(default_factory=list)


@dataclass
class ExecutionOutMessages:
    """In-memory holder for processed execution results in one batch."""

    execution_result_queue: list[dict[str, object]] = field(default_factory=list)
