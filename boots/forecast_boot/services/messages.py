from __future__ import annotations

from dataclasses import dataclass, field

from generated import trading_messages_pb2


@dataclass
class ForecastInMessages:
    """In-memory holder for inbound payloads consumed by forecast boot.

    Keeping this transport object explicit makes service<->algorithm boundaries
    easy to evolve without leaking Kafka details into the algorithm layer.
    """

    data_boot_to_forecast_boot: bytes | None = None
    data_boot_to_forecast_boot_queue: list[bytes] = field(default_factory=list)


@dataclass
class ForecastOutMessages:
    """In-memory holder for outbound payloads produced by forecast boot.

    This dataclass is intentionally located in the messages module as the shared
    contract object between orchestration and algorithm components.
    """

    forecast_boot_to_execution_boot: trading_messages_pb2.ForecastEvent | None = None
    forecast_boot_to_execution_boot_queue: list[trading_messages_pb2.ForecastEvent] = field(default_factory=list)