from __future__ import annotations

import logging
from functools import lru_cache

from boots.execution_boot.services.algo import ExecutionAlgo
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import KafkaConsumerWorker
from common.schemas import (
    PipelineStatusResponse,
    RiskCheckRequest,
    RiskCheckResponse,
    TradeOrderRequest,
    TradeOrderResponse,
)


logger = logging.getLogger(__name__)


class ExecutionService:
    """Service layer for synchronous execution APIs and forecast topic consumption."""

    def __init__(self) -> None:
        self.boot_config = BootsConfigLoader.get_boot("execution_boot")
        self.kafka_config = KafkaConfigLoader.get_kafka()
        self.kafka_settings = KafkaRuntimeSettings(
            service_name=self.boot_config["service_name"],
            kafka_config=self.kafka_config,
        )
        self.forecast_topic_name = TopicConfigLoader.topic_name("forecast_boot", "execution_boot")

        self.algo = ExecutionAlgo(service_name=self.boot_config["service_name"], logger=logger)
        self.forecast_consumer = KafkaConsumerWorker(
            settings=self.kafka_settings,
            topic_name=self.forecast_topic_name,
            group_suffix="decision",
            handler=self._handle_forecast_event,
        )

    def risk_check(self, request: RiskCheckRequest) -> RiskCheckResponse:
        """Evaluate a basic rule-based risk decision.

        Args:
            request: Risk evaluation request containing load, price, budget, and
                renewable coverage information.

        Returns:
            A risk decision object with approval flag, score, and reasons.
        """
        return self.algo.risk_check(request)

    def create_trade_order(self, request: TradeOrderRequest) -> TradeOrderResponse:
        """Create a mock day-ahead purchase order from a validated request."""
        return self.algo.create_trade_order(request)

    def start_pipeline(self) -> None:
        """Start the forecast topic consumer."""
        self.forecast_consumer.start()

    def stop_pipeline(self) -> None:
        """Stop the forecast topic consumer."""
        self.forecast_consumer.stop()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return latest consumed forecast event and processed execution result."""

        return PipelineStatusResponse(
            service_name=self.boot_config["service_name"],
            last_consumed_event_id=(self.algo.last_consumed_event or {}).get("event_id"),
            last_published_event_id=(self.algo.last_processed_result or {}).get("event_id"),
            details={
                "last_consumed_event": self.algo.last_consumed_event or {},
                "last_processed_result": self.algo.last_processed_result or {},
            },
        )

    def _handle_forecast_event(self, payload: bytes) -> None:
        """Consume forecast payload and execute risk/order processing."""
        self.algo.handle_forecast_event(payload)


@lru_cache(maxsize=1)
def get_execution_service() -> ExecutionService:
    """Return a singleton `ExecutionService` instance for the FastAPI process."""
    return ExecutionService()