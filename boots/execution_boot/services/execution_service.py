from __future__ import annotations

import threading
from collections import deque
from functools import lru_cache
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from common.config import ExecutionBootSettings
from common.kafka import KafkaConsumerWorker
from common.proto_loader import trading_messages_pb2
from common.schemas import (
    PipelineStatusResponse,
    RiskCheckRequest,
    RiskCheckResponse,
    TradeOrderRequest,
    TradeOrderResponse,
)
from common.timer import PeriodicWorker


class ExecutionService:
    """Service layer for synchronous execution APIs and Kafka-consumed decisions."""

    def __init__(self) -> None:
        self.settings = ExecutionBootSettings(host="0.0.0.0", port=8003)
        self.forecast_consumer = KafkaConsumerWorker(
            settings=self.settings,
            topic_suffix="forecast.events",
            group_suffix="decision",
            handler=self._handle_forecast_event,
        )
        self._pending_payloads: deque[bytes] = deque()
        self._pending_lock = threading.Lock()
        self._timer_worker = PeriodicWorker(
            name=f"{self.settings.service_name}-timer",
            interval_seconds=self.settings.timer_interval_seconds,
            callback=self._flush_pending_forecast_events,
        )
        self.last_consumed_event: dict[str, object] | None = None
        self.last_published_event: dict[str, object] | None = None

    def risk_check(self, request: RiskCheckRequest) -> RiskCheckResponse:
        """Evaluate a basic rule-based risk decision.

        Args:
            request: Risk evaluation request containing load, price, budget, and
                renewable coverage information.

        Returns:
            A risk decision object with approval flag, score, and reasons.
        """
        reasons = []
        risk_score = 0.0

        expected_cost = request.predicted_load_mw * request.bid_price
        if expected_cost > request.budget_limit:
            reasons.append("Expected cost exceeds budget limit")
            risk_score += 45.0

        if request.available_renewable_mw < request.predicted_load_mw * 0.2:
            reasons.append("Renewable coverage ratio is below 20%")
            risk_score += 30.0

        if request.bid_price > 520:
            reasons.append("Bid price exceeds internal price ceiling")
            risk_score += 35.0

        approved = risk_score < 60.0
        response = RiskCheckResponse(
            enterprise_id=request.enterprise_id,
            approved=approved,
            risk_score=min(risk_score, 100.0),
            reasons=reasons or ["Risk within threshold"],
        )
        return response

    def create_trade_order(self, request: TradeOrderRequest) -> TradeOrderResponse:
        """Create a mock day-ahead purchase order from a validated request."""
        quantity = round(request.predicted_load_mw * 24, 2)
        response = TradeOrderResponse(
            order_id=str(uuid4()),
            enterprise_id=request.enterprise_id,
            order_type="DAY_AHEAD_BUY",
            quantity_mwh=quantity,
            limit_price=request.predicted_price,
            target_date=request.target_date,
            status="CREATED",
        )
        return response

    def start_pipeline(self) -> None:
        """Start the Kafka consumer and timer-driven execution worker."""
        self.forecast_consumer.start()
        self._timer_worker.start()

    def stop_pipeline(self) -> None:
        """Stop the forecast-event Kafka consumer and timer-driven worker."""
        self.forecast_consumer.stop()
        self._timer_worker.stop()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return the latest consumed forecast event and generated execution result."""
        with self._pending_lock:
            pending_event_count = len(self._pending_payloads)

        return PipelineStatusResponse(
            service_name=self.settings.service_name,
            last_consumed_event_id=(self.last_consumed_event or {}).get("event_id"),
            last_published_event_id=(self.last_published_event or {}).get("event_id"),
            details={
                "last_consumed_event": self.last_consumed_event or {},
                "last_published_event": self.last_published_event or {},
                "pending_event_count": pending_event_count,
                "timer_interval_seconds": self.settings.timer_interval_seconds,
            },
        )

    def _handle_forecast_event(self, payload: bytes) -> None:
        """Queue a forecast event payload for timer-driven execution.

        Args:
            payload: Binary protobuf payload consumed from the forecast topic.
        """
        event = trading_messages_pb2.ForecastEvent()
        event.ParseFromString(payload)
        self.last_consumed_event = MessageToDict(event, preserving_proto_field_name=True)

        with self._pending_lock:
            self._pending_payloads.append(payload)

    def _flush_pending_forecast_events(self) -> None:
        """Process all queued forecast events on the current timer tick."""
        with self._pending_lock:
            pending_payloads = list(self._pending_payloads)
            self._pending_payloads.clear()

        for payload in pending_payloads:
            self._process_forecast_event(payload)

    def _process_forecast_event(self, payload: bytes) -> None:
        """Convert one queued forecast event into a risk result and trade order event."""
        event = trading_messages_pb2.ForecastEvent()
        event.ParseFromString(payload)

        predicted_load_mw = sum(point.value for point in event.load_points) / max(len(event.load_points), 1)
        predicted_price = sum(point.value for point in event.price_points) / max(len(event.price_points), 1)
        budget_limit = predicted_load_mw * 460
        risk_request = RiskCheckRequest(
            enterprise_id=event.enterprise_id,
            predicted_load_mw=predicted_load_mw,
            budget_limit=budget_limit,
            bid_price=predicted_price,
            available_renewable_mw=event.available_renewable_mw,
        )
        risk_response = self.risk_check(risk_request)

        execution_event = trading_messages_pb2.ExecutionEvent()
        execution_event.event_id = str(uuid4())
        execution_event.source_service = self.settings.service_name
        execution_event.upstream_event_id = event.event_id
        execution_event.published_at = event.published_at
        execution_event.enterprise_id = event.enterprise_id
        execution_event.target_date = event.target_date
        execution_event.approved = risk_response.approved
        execution_event.risk_score = risk_response.risk_score
        execution_event.reasons.extend(risk_response.reasons)

        if risk_response.approved:
            trade_response = self.create_trade_order(
                TradeOrderRequest(
                    enterprise_id=event.enterprise_id,
                    target_date=event.target_date,
                    predicted_load_mw=predicted_load_mw,
                    predicted_price=predicted_price,
                    approved=True,
                )
            )
            execution_event.order_id = trade_response.order_id
            execution_event.order_type = trade_response.order_type
            execution_event.quantity_mwh = trade_response.quantity_mwh
            execution_event.limit_price = trade_response.limit_price
            execution_event.status = trade_response.status
        else:
            execution_event.status = "REJECTED"

        self.last_published_event = MessageToDict(execution_event, preserving_proto_field_name=True)


@lru_cache(maxsize=1)
def get_execution_service() -> ExecutionService:
    """Return a singleton `ExecutionService` instance for the FastAPI process."""
    return ExecutionService()