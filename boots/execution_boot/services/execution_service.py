from __future__ import annotations

import logging
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


logger = logging.getLogger(__name__)


class ExecutionService:
    """Service layer for synchronous execution APIs and forecast topic consumption."""

    def __init__(self) -> None:
        self.settings = ExecutionBootSettings(host="0.0.0.0", port=8003)
        self.forecast_consumer = KafkaConsumerWorker(
            settings=self.settings,
            topic_suffix="forecast.events",
            group_suffix="decision",
            handler=self._handle_forecast_event,
        )
        self.last_consumed_event: dict[str, object] | None = None
        self.last_processed_result: dict[str, object] | None = None

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
        """Start the forecast topic consumer."""
        self.forecast_consumer.start()

    def stop_pipeline(self) -> None:
        """Stop the forecast topic consumer."""
        self.forecast_consumer.stop()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return latest consumed forecast event and processed execution result."""

        return PipelineStatusResponse(
            service_name=self.settings.service_name,
            last_consumed_event_id=(self.last_consumed_event or {}).get("event_id"),
            last_published_event_id=(self.last_processed_result or {}).get("event_id"),
            details={
                "last_consumed_event": self.last_consumed_event or {},
                "last_processed_result": self.last_processed_result or {},
            },
        )

    def _handle_forecast_event(self, payload: bytes) -> None:
        """Consume forecast.events payload and execute risk/order processing."""
        event = trading_messages_pb2.ForecastEvent()
        event.ParseFromString(payload)
        self.last_consumed_event = MessageToDict(event, preserving_proto_field_name=True)

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

        result: dict[str, object] = {
            "event_id": str(uuid4()),
            "source_service": self.settings.service_name,
            "upstream_event_id": event.event_id,
            "enterprise_id": event.enterprise_id,
            "target_date": event.target_date,
            "approved": risk_response.approved,
            "risk_score": risk_response.risk_score,
            "reasons": list(risk_response.reasons),
        }

        if risk_response.approved:
            trade_order = self.create_trade_order(
                TradeOrderRequest(
                    enterprise_id=event.enterprise_id,
                    target_date=event.target_date,
                    predicted_load_mw=predicted_load_mw,
                    predicted_price=predicted_price,
                    approved=True,
                )
            )
            result["order"] = trade_order.model_dump()
            result["status"] = "CREATED"
        else:
            result["status"] = "REJECTED"

        self.last_processed_result = result
        logger.warning(
            "execution_boot processed forecast event_id=%s approved=%s",
            event.event_id,
            risk_response.approved,
        )


@lru_cache(maxsize=1)
def get_execution_service() -> ExecutionService:
    """Return a singleton `ExecutionService` instance for the FastAPI process."""
    return ExecutionService()