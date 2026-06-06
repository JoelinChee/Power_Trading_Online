from __future__ import annotations

import logging
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from common.proto_loader import trading_messages_pb2
from common.schemas import RiskCheckRequest, RiskCheckResponse, TradeOrderRequest, TradeOrderResponse


class ExecutionAlgo:
    """Algorithm layer for risk evaluation and order generation."""

    def __init__(self, service_name: str, logger: logging.Logger) -> None:
        self.service_name = service_name
        self.logger = logger
        self.last_consumed_event: dict[str, object] | None = None
        self.last_processed_result: dict[str, object] | None = None

    def risk_check(self, request: RiskCheckRequest) -> RiskCheckResponse:
        """Evaluate a basic rule-based risk decision."""

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
        return RiskCheckResponse(
            enterprise_id=request.enterprise_id,
            approved=approved,
            risk_score=min(risk_score, 100.0),
            reasons=reasons or ["Risk within threshold"],
        )

    def create_trade_order(self, request: TradeOrderRequest) -> TradeOrderResponse:
        """Create a mock day-ahead purchase order from a validated request."""

        quantity = round(request.predicted_load_mw * 24, 2)
        return TradeOrderResponse(
            order_id=str(uuid4()),
            enterprise_id=request.enterprise_id,
            order_type="DAY_AHEAD_BUY",
            quantity_mwh=quantity,
            limit_price=request.predicted_price,
            target_date=request.target_date,
            status="CREATED",
        )

    def handle_forecast_event(self, payload: bytes) -> None:
        """Consume the configured forecast route and execute risk/order processing."""

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
            "source_service": self.service_name,
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
        self.logger.warning(
            "execution_boot processed forecast event_id=%s approved=%s",
            event.event_id,
            risk_response.approved,
        )
