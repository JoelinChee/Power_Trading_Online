from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from generated import trading_messages_pb2


class ExecutionAlgo:
    """Algorithm layer for risk evaluation and order generation."""

    def __init__(self, service_name: str, logger: logging.Logger) -> None:
        self.service_name = service_name
        self.logger = logger
        self.last_consumed_event: dict[str, object] | None = None
        self.last_processed_result: dict[str, object] | None = None

    def risk_check(self, request: dict[str, Any]) -> dict[str, object]:
        """Evaluate a basic rule-based risk decision."""

        reasons = []
        risk_score = 0.0

        predicted_load_mw = float(request["predicted_load_mw"])
        bid_price = float(request["bid_price"])
        budget_limit = float(request["budget_limit"])
        available_renewable_mw = float(request["available_renewable_mw"])
        enterprise_id = str(request["enterprise_id"])

        expected_cost = predicted_load_mw * bid_price
        if expected_cost > budget_limit:
            reasons.append("Expected cost exceeds budget limit")
            risk_score += 45.0

        if available_renewable_mw < predicted_load_mw * 0.2:
            reasons.append("Renewable coverage ratio is below 20%")
            risk_score += 30.0

        if bid_price > 520:
            reasons.append("Bid price exceeds internal price ceiling")
            risk_score += 35.0

        approved = risk_score < 60.0
        return {
            "enterprise_id": enterprise_id,
            "approved": approved,
            "risk_score": min(risk_score, 100.0),
            "reasons": reasons or ["Risk within threshold"],
        }

    def create_trade_order(self, request: dict[str, Any]) -> dict[str, object]:
        """Create a mock day-ahead purchase order from a validated request."""

        quantity = round(float(request["predicted_load_mw"]) * 24, 2)
        return {
            "order_id": str(uuid4()),
            "enterprise_id": str(request["enterprise_id"]),
            "order_type": "DAY_AHEAD_BUY",
            "quantity_mwh": quantity,
            "limit_price": float(request["predicted_price"]),
            "target_date": str(request["target_date"]),
            "status": "CREATED",
        }

    def handle_forecast_event(self, payload: bytes) -> None:
        """Consume the configured forecast route and execute risk/order processing."""

        event = trading_messages_pb2.ForecastEvent()
        event.ParseFromString(payload)
        self.last_consumed_event = MessageToDict(event, preserving_proto_field_name=True)

        predicted_load_mw = sum(point.value for point in event.load_points) / max(len(event.load_points), 1)
        predicted_price = sum(point.value for point in event.price_points) / max(len(event.price_points), 1)
        budget_limit = predicted_load_mw * 460

        risk_request = {
            "enterprise_id": event.enterprise_id,
            "predicted_load_mw": predicted_load_mw,
            "budget_limit": budget_limit,
            "bid_price": predicted_price,
            "available_renewable_mw": event.available_renewable_mw,
        }
        risk_response = self.risk_check(risk_request)

        result: dict[str, object] = {
            "event_id": str(uuid4()),
            "source_service": self.service_name,
            "upstream_event_id": event.event_id,
            "enterprise_id": event.enterprise_id,
            "target_date": event.target_date,
            "approved": bool(risk_response["approved"]),
            "risk_score": float(risk_response["risk_score"]),
            "reasons": list(risk_response["reasons"]),
        }

        if bool(risk_response["approved"]):
            trade_order = self.create_trade_order(
                {
                    "enterprise_id": event.enterprise_id,
                    "target_date": event.target_date,
                    "predicted_load_mw": predicted_load_mw,
                    "predicted_price": predicted_price,
                    "approved": True,
                }
            )
            result["order"] = trade_order
            result["status"] = "CREATED"
        else:
            result["status"] = "REJECTED"

        self.last_processed_result = result
        self.logger.warning(
            "execution_boot processed forecast event_id=%s approved=%s",
            event.event_id,
            bool(risk_response["approved"]),
        )
