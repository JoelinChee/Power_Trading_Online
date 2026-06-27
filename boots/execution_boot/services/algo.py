from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from boots.execution_boot.services.messages import ExecutionInMessages
from generated import trading_messages_pb2


@dataclass(frozen=True)
class RiskEvaluationInput:
    """Normalized values used by every risk rule."""

    enterprise_id: str
    predicted_load_mw: float
    bid_price: float
    budget_limit: float
    available_renewable_mw: float


@dataclass(frozen=True)
class RiskRuleResult:
    """Score contribution returned by one risk rule."""

    risk_score_delta: float
    reason: str | None = None


class RiskRule(Protocol):
    """Strategy contract for pluggable risk rules."""

    def evaluate(self, risk_input: RiskEvaluationInput) -> RiskRuleResult:
        """Return this rule's score contribution for a normalized request."""


class ExpectedCostBudgetRule:
    """Risk rule that rejects bids whose expected total cost exceeds budget."""

    def __init__(self, risk_weight: float) -> None:
        self.risk_weight = risk_weight

    def evaluate(self, risk_input: RiskEvaluationInput) -> RiskRuleResult:
        expected_cost = risk_input.predicted_load_mw * risk_input.bid_price
        if expected_cost <= risk_input.budget_limit:
            return RiskRuleResult(0.0)
        return RiskRuleResult(self.risk_weight, "Expected cost exceeds budget limit")


class RenewableCoverageRule:
    """Risk rule that checks whether renewable power covers enough load."""

    def __init__(self, coverage_threshold_ratio: float, risk_weight: float) -> None:
        self.coverage_threshold_ratio = coverage_threshold_ratio
        self.risk_weight = risk_weight

    def evaluate(self, risk_input: RiskEvaluationInput) -> RiskRuleResult:
        required_renewable_mw = risk_input.predicted_load_mw * self.coverage_threshold_ratio
        if risk_input.available_renewable_mw >= required_renewable_mw:
            return RiskRuleResult(0.0)
        return RiskRuleResult(self.risk_weight, "Renewable coverage ratio is below 20%")


class HighPriceRule:
    """Risk rule that applies an internal price ceiling."""

    def __init__(self, high_price_threshold: float, risk_weight: float) -> None:
        self.high_price_threshold = high_price_threshold
        self.risk_weight = risk_weight

    def evaluate(self, risk_input: RiskEvaluationInput) -> RiskRuleResult:
        if risk_input.bid_price <= self.high_price_threshold:
            return RiskRuleResult(0.0)
        return RiskRuleResult(self.risk_weight, "Bid price exceeds internal price ceiling")


class ExecutionAlgo:
    """Algorithm layer for risk evaluation and order generation."""

    def __init__(self, service_name: str, logger: object, algo_config: dict[str, object] | None = None) -> None:
        self.service_name = service_name
        self.logger = logger
        self.algo_config = algo_config if isinstance(algo_config, dict) else {}
        self.risk_rules = self._build_risk_rules()

        self.last_consumed_event: dict[str, object] | None = None
        self.last_processed_result: dict[str, object] | None = None

    def update(self, in_messages: ExecutionInMessages) -> None:
        """Consume one batch of forecast payloads and produce execution results."""

        payloads = self._resolve_inbound_payloads(in_messages)
        if not payloads:
            raise ValueError("Missing inbound forecast payload batch")

        for payload in payloads:
            self._process_one_forecast_payload(payload)

    def _resolve_inbound_payloads(self, in_messages: ExecutionInMessages) -> list[bytes]:
        """Resolve inbound payload batch from the message contract object."""

        if in_messages.forecast_boot_to_execution_boot_queue:
            return [payload for payload in in_messages.forecast_boot_to_execution_boot_queue if payload]
        return []

    def risk_check(self, request: dict[str, Any]) -> dict[str, object]:
        """Evaluate a basic rule-based risk decision."""

        risk_input = self._build_risk_input(request)
        risk_score, reasons = self._evaluate_risk_rules(risk_input)
        approved = self._is_approved(risk_score)
        return {
            "enterprise_id": risk_input.enterprise_id,
            "approved": approved,
            "risk_score": min(risk_score, float(self.algo_config["max_risk_score"])),
            "reasons": reasons or ["Risk within threshold"],
        }

    def _build_risk_rules(self) -> list[RiskRule]:
        """Build the ordered strategy list used by risk evaluation."""
        return [
            ExpectedCostBudgetRule(float(self.algo_config["expected_cost_risk_weight"])),
            RenewableCoverageRule(
                coverage_threshold_ratio=float(self.algo_config["renewable_coverage_threshold_ratio"]),
                risk_weight=float(self.algo_config["renewable_coverage_risk_weight"]),
            ),
            HighPriceRule(
                high_price_threshold=float(self.algo_config["high_price_threshold"]),
                risk_weight=float(self.algo_config["high_price_risk_weight"]),
            ),
        ]

    def _build_risk_input(self, request: dict[str, Any]) -> RiskEvaluationInput:
        """Normalize external request dictionaries before rule evaluation."""
        return RiskEvaluationInput(
            enterprise_id=str(request["enterprise_id"]),
            predicted_load_mw=float(request["predicted_load_mw"]),
            bid_price=float(request["bid_price"]),
            budget_limit=float(request["budget_limit"]),
            available_renewable_mw=float(request["available_renewable_mw"]),
        )

    def _evaluate_risk_rules(self, risk_input: RiskEvaluationInput) -> tuple[float, list[str]]:
        """Run each configured rule and aggregate score plus human-readable reasons."""
        reasons: list[str] = []
        risk_score = 0.0

        for risk_rule in self.risk_rules:
            rule_result = risk_rule.evaluate(risk_input)
            risk_score += rule_result.risk_score_delta
            if rule_result.reason:
                reasons.append(rule_result.reason)

        return risk_score, reasons

    def _is_approved(self, risk_score: float) -> bool:
        """Return whether an aggregated risk score is within the approval limit."""
        return risk_score < float(self.algo_config["approval_risk_threshold"])

    def create_trade_order(self, request: dict[str, Any]) -> dict[str, object]:
        """Create a mock day-ahead purchase order from a validated request."""

        quantity = round(float(request["predicted_load_mw"]) * float(self.algo_config["order_hours"]), 2)
        return {
            "order_id": str(uuid4()),
            "enterprise_id": str(request["enterprise_id"]),
            "order_type": "DAY_AHEAD_BUY",
            "quantity_mwh": quantity,
            "limit_price": float(request["predicted_price"]),
            "target_date": str(request["target_date"]),
            "status": "CREATED",
        }

    def _process_one_forecast_payload(self, payload: bytes) -> dict[str, object]:
        """Transform one forecast payload into one execution result."""

        event = trading_messages_pb2.ForecastEvent()
        event.ParseFromString(payload)
        self.last_consumed_event = MessageToDict(event, preserving_proto_field_name=True)

        predicted_load_mw = sum(point.value for point in event.load_points) / max(len(event.load_points), 1)
        predicted_price = sum(point.value for point in event.price_points) / max(len(event.price_points), 1)
        budget_limit = predicted_load_mw * float(self.algo_config["budget_limit_price_factor"])

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
        return result
