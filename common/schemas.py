"""Shared Pydantic request and response models for the power trading platform.

Each model is used either by a FastAPI endpoint or by the internal debugging
views that expose recent Kafka pipeline state to the caller.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DataIngestionRequest(BaseModel):
    """Composite request submitted to the data ingestion HTTP API."""

    weather: Dict[str, object] = Field(..., description="Current weather observation payload.")
    load: Dict[str, object] = Field(..., description="Current enterprise load observation payload.")
    price: Dict[str, object] = Field(..., description="Current market price observation payload.")
    renewable: Dict[str, object] = Field(..., description="Current renewable generation observation payload.")


class PipelineStatusResponse(BaseModel):
    """Debugging view that summarizes recent event activity for one service."""

    service_name: str = Field(..., description="Logical service name that produced this status response.")
    last_published_event_id: Optional[str] = Field(default=None, description="Identifier of the last Kafka event published by this service, when available.")
    last_consumed_event_id: Optional[str] = Field(default=None, description="Identifier of the last Kafka event consumed by this service, when available.")
    last_feedback_event_id: Optional[str] = Field(default=None, description="Identifier of the last downstream feedback event observed by this service, when available.")
    details: Dict[str, object] = Field(default_factory=dict, description="Expanded debugging payload that includes the latest serialized event content.")


class SeriesPoint(BaseModel):
    """Single point in a 96-slot day-ahead forecast series."""

    slot: int = Field(..., ge=1, le=96, description="Quarter-hour slot number in the 96-point daily forecast horizon.")
    value: float = Field(..., description="Forecasted metric value associated with the slot.")


class RiskCheckRequest(BaseModel):
    """Input payload for the synchronous execution risk-check API."""

    enterprise_id: str = Field(..., description="Enterprise identifier associated with the risk evaluation request.")
    predicted_load_mw: float = Field(..., description="Predicted average load in megawatts used for procurement planning.")
    budget_limit: float = Field(..., description="Maximum acceptable procurement budget for the evaluation window.")
    bid_price: float = Field(..., description="Candidate bid price to be evaluated by the risk rules.")
    available_renewable_mw: float = Field(..., description="Renewable generation capacity available to offset the predicted load.")


class RiskCheckResponse(BaseModel):
    """Risk decision returned by the synchronous execution risk-check API."""

    enterprise_id: str = Field(..., description="Enterprise identifier carried over from the risk request.")
    approved: bool = Field(..., description="Whether the trade can proceed according to the simplified risk rules.")
    risk_score: float = Field(..., description="Aggregated risk score on a 0-100 scale.")
    reasons: List[str] = Field(..., description="Human-readable reasons that explain the decision outcome.")


class TradeOrderRequest(BaseModel):
    """Input payload for the mock trade-order creation API."""

    enterprise_id: str = Field(..., description="Enterprise identifier that will own the generated trade order.")
    target_date: str = Field(..., description="Trading date covered by the order.")
    predicted_load_mw: float = Field(..., description="Predicted average load used to calculate order quantity.")
    predicted_price: float = Field(..., description="Predicted market price used as the order limit price.")
    approved: bool = Field(default=True, description="Explicit approval flag that must be true before order creation is allowed.")


class TradeOrderResponse(BaseModel):
    """Result returned by the mock trade-order creation API."""

    order_id: str = Field(..., description="Generated unique identifier for the synthetic trade order.")
    enterprise_id: str = Field(..., description="Enterprise identifier that owns the order.")
    order_type: str = Field(..., description="Logical order type used by the execution workflow.")
    quantity_mwh: float = Field(..., description="Calculated order quantity in megawatt-hours.")
    limit_price: float = Field(..., description="Limit price attached to the generated order.")
    target_date: str = Field(..., description="Trading date covered by the order.")
    status: str = Field(..., description="Current synthetic order status value.")