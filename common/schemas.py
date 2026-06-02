"""Shared Pydantic request and response models for the power trading platform.

Each model is used either by a FastAPI endpoint or by the internal debugging
views that expose recent Kafka pipeline state to the caller.
"""

from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class WeatherSnapshot(BaseModel):
    """Weather measurements collected by the data boot service."""

    temperature_celsius: float = Field(..., description="Ambient temperature in degrees Celsius.")
    humidity_ratio: float = Field(..., description="Relative humidity percentage, represented as a numeric ratio on a 0-100 scale.")
    wind_speed_mps: float = Field(..., description="Measured wind speed in meters per second.")
    weather_type: str = Field(..., description="Short categorical description of the weather state, such as cloudy or sunny.")


class LoadSnapshot(BaseModel):
    """Enterprise load measurement captured at a specific timestamp."""

    enterprise_id: str = Field(..., description="Enterprise identifier used to correlate all downstream forecast and execution events.")
    timestamp: str = Field(..., description="Observation timestamp in ISO 8601 format.")
    load_mw: float = Field(..., description="Instantaneous enterprise load in megawatts.")


class PriceSnapshot(BaseModel):
    """Observed market price snapshot used as forecast input."""

    timestamp: str = Field(..., description="Price observation timestamp in ISO 8601 format.")
    spot_price: float = Field(..., description="Observed market clearing or spot price value.")
    market: Literal["day_ahead", "real_time"] = Field(default="real_time", description="Market segment associated with the observed price.")


class RenewableSnapshot(BaseModel):
    """Renewable generation measurement used as execution context."""

    timestamp: str = Field(..., description="Renewable output observation timestamp in ISO 8601 format.")
    wind_output_mw: float = Field(..., description="Measured wind generation output in megawatts.")
    solar_output_mw: float = Field(..., description="Measured solar generation output in megawatts.")


class DataIngestionRequest(BaseModel):
    """Composite request submitted to the data ingestion HTTP API."""

    weather: WeatherSnapshot = Field(..., description="Current weather observation payload.")
    load: LoadSnapshot = Field(..., description="Current enterprise load observation payload.")
    price: PriceSnapshot = Field(..., description="Current market price observation payload.")
    renewable: RenewableSnapshot = Field(..., description="Current renewable generation observation payload.")


class ForecastPipelineRequest(DataIngestionRequest):
    """Internal request that forwards an ingested snapshot to forecast boot."""

    upstream_event_id: str = Field(..., description="Identifier of the source ingestion event created by data boot.")
    source_service: str = Field(..., description="Name of the upstream service that forwarded the request.")
    published_at: str = Field(..., description="Publication timestamp copied from the source data snapshot.")
    target_date: str = Field(..., description="Trading date derived from the source load timestamp.")


class PipelineStatusResponse(BaseModel):
    """Debugging view that summarizes recent event activity for one service."""

    service_name: str = Field(..., description="Logical service name that produced this status response.")
    last_published_event_id: Optional[str] = Field(default=None, description="Identifier of the last Kafka event published by this service, when available.")
    last_consumed_event_id: Optional[str] = Field(default=None, description="Identifier of the last Kafka event consumed by this service, when available.")
    last_feedback_event_id: Optional[str] = Field(default=None, description="Identifier of the last downstream feedback event observed by this service, when available.")
    details: Dict[str, object] = Field(default_factory=dict, description="Expanded debugging payload that includes the latest serialized event content.")


class ForecastRequest(BaseModel):
    """Request model used by the synchronous forecast demonstration APIs."""

    target_date: str = Field(..., description="Target trading date for the generated forecast horizon.")
    enterprise_id: str = Field(default="default-enterprise", description="Enterprise identifier that owns the forecast request.")
    base_load_mw: float = Field(default=68.0, description="Base load value used to synthesize the 96-point load forecast curve.")
    base_price: float = Field(default=415.0, description="Base market price used to synthesize the 96-point price forecast curve.")
    weather_type: str = Field(default="cloudy", description="Weather category used by the weather forecast demonstration endpoint.")


class SeriesPoint(BaseModel):
    """Single point in a 96-slot day-ahead forecast series."""

    slot: int = Field(..., ge=1, le=96, description="Quarter-hour slot number in the 96-point daily forecast horizon.")
    value: float = Field(..., description="Forecasted metric value associated with the slot.")


class ForecastResponse(BaseModel):
    """Response model returned by the forecast demonstration APIs."""

    target_date: str = Field(..., description="Target trading date for the forecast horizon.")
    enterprise_id: str = Field(..., description="Enterprise identifier associated with the forecast.")
    metric: str = Field(..., description="Name of the forecasted metric, such as load or price.")
    horizon: int = Field(default=96, description="Number of quarter-hour points included in the forecast response.")
    points: List[SeriesPoint] = Field(..., description="Ordered list of quarter-hour forecast points.")


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