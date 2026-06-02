"""HTTP routes for the forecast boot service."""

from __future__ import annotations

from fastapi import APIRouter

from boots.forecast_boot.services.forecast_service import ForecastService, get_forecast_service
from common.schemas import ForecastPipelineRequest, ForecastRequest, ForecastResponse, PipelineStatusResponse


router = APIRouter(prefix="/api/v1/forecast", tags=["forecast"])


@router.post("/weather")
def forecast_weather(request: ForecastRequest) -> dict:
    """Return a simple weather forecast for direct API-driven inspection."""

    service = get_forecast_service()
    return service.forecast_weather(request)


@router.post("/load-96", response_model=ForecastResponse)
def forecast_load(request: ForecastRequest) -> ForecastResponse:
    """Return a synthetic 96-point load forecast built from the request."""

    service = get_forecast_service()
    return service.forecast_load(request)


@router.post("/price-96", response_model=ForecastResponse)
def forecast_price(request: ForecastRequest) -> ForecastResponse:
    """Return a synthetic 96-point market price forecast built from the request."""

    service = get_forecast_service()
    return service.forecast_price(request)


@router.post("/events")
def publish_forecast_event(request: ForecastPipelineRequest) -> dict:
    """Generate and publish a forecast event for the execution pipeline."""

    service = get_forecast_service()
    return service.publish_forecast_event(request)


@router.get("/pipeline-status", response_model=PipelineStatusResponse)
def get_pipeline_status() -> PipelineStatusResponse:
    """Expose the latest event state seen by the forecast boot service."""

    service = get_forecast_service()
    return service.get_pipeline_status()