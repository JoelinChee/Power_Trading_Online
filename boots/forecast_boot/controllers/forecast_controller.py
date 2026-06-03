"""HTTP routes for the forecast boot service."""

from __future__ import annotations

from fastapi import APIRouter

from boots.forecast_boot.services.forecast_service import get_forecast_service
from common.schemas import PipelineStatusResponse


router = APIRouter(prefix="/api/v1/forecast", tags=["forecast"])


@router.get("/pipeline-status", response_model=PipelineStatusResponse)
def get_pipeline_status() -> PipelineStatusResponse:
    """Expose the latest event state seen by the forecast boot service."""

    service = get_forecast_service()
    return service.get_pipeline_status()