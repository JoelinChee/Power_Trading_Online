"""HTTP routes for the data acquisition boot service."""

from __future__ import annotations

from fastapi import APIRouter

from boots.data_boot.services.data_service import DataService, get_data_service
from common.schemas import DataIngestionRequest, PipelineStatusResponse


router = APIRouter(prefix="/api/v1/data", tags=["data"])


@router.get("/current")
def get_current_data() -> dict:
    """Return a sample real-time data snapshot for manual inspection."""

    service = get_data_service()
    return service.get_current_snapshot()


@router.post("/ingest")
def ingest_data(request: DataIngestionRequest) -> dict:
    """Accept an ingestion request and publish it into the Kafka pipeline.

    Args:
        request: Structured data ingestion payload submitted by the caller.

    Returns:
        Confirmation payload that includes the published protobuf event.
    """

    service = get_data_service()
    return service.ingest(request)


@router.get("/pipeline-status", response_model=PipelineStatusResponse)
def get_pipeline_status() -> PipelineStatusResponse:
    """Expose the latest event state seen by the data boot service."""

    service = get_data_service()
    return service.get_pipeline_status()