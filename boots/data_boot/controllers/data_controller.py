"""HTTP routes for the data acquisition boot service."""

from __future__ import annotations

from fastapi import APIRouter

from boots.data_boot.services.data_service import get_data_service
from infrastructure.schemas import DataIngestionRequest, PipelineStatusResponse


root_router = APIRouter(tags=["data"])
router = APIRouter(prefix="/api/v1/data", tags=["data"])


@root_router.get("/")
def update() -> dict:
    """Publish a sample weather dataset when the browser visits data boot."""

    service = get_data_service()
    return service.update()


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