"""Health endpoint for the data boot service."""

from __future__ import annotations

from fastapi import APIRouter


router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    """Return a minimal liveness payload for orchestration and tests."""

    return {"status": "UP", "service": "data_boot"}