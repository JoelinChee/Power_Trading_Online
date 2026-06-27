"""Shared health endpoint factory for boot services."""

from __future__ import annotations

from fastapi import APIRouter


def create_health_router(service_name: str) -> APIRouter:
    """Create a standard liveness router for one boot service.

    The endpoint shape is intentionally uniform so shell scripts, smoke tests,
    and orchestration code can treat all boot services the same way.
    """

    router = APIRouter(tags=["health"])

    @router.get("/health")
    def health() -> dict[str, str]:
        """Return a minimal liveness payload for orchestration and tests."""

        return {"status": "UP", "service": service_name}

    return router
