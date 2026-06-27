"""FastAPI application entry point for the data acquisition boot service."""

from __future__ import annotations

from fastapi import FastAPI

from boots.common.app_factory import BootApplicationFactory, BootApplicationSpec
from boots.data_boot.controllers.data_controller import root_router, router as data_router
from boots.data_boot.controllers.health_controller import router as health_router
from boots.data_boot.services.data_service import get_data_service


def create_app() -> FastAPI:
    """Construct the FastAPI application for the data boot service.

    Returns:
        Configured FastAPI application instance.
    """

    return BootApplicationFactory(
        BootApplicationSpec(
            title="Data Boot",
            version="0.1.0",
            service_factory=get_data_service,
            routers=(health_router, root_router, data_router),
        )
    ).create()


# Exported ASGI application instance used by uvicorn.
app = create_app()