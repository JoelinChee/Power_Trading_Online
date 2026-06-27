"""FastAPI application entry point for the execution boot service."""

from fastapi import FastAPI

from boots.common.app_factory import BootApplicationFactory, BootApplicationSpec
from boots.execution_boot.controllers.execution_controller import router as execution_router
from boots.execution_boot.controllers.health_controller import router as health_router
from boots.execution_boot.services.execution_service import get_execution_service


def create_app() -> FastAPI:
    """Construct the FastAPI application for the execution boot service.

    Returns:
        Configured FastAPI application instance.
    """

    return BootApplicationFactory(
        BootApplicationSpec(
            title="Execution Boot",
            version="0.1.0",
            service_factory=get_execution_service,
            routers=(health_router, execution_router),
        )
    ).create()


# Exported ASGI application instance used by uvicorn.
app = create_app()