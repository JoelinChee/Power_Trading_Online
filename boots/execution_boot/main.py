"""FastAPI application entry point for the execution boot service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from boots.execution_boot.controllers.execution_controller import router as execution_router
from boots.execution_boot.controllers.health_controller import router as health_router
from boots.execution_boot.services.execution_service import get_execution_service
from common.logging.logging import configure_logging


# Configure process logging from centralized module.
configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Start and stop background Kafka workers with the application lifecycle.

    Args:
        _: FastAPI application instance supplied by FastAPI's lifespan hook.
    """

    service = get_execution_service()
    service.start_pipeline()
    try:
        yield
    finally:
        service.stop_pipeline()


def create_app() -> FastAPI:
    """Construct the FastAPI application for the execution boot service.

    Returns:
        Configured FastAPI application instance.
    """

    app = FastAPI(title="Execution Boot", version="0.1.0", lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(execution_router)
    return app


# Exported ASGI application instance used by uvicorn.
app = create_app()