"""FastAPI application entry point for the data acquisition boot service."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from boots.data_boot.controllers.data_controller import root_router, router as data_router
from boots.data_boot.controllers.health_controller import router as health_router
from boots.data_boot.services.data_service import get_data_service
from infrastructure.logging.logging import configure_logging


# Configure process logging from centralized module.
configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Start and stop background Kafka workers with the application lifecycle.

    Args:
        _: FastAPI application instance supplied by FastAPI's lifespan hook.
    """

    service = get_data_service()
    service.start_pipeline()
    try:
        yield
    finally:
        service.stop_pipeline()


def create_app() -> FastAPI:
    """Construct the FastAPI application for the data boot service.

    Returns:
        Configured FastAPI application instance.
    """

    app = FastAPI(title="Data Boot", version="0.1.0", lifespan=lifespan)

    app.include_router(health_router)
    app.include_router(root_router)
    app.include_router(data_router)
    return app


# Exported ASGI application instance used by uvicorn.
app = create_app()