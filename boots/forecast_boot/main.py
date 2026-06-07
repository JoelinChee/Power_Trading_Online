"""FastAPI application entry point for the forecast boot service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from boots.forecast_boot.controllers.forecast_controller import router as forecast_router
from boots.forecast_boot.controllers.health_controller import router as health_router
from boots.forecast_boot.services.forecast_service import get_forecast_service
from infrastructure.logging.logging import configure_logging


# Configure process logging from centralized module.
configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Start and stop background Kafka workers with the application lifecycle.

    Args:
        _: FastAPI application instance supplied by FastAPI's lifespan hook.
    """

    service = get_forecast_service()
    service.start_pipeline()
    try:
        yield
    finally:
        service.stop_pipeline()


def create_app() -> FastAPI:
    """Construct the FastAPI application for the forecast boot service.

    Returns:
        Configured FastAPI application instance.
    """

    app = FastAPI(title="Forecast Boot", version="0.1.0", lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(forecast_router)
    return app


# Exported ASGI application instance used by uvicorn.
app = create_app()