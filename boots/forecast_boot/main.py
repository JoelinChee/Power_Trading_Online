"""FastAPI application entry point for the forecast boot service."""

from fastapi import FastAPI

from boots.common.app_factory import BootApplicationFactory, BootApplicationSpec
from boots.forecast_boot.controllers.forecast_controller import router as forecast_router
from boots.forecast_boot.controllers.health_controller import router as health_router
from boots.forecast_boot.services.forecast_service import get_forecast_service


def create_app() -> FastAPI:
    """Construct the FastAPI application for the forecast boot service.

    Returns:
        Configured FastAPI application instance.
    """

    return BootApplicationFactory(
        BootApplicationSpec(
            title="Forecast Boot",
            version="0.1.0",
            service_factory=get_forecast_service,
            routers=(health_router, forecast_router),
        )
    ).create()


# Exported ASGI application instance used by uvicorn.
app = create_app()