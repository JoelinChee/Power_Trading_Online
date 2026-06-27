"""Shared FastAPI application factory for boot services.

Each boot service has the same outer lifecycle: configure logging, build the
FastAPI app, register routers, start the background pipeline, and stop it on
shutdown.  The concrete boot modules only provide the varying parts through a
small specification object.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

from fastapi import APIRouter, FastAPI

from infrastructure.logging.logging import configure_logging


class PipelineService(Protocol):
    """Lifecycle contract implemented by every boot-level service.

    The protocol keeps the application factory independent from concrete boot
    services while still making the required lifecycle methods explicit.
    """

    def start_pipeline(self) -> None:
        """Start any background workers owned by the service."""

    def stop_pipeline(self) -> None:
        """Stop background workers and release external resources."""


@dataclass(frozen=True)
class BootApplicationSpec:
    """Declarative configuration used to create one boot FastAPI app."""

    title: str
    version: str
    service_factory: Callable[[], PipelineService]
    routers: Sequence[APIRouter]


class BootApplicationFactory:
    """Factory for FastAPI boot applications.

    This applies the Factory pattern for app construction and centralizes the
    template lifecycle that all boot services share.
    """

    def __init__(self, spec: BootApplicationSpec) -> None:
        self.spec = spec

    def create(self) -> FastAPI:
        """Build a configured FastAPI application from the boot spec."""
        configure_logging()
        app = FastAPI(title=self.spec.title, version=self.spec.version, lifespan=self._lifespan)
        self._register_routers(app)
        return app

    def _register_routers(self, app: FastAPI) -> None:
        """Attach every router declared by the concrete boot module."""
        for router in self.spec.routers:
            app.include_router(router)

    @asynccontextmanager
    async def _lifespan(self, _: FastAPI):
        """Run the boot service pipeline inside FastAPI's lifespan hook."""
        service = self.spec.service_factory()
        service.start_pipeline()
        try:
            yield
        finally:
            service.stop_pipeline()
