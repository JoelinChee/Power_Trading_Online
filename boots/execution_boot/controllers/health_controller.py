"""Health endpoint for the execution boot service."""

from __future__ import annotations

from boots.common.health import create_health_router


router = create_health_router("execution_boot")