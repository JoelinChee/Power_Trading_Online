from __future__ import annotations

import logging
from functools import lru_cache

from boots.forecast_boot.services.algo import ForecastAlgo
from common.config_loader import ForecastBootSettings
from common.kafka import KafkaPublisher
from common.schemas import PipelineStatusResponse
from common.timer import AsyncFixedRateScheduler


logger = logging.getLogger(__name__)


class ForecastService:
    """Service layer for weather-topic consumption and forecast-topic publication."""

    def __init__(self) -> None:
        self.settings = ForecastBootSettings()
        self.publisher = KafkaPublisher(self.settings)
        self.algo = ForecastAlgo(settings=self.settings, publisher=self.publisher, logger=logger)
        self.weather_scheduler = AsyncFixedRateScheduler(
            name=f"{self.settings.service_name}-fixed-rate-weather-pull",
            interval_seconds=self.settings.timer_interval_seconds,
            callback=self._drain_weather_events,
        )

    def start_pipeline(self) -> None:
        """Start the fixed-rate scheduler that pulls weather messages every 5 seconds."""
        self.weather_scheduler.start()

    def stop_pipeline(self) -> None:
        """Stop the scheduler and close the Kafka consumer."""
        self.weather_scheduler.stop()
        self.algo.stop()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return latest consumed weather payload and published forecast payload."""

        return PipelineStatusResponse(
            service_name=self.settings.service_name,
            last_consumed_event_id=(self.algo.last_received_event or {}).get("upstream_event_id"),
            last_published_event_id=(self.algo.last_published_event or {}).get("event_id"),
            details={
                "last_received_event": self.algo.last_received_event or {},
                "last_received_weather_event": self.algo.last_received_weather_event or {},
                "last_published_event": self.algo.last_published_event or {},
            },
        )

    def _drain_weather_events(self) -> None:
        """Framework callback that delegates fixed-rate draining to algorithm layer."""

        self.algo.drain_weather_events()


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    """Return a singleton `ForecastService` instance for the FastAPI process."""
    return ForecastService()