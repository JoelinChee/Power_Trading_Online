from __future__ import annotations

import logging
from functools import lru_cache

from boots.forecast_boot.services.algo import ForecastAlgo
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import KafkaPublisher
from common.schemas import PipelineStatusResponse
from common.timer import AsyncFixedRateScheduler


logger = logging.getLogger(__name__)


class ForecastService:
    """Service layer for weather-topic consumption and forecast-topic publication."""

    def __init__(self) -> None:
        self.boot_config = BootsConfigLoader.get_boot("forecast_boot")
        self.kafka_config = KafkaConfigLoader.get_kafka()
        self.kafka_settings = KafkaRuntimeSettings(
            service_name=self.boot_config["service_name"],
            kafka_config=self.kafka_config,
        )
        self.weather_topic_name = TopicConfigLoader.topic_name("data_boot", "forecast_boot")
        self.forecast_topic_name = TopicConfigLoader.topic_name("forecast_boot", "execution_boot")

        self.publisher = KafkaPublisher(self.kafka_settings)
        self.algo = ForecastAlgo(
            service_name=self.boot_config["service_name"],
            timer_interval_seconds=float(self.boot_config["trigger"].get("timer_interval_seconds", 5.0)),
            kafka_enabled=bool(self.kafka_settings.kafka_enabled),
            kafka_bootstrap_servers=str(self.kafka_settings.kafka_bootstrap_servers),
            kafka_client_id=str(self.kafka_settings.kafka_client_id),
            kafka_auto_offset_reset=str(self.kafka_settings.kafka_auto_offset_reset),
            weather_consumer_group=self.kafka_settings.consumer_group("weather"),
            weather_topic_name=self.weather_topic_name,
            forecast_topic_name=self.forecast_topic_name,
            publisher=self.publisher,
            logger=logger,
        )
        self.weather_scheduler = AsyncFixedRateScheduler(
            name=f"{self.boot_config['service_name']}-fixed-rate-weather-pull",
            interval_seconds=float(self.boot_config["trigger"].get("timer_interval_seconds", 5.0)),
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
            service_name=self.boot_config["service_name"],
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