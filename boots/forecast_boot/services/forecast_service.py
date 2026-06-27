from __future__ import annotations

from functools import lru_cache

from boots.forecast_boot.forecast_loader import ForecastAlgoConfigLoader
from boots.forecast_boot.services.algo import ForecastAlgo
from boots.forecast_boot.services.messages import ForecastInMessages, ForecastOutMessages
from infrastructure.loaders.boots_loader import BootsConfigLoader
from infrastructure.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from infrastructure.loaders.topic_loader import TopicConfigLoader
from infrastructure.kafka import KafkaBatchConsumer, KafkaPublisher
from infrastructure.logging.logging import get_logger
from infrastructure.schemas import PipelineStatusResponse
from infrastructure.scheduler.timer import AsyncFixedRateScheduler


logger = get_logger(__name__)

class ForecastService:
    """Application service orchestrating forecast pipeline lifecycle.

    Design notes:
    - Applies orchestration pattern: service owns scheduling, polling and publishing.
    - Keeps transformation algorithm in ForecastAlgo to preserve clean boundaries.
    - Uses lazy consumer initialization to avoid hard failure before Kafka is ready.
    """

    def __init__(self) -> None:
        self.boot_config = BootsConfigLoader.get_boot("forecast_boot")
        self.service_name = self.boot_config["service_name"]
        self.timer_interval_seconds = float(self.boot_config["trigger"].get("timer_interval_seconds", 5.0))

        self.kafka_settings = KafkaRuntimeSettings(
            service_name=self.service_name,
            kafka_config=KafkaConfigLoader.get_kafka(),
        )
        self.weather_topic_name = TopicConfigLoader.topic_name("data_boot", "forecast_boot")


        self.forecast_topic_name = TopicConfigLoader.topic_name("forecast_boot", "execution_boot")

        self.publisher = KafkaPublisher(self.kafka_settings)
        self.weather_consumer = KafkaBatchConsumer(
            settings=self.kafka_settings,
            topic_name=self.weather_topic_name,
            group_suffix="weather",
            poll_timeout_seconds=0.1,
        )
        self.in_messages = ForecastInMessages()
        self.out_messages = ForecastOutMessages()
        self.algo = ForecastAlgo(
            service_name=self.service_name,
            timer_interval_seconds=self.timer_interval_seconds,
            algo_config=ForecastAlgoConfigLoader.get_algo_config(),
            logger=logger,
        )
        self.update_scheduler = AsyncFixedRateScheduler(
            name=f"{self.service_name}-fixed-rate-weather-pull",
            interval_seconds=self.timer_interval_seconds,
            callback=self._update,
        )

    def start_pipeline(self) -> None:
        """Start the fixed-rate scheduler that pulls weather messages every 5 seconds."""
        self.update_scheduler.start()

    def stop_pipeline(self) -> None:
        """Stop the scheduler and close the Kafka consumer."""
        self.update_scheduler.stop()
        self.weather_consumer.close()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return latest consumed weather payload and published forecast payload."""

        return PipelineStatusResponse(
            service_name=self.service_name,
            last_consumed_event_id=(self.algo.last_received_event or {}).get("upstream_event_id"),
            last_published_event_id=(self.algo.last_published_event or {}).get("event_id"),
            details={
                "last_received_event": self.algo.last_received_event or {},
                "last_received_weather_event": self.algo.last_received_weather_event or {},
                "last_published_event": self.algo.last_published_event or {},
            },
        )

    def _update(self) -> None:
        """Scheduler callback that drains currently available Kafka records.

        The callback is intentionally resilient: a single bad message should not
        tear down the scheduler loop for subsequent cycles.
        """

        logger.info(
            "forecast_boot fixed-rate tick service=%s interval_seconds=%s",
            self.service_name,
            self.timer_interval_seconds,
        )
        polled_payloads = self.weather_consumer.drain_available()
        processed_messages = self._process_polled_batch(polled_payloads)

        if processed_messages:
            logger.info(
                "forecast_boot fixed-rate cycle processed_messages=%s interval_seconds=%s",
                processed_messages,
                self.timer_interval_seconds,
            )
        else:
            logger.info(
                "forecast_boot fixed-rate cycle idle topic=%s interval_seconds=%s",
                self.weather_topic_name,
                self.timer_interval_seconds,
            )

    def _process_polled_batch(self, payloads: list[bytes]) -> int:
        """Process one polled batch and publish all derived forecast events.

        Returns:
            Number of forecast events successfully published.
        """

        if not payloads:
            return 0

        self.in_messages.data_boot_to_forecast_boot_queue = payloads
        try:
            self.out_messages = self.algo.update(self.in_messages)
        except Exception as exc:  # defensive boundary to keep scheduler alive
            logger.exception("Failed to transform weather payload batch into forecast events: %s", exc)
            return 0

        events = self.out_messages.forecast_boot_to_execution_boot_queue

        published = 0
        for forecast_event in events:
            self.publisher.publish_proto(self.forecast_topic_name, forecast_event, key=forecast_event.enterprise_id)
            logger.info("forecast_boot published forecast event_id=%s", forecast_event.event_id)
            published += 1
        return published


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    """Return a singleton ForecastService instance for the FastAPI process."""
    return ForecastService()