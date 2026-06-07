from __future__ import annotations

import logging
from typing import Any
from functools import lru_cache

from boots.forecast_boot.services.algo import ForecastAlgo
from boots.forecast_boot.services.messages import ForecastInMessages, ForecastOutMessages
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import Consumer, KafkaError, KafkaPublisher
from common.schemas import PipelineStatusResponse
from common.timer import AsyncFixedRateScheduler


logger = logging.getLogger(__name__)

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
        self.weather_consumer_group = self.kafka_settings.consumer_group("weather")
        self.weather_consumer: Consumer | None = None
        self.in_messages = ForecastInMessages()
        self.out_messages = ForecastOutMessages()
        self.algo = ForecastAlgo(
            service_name=self.service_name,
            timer_interval_seconds=self.timer_interval_seconds,
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
        if self.weather_consumer is not None:
            self.weather_consumer.close()
            self.weather_consumer = None

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

        consumer = self._get_or_create_weather_consumer()
        logger.info(
            "forecast_boot fixed-rate tick service=%s interval_seconds=%s",
            self.service_name,
            self.timer_interval_seconds,
        )
        if consumer is None:
            logger.info("Consumer is not available; skip current cycle")
            return

        polled_payloads: list[bytes] = []
        while True:
            message = consumer.poll(0.1)
            if message is None:
                break
            if message.error():
                if self._is_partition_eof(message.error()):
                    break
                if self._is_unknown_topic(message.error()):
                    logger.info(
                        "Kafka topic=%s is not available yet for service=%s; waiting for topic auto-creation",
                        self.weather_topic_name,
                        self.service_name,
                    )
                    break
                logger.warning(
                    "Kafka poll returned error service=%s topic=%s error=%s",
                    self.service_name,
                    self.weather_topic_name,
                    message.error(),
                )
                break

            payload = message.value()
            if payload:
                polled_payloads.append(payload)

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

    def _get_or_create_weather_consumer(self) -> Consumer | None:
        """Lazily create one Kafka consumer reused by the fixed-rate scheduler."""

        if not self.kafka_settings.kafka_enabled:
            logger.info("Kafka consumer disabled for service=%s", self.service_name)
            return None

        if Consumer is None:
            logger.warning("confluent-kafka is not installed; consumer will not start")
            return None

        if self.weather_consumer is not None:
            return self.weather_consumer

        self.weather_consumer = Consumer(
            {
                "bootstrap.servers": self.kafka_settings.kafka_bootstrap_servers,
                "group.id": self.weather_consumer_group,
                "client.id": f"{self.kafka_settings.kafka_client_id}-{self.service_name}",
                "auto.offset.reset": self.kafka_settings.kafka_auto_offset_reset,
            }
        )
        self.weather_consumer.subscribe([self.weather_topic_name])
        logger.info(
            "forecast_boot AsyncIOScheduler fixed-rate consumer subscribed group=%s topic=%s interval_seconds=%s",
            self.weather_consumer_group,
            self.weather_topic_name,
            self.timer_interval_seconds,
        )
        return self.weather_consumer

    def _is_partition_eof(self, error: Any) -> bool:
        """Return whether the Kafka error means the current partition is drained."""

        return KafkaError is not None and error.code() == KafkaError._PARTITION_EOF

    def _is_unknown_topic(self, error: Any) -> bool:
        """Return whether the Kafka error means the topic does not exist yet."""

        return KafkaError is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    """Return a singleton ForecastService instance for the FastAPI process."""
    return ForecastService()