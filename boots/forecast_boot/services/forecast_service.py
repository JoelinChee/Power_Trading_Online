from __future__ import annotations

import logging
from typing import Any
from functools import lru_cache

from boots.forecast_boot.services.algo import ForecastAlgo
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import Consumer, KafkaError, KafkaPublisher
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
        self.weather_consumer_group = self.kafka_settings.consumer_group("weather")
        self.weather_consumer: Consumer | None = None
        self.algo = ForecastAlgo(
            service_name=self.boot_config["service_name"],
            timer_interval_seconds=float(self.boot_config["trigger"].get("timer_interval_seconds", 5.0)),
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
        if self.weather_consumer is not None:
            self.weather_consumer.close()
            self.weather_consumer = None

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

        consumer = self._get_or_create_weather_consumer()
        logger.warning(
            "forecast_boot fixed-rate tick service=%s interval_seconds=%s",
            self.boot_config["service_name"],
            self.algo.timer_interval_seconds,
        )
        if consumer is None:
            logger.warning("Consumer is not available.")
            return

        processed_messages = 0
        while True:
            message = consumer.poll(0.1)
            if message is None:
                logger.warning(
                    "message is None for service=%s topic=%s interval_seconds=%s",
                    self.boot_config["service_name"],
                    self.weather_topic_name,
                    self.algo.timer_interval_seconds,
                )
                break
            if message.error():
                if self._is_partition_eof(message.error()):
                    break
                if self._is_unknown_topic(message.error()):
                    logger.info(
                        "Kafka topic=%s is not available yet for service=%s; waiting for topic auto-creation",
                        self.weather_topic_name,
                        self.boot_config["service_name"],
                    )
                    break
                logger.warning(
                    "Kafka poll returned error service=%s topic=%s error=%s",
                    self.boot_config["service_name"],
                    self.weather_topic_name,
                    message.error(),
                )
                break

            payload = message.value()
            logger.warning(
                "message is available for service=%s topic=%s interval_seconds=%s",
                self.boot_config["service_name"],
                self.weather_topic_name,
                self.algo.timer_interval_seconds,
            )
            if not payload:
                continue

            forecast_event = self.algo.handle_weather_event(payload)
            self.publisher.publish_proto(self.forecast_topic_name, forecast_event, key=forecast_event.enterprise_id)
            logger.warning("forecast_boot published forecast event_id=%s", forecast_event.event_id)
            processed_messages += 1

        if processed_messages:
            logger.warning(
                "forecast_boot fixed-rate cycle processed_messages=%s interval_seconds=%s",
                processed_messages,
                self.algo.timer_interval_seconds,
            )
        else:
            logger.warning(
                "forecast_boot fixed-rate cycle idle topic=%s interval_seconds=%s",
                self.weather_topic_name,
                self.algo.timer_interval_seconds,
            )

    def _get_or_create_weather_consumer(self) -> Consumer | None:
        """Lazily create one Kafka consumer reused by the fixed-rate scheduler."""

        if not self.kafka_settings.kafka_enabled:
            logger.info("Kafka consumer disabled for service=%s", self.boot_config["service_name"])
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
                "client.id": f"{self.kafka_settings.kafka_client_id}-{self.boot_config['service_name']}",
                "auto.offset.reset": self.kafka_settings.kafka_auto_offset_reset,
            }
        )
        self.weather_consumer.subscribe([self.weather_topic_name])
        logger.warning(
            "forecast_boot AsyncIOScheduler fixed-rate consumer subscribed group=%s topic=%s interval_seconds=%s",
            self.weather_consumer_group,
            self.weather_topic_name,
            self.algo.timer_interval_seconds,
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
    """Return a singleton `ForecastService` instance for the FastAPI process."""
    return ForecastService()