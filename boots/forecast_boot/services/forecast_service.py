from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from common.config import ForecastBootSettings
from common.kafka import Consumer, KafkaError, KafkaPublisher
from common.proto_loader import trading_messages_pb2, weather_pb2
from common.schemas import PipelineStatusResponse, SeriesPoint
from common.timer import AsyncFixedRateScheduler


logger = logging.getLogger(__name__)


class ForecastService:
    """Service layer for weather-topic consumption and forecast-topic publication."""

    def __init__(self) -> None:
        self.settings = ForecastBootSettings(host="0.0.0.0", port=8002, timer_interval_seconds=5.0)
        self.publisher = KafkaPublisher(self.settings)
        self.weather_consumer: Consumer | None = None
        self.weather_topic = self.settings.topic_name("weather.events")
        self.weather_scheduler = AsyncFixedRateScheduler(
            name=f"{self.settings.service_name}-fixed-rate-weather-pull",
            interval_seconds=self.settings.timer_interval_seconds,
            callback=self._drain_weather_events,
        )
        self.last_received_event: dict[str, object] | None = None
        self.last_received_weather_event: dict[str, object] | None = None
        self.last_published_event: dict[str, object] | None = None

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
            service_name=self.settings.service_name,
            last_consumed_event_id=(self.last_received_event or {}).get("upstream_event_id"),
            last_published_event_id=(self.last_published_event or {}).get("event_id"),
            details={
                "last_received_event": self.last_received_event or {},
                "last_received_weather_event": self.last_received_weather_event or {},
                "last_published_event": self.last_published_event or {},
            },
        )

    def _drain_weather_events(self) -> None:
        """Pull all currently available weather messages on each fixed-rate cycle."""

        consumer = self._get_or_create_weather_consumer()
        logger.warning(
            "forecast_boot fixed-rate tick service=%s interval_seconds=%s",
            self.settings.service_name,
            self.settings.timer_interval_seconds,
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
                        self.settings.service_name,
                        self.weather_topic,
                        self.settings.timer_interval_seconds,)
                break
            if message.error():
                if self._is_partition_eof(message.error()):
                    break
                if self._is_unknown_topic(message.error()):
                    logger.info(
                        "Kafka topic=%s is not available yet for service=%s; waiting for topic auto-creation",
                        self.weather_topic,
                        self.settings.service_name,
                    )
                    break
                logger.warning(
                    "Kafka poll returned error service=%s topic=%s error=%s",
                    self.settings.service_name,
                    self.weather_topic,
                    message.error(),
                )
                break

            payload = message.value()
            logger.warning(
                        "message is available for service=%s topic=%s interval_seconds=%s",
                        self.settings.service_name,
                        self.weather_topic,
                        self.settings.timer_interval_seconds,)
            if not payload:
                continue

            processed_messages += 1
            self._handle_weather_event(payload)

        if processed_messages:
            logger.warning(
                "forecast_boot fixed-rate cycle processed_messages=%s interval_seconds=%s",
                processed_messages,
                self.settings.timer_interval_seconds,
            )
        else:
            logger.warning(
                "forecast_boot fixed-rate cycle idle topic=%s interval_seconds=%s",
                self.weather_topic,
                self.settings.timer_interval_seconds,
            )

    def _get_or_create_weather_consumer(self) -> Consumer | None:
        """Lazily create one Kafka consumer reused by the fixed-rate scheduler."""

        if not self.settings.kafka_enabled:
            logger.info("Kafka consumer disabled for service=%s", self.settings.service_name)
            return None

        if Consumer is None:
            logger.warning("confluent-kafka is not installed; consumer will not start")
            return None

        if self.weather_consumer is not None:
            return self.weather_consumer

        self.weather_consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.consumer_group("weather"),
                "client.id": f"{self.settings.kafka_client_id}-{self.settings.service_name}",
                "auto.offset.reset": self.settings.kafka_auto_offset_reset,
            }
        )
        self.weather_consumer.subscribe([self.weather_topic])
        logger.warning(
            "forecast_boot AsyncIOScheduler fixed-rate consumer subscribed group=%s topic=%s interval_seconds=%s",
            self.settings.consumer_group("weather"),
            self.weather_topic,
            self.settings.timer_interval_seconds,
        )
        return self.weather_consumer

    def _is_partition_eof(self, error: Any) -> bool:
        """Return whether the Kafka error means the current partition is drained."""

        return KafkaError is not None and error.code() == KafkaError._PARTITION_EOF

    def _is_unknown_topic(self, error: Any) -> bool:
        """Return whether the Kafka error means the topic does not exist yet."""

        return KafkaError is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART

    def _handle_weather_event(self, payload: bytes) -> None:
        """Consume weather.events and publish one trading_messages ForecastEvent."""

        dataset = weather_pb2.HourlyWeatherDataset()
        dataset.ParseFromString(payload)
        logger.warning(
            "forecast_boot weather handler invoked: source=%s generated_at=%s",
            dataset.source,
            dataset.generated_at,
        )
        upstream_event_id = str(uuid4())
        self.last_received_weather_event = MessageToDict(dataset, preserving_proto_field_name=True)
        self.last_received_event = {
            "upstream_event_id": upstream_event_id,
            "source_service": dataset.source,
            "published_at": dataset.generated_at,
            "target_date": (dataset.daily_weather[0].target_date if dataset.daily_weather else ""),
            "enterprise_id": (dataset.daily_weather[0].region_code if dataset.daily_weather else "default-enterprise"),
        }

        forecast_event = self._build_forecast_event(dataset=dataset, upstream_event_id=upstream_event_id)
        self.publisher.publish_proto("forecast.events", forecast_event, key=forecast_event.enterprise_id)
        self.last_published_event = MessageToDict(forecast_event, preserving_proto_field_name=True)
        logger.warning("forecast_boot published forecast event_id=%s", forecast_event.event_id)

    def _build_forecast_event(
        self, dataset: weather_pb2.HourlyWeatherDataset, upstream_event_id: str
    ) -> trading_messages_pb2.ForecastEvent:
        """Create one ForecastEvent from an incoming weather dataset."""
        weather_points, load_points, price_points, weather_type, target_date, enterprise_id, published_at, renewable_mw = self._build_forecast_points(dataset)

        forecast_event = trading_messages_pb2.ForecastEvent()
        forecast_event.event_id = str(uuid4())
        forecast_event.source_service = self.settings.service_name
        forecast_event.upstream_event_id = upstream_event_id
        forecast_event.published_at = published_at
        forecast_event.enterprise_id = enterprise_id
        forecast_event.target_date = target_date
        forecast_event.weather_type = weather_type
        forecast_event.available_renewable_mw = renewable_mw

        for point in weather_points:
            proto_point = forecast_event.weather_points.add()
            proto_point.slot = point.slot
            proto_point.value = point.value

        for point in load_points:
            proto_point = forecast_event.load_points.add()
            proto_point.slot = point.slot
            proto_point.value = point.value

        for point in price_points:
            proto_point = forecast_event.price_points.add()
            proto_point.slot = point.slot
            proto_point.value = point.value

        return forecast_event

    def _build_forecast_points(
        self, dataset: weather_pb2.HourlyWeatherDataset
    ) -> tuple[list[SeriesPoint], list[SeriesPoint], list[SeriesPoint], str, str, str, str, float]:
        """Build synthetic weather/load/price forecast points from one weather dataset."""
        weather_points: list[SeriesPoint] = []
        load_points: list[SeriesPoint] = []
        price_points: list[SeriesPoint] = []

        daily = dataset.daily_weather[0] if dataset.daily_weather else None
        hourly = list(daily.hourly_weather) if daily and daily.hourly_weather else []

        weather_type = hourly[0].condition_text if hourly else "unknown"
        target_date = daily.target_date if daily else (dataset.generated_at[:10] if dataset.generated_at else "")
        enterprise_id = daily.region_code if daily else "default-enterprise"
        published_at = dataset.generated_at

        base_temp = hourly[0].temperature_celsius if hourly else 28.0
        base_wind = hourly[0].wind.speed_mps if hourly else 4.0
        base_load = 60.0 + max(base_temp - 20.0, 0.0) * 1.2
        base_price = 420.0 + max(base_temp - 24.0, 0.0) * 2.8
        renewable_mw = round((base_wind * 2.2) + (hourly[0].solar_irradiance_wm2 / 40.0 if hourly else 8.0), 2)

        for slot in range(1, 97):
            source_hour = hourly[(slot - 1) % len(hourly)] if hourly else None
            weather_seed = source_hour.temperature_celsius if source_hour else base_temp

            weather_adjustment = ((slot % 16) - 8) * 0.18
            weather_value = round(weather_seed + weather_adjustment, 2)
            weather_points.append(SeriesPoint(slot=slot, value=weather_value))

            peak_factor = 1.18 if 33 <= slot <= 76 else 0.92
            intra_day_adjustment = ((slot % 12) - 6) * 0.35
            load_value = round(base_load * peak_factor + intra_day_adjustment, 2)
            load_points.append(SeriesPoint(slot=slot, value=load_value))

            demand_factor = 1.12 if 29 <= slot <= 80 else 0.95
            volatility = ((slot % 8) - 4) * 1.8
            price_value = round(base_price * demand_factor + volatility, 2)
            price_points.append(SeriesPoint(slot=slot, value=price_value))

        return weather_points, load_points, price_points, weather_type, target_date, enterprise_id, published_at, renewable_mw


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    """Return a singleton `ForecastService` instance for the FastAPI process."""
    return ForecastService()