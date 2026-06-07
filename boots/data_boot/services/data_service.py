from __future__ import annotations

from functools import lru_cache

from boots.data_boot.services.algo import DataAlgo
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import KafkaPublisher
from common.schemas import DataIngestionRequest, PipelineStatusResponse


class DataService:
    """Service layer for data acquisition and forecast dispatch handling."""

    def __init__(self) -> None:
        self.boot_config = BootsConfigLoader.get_boot("data_boot")
        self.kafka_config = KafkaConfigLoader.get_kafka()
        self.kafka_settings = KafkaRuntimeSettings(
            service_name=self.boot_config["service_name"],
            kafka_config=self.kafka_config,
        )
        self.weather_topic_name = TopicConfigLoader.topic_name("data_boot", "forecast_boot")
        self.publisher = KafkaPublisher(self.kafka_settings)
        self.algo = DataAlgo(service_name=self.boot_config["service_name"])

    def get_current_snapshot(self) -> dict[str, object]:
        """Return a mock real-time snapshot for manual API inspection."""
        return self.algo.get_current_snapshot()

    def publish_browser_weather(self) -> dict[str, object]:
        """Build one sample hourly weather dataset and publish it to Kafka.

        Returns:
            Browser-facing acknowledgement that the weather dataset was sent.
        """
        dataset = self.algo.build_browser_weather_dataset()
        self.publisher.publish_proto(self.weather_topic_name, dataset, key=dataset.daily_weather[0].region_code)
        return self.algo.record_browser_weather_publication(dataset, self.weather_topic_name)

    def ingest(self, request: DataIngestionRequest) -> dict[str, object]:
        """Accept an ingestion request without publishing to Kafka topics.

        Args:
            request: Structured HTTP payload containing weather, load, price,
                and renewable generation information.

        Returns:
            Response payload that confirms the request was accepted.
        """
        return self.algo.ingest(request)

    def start_pipeline(self) -> None:
        """Data boot has no background worker in the current pipeline design."""

    def stop_pipeline(self) -> None:
        """Data boot has no background worker to stop."""

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Expose the latest accepted source event and forwarded forecast event."""
        return PipelineStatusResponse(
            service_name=self.boot_config["service_name"],
            last_published_event_id=(self.algo.last_published_event or {}).get("event_id"),
            last_feedback_event_id=(self.algo.last_feedback_event or {}).get("upstream_event_id") or (self.algo.last_feedback_event or {}).get("event_id"),
            details={
                "last_published_event": self.algo.last_published_event or {},
                "last_feedback_event": self.algo.last_feedback_event or {},
            },
        )


@lru_cache(maxsize=1)
def get_data_service() -> DataService:
    """Return a singleton `DataService` instance for the FastAPI process."""
    return DataService()