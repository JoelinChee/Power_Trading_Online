from __future__ import annotations

from functools import lru_cache

from google.protobuf.json_format import MessageToDict

from boots.data_boot.services.algo import DataAlgo
from common.loaders.boots_loader import BootsConfigLoader
from common.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings
from common.loaders.topic_loader import TopicConfigLoader
from common.kafka import KafkaPublisher
from common.schemas import DataIngestionRequest, PipelineStatusResponse
from generated import weather_pb2


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
        self.last_published_event: dict[str, object] | None = None
        self.last_feedback_event: dict[str, object] | None = None

    def update(self) -> dict[str, object]:
        """Build one sample hourly weather dataset and publish it to Kafka.

        Returns:
            Browser-facing acknowledgement that the weather dataset was sent.
        """
        dataset = self.algo.update()
        self.publisher.publish_proto(self.weather_topic_name, dataset, key=dataset.daily_weather[0].region_code)
        return self.response_to_http(dataset, self.weather_topic_name)

    def response_to_http(
        self,
        dataset: weather_pb2.HourlyWeatherDataset,
        weather_topic_name: str,
    ) -> dict[str, object]:
        """Build one browser-facing acknowledgement payload for weather publication."""

        self.last_published_event = MessageToDict(dataset, preserving_proto_field_name=True)
        self.last_feedback_event = {
            "accepted": True,
            "message": "Kafka发送成功",
            "topic": weather_topic_name,
            "generated_at": dataset.generated_at,
            "target_service": "forecast_boot",
        }
        print(f"data_boot response_to_http topic={weather_topic_name} generated_at={dataset.generated_at}")
        return self.last_feedback_event

    def ingest(self, request: DataIngestionRequest) -> dict[str, object]:
        """Accept an ingestion request without publishing to Kafka topics.

        Args:
            request: Structured HTTP payload containing weather, load, price,
                and renewable generation information.

        Returns:
            Response payload that confirms the request was accepted.
        """
        return self.algo.ingest(request.model_dump())

    def start_pipeline(self) -> None:
        """Data boot has no background worker in the current pipeline design."""

    def stop_pipeline(self) -> None:
        """Data boot has no background worker to stop."""

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Expose the latest accepted source event and forwarded forecast event."""
        last_published_event = self.last_published_event or self.algo.last_published_event or {}
        last_feedback_event = self.last_feedback_event or self.algo.last_feedback_event or {}

        return PipelineStatusResponse(
            service_name=self.boot_config["service_name"],
            last_published_event_id=last_published_event.get("event_id"),
            last_feedback_event_id=last_feedback_event.get("upstream_event_id") or last_feedback_event.get("event_id"),
            details={
                "last_published_event": last_published_event,
                "last_feedback_event": last_feedback_event,
            },
        )


@lru_cache(maxsize=1)
def get_data_service() -> DataService:
    """Return a singleton `DataService` instance for the FastAPI process."""
    return DataService()