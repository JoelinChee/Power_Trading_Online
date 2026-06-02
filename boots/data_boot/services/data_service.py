from __future__ import annotations

from functools import lru_cache
from uuid import uuid4

import httpx
from google.protobuf.json_format import MessageToDict

from common.config import DataBootSettings
from common.proto_loader import trading_messages_pb2
from common.schemas import DataIngestionRequest, ForecastPipelineRequest, PipelineStatusResponse


class DataService:
    """Service layer for data acquisition and forecast dispatch handling."""

    def __init__(self) -> None:
        self.settings = DataBootSettings(host="0.0.0.0", port=8001)
        self.last_published_event: dict[str, object] | None = None
        self.last_feedback_event: dict[str, object] | None = None

    def get_current_snapshot(self) -> dict[str, object]:
        """Return a mock real-time snapshot for manual API inspection."""
        return {
            "weather": {
                "temperature_celsius": 31.6,
                "humidity_ratio": 72.0,
                "wind_speed_mps": 5.4,
                "weather_type": "cloudy",
            },
            "load": {
                "enterprise_id": "default-enterprise",
                "timestamp": "2026-06-01T09:30:00+08:00",
                "load_mw": 68.4,
            },
            "price": {
                "timestamp": "2026-06-01T09:30:00+08:00",
                "spot_price": 436.5,
                "market": "real_time",
            },
            "renewable": {
                "timestamp": "2026-06-01T09:30:00+08:00",
                "wind_output_mw": 25.2,
                "solar_output_mw": 18.9,
            },
        }

    def ingest(self, request: DataIngestionRequest) -> dict[str, object]:
        """Convert an HTTP ingestion request into a source event and forward it.

        Args:
            request: Structured HTTP payload containing weather, load, price,
                and renewable generation information.

        Returns:
            Response payload that confirms the accepted event and the forecast
                queue acknowledgement returned by forecast boot.
        """
        event = trading_messages_pb2.DataIngestionEvent()
        event.event_id = str(uuid4())
        event.source_service = self.settings.service_name
        event.published_at = request.load.timestamp
        event.enterprise_id = request.load.enterprise_id
        event.target_date = request.load.timestamp[:10]

        event.weather.temperature_celsius = request.weather.temperature_celsius
        event.weather.humidity_ratio = request.weather.humidity_ratio
        event.weather.wind_speed_mps = request.weather.wind_speed_mps
        event.weather.weather_type = request.weather.weather_type

        event.load.timestamp = request.load.timestamp
        event.load.load_mw = request.load.load_mw

        event.price.timestamp = request.price.timestamp
        event.price.spot_price = request.price.spot_price
        event.price.market = request.price.market

        event.renewable.timestamp = request.renewable.timestamp
        event.renewable.wind_output_mw = request.renewable.wind_output_mw
        event.renewable.solar_output_mw = request.renewable.solar_output_mw

        self.last_published_event = MessageToDict(event, preserving_proto_field_name=True)
        forecast_request = ForecastPipelineRequest(
            upstream_event_id=event.event_id,
            source_service=self.settings.service_name,
            published_at=event.published_at,
            target_date=event.target_date,
            weather=request.weather,
            load=request.load,
            price=request.price,
            renewable=request.renewable,
        )

        with httpx.Client() as client:
            response = client.post(
                f"{self.settings.forecast_boot_base_url}/api/v1/forecast/events",
                json=forecast_request.model_dump(),
                timeout=10.0,
            )
            response.raise_for_status()

        self.last_feedback_event = response.json()
        return {
            "accepted": True,
            "message": "Data accepted and queued in forecast boot",
            "event": self.last_published_event,
            "forecast_ack": self.last_feedback_event,
        }

    def start_pipeline(self) -> None:
        """Data boot has no background worker in the current pipeline design."""

    def stop_pipeline(self) -> None:
        """Data boot has no background worker to stop."""

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Expose the latest accepted source event and forwarded forecast event."""
        return PipelineStatusResponse(
            service_name=self.settings.service_name,
            last_published_event_id=(self.last_published_event or {}).get("event_id"),
            last_feedback_event_id=(self.last_feedback_event or {}).get("upstream_event_id") or (self.last_feedback_event or {}).get("event_id"),
            details={
                "last_published_event": self.last_published_event or {},
                "last_feedback_event": self.last_feedback_event or {},
            },
        )


@lru_cache(maxsize=1)
def get_data_service() -> DataService:
    """Return a singleton `DataService` instance for the FastAPI process."""
    return DataService()