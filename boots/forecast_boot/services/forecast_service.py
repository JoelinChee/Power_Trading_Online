from __future__ import annotations

import threading
from collections import deque
from functools import lru_cache
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from common.config import ForecastBootSettings
from common.kafka import KafkaPublisher
from common.proto_loader import trading_messages_pb2
from common.schemas import DataIngestionRequest, ForecastPipelineRequest, ForecastRequest, ForecastResponse, PipelineStatusResponse, SeriesPoint
from common.timer import PeriodicWorker


class ForecastService:
    """Service layer for HTTP forecast APIs and Kafka-backed forecast publication."""

    def __init__(self) -> None:
        self.settings = ForecastBootSettings(host="0.0.0.0", port=8002)
        self.publisher = KafkaPublisher(self.settings)
        self._pending_requests: deque[ForecastPipelineRequest] = deque()
        self._pending_lock = threading.Lock()
        self._timer_worker = PeriodicWorker(
            name=f"{self.settings.service_name}-timer",
            interval_seconds=self.settings.timer_interval_seconds,
            callback=self._flush_pending_requests,
        )
        self.last_received_event: dict[str, object] | None = None
        self.last_published_event: dict[str, object] | None = None

    def forecast_weather(self, request: ForecastRequest) -> dict[str, object]:
        """Return a simple synchronous weather forecast for direct API calls.

        Args:
            request: Forecast request submitted through the HTTP endpoint.

        Returns:
            A summarized weather forecast payload for manual invocation flows.
        """
        response = {
            "target_date": request.target_date,
            "enterprise_id": request.enterprise_id,
            "weather_type": request.weather_type,
            "temperature_range": [27.5, 33.8],
            "rain_probability": 0.22,
            "wind_speed_range": [3.2, 7.5],
        }
        return response

    def forecast_load(self, request: ForecastRequest) -> ForecastResponse:
        """Build a mock 96-point enterprise load curve for direct API calls."""
        points = []
        for slot in range(1, 97):
            peak_factor = 1.18 if 33 <= slot <= 76 else 0.92
            intra_day_adjustment = ((slot % 12) - 6) * 0.35
            value = round(request.base_load_mw * peak_factor + intra_day_adjustment, 2)
            points.append(SeriesPoint(slot=slot, value=value))

        response = ForecastResponse(
            target_date=request.target_date,
            enterprise_id=request.enterprise_id,
            metric="load_mw",
            points=points,
        )
        return response

    def forecast_price(self, request: ForecastRequest) -> ForecastResponse:
        """Build a mock 96-point power price curve for direct API calls."""
        points = []
        for slot in range(1, 97):
            demand_factor = 1.12 if 29 <= slot <= 80 else 0.95
            volatility = ((slot % 8) - 4) * 1.8
            value = round(request.base_price * demand_factor + volatility, 2)
            points.append(SeriesPoint(slot=slot, value=value))

        response = ForecastResponse(
            target_date=request.target_date,
            enterprise_id=request.enterprise_id,
            metric="price_cny_per_mwh",
            points=points,
        )
        return response

    def start_pipeline(self) -> None:
        """Start the timer-driven forecast publication worker."""
        self._timer_worker.start()

    def stop_pipeline(self) -> None:
        """Stop the timer-driven forecast publication worker."""
        self._timer_worker.stop()

    def get_pipeline_status(self) -> PipelineStatusResponse:
        """Return the latest received source payload and published forecast event."""
        with self._pending_lock:
            pending_request_count = len(self._pending_requests)

        return PipelineStatusResponse(
            service_name=self.settings.service_name,
            last_consumed_event_id=(self.last_received_event or {}).get("upstream_event_id"),
            last_published_event_id=(self.last_published_event or {}).get("event_id"),
            details={
                "last_received_event": self.last_received_event or {},
                "last_published_event": self.last_published_event or {},
                "pending_request_count": pending_request_count,
                "timer_interval_seconds": self.settings.timer_interval_seconds,
            },
        )

    def publish_forecast_event(self, request: ForecastPipelineRequest) -> dict[str, object]:
        """Queue a forecast request that will be published on the next timer tick.

        Args:
            request: Structured upstream payload forwarded by data boot.

        Returns:
            Queue acknowledgement for observability.
        """
        self.last_received_event = {
            "upstream_event_id": request.upstream_event_id,
            "source_service": request.source_service,
            "published_at": request.published_at,
            "target_date": request.target_date,
            "enterprise_id": request.load.enterprise_id,
            "weather": request.weather.model_dump(),
            "load": request.load.model_dump(),
            "price": request.price.model_dump(),
            "renewable": request.renewable.model_dump(),
        }

        with self._pending_lock:
            self._pending_requests.append(request)
            pending_request_count = len(self._pending_requests)

        return {
            "accepted": True,
            "message": "Forecast request queued",
            "upstream_event_id": request.upstream_event_id,
            "pending_request_count": pending_request_count,
        }

    def _flush_pending_requests(self) -> None:
        """Publish all queued forecast requests on the current timer tick."""
        with self._pending_lock:
            pending_requests = list(self._pending_requests)
            self._pending_requests.clear()

        for request in pending_requests:
            forecast_event = self._build_forecast_event(request)
            self.publisher.publish_proto("forecast.events", forecast_event, key=request.load.enterprise_id)
            self.last_published_event = MessageToDict(forecast_event, preserving_proto_field_name=True)

    def _build_forecast_event(self, request: ForecastPipelineRequest) -> trading_messages_pb2.ForecastEvent:
        """Create the protobuf forecast event for one queued source payload."""

        weather_points, load_points, price_points = self._build_forecast_points(
            DataIngestionRequest(
                weather=request.weather,
                load=request.load,
                price=request.price,
                renewable=request.renewable,
            )
        )

        forecast_event = trading_messages_pb2.ForecastEvent()
        forecast_event.event_id = str(uuid4())
        forecast_event.source_service = self.settings.service_name
        forecast_event.upstream_event_id = request.upstream_event_id
        forecast_event.published_at = request.published_at
        forecast_event.enterprise_id = request.load.enterprise_id
        forecast_event.target_date = request.target_date
        forecast_event.weather_type = request.weather.weather_type
        forecast_event.available_renewable_mw = request.renewable.wind_output_mw + request.renewable.solar_output_mw

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

    def _build_forecast_points(self, request: DataIngestionRequest) -> tuple[list[SeriesPoint], list[SeriesPoint], list[SeriesPoint]]:
        """Build synthetic weather, load, and price series from one source snapshot."""
        weather_points = []
        load_points = []
        price_points = []

        for slot in range(1, 97):
            weather_adjustment = ((slot % 16) - 8) * 0.18
            weather_value = round(request.weather.temperature_celsius + weather_adjustment, 2)
            weather_points.append(SeriesPoint(slot=slot, value=weather_value))

            peak_factor = 1.18 if 33 <= slot <= 76 else 0.92
            intra_day_adjustment = ((slot % 12) - 6) * 0.35
            load_value = round(request.load.load_mw * peak_factor + intra_day_adjustment, 2)
            load_points.append(SeriesPoint(slot=slot, value=load_value))

            demand_factor = 1.12 if 29 <= slot <= 80 else 0.95
            volatility = ((slot % 8) - 4) * 1.8
            price_value = round(request.price.spot_price * demand_factor + volatility, 2)
            price_points.append(SeriesPoint(slot=slot, value=price_value))
        return weather_points, load_points, price_points


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    """Return a singleton `ForecastService` instance for the FastAPI process."""
    return ForecastService()