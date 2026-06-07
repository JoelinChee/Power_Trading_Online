from __future__ import annotations

from datetime import datetime, timedelta, timezone

from google.protobuf.json_format import MessageToDict

from generated import weather_pb2
from common.schemas import DataIngestionRequest


class DataAlgo:
    """Algorithm layer for data shaping and weather event publication."""

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
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

    def build_browser_weather_dataset(self) -> weather_pb2.HourlyWeatherDataset:
        """Build one sample hourly weather dataset for service-layer publication."""

        return self._build_hourly_weather_dataset()

    def record_browser_weather_publication(
        self,
        dataset: weather_pb2.HourlyWeatherDataset,
        weather_topic_name: str,
    ) -> dict[str, object]:
        """Record the result of one browser-triggered weather publication."""

        self.last_published_event = MessageToDict(dataset, preserving_proto_field_name=True)
        self.last_feedback_event = {
            "accepted": True,
            "message": "Kafka发送成功",
            "topic": weather_topic_name,
            "generated_at": dataset.generated_at,
            "target_service": "forecast_boot",
        }
        return self.last_feedback_event

    def ingest(self, request: DataIngestionRequest) -> dict[str, object]:
        """Accept an ingestion request without publishing to Kafka topics."""

        self.last_published_event = {
            "source_service": self.service_name,
            "published_at": request.load.timestamp,
            "enterprise_id": request.load.enterprise_id,
            "target_date": request.load.timestamp[:10],
            "weather": request.weather.model_dump(),
            "load": request.load.model_dump(),
            "price": request.price.model_dump(),
            "renewable": request.renewable.model_dump(),
        }
        self.last_feedback_event = {
            "accepted": True,
            "message": "Data accepted (no Kafka topic publication)",
        }
        return {
            "accepted": True,
            "message": "Data accepted",
            "event": self.last_published_event,
            "publish_ack": self.last_feedback_event,
        }

    def _build_hourly_weather_dataset(self) -> weather_pb2.HourlyWeatherDataset:
        """Create a 24-hour sample weather dataset for browser-triggered publication."""

        base_time = datetime.now(timezone(timedelta(hours=8))).replace(minute=0, second=0, microsecond=0)
        dataset = weather_pb2.HourlyWeatherDataset()
        dataset.source = self.service_name
        dataset.generated_at = base_time.isoformat()

        daily_weather = dataset.daily_weather.add()
        daily_weather.target_date = base_time.date().isoformat()
        daily_weather.region_code = "CN-SH"
        daily_weather.region_name = "Shanghai"

        base_temperature = 29.5
        base_humidity = 74.0
        base_pressure = 1008.0

        for hour in range(24):
            hourly = daily_weather.hourly_weather.add()
            timestamp = base_time + timedelta(hours=hour)
            hourly.timestamp = timestamp.isoformat()
            hourly.target_date = timestamp.date().isoformat()
            hourly.hour_of_day = timestamp.hour
            hourly.forecast_hour = hour
            hourly.region_code = daily_weather.region_code
            hourly.region_name = daily_weather.region_name
            hourly.condition = weather_pb2.WEATHER_CONDITION_PARTLY_CLOUDY if 6 <= timestamp.hour <= 18 else weather_pb2.WEATHER_CONDITION_CLOUDY
            hourly.condition_text = "partly_cloudy" if 6 <= timestamp.hour <= 18 else "cloudy"
            hourly.temperature_celsius = round(base_temperature + ((hour % 8) - 4) * 0.8, 2)
            hourly.apparent_temperature_celsius = round(hourly.temperature_celsius + 1.6, 2)
            hourly.humidity_percent = round(base_humidity + ((hour % 6) - 3) * 2.5, 2)
            hourly.dew_point_celsius = round(hourly.temperature_celsius - 4.2, 2)
            hourly.pressure_hpa = round(base_pressure + ((hour % 5) - 2) * 1.3, 2)
            hourly.visibility_km = 8.5 if 5 <= timestamp.hour <= 21 else 6.2
            hourly.cloud_cover_percent = 38.0 if 6 <= timestamp.hour <= 18 else 68.0
            hourly.solar_irradiance_wm2 = max(0.0, round((12 - abs(timestamp.hour - 12)) * 62.5, 2))
            hourly.uv_index = max(0.0, round((12 - abs(timestamp.hour - 12)) * 0.6, 2))

            hourly.wind.speed_mps = round(4.2 + (hour % 4) * 0.7, 2)
            hourly.wind.gust_speed_mps = round(hourly.wind.speed_mps + 2.1, 2)
            hourly.wind.direction_degrees = float((hour * 15) % 360)
            hourly.wind.direction_text = self._wind_direction_text(hourly.wind.direction_degrees)
            hourly.wind.level = weather_pb2.WIND_LEVEL_GENTLE_BREEZE if hourly.wind.speed_mps < 6.0 else weather_pb2.WIND_LEVEL_MODERATE_BREEZE

            hourly.precipitation.type = weather_pb2.PRECIPITATION_TYPE_RAIN if 14 <= timestamp.hour <= 17 else weather_pb2.PRECIPITATION_TYPE_NONE
            hourly.precipitation.amount_mm = 1.6 if 14 <= timestamp.hour <= 17 else 0.0
            hourly.precipitation.probability_percent = 62.0 if 14 <= timestamp.hour <= 17 else 12.0
            hourly.precipitation.snow_depth_cm = 0.0

        return dataset

    def _wind_direction_text(self, direction_degrees: float) -> str:
        """Convert numeric wind direction into a compact compass label."""

        labels = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return labels[int((direction_degrees + 22.5) % 360 // 45)]
