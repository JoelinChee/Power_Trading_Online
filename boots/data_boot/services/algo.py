from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from generated import weather_pb2


@dataclass(frozen=True)
class WeatherGenerationConfig:
    """Typed configuration used when generating deterministic weather samples."""

    region_code: str
    region_name: str
    base_temperature_celsius: float
    base_humidity_percent: float
    base_pressure_hpa: float
    day_visibility_km: float
    night_visibility_km: float
    day_cloud_cover_percent: float
    night_cloud_cover_percent: float
    rain_start_hour: int
    rain_end_hour: int
    rain_amount_mm: float
    rain_probability_percent: float
    dry_probability_percent: float


class DataAlgo:
    """Algorithm layer for data shaping and weather event publication."""

    def __init__(self, service_name: str, algo_config: dict[str, object] | None = None) -> None:
        self.service_name = service_name
        self.algo_config = algo_config if isinstance(algo_config, dict) else {}

        self.last_published_event: dict[str, object] | None = None
        self.last_feedback_event: dict[str, object] | None = None

    def update(self) -> weather_pb2.HourlyWeatherDataset:
        """Build one sample hourly weather dataset for service-layer publication."""

        return self._build_hourly_weather_dataset()

    def ingest(self, request: dict[str, dict[str, Any]]) -> dict[str, object]:
        """Accept an ingestion request without publishing to Kafka topics."""

        weather_payload = request["weather"]
        load_payload = request["load"]
        price_payload = request["price"]
        renewable_payload = request["renewable"]

        self.last_published_event = {
            "source_service": self.service_name,
            "published_at": str(load_payload["timestamp"]),
            "enterprise_id": str(load_payload["enterprise_id"]),
            "target_date": str(load_payload["timestamp"])[:10],
            "weather": dict(weather_payload),
            "load": dict(load_payload),
            "price": dict(price_payload),
            "renewable": dict(renewable_payload),
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
        generation_config = self._build_generation_config()
        dataset = weather_pb2.HourlyWeatherDataset()
        dataset.source = self.service_name
        dataset.generated_at = base_time.isoformat()

        daily_weather = dataset.daily_weather.add()
        daily_weather.target_date = base_time.date().isoformat()
        daily_weather.region_code = generation_config.region_code
        daily_weather.region_name = generation_config.region_name

        for hour in range(24):
            timestamp = base_time + timedelta(hours=hour)
            hourly = daily_weather.hourly_weather.add()
            self._populate_hourly_weather(hourly, timestamp, hour, generation_config)

        return dataset

    def _build_generation_config(self) -> WeatherGenerationConfig:
        """Convert raw config dictionaries into a typed generation config object."""
        return WeatherGenerationConfig(
            region_code=str(self.algo_config["region_code"]),
            region_name=str(self.algo_config["region_name"]),
            base_temperature_celsius=float(self.algo_config["base_temperature_celsius"]),
            base_humidity_percent=float(self.algo_config["base_humidity_percent"]),
            base_pressure_hpa=float(self.algo_config["base_pressure_hpa"]),
            day_visibility_km=float(self.algo_config["day_visibility_km"]),
            night_visibility_km=float(self.algo_config["night_visibility_km"]),
            day_cloud_cover_percent=float(self.algo_config["day_cloud_cover_percent"]),
            night_cloud_cover_percent=float(self.algo_config["night_cloud_cover_percent"]),
            rain_start_hour=int(self.algo_config["rain_start_hour"]),
            rain_end_hour=int(self.algo_config["rain_end_hour"]),
            rain_amount_mm=float(self.algo_config["rain_amount_mm"]),
            rain_probability_percent=float(self.algo_config["rain_probability_percent"]),
            dry_probability_percent=float(self.algo_config["dry_probability_percent"]),
        )

    def _populate_hourly_weather(
        self,
        hourly: weather_pb2.HourlyWeather,
        timestamp: datetime,
        forecast_hour: int,
        generation_config: WeatherGenerationConfig,
    ) -> None:
        """Populate one hourly weather protobuf message with deterministic values."""
        is_daytime = 6 <= timestamp.hour <= 18
        hourly.timestamp = timestamp.isoformat()
        hourly.target_date = timestamp.date().isoformat()
        hourly.hour_of_day = timestamp.hour
        hourly.forecast_hour = forecast_hour
        hourly.region_code = generation_config.region_code
        hourly.region_name = generation_config.region_name
        hourly.condition = weather_pb2.WEATHER_CONDITION_PARTLY_CLOUDY if is_daytime else weather_pb2.WEATHER_CONDITION_CLOUDY
        hourly.condition_text = "partly_cloudy" if is_daytime else "cloudy"

        hourly.temperature_celsius = round(generation_config.base_temperature_celsius + ((forecast_hour % 8) - 4) * 0.8, 2)
        hourly.apparent_temperature_celsius = round(hourly.temperature_celsius + 1.6, 2)
        hourly.humidity_percent = round(generation_config.base_humidity_percent + ((forecast_hour % 6) - 3) * 2.5, 2)
        hourly.dew_point_celsius = round(hourly.temperature_celsius - 4.2, 2)
        hourly.pressure_hpa = round(generation_config.base_pressure_hpa + ((forecast_hour % 5) - 2) * 1.3, 2)
        hourly.visibility_km = generation_config.day_visibility_km if 5 <= timestamp.hour <= 21 else generation_config.night_visibility_km
        hourly.cloud_cover_percent = generation_config.day_cloud_cover_percent if is_daytime else generation_config.night_cloud_cover_percent
        hourly.solar_irradiance_wm2 = max(0.0, round((12 - abs(timestamp.hour - 12)) * 62.5, 2))
        hourly.uv_index = max(0.0, round((12 - abs(timestamp.hour - 12)) * 0.6, 2))

        self._populate_wind(hourly, forecast_hour)
        self._populate_precipitation(hourly, timestamp.hour, generation_config)

    def _populate_wind(self, hourly: weather_pb2.HourlyWeather, forecast_hour: int) -> None:
        """Populate wind values for one hourly weather point."""
        hourly.wind.speed_mps = round(4.2 + (forecast_hour % 4) * 0.7, 2)
        hourly.wind.gust_speed_mps = round(hourly.wind.speed_mps + 2.1, 2)
        hourly.wind.direction_degrees = float((forecast_hour * 15) % 360)
        hourly.wind.direction_text = self._wind_direction_text(hourly.wind.direction_degrees)
        hourly.wind.level = weather_pb2.WIND_LEVEL_GENTLE_BREEZE if hourly.wind.speed_mps < 6.0 else weather_pb2.WIND_LEVEL_MODERATE_BREEZE

    def _populate_precipitation(
        self,
        hourly: weather_pb2.HourlyWeather,
        hour_of_day: int,
        generation_config: WeatherGenerationConfig,
    ) -> None:
        """Populate precipitation fields for one hourly weather point."""
        is_rain_window = generation_config.rain_start_hour <= hour_of_day <= generation_config.rain_end_hour
        hourly.precipitation.type = weather_pb2.PRECIPITATION_TYPE_RAIN if is_rain_window else weather_pb2.PRECIPITATION_TYPE_NONE
        hourly.precipitation.amount_mm = generation_config.rain_amount_mm if is_rain_window else 0.0
        hourly.precipitation.probability_percent = generation_config.rain_probability_percent if is_rain_window else generation_config.dry_probability_percent
        hourly.precipitation.snow_depth_cm = 0.0

    def _wind_direction_text(self, direction_degrees: float) -> str:
        """Convert numeric wind direction into a compact compass label."""

        labels = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return labels[int((direction_degrees + 22.5) % 360 // 45)]

