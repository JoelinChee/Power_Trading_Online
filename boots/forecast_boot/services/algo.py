from __future__ import annotations

import logging
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from generated import trading_messages_pb2, weather_pb2
from common.schemas import SeriesPoint


class ForecastAlgo:
	"""Algorithm layer for weather consumption and forecast generation."""

	def __init__(
		self,
		service_name: str,
		timer_interval_seconds: float,
		logger: logging.Logger,
	) -> None:
		self.service_name = service_name
		self.timer_interval_seconds = timer_interval_seconds
		self.logger = logger

		self.last_received_event: dict[str, object] | None = None
		self.last_received_weather_event: dict[str, object] | None = None
		self.last_published_event: dict[str, object] | None = None

	def handle_weather_event(self, payload: bytes) -> trading_messages_pb2.ForecastEvent:
		"""Consume one weather payload and build the next ForecastEvent."""

		dataset = weather_pb2.HourlyWeatherDataset()
		dataset.ParseFromString(payload)
		self.logger.warning(
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
		self.last_published_event = MessageToDict(forecast_event, preserving_proto_field_name=True)
		return forecast_event

	def _build_forecast_event(
		self, dataset: weather_pb2.HourlyWeatherDataset, upstream_event_id: str
	) -> trading_messages_pb2.ForecastEvent:
		"""Create one ForecastEvent from an incoming weather dataset."""

		(
			weather_points,
			load_points,
			price_points,
			weather_type,
			target_date,
			enterprise_id,
			published_at,
			renewable_mw,
		) = self._build_forecast_points(dataset)

		forecast_event = trading_messages_pb2.ForecastEvent()
		forecast_event.event_id = str(uuid4())
		forecast_event.source_service = self.service_name
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
