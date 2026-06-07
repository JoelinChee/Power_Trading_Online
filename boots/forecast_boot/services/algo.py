from __future__ import annotations

from dataclasses import dataclass
import logging
from uuid import uuid4

from google.protobuf.json_format import MessageToDict

from boots.forecast_boot.services.messages import ForecastInMessages
from boots.forecast_boot.services.messages import ForecastOutMessages
from generated import trading_messages_pb2, weather_pb2


ForecastPoint = tuple[int, float]


@dataclass(frozen=True)
class ForecastComputationContext:
	"""Immutable context that captures event-level metadata for one computation pass."""

	upstream_event_id: str
	weather_type: str
	target_date: str
	enterprise_id: str
	published_at: str
	available_renewable_mw: float


class ForecastAlgo:
	"""Pure forecast transformation layer.

	Design notes:
	- Keeps domain logic isolated from transport/framework concerns.
	- Accepts typed in-memory messages and returns typed out-memory messages.
	- Maintains lightweight snapshots for observability endpoints.
	"""

	SLOTS_PER_DAY = 96
	DEFAULT_WEATHER_TYPE = "unknown"
	DEFAULT_ENTERPRISE_ID = "default-enterprise"

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

	def update(self, in_messages: ForecastInMessages) -> ForecastOutMessages:
		"""Consume one batch of weather payloads and build ForecastEvents.

		Args:
			in_messages: Typed in-memory input payload container. Supports either
				a single payload or a batch queue payload.

		Returns:
			Typed output payload container with generated ForecastEvent queue.

		Raises:
			ValueError: If the inbound payload batch is missing.
		"""

		payloads = self._resolve_inbound_payloads(in_messages)
		if not payloads:
			raise ValueError("Missing inbound weather payload batch")

		generated_events: list[trading_messages_pb2.ForecastEvent] = []
		for payload in payloads:
			forecast_event = self._build_forecast_for_one_payload(payload)
			generated_events.append(forecast_event)

		return ForecastOutMessages(
			forecast_boot_to_execution_boot_queue=generated_events,
		)

	def _resolve_inbound_payloads(self, in_messages: ForecastInMessages) -> list[bytes]:
		"""Resolve inbound payload batch from the message contract object."""

		if in_messages.data_boot_to_forecast_boot_queue:
			return [payload for payload in in_messages.data_boot_to_forecast_boot_queue if payload]
		return []

	def _build_forecast_for_one_payload(self, payload: bytes) -> trading_messages_pb2.ForecastEvent:
		"""Transform one weather payload to one forecast event."""

		dataset = self._parse_dataset(payload)
		self.logger.warning(
			"forecast_boot weather handler invoked: source=%s generated_at=%s",
			dataset.source,
			dataset.generated_at,
		)

		context, weather_points, load_points, price_points = self._build_features(dataset)

		self.last_received_weather_event = MessageToDict(dataset, preserving_proto_field_name=True)
		self.last_received_event = {
			"upstream_event_id": context.upstream_event_id,
			"source_service": dataset.source,
			"published_at": context.published_at,
			"target_date": context.target_date,
			"enterprise_id": context.enterprise_id,
		}

		forecast_event = self._build_forecast_event(
			context=context,
			weather_points=weather_points,
			load_points=load_points,
			price_points=price_points,
		)
		self.last_published_event = MessageToDict(forecast_event, preserving_proto_field_name=True)
		return forecast_event

	def _parse_dataset(self, payload: bytes) -> weather_pb2.HourlyWeatherDataset:
		"""Deserialize protobuf payload into a strongly typed weather dataset."""

		dataset = weather_pb2.HourlyWeatherDataset()
		dataset.ParseFromString(payload)
		return dataset

	def _build_forecast_event(
		self,
		context: ForecastComputationContext,
		weather_points: list[ForecastPoint],
		load_points: list[ForecastPoint],
		price_points: list[ForecastPoint],
	) -> trading_messages_pb2.ForecastEvent:
		"""Create one ForecastEvent from extracted features and metadata context."""

		forecast_event = trading_messages_pb2.ForecastEvent()
		forecast_event.event_id = str(uuid4())
		forecast_event.source_service = self.service_name
		forecast_event.upstream_event_id = context.upstream_event_id
		forecast_event.published_at = context.published_at
		forecast_event.enterprise_id = context.enterprise_id
		forecast_event.target_date = context.target_date
		forecast_event.weather_type = context.weather_type
		forecast_event.available_renewable_mw = context.available_renewable_mw

		for slot, value in weather_points:
			proto_point = forecast_event.weather_points.add()
			proto_point.slot = slot
			proto_point.value = value

		for slot, value in load_points:
			proto_point = forecast_event.load_points.add()
			proto_point.slot = slot
			proto_point.value = value

		for slot, value in price_points:
			proto_point = forecast_event.price_points.add()
			proto_point.slot = slot
			proto_point.value = value

		return forecast_event

	def _build_features(
		self, dataset: weather_pb2.HourlyWeatherDataset
	) -> tuple[ForecastComputationContext, list[ForecastPoint], list[ForecastPoint], list[ForecastPoint]]:
		"""Extract metadata context and generate synthetic feature series.

		The current implementation intentionally keeps deterministic synthetic generation
		for smoke-test stability. Feature formulas can be swapped later without changing
		the service transport contract.
		"""

		daily = dataset.daily_weather[0] if dataset.daily_weather else None
		hourly = list(daily.hourly_weather) if daily and daily.hourly_weather else []

		weather_type = hourly[0].condition_text if hourly else self.DEFAULT_WEATHER_TYPE
		target_date = daily.target_date if daily else (dataset.generated_at[:10] if dataset.generated_at else "")
		enterprise_id = daily.region_code if daily else self.DEFAULT_ENTERPRISE_ID
		published_at = dataset.generated_at
		upstream_event_id = str(uuid4())

		base_temp = hourly[0].temperature_celsius if hourly else 28.0
		base_wind = hourly[0].wind.speed_mps if hourly else 4.0
		base_load = 60.0 + max(base_temp - 20.0, 0.0) * 1.2
		base_price = 420.0 + max(base_temp - 24.0, 0.0) * 2.8
		renewable_mw = round((base_wind * 2.2) + (hourly[0].solar_irradiance_wm2 / 40.0 if hourly else 8.0), 2)

		weather_points: list[ForecastPoint] = []
		load_points: list[ForecastPoint] = []
		price_points: list[ForecastPoint] = []

		for slot in range(1, self.SLOTS_PER_DAY + 1):
			source_hour = hourly[(slot - 1) % len(hourly)] if hourly else None
			weather_seed = source_hour.temperature_celsius if source_hour else base_temp

			weather_adjustment = ((slot % 16) - 8) * 0.18
			weather_value = round(weather_seed + weather_adjustment, 2)
			weather_points.append((slot, weather_value))

			peak_factor = 1.18 if 33 <= slot <= 76 else 0.92
			intra_day_adjustment = ((slot % 12) - 6) * 0.35
			load_value = round(base_load * peak_factor + intra_day_adjustment, 2)
			load_points.append((slot, load_value))

			demand_factor = 1.12 if 29 <= slot <= 80 else 0.95
			volatility = ((slot % 8) - 4) * 1.8
			price_value = round(base_price * demand_factor + volatility, 2)
			price_points.append((slot, price_value))

		context = ForecastComputationContext(
			upstream_event_id=upstream_event_id,
			weather_type=weather_type,
			target_date=target_date,
			enterprise_id=enterprise_id,
			published_at=published_at,
			available_renewable_mw=renewable_mw,
		)
		return context, weather_points, load_points, price_points
