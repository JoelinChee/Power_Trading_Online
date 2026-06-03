"""Configuration objects shared by all three boot services.

This module centralizes environment-driven runtime settings so every service uses
the same naming rules, topic naming strategy, and broker connectivity defaults.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from common.paths import ENV_FILE_PATH


class BaseBootSettings(BaseSettings):
    """Base configuration shared by every boot application.

    The values in this class are resolved from the repository's `.env` file and
    can also be overridden by process environment variables when needed.
    """

    model_config = SettingsConfigDict(env_file=str(ENV_FILE_PATH), env_file_encoding="utf-8", extra="ignore")

    global_env: str = Field(default="dev", description="Logical deployment environment, for example dev or prod.")
    kafka_bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated Kafka bootstrap server list.")
    kafka_client_id: str = Field(default="power-trading-platform", description="Client identifier used by Kafka producers and consumers.")
    kafka_topic_prefix: str = Field(default="power_trading", description="Prefix applied to all business Kafka topics.")
    kafka_enabled: bool = Field(default=False, description="Feature switch that enables or disables Kafka producers and consumers.")
    kafka_consumer_group_prefix: str = Field(default="power_trading", description="Prefix used when building service consumer-group identifiers.")
    kafka_consumer_group_salt: str = Field(default="", description="Optional suffix appended to Kafka consumer-group names to force replay with a fresh group.")
    kafka_auto_offset_reset: str = Field(default="earliest", description="Offset reset strategy used when a consumer group has no committed offsets.")

    service_name: str = Field(description="Stable service identifier used in logs, topics, and consumer-group names.")
    host: str = Field(default="0.0.0.0", description="Bind address used when the FastAPI service starts with Uvicorn.")
    port: int = Field(description="Network port exposed by the service process.")

    def topic_name(self, topic_suffix: str) -> str:
        """Build the fully qualified topic name for a given business suffix.

        Args:
            topic_suffix: The business-specific part of the topic name, such as
                `weather.events` or `forecast.events`.

        Returns:
            The full topic name with the configured prefix applied.
        """
        return f"{self.kafka_topic_prefix}.{topic_suffix}"

    def consumer_group(self, group_suffix: str) -> str:
        """Build the consumer-group name for a service-specific Kafka worker.

        Args:
            group_suffix: The logical consumer role, such as `feedback` or
                `projection`.

        Returns:
            A stable consumer-group name that is unique per service and role.
        """
        base_group = f"{self.kafka_consumer_group_prefix}.{self.service_name}.{group_suffix}"
        if self.kafka_consumer_group_salt:
            return f"{base_group}.{self.kafka_consumer_group_salt}"
        return base_group


class DataBootSettings(BaseBootSettings):
    """Settings for the data acquisition boot service."""

    service_name: str = "data_boot"
    host: str = "0.0.0.0"
    port: int = 8001


class ForecastBootSettings(BaseBootSettings):
    """Settings for the forecasting boot service."""

    service_name: str = "forecast_boot"
    host: str = "0.0.0.0"
    port: int = 8002
    timer_interval_seconds: float = Field(default=5.0, description="Seconds between timer-triggered forecast publication cycles.")


class ExecutionBootSettings(BaseBootSettings):
    """Settings for the execution and risk-control boot service."""

    service_name: str = "execution_boot"
    host: str = "0.0.0.0"
    port: int = 8003
    timer_interval_seconds: float = Field(default=10.0, description="Seconds between timer-triggered execution evaluation cycles.")