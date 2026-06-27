from __future__ import annotations

from typing import Any

from infrastructure.loaders.loader_base import BaseConfigReader


class KafkaRuntimeSettings:
    """Runtime Kafka settings built in service layer from loader values."""

    def __init__(self, service_name: str, kafka_config: dict[str, Any]) -> None:
        self.service_name = service_name
        self.kafka_global_env = kafka_config.get("kafka_global_env")
        self.kafka_bootstrap_servers = kafka_config.get("kafka_bootstrap_servers")
        self.kafka_client_id = kafka_config.get("kafka_client_id")
        self.kafka_enabled = bool(kafka_config.get("kafka_enabled"))
        self.kafka_consumer_group_prefix = kafka_config.get("kafka_consumer_group_prefix")
        self.kafka_consumer_group_salt = kafka_config.get("kafka_consumer_group_salt", "")
        self.kafka_auto_offset_reset = kafka_config.get("kafka_auto_offset_reset")

    def consumer_group(self, group_suffix: str) -> str:
        base_group = f"{self.kafka_consumer_group_prefix}.{self.service_name}.{group_suffix}"
        if self.kafka_consumer_group_salt:
            return f"{base_group}.{self.kafka_consumer_group_salt}"
        return base_group


class KafkaConfigLoader(BaseConfigReader):
    """Loader for Kafka shared settings from kafka_config.yaml."""

    _default_config_path = BaseConfigReader.runtime_config_path("config", "common", "kafka_config.yaml")

    @classmethod
    def _config_path(cls):
        return cls.runtime_config_path("config", "common", "kafka_config.yaml")

    @classmethod
    def get_kafka(cls) -> dict[str, Any]:
        return cls.required_config_section(cls._config_path(), "kafka")


__all__ = ["KafkaConfigLoader", "KafkaRuntimeSettings"]