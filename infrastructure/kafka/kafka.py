"""Kafka producer and consumer helpers used by the boot services.

The goal of this module is to keep the service-layer code focused on business
logic while encapsulating the repetitive producer and background consumer setup.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Optional, Protocol

from infrastructure.logging.logging import get_logger

try:
    from confluent_kafka import Consumer, KafkaError, Producer
except ImportError:  # pragma: no cover
    Consumer = None
    KafkaError = None
    Producer = None


class KafkaSettingsProtocol(Protocol):
    service_name: str
    kafka_enabled: bool
    kafka_bootstrap_servers: str
    kafka_client_id: str
    kafka_auto_offset_reset: str

    def consumer_group(self, group_suffix: str) -> str:
        ...


logger = get_logger(__name__)


class KafkaPublisher:
    """Small protobuf-oriented Kafka producer wrapper.

    Args:
        settings: The boot configuration that provides broker connectivity,
            client identifiers, and topic naming rules.
    """

    def __init__(self, settings: KafkaSettingsProtocol) -> None:
        self.settings = settings
        self._producer = self._build_producer()

    def _build_producer(self) -> Optional[Any]:
        """Create the underlying `confluent-kafka` producer if Kafka is enabled.

        Returns:
            A configured producer instance, or `None` when Kafka is disabled or
            the optional dependency is unavailable.
        """
        if not self.settings.kafka_enabled:
            logger.info("Kafka disabled for service=%s", self.settings.service_name)
            return None

        if Producer is None:
            logger.warning("confluent-kafka is not installed; messages will be skipped")
            return None

        return Producer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "client.id": self.settings.kafka_client_id,
            }
        )

    def publish_proto(self, topic_name: str, payload: object, key: Optional[str] = None) -> None:
        """Publish a compiled protobuf message to Kafka.

        Args:
            topic_name: Fully qualified Kafka topic name loaded from config.
            payload: Compiled protobuf message instance that exposes
                `SerializeToString`.
            key: Optional Kafka record key used for partition affinity.
        """
        if self._producer is None:
            logger.info("Skip publish topic=%s payload=%s", topic_name, payload)
            return

        self._producer.produce(topic=topic_name, key=key, value=payload.SerializeToString())
        self._producer.flush(2.0)


class KafkaBatchConsumer:
    """Lazy Kafka consumer that drains currently available binary payloads.

    Timer-driven services can use this class when they want polling to happen
    on a scheduler tick instead of in a dedicated background thread.
    """

    def __init__(
        self,
        settings: KafkaSettingsProtocol,
        topic_name: str,
        group_suffix: str,
        poll_timeout_seconds: float = 0.1,
    ) -> None:
        self.settings = settings
        self.topic_name = topic_name
        self.group_suffix = group_suffix
        self.poll_timeout_seconds = poll_timeout_seconds
        self._consumer: Optional[Any] = None

    def drain_available(self) -> list[bytes]:
        """Poll until Kafka has no immediately available payloads."""
        consumer = self._get_or_create_consumer()
        if consumer is None:
            return []

        payloads: list[bytes] = []
        while True:
            message = consumer.poll(self.poll_timeout_seconds)
            if message is None:
                break
            if message.error():
                if self._should_ignore_poll_error(message.error()):
                    break
                self._log_poll_error(message.error())
                break

            payload = message.value()
            if payload:
                payloads.append(payload)
        return payloads

    def close(self) -> None:
        """Close the underlying consumer if it has been created."""
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def _get_or_create_consumer(self) -> Optional[Any]:
        """Create one Kafka consumer lazily and reuse it across scheduler ticks."""
        if not self.settings.kafka_enabled:
            logger.info("Kafka consumer disabled for service=%s", self.settings.service_name)
            return None

        if Consumer is None:
            logger.warning("confluent-kafka is not installed; consumer will not start")
            return None

        if self._consumer is not None:
            return self._consumer

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.consumer_group(self.group_suffix),
                "client.id": f"{self.settings.kafka_client_id}-{self.settings.service_name}",
                "auto.offset.reset": self.settings.kafka_auto_offset_reset,
            }
        )
        self._consumer.subscribe([self.topic_name])
        logger.info(
            "Kafka batch consumer subscribed service=%s group=%s topic=%s",
            self.settings.service_name,
            self.settings.consumer_group(self.group_suffix),
            self.topic_name,
        )
        return self._consumer

    def _should_ignore_poll_error(self, error: Any) -> bool:
        """Return whether a Kafka poll error represents an idle/retryable state."""
        if KafkaError is None:
            return False

        error_code = error.code()
        if error_code == KafkaError._PARTITION_EOF:
            return True
        if error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
            logger.info(
                "Kafka topic=%s is not available yet for service=%s; waiting for topic auto-creation",
                self.topic_name,
                self.settings.service_name,
            )
            return True
        return False

    def _log_poll_error(self, error: Any) -> None:
        """Log non-retryable poll errors with service and topic context."""
        logger.warning(
            "Kafka poll returned error service=%s group=%s topic=%s error=%s",
            self.settings.service_name,
            self.settings.consumer_group(self.group_suffix),
            self.topic_name,
            error,
        )


class KafkaConsumerWorker:
    """Background Kafka consumer that dispatches binary payloads to a handler.

    Args:
        settings: Service settings that define broker connectivity and naming.
        topic_suffix: Logical topic suffix to subscribe to.
        group_suffix: Logical consumer role used when building the group name.
        handler: Callback invoked for every successfully consumed message value.
    """

    def __init__(
        self,
        settings: KafkaSettingsProtocol,
        topic_name: str,
        group_suffix: str,
        handler: Callable[[bytes], None],
    ) -> None:
        self.settings = settings
        self.topic_name = topic_name
        self.group_suffix = group_suffix
        self.handler = handler
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._consumer: Optional[Any] = None

    def start(self) -> None:
        """Start the background polling thread exactly once."""
        if self._thread is not None:
            return

        if not self.settings.kafka_enabled:
            logger.info("Kafka consumer disabled for service=%s", self.settings.service_name)
            return

        if Consumer is None:
            logger.warning("confluent-kafka is not installed; consumer will not start")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"{self.settings.service_name}-{self.group_suffix}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the polling loop and close the consumer cleanly."""
        self._stop_event.set()
        logger.warning(
            "Stopping Kafka consumer worker service=%s group=%s topic_suffix=%s",
            self.settings.service_name,
            self.settings.consumer_group(self.group_suffix),
            self.topic_name,
        )
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning(
                    "Kafka consumer worker did not stop in time service=%s group=%s",
                    self.settings.service_name,
                    self.settings.consumer_group(self.group_suffix),
                )
            self._thread = None

    def _run(self) -> None:
        """Subscribe to the configured topic and poll for messages forever."""
        self._consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "group.id": self.settings.consumer_group(self.group_suffix),
                "client.id": f"{self.settings.kafka_client_id}-{self.settings.service_name}",
                "auto.offset.reset": self.settings.kafka_auto_offset_reset,
            }
        )
        topic = self.topic_name
        self._consumer.subscribe([topic])
        logger.warning(
            "Kafka consumer worker started service=%s group=%s topic=%s bootstrap=%s",
            self.settings.service_name,
            self.settings.consumer_group(self.group_suffix),
            topic,
            self.settings.kafka_bootstrap_servers,
        )

        try:
            while not self._stop_event.is_set():
                message = self._consumer.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    logger.warning(
                        "Kafka poll returned error service=%s group=%s topic=%s error=%s",
                        self.settings.service_name,
                        self.settings.consumer_group(self.group_suffix),
                        topic,
                        message.error(),
                    )
                    if KafkaError is not None:
                        error_code = message.error().code()
                        if error_code == KafkaError._PARTITION_EOF:
                            continue
                        if error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            logger.info(
                                "Kafka topic=%s is not available yet for service=%s; waiting for topic auto-creation",
                                topic,
                                self.settings.service_name,
                            )
                            continue
                    logger.error("Kafka consume error on topic=%s error=%s", topic, message.error())
                    continue
                logger.warning(
                    "Kafka message received service=%s group=%s topic=%s bytes=%s",
                    self.settings.service_name,
                    self.settings.consumer_group(self.group_suffix),
                    topic,
                    len(message.value() or b""),
                )
                self.handler(message.value())
        except Exception:  # pragma: no cover
            logger.exception("Kafka consumer worker crashed for service=%s topic=%s", self.settings.service_name, topic)
        finally:
            if self._consumer is not None:
                self._consumer.close()
                self._consumer = None