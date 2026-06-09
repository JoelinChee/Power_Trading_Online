from __future__ import annotations

import argparse
import base64
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer, KafkaError, KafkaException  # type: ignore

from infrastructure.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings


def _build_consumer(settings: KafkaRuntimeSettings, subscription: str, group_suffix: str, offset_reset: str) -> Consumer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.consumer_group(group_suffix),
        "client.id": f"{settings.kafka_client_id}.recorder.{group_suffix}",
        "enable.auto.commit": False,
        "auto.offset.reset": offset_reset,
    }
    consumer = Consumer(conf)
    consumer.subscribe([subscription])
    return consumer


def _b64(data: bytes | None) -> str | None:
    if data is None:
        return None
    return base64.b64encode(data).decode("ascii")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record Kafka messages to NDJSON for replay.")
    topic_group = parser.add_mutually_exclusive_group(required=True)
    topic_group.add_argument("--topic", help="Kafka topic to record from.")
    topic_group.add_argument(
        "--topic-pattern",
        default="",
        help="Regex topic pattern for subscription, for example '^power_trading\\..*$'.",
    )
    topic_group.add_argument("--all-topics", action="store_true", help="Subscribe all topics using regex pattern '^.*$'.")
    parser.add_argument("--output", required=True, help="Output NDJSON file path.")
    parser.add_argument("--service-name", default="tooling", help="Logical service name used for consumer group prefixing.")
    parser.add_argument("--group-suffix", default="record", help="Consumer group suffix.")
    parser.add_argument("--max-seconds", type=float, default=0.0, help="Stop after N seconds (0 means unlimited).")
    parser.add_argument("--idle-timeout", type=float, default=0.0, help="Stop if no messages arrive for N seconds (0 means disabled).")
    parser.add_argument(
        "--offset-reset",
        choices=["latest", "earliest", "config"],
        default="latest",
        help="Recorder start policy: latest (now), earliest (history), or config (kafka_auto_offset_reset).",
    )
    parser.add_argument("--from-beginning", action="store_true", help="Backward-compatible alias of --offset-reset earliest.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    kafka_config = KafkaConfigLoader.get_kafka()
    settings = KafkaRuntimeSettings(service_name=args.service_name, kafka_config=kafka_config)

    if not settings.kafka_enabled:
        print("Kafka is disabled in config; recording aborted.", file=sys.stderr)
        return 1

    subscription = args.topic
    if args.topic_pattern:
        subscription = args.topic_pattern
    if args.all_topics:
        subscription = "^.*$"

    if not subscription:
        print("No topic subscription configured.", file=sys.stderr)
        return 1

    offset_reset = args.offset_reset
    if args.from_beginning:
        offset_reset = "earliest"
    if offset_reset == "config":
        offset_reset = str(settings.kafka_auto_offset_reset)

    consumer = _build_consumer(
        settings=settings,
        subscription=subscription,
        group_suffix=args.group_suffix,
        offset_reset=offset_reset,
    )

    should_stop = False

    def _handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    start_at = time.time()
    last_message_at = start_at
    recorded = 0

    print(f"Recording subscription={subscription} offset_reset={offset_reset} -> {output_path}")

    try:
        with output_path.open("w", encoding="utf-8") as fp:
            while not should_stop:
                now = time.time()
                if args.max_seconds > 0 and (now - start_at) >= args.max_seconds:
                    break
                if args.idle_timeout > 0 and (now - last_message_at) >= args.idle_timeout and recorded > 0:
                    break

                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    error = message.error()
                    if error.code() in {KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART}:
                        continue
                    raise KafkaException(message.error())

                headers = message.headers() or []
                record = {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "timestamp": message.timestamp()[1],
                    "key_b64": _b64(message.key()),
                    "value_b64": _b64(message.value()),
                    "headers": [{"key": key, "value_b64": _b64(value)} for key, value in headers],
                }
                fp.write(json.dumps(record, ensure_ascii=True) + "\n")
                fp.flush()

                recorded += 1
                last_message_at = time.time()
    finally:
        consumer.close()

    print(f"Recorded {recorded} messages to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
