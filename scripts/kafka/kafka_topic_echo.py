#!/bin/sh
'''exec' python "$0" "$@"
' '''

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message

from confluent_kafka import Consumer, KafkaError, KafkaException  # type: ignore


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from generated import trading_messages_pb2, weather_pb2


TOPIC_MESSAGE_TYPES: dict[str, Type[Message]] = {
    "power_trading.weather.events": weather_pb2.HourlyWeatherDataset,
    "power_trading.weather.events.replay": weather_pb2.HourlyWeatherDataset,
    "power_trading.forecast.events": trading_messages_pb2.ForecastEvent,
}

FALLBACK_MESSAGE_TYPES: tuple[Type[Message], ...] = (
    weather_pb2.HourlyWeatherDataset,
    trading_messages_pb2.ForecastEvent,
    trading_messages_pb2.ExecutionEvent,
    trading_messages_pb2.DataIngestionEvent,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Echo Kafka topics and decode protobuf payloads from generated/*_pb2.py.")
    parser.add_argument("topics", nargs="*", help="Kafka topic names. Comma-separated values are accepted.")
    parser.add_argument("--all-topics", action="store_true", help="Echo all non-internal topics.")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest retained offsets.")
    parser.add_argument("--brokers", "-b", default=os.environ.get("BROKERS", "127.0.0.1:9092"), help="Kafka bootstrap servers.")
    parser.add_argument("--group-id", default="", help="Consumer group id. Defaults to a unique echo group.")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages. Default: unlimited.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print decoded protobuf JSON. This is the default.")
    parser.add_argument("--compact", action="store_true", help="Print each decoded message as one compact JSON line.")
    parser.add_argument("--no-separator", action="store_true", help="Do not print rostopic-style '---' between messages.")
    parser.add_argument("--raw", action="store_true", help="Print raw payload bytes instead of protobuf JSON.")
    return parser.parse_args()


def _split_topics(values: list[str]) -> list[str]:
    topics: list[str] = []
    for value in values:
        topics.extend(part.strip() for part in value.split(",") if part.strip())
    return topics


def _build_consumer(brokers: str, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "client.id": "power-trading-kafka-topic-echo",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )


def _list_topics(consumer: Consumer) -> list[str]:
    metadata = consumer.list_topics(timeout=10.0)
    return sorted(topic for topic in metadata.topics if topic and not topic.startswith("__"))


def _decode_known_type(payload: bytes, message_type: Type[Message]) -> Message:
    message = message_type()
    message.ParseFromString(payload)
    return message


def _decode_fallback(payload: bytes) -> tuple[str, Message] | None:
    best: tuple[int, str, Message] | None = None
    for message_type in FALLBACK_MESSAGE_TYPES:
        message = message_type()
        try:
            message.ParseFromString(payload)
        except DecodeError:
            continue
        score = len(message.ListFields())
        if score <= 0:
            continue
        full_name = message.DESCRIPTOR.full_name
        if best is None or score > best[0]:
            best = (score, full_name, message)
    if best is None:
        return None
    return best[1], best[2]


def _decode_payload(topic: str, payload: bytes | None) -> tuple[str, dict[str, Any] | str | None]:
    if payload is None:
        return "null", None

    message_type = TOPIC_MESSAGE_TYPES.get(topic)
    if message_type is not None:
        try:
            message = _decode_known_type(payload, message_type)
            return message.DESCRIPTOR.full_name, MessageToDict(message, preserving_proto_field_name=True)
        except DecodeError as exc:
            return "decode_error", str(exc)

    fallback = _decode_fallback(payload)
    if fallback is None:
        return "unknown_binary", f"{len(payload)} bytes"
    full_name, message = fallback
    return full_name, MessageToDict(message, preserving_proto_field_name=True)


def _record_to_json(message: Any, decoded_type: str, decoded_payload: Any, compact: bool) -> str:
    record = {
        "timestamp": message.timestamp()[1],
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": message.key().decode("utf-8", errors="replace") if message.key() else None,
        "headers": message.headers() or [],
        "protobuf_type": decoded_type,
        "payload": decoded_payload,
    }
    return json.dumps(record, ensure_ascii=False, indent=None if compact else 2)


def main() -> int:
    args = _parse_args()
    group_id = args.group_id or f"power_trading_echo_{time.strftime('%Y%m%d_%H%M%S')}"
    consumer = _build_consumer(args.brokers, group_id, args.from_beginning)
    topics = _list_topics(consumer) if args.all_topics else _split_topics(args.topics)
    if not topics:
        print("No topics selected. Use topic names or --all-topics.", file=sys.stderr)
        consumer.close()
        return 1

    should_stop = False

    def _handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    consumer.subscribe(topics)
    consumed = 0
    try:
        while not should_stop:
            if args.max_messages > 0 and consumed >= args.max_messages:
                break
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                error = message.error()
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"Topic not available: {message.topic()}", file=sys.stderr)
                    continue
                raise KafkaException(error)

            if args.raw:
                payload = message.value() or b""
                sys.stdout.buffer.write(payload + b"\n")
                sys.stdout.buffer.flush()
            else:
                decoded_type, decoded_payload = _decode_payload(message.topic(), message.value())
                if consumed > 0 and not args.no_separator:
                    print("---", flush=True)
                print(_record_to_json(message, decoded_type, decoded_payload, args.compact), flush=True)
            consumed += 1
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())