#!/bin/sh
'''exec' python "$0" "$@"
' '''

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import struct
import sys
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException  # type: ignore

MAGIC = b"PTO_KAFKA_BIN_V1\n"
NULL_BYTES_LENGTH = -1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record one, many, or all Kafka topics to a local binary bag file.")
    parser.add_argument("topics", nargs="*", help="Kafka topic names to record. Comma-separated values are also accepted.")
    parser.add_argument("--all-topics", action="store_true", help="Record all non-internal topics from the broker.")
    parser.add_argument("--output", "-o", default="generated/recordings/kafka_rec.bin", help="Output recording file.")
    parser.add_argument("--brokers", "-b", default=os.environ.get("BROKERS", "127.0.0.1:9092"), help="Kafka bootstrap servers.")
    parser.add_argument("--from-beginning", action="store_true", help="Record retained history from earliest offsets.")
    parser.add_argument("--end-on-eof", "-e", action="store_true", help="Exit when current topic ends are reached.")
    parser.add_argument("--group-id", default="", help="Consumer group id. Defaults to a unique recording group.")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages. Default: unlimited.")
    parser.add_argument("--max-seconds", type=float, default=0.0, help="Stop after N seconds. Default: unlimited.")
    return parser.parse_args()


def _split_topics(values: list[str]) -> list[str]:
    topics: list[str] = []
    for value in values:
        topics.extend(part.strip() for part in value.split(",") if part.strip())
    return topics


def _list_topics(brokers: str) -> list[str]:
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": "power-trading-topic-list"})
    try:
        metadata = consumer.list_topics(timeout=10.0)
    finally:
        consumer.close()
    return sorted(topic for topic in metadata.topics if topic and not topic.startswith("__"))


def _encode_headers(headers: list[tuple[str, bytes | None]] | None) -> list[dict[str, str | None]]:
    encoded: list[dict[str, str | None]] = []
    for key, value in headers or []:
        encoded.append(
            {
                "key": key,
                "value_b64": base64.b64encode(value).decode("ascii") if value is not None else None,
            }
        )
    return encoded


def _write_bytes(fp: Any, data: bytes | None) -> None:
    if data is None:
        fp.write(struct.pack(">i", NULL_BYTES_LENGTH))
        return
    fp.write(struct.pack(">i", len(data)))
    fp.write(data)


def _write_record(fp: Any, message: Any) -> None:
    header = {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "timestamp": message.timestamp()[1],
        "headers": _encode_headers(message.headers()),
    }
    header_bytes = json.dumps(header, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    fp.write(struct.pack(">I", len(header_bytes)))
    fp.write(header_bytes)
    _write_bytes(fp, message.key())
    _write_bytes(fp, message.value())


def _build_consumer(brokers: str, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "client.id": "power-trading-kafka-record",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )


def main() -> int:
    args = _parse_args()
    topics = _list_topics(args.brokers) if args.all_topics else _split_topics(args.topics)
    if not topics:
        print("No topics selected. Use topic names or --all-topics.", file=sys.stderr)
        return 1

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    group_id = args.group_id or f"power_trading_kcat_record_{time.strftime('%Y%m%d_%H%M%S')}"

    print(f"Recording topics: {', '.join(topics)}")
    print(f"Brokers: {args.brokers}")
    print(f"Output: {output_path}")
    print(f"Consumer group: {group_id}")

    should_stop = False
    assigned: set[tuple[str, int]] = set()
    reached_eof: set[tuple[str, int]] = set()

    def handle_signal(_sig: int, _frame: Any) -> None:
        nonlocal should_stop
        should_stop = True

    def on_assign(_consumer: Consumer, partitions: list[Any]) -> None:
        assigned.clear()
        reached_eof.clear()
        for partition in partitions:
            assigned.add((partition.topic, partition.partition))

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    consumer = _build_consumer(args.brokers, group_id, args.from_beginning)
    consumer.subscribe(topics, on_assign=on_assign)
    recorded = 0
    start_time = time.time()

    try:
        with output_path.open("wb") as output_file:
            output_file.write(MAGIC)
            while not should_stop:
                if args.max_messages > 0 and recorded >= args.max_messages:
                    break
                if args.max_seconds > 0 and (time.time() - start_time) >= args.max_seconds:
                    break
                if args.end_on_eof and assigned and assigned.issubset(reached_eof):
                    break

                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    error = message.error()
                    if error.code() == KafkaError._PARTITION_EOF:
                        reached_eof.add((message.topic(), message.partition()))
                        continue
                    raise KafkaException(error)

                _write_record(output_file, message)
                output_file.flush()
                recorded += 1
    finally:
        consumer.close()

    print(f"Recorded {recorded} messages to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
