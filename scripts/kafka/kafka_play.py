#!/bin/sh
'''exec' python "$0" "$@"
' '''

from __future__ import annotations

import argparse
import base64
import json
import os
import struct
import sys
import time
from pathlib import Path
from typing import Any, Iterator

from confluent_kafka import Producer  # type: ignore

MAGIC = b"PTO_KAFKA_BIN_V1\n"
NULL_BYTES_LENGTH = -1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay a kafka_record.py binary bag recording back to Kafka.")
    parser.add_argument("--input", "-i", required=True, help="Input bin file produced by kafka_record.py.")
    parser.add_argument("--target-topic", "-t", default="", help="Replay all messages to this topic. Default: original recorded topic.")
    parser.add_argument("--brokers", "-b", default=os.environ.get("BROKERS", "127.0.0.1:9092"), help="Kafka bootstrap servers.")
    parser.add_argument("--rate", "-l", type=float, default=0.0, help="Limit replay to N messages/sec. Overrides recorded intervals.")
    parser.add_argument("--limit", type=int, default=0, help="Replay at most N messages. Default: all.")
    parser.add_argument("--full-speed", action="store_true", help="Replay as fast as possible instead of preserving recorded intervals.")
    parser.add_argument("--preserve-intervals", action="store_true", help="Replay with original inter-message timestamp intervals. This is the default.")
    return parser.parse_args()


def _read_exact(fp: Any, size: int) -> bytes:
    data = fp.read(size)
    if len(data) != size:
        raise EOFError("unexpected end of recording")
    return data


def _read_bytes(fp: Any) -> bytes | None:
    length = struct.unpack(">i", _read_exact(fp, 4))[0]
    if length == NULL_BYTES_LENGTH:
        return None
    if length < 0:
        raise ValueError(f"invalid byte field length: {length}")
    return _read_exact(fp, length)


def _decode_headers(items: list[dict[str, Any]]) -> list[tuple[str, bytes | None]]:
    headers: list[tuple[str, bytes | None]] = []
    for item in items:
        key = item.get("key")
        if not isinstance(key, str):
            continue
        value_b64 = item.get("value_b64")
        value = base64.b64decode(value_b64.encode("ascii")) if isinstance(value_b64, str) else None
        headers.append((key, value))
    return headers


def _iter_records(fp: Any) -> Iterator[tuple[dict[str, Any], bytes | None, bytes | None]]:
    magic = _read_exact(fp, len(MAGIC))
    if magic != MAGIC:
        raise ValueError("unsupported recording format; expected PTO_KAFKA_BIN_V1")
    while True:
        header_length_data = fp.read(4)
        if not header_length_data:
            return
        if len(header_length_data) != 4:
            raise EOFError("truncated record header length")
        header_length = struct.unpack(">I", header_length_data)[0]
        header = json.loads(_read_exact(fp, header_length).decode("utf-8"))
        key = _read_bytes(fp)
        value = _read_bytes(fp)
        yield header, key, value


def _record_timestamp(header: dict[str, Any]) -> int | None:
    timestamp = header.get("timestamp")
    if isinstance(timestamp, (int, float)):
        return int(timestamp)
    return None


def _format_seconds(seconds: float) -> str:
    return f"{seconds:.1f}s"


def _load_records(input_path: Path) -> list[tuple[dict[str, Any], bytes | None, bytes | None]]:
    with input_path.open("rb") as input_file:
        return list(_iter_records(input_file))


def _print_progress(
    *,
    elapsed_recording_seconds: float,
    total_recording_seconds: float,
    sent: int,
    total: int,
    topic: str,
    final: bool = False,
) -> None:
    percent = (sent / total * 100.0) if total > 0 else 100.0
    line = (
        "[PLAY] "
        f"{_format_seconds(elapsed_recording_seconds)} / {_format_seconds(total_recording_seconds)} "
        f"({sent}/{total}, {percent:5.1f}%) topic={topic}"
    )
    end = "\n" if final else "\r"
    print(line, end=end, file=sys.stderr, flush=True)


def _sleep_with_progress(
    *,
    duration_seconds: float,
    elapsed_recording_seconds: float,
    total_recording_seconds: float,
    sent: int,
    total: int,
    topic: str,
) -> None:
    if duration_seconds <= 0:
        return

    sleep_start = time.monotonic()
    sleep_end = sleep_start + duration_seconds
    while True:
        remaining_seconds = sleep_end - time.monotonic()
        if remaining_seconds <= 0:
            return
        time.sleep(min(0.1, remaining_seconds))
        progressed_seconds = min(duration_seconds, time.monotonic() - sleep_start)
        _print_progress(
            elapsed_recording_seconds=elapsed_recording_seconds + progressed_seconds,
            total_recording_seconds=total_recording_seconds,
            sent=sent,
            total=total,
            topic=topic,
        )


def main() -> int:
    args = _parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    records = _load_records(input_path)
    if args.limit > 0:
        records = records[: args.limit]

    timestamps = [_record_timestamp(header) for header, _key, _value in records]
    valid_timestamps = [timestamp for timestamp in timestamps if timestamp is not None]
    first_ts = valid_timestamps[0] if valid_timestamps else None
    last_ts = valid_timestamps[-1] if valid_timestamps else first_ts
    total_recording_seconds = ((last_ts - first_ts) / 1000.0) if first_ts is not None and last_ts is not None else 0.0
    total = len(records)

    producer = Producer({"bootstrap.servers": args.brokers, "client.id": "power-trading-kafka-play"})
    sent = 0
    previous_ts: int | None = None
    previous_elapsed_recording_seconds = 0.0

    for header, key, value in records:
            source_topic = str(header.get("topic", ""))
            topic = args.target_topic or source_topic
            if not topic:
                raise ValueError("record does not include a source topic; use --target-topic")

            current_ts = _record_timestamp(header)
            if args.rate > 0:
                _sleep_with_progress(
                    duration_seconds=1.0 / args.rate,
                    elapsed_recording_seconds=previous_elapsed_recording_seconds,
                    total_recording_seconds=total_recording_seconds,
                    sent=sent,
                    total=total,
                    topic=topic,
                )
            elif not args.full_speed and previous_ts is not None and current_ts is not None and current_ts >= previous_ts:
                _sleep_with_progress(
                    duration_seconds=(current_ts - previous_ts) / 1000.0,
                    elapsed_recording_seconds=previous_elapsed_recording_seconds,
                    total_recording_seconds=total_recording_seconds,
                    sent=sent,
                    total=total,
                    topic=topic,
                )

            producer.produce(topic=topic, key=key, value=value, headers=_decode_headers(header.get("headers", [])))
            producer.poll(0)
            sent += 1
            previous_ts = current_ts
            elapsed_recording_seconds = ((current_ts - first_ts) / 1000.0) if first_ts is not None and current_ts is not None else 0.0
            previous_elapsed_recording_seconds = elapsed_recording_seconds
            _print_progress(
                elapsed_recording_seconds=elapsed_recording_seconds,
                total_recording_seconds=total_recording_seconds,
                sent=sent,
                total=total,
                topic=topic,
            )

    producer.flush(10.0)
    _print_progress(
        elapsed_recording_seconds=total_recording_seconds,
        total_recording_seconds=total_recording_seconds,
        sent=sent,
        total=total,
        topic="done",
        final=True,
    )
    print(f"Replayed {sent} messages from {input_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())