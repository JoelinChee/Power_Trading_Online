from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Producer  # type: ignore

from infrastructure.loaders.kafka_loader import KafkaConfigLoader, KafkaRuntimeSettings


def _decode_b64(value: str | None) -> bytes | None:
    if value is None:
        return None
    return base64.b64decode(value.encode("ascii"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Kafka messages from NDJSON recording.")
    parser.add_argument("--input", required=True, help="Input NDJSON file produced by kafka_record.py.")
    parser.add_argument("--target-topic", default="", help="Override topic for all replayed messages.")
    parser.add_argument("--service-name", default="tooling", help="Logical service name for runtime Kafka settings.")
    parser.add_argument("--limit", type=int, default=0, help="Replay at most N messages (0 means all).")
    parser.add_argument("--rate", type=float, default=0.0, help="Fixed replay rate (messages/sec). 0 means no rate limit.")
    parser.add_argument("--preserve-intervals", action="store_true", help="Replay with original inter-message timing.")
    return parser.parse_args()


def _sleep_by_rate(rate: float) -> None:
    if rate <= 0:
        return
    time.sleep(1.0 / rate)


def main() -> int:
    args = _parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    kafka_config = KafkaConfigLoader.get_kafka()
    settings = KafkaRuntimeSettings(service_name=args.service_name, kafka_config=kafka_config)
    if not settings.kafka_enabled:
        print("Kafka is disabled in config; replay aborted.", file=sys.stderr)
        return 1

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"{settings.kafka_client_id}.replay",
        }
    )

    sent = 0
    previous_ts: int | None = None

    with input_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            if not line.strip():
                continue
            if args.limit > 0 and sent >= args.limit:
                break

            record = json.loads(line)
            source_topic = str(record.get("topic", "")).strip()
            topic = args.target_topic.strip() or source_topic
            if not topic:
                raise ValueError("Each record must have topic or use --target-topic")

            timestamp = record.get("timestamp")
            current_ts = int(timestamp) if isinstance(timestamp, (int, float)) else None
            if args.preserve_intervals and previous_ts is not None and current_ts is not None and current_ts >= previous_ts:
                time.sleep((current_ts - previous_ts) / 1000.0)
            elif args.rate > 0:
                _sleep_by_rate(args.rate)

            headers_data = record.get("headers", [])
            headers: list[tuple[str, bytes | None]] = []
            if isinstance(headers_data, list):
                for item in headers_data:
                    if not isinstance(item, dict):
                        continue
                    header_key = item.get("key")
                    if not isinstance(header_key, str):
                        continue
                    headers.append((header_key, _decode_b64(item.get("value_b64"))))

            producer.produce(
                topic=topic,
                key=_decode_b64(record.get("key_b64")),
                value=_decode_b64(record.get("value_b64")),
                headers=headers,
            )
            producer.poll(0)

            sent += 1
            previous_ts = current_ts

    producer.flush(10.0)
    print(f"Replayed {sent} messages from {input_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
