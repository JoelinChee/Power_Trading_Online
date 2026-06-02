from __future__ import annotations

import sys
import time

import httpx


DATA_BOOT_URL = "http://127.0.0.1:8001"
FORECAST_BOOT_URL = "http://127.0.0.1:8002"
EXECUTION_BOOT_URL = "http://127.0.0.1:8003"


def fetch_json(client: httpx.Client, url: str) -> dict:
    response = client.get(url, timeout=5.0)
    response.raise_for_status()
    return response.json()


def main() -> int:
    payload = {
        "weather": {
            "temperature_celsius": 30.5,
            "humidity_ratio": 71.0,
            "wind_speed_mps": 4.8,
            "weather_type": "cloudy",
        },
        "load": {
            "enterprise_id": "demo-enterprise",
            "timestamp": "2026-06-01T09:30:00+08:00",
            "load_mw": 72.6,
        },
        "price": {
            "timestamp": "2026-06-01T09:30:00+08:00",
            "spot_price": 438.5,
            "market": "real_time",
        },
        "renewable": {
            "timestamp": "2026-06-01T09:30:00+08:00",
            "wind_output_mw": 28.0,
            "solar_output_mw": 19.5,
        },
    }

    with httpx.Client() as client:
        response = client.post(f"{DATA_BOOT_URL}/api/v1/data/ingest", json=payload, timeout=10.0)
        response.raise_for_status()
        data_event = response.json()["event"]
        upstream_event_id = data_event["event_id"]

        deadline = time.time() + 50.0
        while time.time() < deadline:
            data_status = fetch_json(client, f"{DATA_BOOT_URL}/api/v1/data/pipeline-status")
            forecast_status = fetch_json(client, f"{FORECAST_BOOT_URL}/api/v1/forecast/pipeline-status")
            execution_status = fetch_json(client, f"{EXECUTION_BOOT_URL}/api/v1/execution/pipeline-status")

            forecast_consumed = forecast_status.get("details", {}).get("last_received_event", {}).get("upstream_event_id")
            forecast_published = forecast_status.get("details", {}).get("last_published_event", {}).get("upstream_event_id")
            execution_consumed = execution_status.get("details", {}).get("last_consumed_event", {}).get("upstream_event_id")
            execution_generated = execution_status.get("details", {}).get("last_published_event", {}).get("upstream_event_id")
            data_feedback = data_status.get("details", {}).get("last_feedback_event", {}).get("upstream_event_id")

            if (
                forecast_consumed == upstream_event_id
                and forecast_published == upstream_event_id
                and execution_consumed == upstream_event_id
                and execution_generated == forecast_status.get("last_published_event_id")
                and data_feedback == upstream_event_id
            ):
                print("Kafka protobuf pipeline smoke test passed")
                print(f"data_event_id={upstream_event_id}")
                print(f"forecast_event_id={forecast_status.get('last_published_event_id')}")
                print(f"execution_event_id={execution_status.get('last_published_event_id')}")
                return 0

            time.sleep(1.0)

    print("Kafka protobuf pipeline smoke test timed out", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())