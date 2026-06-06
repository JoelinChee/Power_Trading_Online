from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.config_loader import DataBootSettings, ExecutionBootSettings, ForecastBootSettings


DATA_BOOT_URL = "http://127.0.0.1:8001"
FORECAST_BOOT_URL = "http://127.0.0.1:8002"
EXECUTION_BOOT_URL = "http://127.0.0.1:8003"
WEATHER_TOPIC = DataBootSettings().topic_name("data_boot", "forecast_boot")
FORECAST_TOPIC = ForecastBootSettings().topic_name("forecast_boot", "execution_boot")
EXECUTION_TOPIC = ExecutionBootSettings().topic_name("forecast_boot", "execution_boot")


def fetch_json(client: httpx.Client, url: str) -> dict:
    response = client.get(url, timeout=5.0)
    response.raise_for_status()
    return response.json()


def main() -> int:
    with httpx.Client(trust_env=False) as client:
        trigger_payload: dict | None = None
        deadline = time.time() + 20.0
        while time.time() < deadline:
            try:
                trigger_response = client.get(f"{DATA_BOOT_URL}/", timeout=10.0)
                trigger_response.raise_for_status()
                trigger_payload = trigger_response.json()
                break
            except httpx.HTTPError:
                time.sleep(1.0)

        if trigger_payload is None:
            print("data_boot did not become ready in time", file=sys.stderr)
            return 1

        if trigger_payload.get("message") != "Kafka发送成功":
            print(f"Unexpected browser response: {trigger_payload}", file=sys.stderr)
            return 1

        deadline = time.time() + 50.0
        while time.time() < deadline:
            data_status = fetch_json(client, f"{DATA_BOOT_URL}/api/v1/data/pipeline-status")
            forecast_status = fetch_json(client, f"{FORECAST_BOOT_URL}/api/v1/forecast/pipeline-status")
            execution_status = fetch_json(client, f"{EXECUTION_BOOT_URL}/api/v1/execution/pipeline-status")

            weather_feedback = data_status.get("details", {}).get("last_feedback_event", {})
            forecast_weather = forecast_status.get("details", {}).get("last_received_weather_event", {})
            forecast_event_id = forecast_status.get("last_published_event_id")
            execution_consumed_id = execution_status.get("last_consumed_event_id")
            execution_result = execution_status.get("details", {}).get("last_processed_result", {})

            if (
                weather_feedback.get("topic") == WEATHER_TOPIC
                and forecast_weather.get("source") == "data_boot"
                and forecast_event_id
                and execution_consumed_id == forecast_event_id
                and execution_result.get("upstream_event_id") == forecast_event_id
            ):
                print(f"Kafka protobuf pipeline smoke test passed ({WEATHER_TOPIC} -> {FORECAST_TOPIC} -> {EXECUTION_TOPIC})")
                print(f"forecast_event_id={forecast_event_id}")
                print(f"execution_result_id={execution_result.get('event_id')}")
                return 0

            time.sleep(1.0)

    print("Kafka protobuf pipeline smoke test timed out", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())