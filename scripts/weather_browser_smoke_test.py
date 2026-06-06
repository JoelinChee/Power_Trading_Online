from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.loaders.topic_loader import TopicConfigLoader


DATA_BOOT_URL = "http://127.0.0.1:8001"
FORECAST_BOOT_URL = "http://127.0.0.1:8002"
WEATHER_TOPIC = TopicConfigLoader.topic_name("data_boot", "forecast_boot")


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

        deadline = time.time() + 20.0
        while time.time() < deadline:
            data_status = fetch_json(client, f"{DATA_BOOT_URL}/api/v1/data/pipeline-status")
            forecast_status = fetch_json(client, f"{FORECAST_BOOT_URL}/api/v1/forecast/pipeline-status")

            data_feedback = data_status.get("details", {}).get("last_feedback_event", {})
            forecast_weather = forecast_status.get("details", {}).get("last_received_weather_event", {})
            daily_weather = forecast_weather.get("daily_weather", [])

            if (
                data_feedback.get("message") == "Kafka发送成功"
                and forecast_weather.get("source") == "data_boot"
                and daily_weather
                and daily_weather[0].get("hourly_weather")
            ):
                print("Weather browser-to-Kafka smoke test passed")
                print(f"topic={WEATHER_TOPIC}")
                print(f"region={daily_weather[0].get('region_name')}")
                print(f"hour_count={len(daily_weather[0].get('hourly_weather', []))}")
                return 0

            time.sleep(1.0)

    print("Weather browser-to-Kafka smoke test timed out", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())