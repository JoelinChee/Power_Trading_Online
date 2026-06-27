import os
import json
import re
import shutil
import subprocess
import time
import unittest
import urllib.request
from pathlib import Path


class KafkaRecordAllTopicsTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    start_script = repo_root / "scripts" / "runtime" / "start_all.sh"
    stop_script = repo_root / "scripts" / "runtime" / "stop_all.sh"
    start_web_script = repo_root / "scripts" / "web" / "start_web.sh"
    stop_web_script = repo_root / "scripts" / "web" / "stop_web.sh"
    record_script = repo_root / "scripts" / "kafka" / "kafka_record.sh"
    info_script = repo_root / "scripts" / "kafka" / "kafka_info.sh"
    recording_path = repo_root / "generated" / "recordings" / "unittest_all_topics.bin"

    def setUp(self):
        self.env = os.environ.copy()
        self.env.setdefault("PYTHON_BIN", self._detect_project_python())
        self.env.setdefault("WEB_PORT", "8088")
        self.env.setdefault("WEB_URL", "http://127.0.0.1:8088")
        self._stop_all()
        self.recording_path.unlink(missing_ok=True)

    def tearDown(self):
        self._stop_all()

    def test_record_all_topics_for_three_minutes(self):
        start_result = subprocess.run(
            ["bash", str(self.start_script)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=300,
            check=False,
        )
        self.assertEqual(
            start_result.returncode,
            0,
            f"start_all.sh exited with status {start_result.returncode}\n{start_result.stdout}",
        )

        web_result = subprocess.run(
            ["bash", str(self.start_web_script)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            web_result.returncode,
            0,
            f"start_web.sh exited with status {web_result.returncode}\n{web_result.stdout}",
        )

        self._wait_for_web_frontend()
        time.sleep(12)
        self._wait_for_forecast_publication()

        record_result = subprocess.run(
            [
                "bash",
                str(self.record_script),
                "--all-topics",
                "--from-beginning",
                "--max-seconds",
                "180",
                "--output",
                str(self.recording_path),
            ],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=240,
            check=False,
        )

        self.assertEqual(
            record_result.returncode,
            0,
            f"kafka_record.sh exited with status {record_result.returncode}\n{record_result.stdout}",
        )
        self.assertIn("Recording topics:", record_result.stdout)
        self.assertIn("Recorded", record_result.stdout)
        self.assertTrue(self.recording_path.is_file(), f"missing recording file: {self.recording_path}")
        self.assertGreater(self.recording_path.stat().st_size, len(b"PTO_KAFKA_BIN_V1\n"))

        info_result = subprocess.run(
            ["bash", str(self.info_script), str(self.recording_path)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            info_result.returncode,
            0,
            f"kafka_info.sh exited with status {info_result.returncode}\n{info_result.stdout}",
        )
        topic_counts = self._parse_topic_counts(info_result.stdout)
        self.assertGreater(topic_counts.get("power_trading.forecast.events", 0), 0, info_result.stdout)
        self.assertGreater(topic_counts.get("power_trading.weather.events", 0), 0, info_result.stdout)

    @staticmethod
    def _parse_topic_counts(output):
        topic_counts = {}
        for line in output.splitlines():
            match = re.match(r"^\s*(\d+)\s+(\S+)\s*$", line)
            if match:
                topic_counts[match.group(2)] = int(match.group(1))
        return topic_counts

    def _wait_for_web_frontend(self):
        deadline = time.monotonic() + 30
        last_error = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(self.env["WEB_URL"], timeout=3) as response:
                    self.assertEqual(response.status, 200)
                    return
            except OSError as exc:
                last_error = exc
                time.sleep(1)

        self.fail(f"web frontend did not become reachable before timeout: {last_error}")

    def _wait_for_forecast_publication(self):
        deadline = time.monotonic() + 60
        last_payload = None
        last_error = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen("http://127.0.0.1:8002/api/v1/forecast/pipeline-status", timeout=3) as response:
                    self.assertEqual(response.status, 200)
                    last_payload = json.loads(response.read().decode("utf-8"))
                    if last_payload.get("last_published_event_id"):
                        return
            except (OSError, json.JSONDecodeError) as exc:
                last_error = exc
            time.sleep(1)

        self.fail(
            "forecast_boot did not publish forecast events before recording started; "
            f"last_payload={last_payload!r} last_error={last_error!r}"
        )

    def _stop_all(self):
        subprocess.run(
            ["bash", str(self.stop_web_script)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            ["bash", str(self.stop_script), "all"],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    @staticmethod
    def _detect_project_python():
        conda_env_name = os.environ.get("CONDA_ENV_NAME", "power_trading_online")
        conda_exe = shutil.which("conda")
        if conda_exe:
            result = subprocess.run(
                [conda_exe, "info", "--base"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                conda_python = Path(result.stdout.strip()) / "envs" / conda_env_name / "bin" / "python"
                if conda_python.is_file():
                    return str(conda_python)

        return "python3"


if __name__ == "__main__":
    unittest.main()