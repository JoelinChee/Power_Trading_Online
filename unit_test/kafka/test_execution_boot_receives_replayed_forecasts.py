import os
import shutil
import subprocess
import time
import unittest
from pathlib import Path


class ExecutionBootReceivesReplayedForecastsTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    start_script = repo_root / "scripts" / "runtime" / "start_all.sh"
    stop_script = repo_root / "scripts" / "runtime" / "stop_all.sh"
    stop_web_script = repo_root / "scripts" / "web" / "stop_web.sh"
    play_script = repo_root / "scripts" / "kafka" / "kafka_play.sh"
    recording_path = repo_root / "generated" / "recordings" / "unittest_all_topics.bin"
    execution_log_path = repo_root / "generated" / "logs" / "execution_boot.log"
    received_log_text = "Kafka message received service=execution_boot"

    def setUp(self):
        self.env = os.environ.copy()
        self.env.setdefault("PYTHON_BIN", self._detect_project_python())
        self._stop_test_processes()

    def tearDown(self):
        self._stop_test_processes()

    def test_execution_boot_receives_at_least_five_replayed_messages(self):
        self.assertTrue(
            self.recording_path.is_file(),
            f"recording file does not exist: {self.recording_path}",
        )

        start_result = subprocess.run(
            ["bash", str(self.start_script), "kafka", "execution"],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=180,
            check=False,
        )
        self.assertEqual(
            start_result.returncode,
            0,
            f"start_all.sh kafka execution exited with status {start_result.returncode}\n{start_result.stdout}",
        )

        self.execution_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.execution_log_path.write_text("", encoding="utf-8")

        play_result = subprocess.run(
            [
                "bash",
                str(self.play_script),
                "-i",
                str(self.recording_path),
                "--full-speed",
            ],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(
            play_result.returncode,
            0,
            f"kafka_play.sh exited with status {play_result.returncode}\n{play_result.stdout}",
        )

        received_count = self._wait_for_received_log_count(min_count=5, timeout_seconds=60)
        self.assertGreaterEqual(
            received_count,
            5,
            f"execution_boot received only {received_count} replayed messages\n{self._read_execution_log_tail()}",
        )

    def _wait_for_received_log_count(self, min_count, timeout_seconds):
        deadline = time.monotonic() + timeout_seconds
        last_count = 0
        while time.monotonic() < deadline:
            last_count = self._received_log_count()
            if last_count >= min_count:
                return last_count
            time.sleep(1)
        return last_count

    def _received_log_count(self):
        if not self.execution_log_path.is_file():
            return 0
        return self.execution_log_path.read_text(encoding="utf-8", errors="replace").count(self.received_log_text)

    def _read_execution_log_tail(self):
        if not self.execution_log_path.is_file():
            return "execution log file does not exist"
        lines = self.execution_log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-80:])

    def _stop_test_processes(self):
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