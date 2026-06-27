import os
import shutil
import subprocess
import time
import unittest
from pathlib import Path


class KafkaPlayEchoRecordingTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    stop_web_script = repo_root / "scripts" / "web" / "stop_web.sh"
    start_kafka_script = repo_root / "scripts" / "kafka" / "start_local_kafka.sh"
    stop_kafka_script = repo_root / "scripts" / "kafka" / "stop_local_kafka.sh"
    play_script = repo_root / "scripts" / "kafka" / "kafka_play.sh"
    echo_script = repo_root / "scripts" / "kafka" / "kafka_topic_echo.sh"
    recording_path = repo_root / "generated" / "recordings" / "unittest_all_topics.bin"

    def setUp(self):
        self.env = os.environ.copy()
        self.env.setdefault("PYTHON_BIN", self._detect_project_python())
        self._stop_test_processes()

    def tearDown(self):
        self._stop_test_processes()

    def test_play_recording_and_echo_prints_message(self):
        self.assertTrue(
            self.recording_path.is_file(),
            f"recording file does not exist: {self.recording_path}",
        )

        kafka_result = subprocess.run(
            ["bash", str(self.start_kafka_script)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(
            kafka_result.returncode,
            0,
            f"start_local_kafka.sh exited with status {kafka_result.returncode}\n{kafka_result.stdout}",
        )

        echo_process = subprocess.Popen(
            [
                "bash",
                str(self.echo_script),
                "power_trading.weather.events",
                "power_trading.forecast.events",
                "--max-messages",
                "1",
                "--compact",
            ],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        time.sleep(2)

        play_result = subprocess.run(
            [
                "bash",
                str(self.play_script),
                "-i",
                str(self.recording_path),
                "--full-speed",
                "--limit",
                "1",
            ],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
            check=False,
        )

        try:
            echo_output, _ = echo_process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            echo_process.terminate()
            echo_output, _ = echo_process.communicate(timeout=10)

        self.assertEqual(
            play_result.returncode,
            0,
            f"kafka_play.sh exited with status {play_result.returncode}\n{play_result.stdout}",
        )
        self.assertEqual(
            echo_process.returncode,
            0,
            f"kafka_topic_echo.sh exited with status {echo_process.returncode}\n{echo_output}",
        )
        self.assertIn("power_trading.", echo_output, echo_output)
        self.assertIn("payload", echo_output, echo_output)

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
            ["bash", str(self.stop_kafka_script)],
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