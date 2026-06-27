import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path


class KafkaTopicListAndInfoTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    stop_web_script = repo_root / "scripts" / "web" / "stop_web.sh"
    start_kafka_script = repo_root / "scripts" / "kafka" / "start_local_kafka.sh"
    stop_kafka_script = repo_root / "scripts" / "kafka" / "stop_local_kafka.sh"
    play_script = repo_root / "scripts" / "kafka" / "kafka_play.sh"
    topic_list_script = repo_root / "scripts" / "kafka" / "kafka_topic_list.sh"
    info_script = repo_root / "scripts" / "kafka" / "kafka_info.sh"
    recording_path = repo_root / "generated" / "recordings" / "unittest_all_topics.bin"
    replay_topic = "power_trading.unittest.replay.events"

    def setUp(self):
        self.env = os.environ.copy()
        python_bin = self._detect_project_python()
        self.env.setdefault("PYTHON_BIN", python_bin)
        python_bin_dir = str(Path(self.env["PYTHON_BIN"]).resolve().parent)
        self.env["PATH"] = f"{python_bin_dir}{os.pathsep}{self.env.get('PATH', '')}"
        self._stop_test_processes()

    def tearDown(self):
        self._stop_test_processes()

    def test_topic_list_has_at_least_three_topics_and_info_reads_recording(self):
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

        self._play_recording()
        self._play_recording("--target-topic", self.replay_topic, "--limit", "3")

        topic_list_result = subprocess.run(
            ["bash", str(self.topic_list_script)],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            topic_list_result.returncode,
            0,
            f"kafka_topic_list.sh exited with status {topic_list_result.returncode}\n{topic_list_result.stdout}",
        )
        topics = self._parse_topic_list(topic_list_result.stdout)
        self.assertGreaterEqual(len(topics), 3, topic_list_result.stdout)

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
        self.assertIn("Records:", info_result.stdout)
        self.assertIn("Topics:", info_result.stdout)
        self.assertGreater(self._parse_record_count(info_result.stdout), 0, info_result.stdout)

    def _play_recording(self, *extra_args):
        play_result = subprocess.run(
            [
                "bash",
                str(self.play_script),
                "-i",
                str(self.recording_path),
                "--full-speed",
                *extra_args,
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

    @staticmethod
    def _parse_topic_list(output):
        topics = set()
        for line in output.splitlines():
            match = re.search(r'topic "([^"]+)"', line)
            if match and not match.group(1).startswith("__"):
                topics.add(match.group(1))
        return topics

    @staticmethod
    def _parse_record_count(output):
        match = re.search(r"^Records:\s*(\d+)\s*$", output, re.MULTILINE)
        return int(match.group(1)) if match else 0

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