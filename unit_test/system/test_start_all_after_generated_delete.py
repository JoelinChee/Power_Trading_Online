import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path


class StartAllAfterGeneratedDeleteTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    generated_dir = repo_root / "generated"
    start_script = repo_root / "scripts" / "runtime" / "start_all.sh"
    stop_script = repo_root / "scripts" / "runtime" / "stop_all.sh"

    def setUp(self):
        self.env = os.environ.copy()
        self.env.setdefault("PYTHON_BIN", self._detect_project_python())
        self._stop_all()

    def tearDown(self):
        self._stop_all()

    def test_start_all_recreates_generated_and_starts_services(self):
        shutil.rmtree(self.generated_dir, ignore_errors=True)

        result = subprocess.run(
            [str(self.start_script), "all"],
            cwd=self.repo_root,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=300,
            check=False,
        )
        output = result.stdout

        self.assertEqual(
            result.returncode,
            0,
            f"start_all.sh exited with status {result.returncode}\n{output}",
        )
        self.assertNotIn("error", output.lower(), output)

        expected_patterns = [
            r"^Started local Kafka with PID \d+$",
            r"^Kafka is ready on 127\.0\.0\.1:9092$",
            r"^Started data_boot on port 8001 with PID \d+$",
            r"^Started forecast_boot on port 8002 with PID \d+$",
            r"^Started execution_boot on port 8003 with PID \d+$",
        ]

        for pattern in expected_patterns:
            with self.subTest(pattern=pattern):
                self.assertRegex(output, re.compile(pattern, re.MULTILINE))

    def _stop_all(self):
        subprocess.run(
            [str(self.stop_script), "all"],
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