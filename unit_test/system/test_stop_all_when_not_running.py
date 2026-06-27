import subprocess
import unittest
from pathlib import Path


class StopAllWhenNotRunningTest(unittest.TestCase):
    repo_root = Path(__file__).resolve().parents[2]
    stop_script = repo_root / "scripts" / "runtime" / "stop_all.sh"

    def setUp(self):
        subprocess.run(
            [str(self.stop_script), "all"],
            cwd=self.repo_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def test_stop_all_reports_everything_not_running(self):
        result = subprocess.run(
            [str(self.stop_script), "all"],
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        output = result.stdout

        self.assertEqual(
            result.returncode,
            0,
            f"stop_all.sh exited with status {result.returncode}\n{output}",
        )

        expected_lines = [
            "execution_boot is not running",
            "forecast_boot is not running",
            "data_boot is not running",
            "Local Kafka is not running",
            "Requested shutdown complete",
        ]

        for line in expected_lines:
            with self.subTest(line=line):
                self.assertIn(line, output)


if __name__ == "__main__":
    unittest.main()