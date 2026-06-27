import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TestTarget:
    name: str
    path: Path


@dataclass(frozen=True)
class TestResult:
    target: TestTarget
    returncode: int
    elapsed_seconds: float
    output: str

    @property
    def passed(self):
        return self.returncode == 0


REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT_TEST_ROOT = Path(__file__).resolve().parent
STOP_ALL_SCRIPT = REPO_ROOT / "scripts" / "runtime" / "stop_all.sh"
STOP_WEB_SCRIPT = REPO_ROOT / "scripts" / "web" / "stop_web.sh"

FORCE_KILL_PATTERNS = [
    "uvicorn boots.data_boot.main:app",
    "uvicorn boots.forecast_boot.main:app",
    "uvicorn boots.execution_boot.main:app",
    f"streamlit run {REPO_ROOT / 'web' / 'app.py'}",
    "scripts/kafka/kafka_record.py",
    "scripts/kafka/kafka_play.py",
    "scripts/kafka/kafka_topic_echo.py",
    "kafka-server-start.sh",
    "kafka.Kafka",
]

TEST_TARGETS = [
    TestTarget(
        name="system_start_all_after_generated_delete",
        path=UNIT_TEST_ROOT / "system" / "test_start_all_after_generated_delete.py",
    ),
    TestTarget(
        name="system_stop_all_when_not_running",
        path=UNIT_TEST_ROOT / "system" / "test_stop_all_when_not_running.py",
    ),
    TestTarget(
        name="kafka_record_all_topics",
        path=UNIT_TEST_ROOT / "kafka" / "test_kafka_record_all_topics.py",
    ),
    TestTarget(
        name="kafka_play_echo_recording",
        path=UNIT_TEST_ROOT / "kafka" / "test_kafka_play_echo_recording.py",
    ),
    TestTarget(
        name="kafka_topic_list_and_info",
        path=UNIT_TEST_ROOT / "kafka" / "test_kafka_topic_list_and_info.py",
    ),
    TestTarget(
        name="execution_boot_receives_replayed_forecasts",
        path=UNIT_TEST_ROOT / "kafka" / "test_execution_boot_receives_replayed_forecasts.py",
    ),
]


def main():
    results = []

    print("Starting unit test suite")
    print(f"Repository: {REPO_ROOT}")
    print()

    static_errors = run_static_checks()
    if static_errors:
        print("Static checks: FAIL")
        for error in static_errors:
            print(f"- {error}")
        return 1
    print("Static checks: PASS")
    print()

    cleanup_result = cleanup_before_suite()
    print("Startup cleanup")
    print(indent_output(cleanup_result.rstrip() or "<no output>"))
    print()

    for index, target in enumerate(TEST_TARGETS, start=1):
        result = run_test(index, len(TEST_TARGETS), target)
        results.append(result)
        print_result(result)

    print_summary(results)
    return 0 if all(result.passed for result in results) else 1


def run_static_checks():
    errors = []
    for target in TEST_TARGETS:
        if not target.path.is_file():
            errors.append(f"missing test target: {target.path}")

    for script in (STOP_ALL_SCRIPT, STOP_WEB_SCRIPT):
        if not script.is_file():
            errors.append(f"missing cleanup script: {script}")

    return errors


def cleanup_before_suite():
    output_parts = []
    for command in (["bash", str(STOP_WEB_SCRIPT)], ["bash", str(STOP_ALL_SCRIPT), "all"]):
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        output_parts.append(f"$ {' '.join(command)}")
        output_parts.append(completed.stdout.rstrip() or "<no output>")
        output_parts.append(f"exit={completed.returncode}")

    killed_pids = force_kill_possible_processes()
    if killed_pids:
        output_parts.append(f"force-killed pids: {', '.join(str(pid) for pid in killed_pids)}")
    else:
        output_parts.append("force-killed pids: <none>")

    return "\n".join(output_parts)


def force_kill_possible_processes():
    current_pid = str(os.getpid())
    killed_pids = []

    for pattern in FORCE_KILL_PATTERNS:
        completed = subprocess.run(
            ["pgrep", "-f", pattern],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        for pid_text in completed.stdout.split():
            if pid_text == current_pid:
                continue
            kill_result = subprocess.run(
                ["kill", "-9", pid_text],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if kill_result.returncode == 0:
                killed_pids.append(int(pid_text))

    return sorted(set(killed_pids))


def run_test(index, total, target):
    print(f"[{index}/{total}] Running {target.name}")
    print(f"Command: {sys.executable} {target.path}")

    started_at = time.monotonic()
    completed = subprocess.run(
        [sys.executable, str(target.path)],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    elapsed_seconds = time.monotonic() - started_at
    return TestResult(target, completed.returncode, elapsed_seconds, completed.stdout)


def print_result(result):
    status = "PASS" if result.passed else "FAIL"
    print(f"Result: {status} {result.target.name} ({result.elapsed_seconds:.3f}s)")
    print("Output:")
    print(indent_output(result.output.rstrip() or "<no output>"))
    print()


def print_summary(results):
    passed_count = sum(1 for result in results if result.passed)
    failed_count = len(results) - passed_count
    total_seconds = sum(result.elapsed_seconds for result in results)

    print("Overall result")
    print(f"Total: {len(results)}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {failed_count}")
    print(f"Elapsed: {total_seconds:.3f}s")

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"- {status}: {result.target.name} ({result.elapsed_seconds:.3f}s)")


def indent_output(output):
    return "\n".join(f"    {line}" for line in output.splitlines())


if __name__ == "__main__":
    raise SystemExit(main())