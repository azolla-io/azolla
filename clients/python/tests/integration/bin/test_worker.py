#!/usr/bin/env python3
"""Python worker binary used by integration tests.

This script registers the test tasks required by the suite and executes a single
invocation, reporting the result back to the shepherd using the azolla client
library's single-run worker runtime.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# Ensure the azolla package (from clients/python/src) is importable
PROJECT_ROOT = Path(__file__).resolve().parents[5]
PYTHON_SRC = PROJECT_ROOT / "clients" / "python" / "src"

if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

VENV_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"


def ensure_runtime() -> None:
    if importlib.util.find_spec("grpc") is None:
        if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
            os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])
        raise ModuleNotFoundError("grpc")


ensure_runtime()

from azolla import Task, Worker, WorkerInvocation  # noqa: E402
from azolla.exceptions import TaskError, ValidationError  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class EchoTask(Task):
    def name(self) -> str:
        return "echo"

    async def execute(self, args: Any) -> Any:
        if isinstance(args, list) and len(args) == 1:
            payload = args[0]
        else:
            payload = args
        return [payload]


class AlwaysFailTask(Task):
    def name(self) -> str:
        return "always_fail"

    async def execute(self, args: Any) -> Any:
        raise TaskError("Task designed to always fail", error_type="TestError", retryable=False)


class FlakyTask(Task):
    def name(self) -> str:
        return "flaky"

    async def execute(self, args: Any) -> Any:
        state_file = Path(tempfile.gettempdir()) / "flaky_task_state_shared"
        try:
            attempt_count = int(state_file.read_text().strip())
        except (FileNotFoundError, ValueError):
            attempt_count = 0

        attempt_count += 1
        state_file.write_text(str(attempt_count))

        if attempt_count == 1:
            raise TaskError(
                "First attempt failure",
                error_type="TestError",
                error_code="FLAKY_TASK_FIRST_ATTEMPT",
                retryable=True,
            )

        return "Flaky task succeeded on retry"


class MathAddTask(Task):
    def name(self) -> str:
        return "math_add"

    async def execute(self, args: Any) -> Any:
        if not isinstance(args, list) or len(args) != 2:
            raise ValidationError("math_add expects two numeric arguments")

        try:
            return float(args[0]) + float(args[1])
        except (TypeError, ValueError) as exc:
            raise ValidationError("math_add arguments must be numeric") from exc


class CountArgsTask(Task):
    def name(self) -> str:
        return "count_args"

    async def execute(self, args: Any) -> Any:
        original = args
        if isinstance(args, list):
            if len(args) == 1:
                original = args[0]
            elif len(args) == 0:
                original = []

        if isinstance(original, list):
            count = len(original)
        elif isinstance(original, dict):
            count = len(original)
        elif original is None:
            count = 0
        else:
            count = 1

        return {"count": count, "args": original}


def build_worker() -> Worker:
    return (
        Worker.builder()
        .register_task(EchoTask())
        .register_task(AlwaysFailTask())
        .register_task(FlakyTask())
        .register_task(MathAddTask())
        .register_task(CountArgsTask())
        .build()
    )


async def run_task_mode(invocation: WorkerInvocation, worker: Worker) -> None:
    execution = await worker.execute(invocation)

    if execution.is_success():
        result_value = execution.value
        if isinstance(result_value, list) and len(result_value) == 1:
            result_value = result_value[0]

        payload = {
            "success": True,
            "result": result_value,
            "error": None,
            "error_type": None,
        }
    else:
        error = execution.error
        payload = {
            "success": False,
            "result": None,
            "error": error.message if error else "unknown error",
            "error_type": "TaskError",
        }

    print(json.dumps(payload))

    if not execution.is_success():
        raise SystemExit(1)


async def run_worker_mode(invocation: WorkerInvocation, worker: Worker) -> None:
    execution = await worker.run_invocation(invocation)
    if execution.is_success():
        logger.info("Task %s completed successfully", invocation.task_id)
    else:
        err = execution.error.message if execution.error else "unknown"
        logger.error("Task %s failed: %s", invocation.task_id, err)
        raise SystemExit(1)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Test worker binary")
    parser.add_argument(
        "--mode",
        choices=["worker", "task"],
        default="worker",
        help="Execution mode",
    )
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--args", default="[]")
    parser.add_argument("--kwargs", default="{}")
    parser.add_argument("--shepherd-endpoint")
    parser.add_argument("--orchestrator-endpoint")
    args = parser.parse_args()

    worker = build_worker()

    shepherd_endpoint = (
        args.shepherd_endpoint or args.orchestrator_endpoint or "http://127.0.0.1:60000"
    )

    invocation = WorkerInvocation.from_json(
        task_id=args.task_id,
        task_name=args.name,
        args_json=args.args,
        kwargs_json=args.kwargs,
        shepherd_endpoint=shepherd_endpoint,
    )
    if args.mode == "task":
        await run_task_mode(invocation, worker)
    else:
        await run_worker_mode(invocation, worker)


if __name__ == "__main__":
    asyncio.run(main())
