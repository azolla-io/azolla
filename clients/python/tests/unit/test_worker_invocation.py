"""Tests for WorkerInvocation JSON parsing."""

from azolla import WorkerInvocation
from azolla.exceptions import WorkerError


def test_worker_invocation_parses_args_and_kwargs() -> None:
    invocation = WorkerInvocation.from_json(
        task_id="task-1",
        task_name="sample",
        args_json="[1, 2, 3]",
        kwargs_json='{"flag": true}',
        shepherd_endpoint="http://127.0.0.1:50052",
    )

    assert invocation.task_id == "task-1"
    assert invocation.task_name == "sample"
    assert invocation.args == [1, 2, 3]
    assert invocation.kwargs == {"flag": True}


def test_worker_invocation_invalid_json_raises() -> None:
    try:
        WorkerInvocation.from_json(
            task_id="task-2",
            task_name="sample",
            args_json="not-json",
            kwargs_json="{}",
            shepherd_endpoint="http://127.0.0.1:50052",
        )
    except WorkerError as exc:
        assert "Invalid args JSON" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("WorkerError was not raised for invalid JSON")
