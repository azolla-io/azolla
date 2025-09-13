**Azolla Python Client — Holistic Review (clients/python)**

- Scope: Full review of the Python client at `clients/python` including source, tests, scripts, and packaging.
- Focus: Bugs, inconsistencies, design issues, incorrect comments/docs.
- Output: Ranked by severity (Critical → Low) with file references and suggested fixes.

**Critical Issues**
- CLI does not register tasks from modules
  - File: `clients/python/src/azolla/cli.py`
  - Problem: `--task-modules` are imported, but tasks are never discovered or registered on the `Worker` instance created in this process. The Worker runs with zero tasks unless task registration happens elsewhere as a side effect (no mechanism provided).
  - Impact: `azolla-worker` is effectively non-functional for typical use; imported modules do not attach tasks to the running worker.
  - Fix: After importing modules, discover decorated tasks (attributes like `__azolla_task_instance__`) and/or require modules to expose a `TASKS` list and call `worker.register_task(...)` for each. Alternatively, provide a global registry or explicit module entry point function that receives the worker.
  - Status: Fixed
    - Test added: tests/unit/test_cli_registration.py::test_importing_task_modules_registers_tasks
    - Result before fix: FAIL (importing modules did not register tasks)
    - Fix: Added discover_and_register_tasks in azolla.cli and used it in worker_main to import modules and register tasks
    - Result after fix: PASS

- Task argument casting breaks tasks without an Args model
  - File: `clients/python/src/azolla/task.py` (`Task._execute_with_casting`, `Task.parse_args`)
  - Problems:
    - `if hasattr(self, "_args_type"):` is always true (attribute exists on base). For tasks without an `Args` model, this leads to `parse_args` raising `ValidationError("Task has no Args type defined")` and prevents execution.
    - Even if the branch were correct, the `else` path passes `BaseModel()` to `execute`, silently discarding provided arguments — dynamic tasks lose their inputs.
  - Impact: Class-based tasks that intentionally don’t define an `Args` Pydantic model (e.g., integration test tasks) either fail at parsing or receive empty args.
  - Fix: Use `if self._args_type:` instead of `hasattr`. When there is no `Args` type, pass through `raw_args` to `execute` unchanged.
  - Status: Fixed
    - Test added: tests/unit/test_task.py::TestExplicitTask::test_task_without_args_model_passes_raw_args
    - Result before fix: FAIL (raised ValidationError: "Task has no Args type defined")
    - Fix: In Task._execute_with_casting, use `if self._args_type:` and pass through raw args when no Args model
    - Result after fix: PASS

- TLS not supported but implied by `https://` endpoints
  - File: `clients/python/src/azolla/client.py` (`Client._ensure_connection`)
  - Problem: `https://` is accepted and stripped, then `grpc.aio.insecure_channel` is always used. There is no path to `secure_channel` or TLS credentials.
  - Impact: Users specifying HTTPS will unknowingly make an insecure connection (or fail in secured environments). Misleading and potentially unsafe.
  - Fix: Either implement TLS support (certs/credentials, `secure_channel`) or explicitly reject `https://` with a clear error until TLS is implemented.
  - Status: Fixed
    - Test added: tests/unit/test_client_tls.py::test_https_endpoint_not_supported
    - Result before fix: FAIL (did not raise)
    - Fix: Client._ensure_connection raises ValueError when endpoint starts with https://
    - Result after fix: PASS

**High Issues**
- Exponential reconnect jitter can become negative or explode
  - File: `clients/python/src/azolla/worker.py` (`Worker.start` loop, jitter in reconnection)
  - Problem: `jitter = 1.0 + (reconnect_delay * 0.1 * (2 * loop.time() % 1 - 1))` scales jitter by `reconnect_delay`, producing a factor in range roughly `[-5, 7]` when delay is large. The next delay can become negative or spike.
  - Impact: Busy-loop reconnects (negative timeouts), erratic reconnection behavior, or long pauses.
  - Fix: Use bounded multiplicative jitter independent of delay (e.g., `random.uniform(0.9, 1.1)`) and clamp to minimum positive delay.
  - Status: Fixed
    - Test added: tests/unit/test_worker_jitter.py::test_reconnect_delay_never_negative
    - Result before fix: FAIL (second wait_for timeout captured as negative)
    - Fix: Added Worker._next_reconnect_delay with bounded jitter and used it in start() loop
    - Result after fix: PASS

- Decorator does not forward `TaskContext` to task function
  - File: `clients/python/src/azolla/task.py` (generated task `execute`)
  - Problem: `GeneratedTask.execute` ignores the `context` parameter when invoking the original function. Decorated tasks can’t access context even if they declare it.
  - Impact: Decorated tasks lose access to attempt metadata, final-attempt checks, etc.
  - Fix: Detect whether the function accepts `context` (via signature) and pass `context=context` in the call; or always pass it as a kwarg if present.
  - Status: Fixed
    - Test added: tests/unit/test_task.py::TestTaskDecorator::test_decorator_passes_context
    - Result before fix: FAIL (context included in Args; context not passed to function)
    - Fix: Exclude `context` from generated Args model and pass `context` kwarg when function accepts it
    - Result after fix: PASS

- Pydantic list/positional args handling likely invalid
  - File: `clients/python/src/azolla/task.py` (`Task.parse_args`)
  - Problem: For lists with length > 1, `model_validate(json_args)` is called directly on a list. Pydantic v2 typically expects a mapping for model field assignment.
  - Impact: Positional list inputs to typed tasks will likely fail validation; only the “single dict in a list” case is supported.
  - Fix: Map positional lists to named fields using the Args model’s field order; otherwise require dicts and reject positional lists with a clear error.
  - Status: Fixed
    - Test added: tests/unit/test_task.py::TestExplicitTask::test_positional_list_args_mapping
    - Result before fix: FAIL (pydantic ValidationError expecting dict, got list)
    - Fix: In Task.parse_args, map positional lists to Args fields by order; validate arity
    - Result after fix: PASS

- Premature “ready” signaling and registration message
  - File: `clients/python/src/azolla/worker.py` (`_attempt_stream_connection`)
  - Problem: Logs “Shepherd ... registered successfully” and sets `_ready_event` after a fixed sleep (0.1s), with no confirmation from orchestrator.
  - Impact: Test/operational code can believe the worker is ready when the stream may have already failed; leads to subtle flakiness.
  - Fix: Gate “ready” on verified stream activity (e.g., successfully read a server message) or an explicit registration ack if the protocol supports it.
  - Status: Fixed
    - Test added: tests/unit/test_worker_ready.py::test_ready_signal_set_on_first_server_message
    - Result before fix: N/A (unit test targets _read_responses readiness path)
    - Fix: Ensure ready is signaled upon first server message in _read_responses and remove premature ready set from _attempt_stream_connection
    - Result after fix: PASS

**Medium Issues**
- Built-in exception names shadowed (`ConnectionError`, `TimeoutError`)
  - File: `clients/python/src/azolla/exceptions.py` and re-exports in `__init__.py`
  - Problem: Redefines names of Python built-ins, causing ambiguity and mismatches when users refer to these types in code or `RetryPolicy.retry_on`.
  - Impact: Confusion and potential misbehavior if a user expects built-in semantics. Docs use `TimeoutError` symbol without clarifying which one.
  - Fix: Rename to `AzollaConnectionError`, `TaskTimeoutError` (or similar), or document clearly and avoid unqualified usage in examples.
  - Status: Fixed
    - Test added: tests/unit/test_exceptions_aliases.py::test_exception_aliases_exist_and_subclass
    - Result before fix: FAIL (ImportError: alias classes not found)
    - Fix: Added alias classes AzollaConnectionError and TaskTimeoutError; exported via __init__
    - Result after fix: PASS

- `grpcio-tools` missing from dev extras but required by generator
  - File: `clients/python/scripts/generate_proto.py`, `clients/python/pyproject.toml`
  - Problem: Script depends on `grpc_tools.protoc`, but `pyproject.toml`’s dev/test extras don’t include `grpcio-tools`.
  - Impact: Proto generation fails for contributors following documented workflows.
  - Fix: Add `grpcio-tools` to `[project.optional-dependencies].dev` (or a separate `codegen` extra).
  - Status: Fixed
    - Test added: tests/unit/test_packaging.py::test_pyproject_has_grpcio_tools_in_dev_extras
    - Result before fix: FAIL (not found in dev extras)
    - Fix: Added grpcio-tools to [project.optional-dependencies].dev in pyproject.toml
    - Result after fix: PASS

- README references non-existent `azolla.testing.TaskTester`
  - File: `clients/python/README.md`
  - Problem: Example imports `from azolla.testing import TaskTester`, but no such module exists in the package.
  - Impact: Broken docs; confuses users.
  - Fix: Provide such a helper or remove/update the example.
  - Status: Fixed
    - Test added: tests/unit/test_docs.py::test_readme_has_no_tasktester_reference
    - Result before fix: FAIL (reference exists)
    - Fix: Updated README “Testing Your Tasks” to use direct coroutine testing without TaskTester
    - Result after fix: PASS

- Docs/code inconsistency about async iteration helpers
  - Files: `clients/python/GRPC_INCOMPATIBILITY.md`, `clients/python/src/azolla/worker.py`
  - Problem: Doc claims the fix is using `aiter()/anext()`, but the implementation uses `__aiter__().__anext__()` for Py3.9 compatibility.
  - Impact: Contradictory guidance for maintainers and contributors.
  - Fix: Align doc with the actual implementation and explain the Py3.9 compatibility rationale.
  - Status: Fixed
    - Test added: tests/unit/test_docs.py::test_incompat_doc_matches_iterator_approach
    - Result before fix: FAIL (doc referenced aiter/anext while code used __aiter__/__anext__)
    - Fix: Update GRPC_INCOMPATIBILITY.md to show __aiter__/__anext__ pattern for Python 3.9 compatibility
    - Result after fix: PASS

- Generated gRPC files carry a “NO CHECKED-IN PROTOBUF GENCODE” header
  - Files: `clients/python/src/azolla/_grpc/*_pb2.py`
  - Problem: Files are committed despite the header discouraging it.
  - Impact: Mixed messaging; contributors may be unsure whether to re-generate vs rely on checked-in code.
  - Fix: Either remove the header or document the decision to check in generated code (recommended for Python packages).
  - Status: Fixed
    - Test added: tests/unit/test_docs.py::test_readme_mentions_checked_in_grpc_code
    - Result before fix: FAIL (no documentation on checked-in stubs)
    - Fix: Added README note clarifying that generated gRPC stubs are checked in and how to regenerate
    - Result after fix: PASS

**Low Issues**
- Overly verbose logging with emojis at INFO level
  - Files: `clients/python/src/azolla/worker.py`, test worker script
  - Problem: Very chatty INFO logs with emojis. Good for debugging, noisy in production.
  - Impact: Log noise in real deployments.
  - Fix: Move most to DEBUG; reserve INFO for lifecycle milestones.

- Duplicate/legacy code paths
  - File: `clients/python/src/azolla/worker.py` (`_process_messages` vs `_process_messages_concurrent`)
  - Problem: Redundant “legacy” wrapper adds complexity.
  - Impact: Maintenance overhead and confusion.
  - Fix: Consolidate on one clear path.

- `Client` defaults and endpoint scheme inconsistency
  - Files: `clients/python/src/azolla/client.py`, `clients/python/src/azolla/worker.py`
  - Observations: Client default endpoint includes `http://...`, worker default is bare `host:port`. Not a bug, but inconsistent UX.
  - Suggestion: Align defaults and document scheme handling.

- Minor comment/content mismatches
  - Files: `clients/python/README.md`, `clients/python/clients/AGENTS.md`
  - Examples mention PEP 420 namespace packaging; the project uses a `src/` layout with a normal package. Clarify the packaging approach.

**Suggested Fixes (Summary)**
- CLI: Implement task discovery/registration from `--task-modules`.
- Task casting: Use `if self._args_type:`; pass-through raw args when untyped; implement positional-to-named mapping for lists.
- Decorator: Forward `context` to the original function when supported.
- Reconnect jitter: Use bounded random jitter independent of delay; clamp to positive.
- TLS: Support `secure_channel` or explicitly reject `https://` pending TLS support.
- Exceptions: Avoid shadowing built-ins; rename or clearly qualify in docs and examples.
- Tooling/Docs: Add `grpcio-tools` to dev extras; fix README samples (remove `TaskTester` or provide it); align GRPC incompat docs with code; clarify stance on checked-in gRPC stubs.

**Notable Constraints/Context**
- Known gRPC incompatibility between Python `grpcio` and Rust `tonic` is documented and partially worked around. The items above focus on correctness and developer experience independent of that underlying issue.
