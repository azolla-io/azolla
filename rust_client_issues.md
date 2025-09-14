Azolla Rust Client — Issues Review and Fix Plan

Severity legend: [Critical] [High] [Medium] [Low]

1) [High] Client timeout is unused in requests and waits
- Category: Design gap / Robustness
- Details: `ClientConfig::timeout` is never applied. `Client::with_config` does not set connect/request deadlines on the gRPC `Channel`. `TaskHandle::wait` loops indefinitely and never returns `AzollaError::Timeout` even if the client timeout is configured.
- Impact: Risk of unbounded hangs in adverse network/server conditions; violates expected behavior implied by config.
- Fix: Use `tonic::transport::Endpoint` to set `connect_timeout` and `timeout` on the channel; bound `TaskHandle::wait` with an overall `tokio::time::timeout` using the client’s configured timeout, returning `AzollaError::Timeout` on expiry.
- Status: Fixed (design issue; no failing test required per instructions)

2) [High] azolla-macros tests mismatch the real Task trait (won’t compile)
- Category: Test bug / Incompatibility
- Details: The proc-macro expands to implement `azolla_client::task::Task` with associated `type Args` and a typed `execute(&self, args: Self::Args)` method. The test suite mocks a different `Task` trait taking `Vec<Value>` and no associated type, and then calls `execute(Vec<Value>)`. This cannot compile.
- Impact: The macros crate tests fail to compile, blocking CI and hiding true regressions.
- Fix: Align the test mock trait to the real signature and update tests to call the typed `execute` API rather than passing JSON vectors. Remove/adjust tests that were validating JSON extraction (that logic lives in the runtime, not the macro).
- Status: Fixed (tests updated)

3) [Medium] f64 JSON conversion silently falls back to 0.0 on failure
- Category: Functional bug
- Details: In `convert.rs`, `FromJsonValue for f64` uses `as_f64().unwrap_or(0.0)`. JSON values that cannot be represented as `f64` (e.g., extremely large numbers) will map to `0.0` instead of returning an error.
- Impact: Data corruption and silent misinterpretation in edge cases.
- Fix: Return `ConversionError` when `as_f64()` returns `None`.
- Status: Fixed (failing unit test added; verified before/after)

4) [Medium] TaskError API: `error_type()` returns `Option<String>` but is always `Some`
- Category: API design / Inefficiency
- Details: `TaskError::error_type()` unnecessarily returns `Option<String>` (always `Some`) and clones. `error_code()` clones `String` too.
- Impact: Awkward API and avoidable allocations; confusing semantics.
- Fix: Change to `fn error_type(&self) -> &str` and `fn error_code(&self) -> Option<&str>`, update call sites.
- Status: Fixed (API updated; call sites adjusted)

5) [Medium] Duplicate type name `TaskExecutionResult` in both client.rs and task.rs
- Category: Consistency / Maintainability
- Details: Two distinct public enums with the same name exist in separate modules. This invites confusion and accidental misuse.
- Impact: Reduced clarity; potential import confusion.
- Fix: Consolidate into a single canonical type and re-export from crate root; or rename the internal one used only for tests. For now, documented; deferring unification to avoid broad API churn.
- Status: Acknowledged (deferred)

6) [Low] Duplicated test module content in worker.rs
- Category: Test hygiene / Duplication
- Details: The test module in `worker.rs` is duplicated in the file.
- Impact: Longer compile times; maintenance hazard.
- Fix: Remove duplicate block, keep single authoritative tests.
- Status: Fixed

7) [Low] Format macro style inconsistencies (clippy: uninlined format args)
- Category: Style / Linting
- Details: Some occurrences of `format!("... {} ...", var)` in code/tests (e.g., macro crate) instead of inline format args.
- Impact: Clippy warnings; style drift from project guidelines.
- Fix: Use inline format args `"... {var} ..."` where touched.
- Status: Partially fixed (updated where code was modified)

8) [Low] Unused/underused types: `TaskContext`
- Category: Design gap
- Details: `TaskContext` is defined but not currently wired into worker execution paths.
- Impact: Potentially confusing API surface; indicates missing integration.
- Fix: Consider passing `TaskContext` into `Task::execute` in future; out of scope for this pass.
- Status: Acknowledged (deferred)

9) [Low] Concurrency not enforced in worker task intake
- Category: Design gap
- Details: `current_load` is tracked with `LoadGuard` but no explicit back-pressure check against `max_concurrency` before accepting tasks.
- Impact: Possible over-commit if orchestrator misbehaves; mitigated by orchestrator contract.
- Fix: Consider rejecting/deferring tasks when `current_load >= max_concurrency`.
- Status: Acknowledged (deferred)


Fix Log

- Issue 2 (macros test mismatch): Updated mock Task trait to match real signature; rewrote tests to use typed arguments. Status: Fixed.
- Issue 3 (f64 conversion bug): Added failing unit test; fixed conversion to return error not 0.0. Status: Fixed.
- Issue 4 (TaskError API): Switched to borrowing getters; updated worker and tests. Status: Fixed.
- Issue 6 (duplicate tests in worker.rs): Removed duplicate block. Status: Fixed.
- Issue 1 (timeouts): Channel deadlines and overall wait timeout added. Status: Fixed.
