# gRPC Incompatibility Between Python and Rust

## Problem Description

The Python Azolla client has a known incompatibility when connecting to Rust orchestrators via bidirectional gRPC streams. This manifests as:

- Immediate connection drops after worker registration
- Error: "h2 protocol error: connection reset"
- Tests hanging indefinitely waiting for task results
- Workers unable to receive task messages from orchestrator

## Root Cause

The issue stems from differences in how Python's `grpcio` library and Rust's `tonic` library handle HTTP/2 stream termination:

1. **Rust tonic/h2 behavior**: Sends `RST_STREAM(CANCEL)` when closing streams
2. **Python grpcio behavior**: Expects `RST_STREAM(NO_ERROR)` for normal termination
3. **Result**: Python interprets `RST_STREAM(CANCEL)` as a protocol violation, causing connection reset

### Technical Details

- **Issue tracking**: https://github.com/hyperium/h2/issues/681
- **Protocol**: HTTP/2 RST_STREAM frame error code handling
- **Affected components**: Bidirectional gRPC streams in ClusterService
- **Scope**: Python worker ↔ Rust orchestrator communication

## Workaround Implementation

A timeout/retry mechanism has been implemented in the Python worker to handle this incompatibility:

### 1. Stream Timeout Detection
**Location**: `src/azolla/worker.py:_read_responses()`
```python
# 5-second timeout to detect stuck streams
stream_timeout = 5.0
message = await asyncio.wait_for(
    self._get_next_message(stream_call),
    timeout=stream_timeout
)
```

### 2. Connection Retry with Exponential Backoff
**Location**: `src/azolla/worker.py:_run_bidirectional_stream()`
```python
max_stream_attempts = 3
# Retry delays: 1s, 2s, 4s
delay = 2 ** (stream_attempt - 1)
```

### 3. Proper Async Iterator Handling
**Location**: `src/azolla/worker.py:_get_next_message()`
```python
# Fixed: Use aiter()/anext() instead of __anext__()
async_iter = aiter(stream_call)
return await anext(async_iter)
```

## Current Status

### ✅ Improvements Achieved
- **No more hanging tests**: Tests complete in ~0.4s instead of hanging indefinitely
- **Connection resilience**: Automatic detection and retry of failed connections
- **Resource leak prevention**: Proper cleanup of stuck gRPC streams
- **Fast failure detection**: Issues detected within 5 seconds

### ⚠️ Limitations Remaining
- **Task execution**: Tasks complete with `success=True, value={}` instead of proper execution
- **Protocol incompatibility**: Core gRPC stream communication still fails
- **Workaround dependency**: System relies on timeout/retry rather than fixing root cause

## Test Impact

The integration test `test_task_fails_after_exhausting_attempts` demonstrates the issue:

**Before workaround**:
```
- Test hangs indefinitely
- Worker stream never receives task messages
- Manual intervention required to stop tests
```

**After workaround**:
```
- Test completes in ~0.4s
- Connection drops are handled gracefully
- Task completes with empty result instead of hanging
```

## Potential Solutions

### 1. HTTP/REST Fallback ⭐ (Recommended)
- Implement HTTP/REST endpoints alongside gRPC
- Avoid gRPC incompatibility entirely
- Maintain similar API surface

### 2. Compatible gRPC Versions
- Upgrade to newer grpcio/tonic versions with better compatibility
- Test thoroughly across versions
- Monitor for regressions

### 3. Protocol Bridge
- Implement translation layer between Python and Rust gRPC
- Complex but would preserve bidirectional streaming
- Higher maintenance overhead

### 4. Alternative Python gRPC Library
- Replace grpcio with grpclib (pure Python implementation)
- May have different compatibility characteristics
- Requires thorough testing

## Monitoring

Watch for these log patterns indicating the incompatibility:

**Orchestrator logs (Rust)**:
```
ERROR azolla::orchestrator::cluster_service] Error receiving message from shepherd:
status: Unknown, message: "h2 protocol error: error reading a body from connection: connection reset"
```

**Worker logs (Python)**:
```
INFO - 🔗 WORKER: Stream attempt X/3 failed: [connection error]
INFO - 🔗 WORKER: Retrying in X.Xs...
```

## References

- [Hyperium h2 Issue #681](https://github.com/hyperium/h2/issues/681)
- [gRPC HTTP/2 Specification](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
- [Python grpcio Documentation](https://grpc.github.io/grpc/python/)
- [Rust tonic Documentation](https://docs.rs/tonic/)