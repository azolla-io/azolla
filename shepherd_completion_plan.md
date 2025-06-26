# Shepherd Implementation Completion Plan

## Overview
Complete implementation of azolla-shepherd with proper component architecture, testing, and configuration support.

## Implementation Strategy
- **Approach**: One component at a time with testing
- **Error Handling**: Infinite retry for orchestrator connection
- **Resource Limits**: Placeholder implementation
- **Configuration**: Support both config files and CLI args
- **Testing**: Include simple test worker binary

## Phase 1: Foundation Components

### 1.1 Create Test Worker Binary ✅
**File**: `src/bin/azolla-worker.rs`
- Accept command line args: `--task-id`, `--name`, `--args`, `--kwargs`, `--shepherd-endpoint`
- Connect to shepherd's worker service via gRPC
- Simulate task execution (sleep + random success/failure)
- Report results back to shepherd
- **Testing**: Verify CLI parsing and basic execution

### 1.2 Configuration System ✅
**File**: `src/shepherd/config.rs`
- Load from TOML config file with CLI override support
- Validate worker binary path exists
- Environment variable support
- Configuration structure matching current `ShepherdConfig`
- **Testing**: Config file parsing and CLI override behavior

### 1.3 Worker Service (gRPC Server) ✅
**File**: `src/shepherd/worker_service.rs`
- Implement `Worker` service from proto definition
- `ReportResult` RPC handler
- Result validation and forwarding
- Listen on configured port
- **Testing**: Mock worker connections and result reporting

## Phase 2: Core Communication

### 2.1 Stream Handler ✅
**File**: `src/shepherd/stream_handler.rs`
- Connect to orchestrator via gRPC `Dispatch.Stream`
- Send `Hello` message on connection
- Handle incoming `Task` and `Ping` messages
- Send `Status` and `TaskResult` messages
- Infinite retry with exponential backoff
- **Testing**: Mock orchestrator server for connection testing

### 2.2 Task Manager ✅
**File**: `src/shepherd/task_manager.rs`
- Task queue (FIFO) with capacity management
- Track current load vs max_concurrency  
- Interface with stream handler and process monitor
- Task lifecycle state management
- **Testing**: Task queuing and capacity enforcement

## Phase 3: Process Management

### 3.1 Process Monitor ✅
**File**: `src/shepherd/process_monitor.rs`
- Spawn worker processes with correct command line
- Monitor process health and exit codes
- Handle process cleanup and orphan detection
- Worker timeout handling
- **Testing**: Process spawning and monitoring with test worker

## Phase 4: Integration

### 4.1 Service Integration ✅
**File**: Update `src/bin/azolla-shepherd.rs`
- Wire all components together
- Proper component lifecycle management
- Graceful shutdown handling
- Error propagation and logging
- **Testing**: End-to-end integration test

### 4.2 End-to-End Testing
- Test full workflow: orchestrator ↔ shepherd ↔ worker
- Error scenarios (worker failures, connection drops)
- Load testing with multiple concurrent tasks
- Configuration validation

## Implementation Order

1. **Test Worker Binary** ✅ - Build foundation for testing
2. **Configuration System** ✅ - Enable proper config management  
3. **Worker Service** ✅ - Handle worker result reporting
4. **Stream Handler** ✅ - Core orchestrator communication
5. **Task Manager** ✅ - Task queuing and lifecycle
6. **Process Monitor** ✅ - Worker process management
7. **Service Integration** ✅ - Wire everything together
8. **End-to-End Testing** - Validate complete system

## Progress Tracking
- ⏳ In Progress
- ✅ Completed
- ❌ Blocked
- 📝 Needs Review

---

*Last Updated: Starting implementation*