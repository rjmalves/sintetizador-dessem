# ticket-023 Add Multiprocessing-Safe Logger

## Context

### Background

The current logger at `app/utils/log.py` is a simple Singleton that creates a single `StreamHandler` on the root "main" logger. It is initialized once in `main.py` via `Log.configure_logging(BASEDIR)` and then accessed throughout the app via `Log.log()`. This design is not safe for multiprocessing: when child processes are spawned (via `spawn` start method), the Singleton state is not shared, and concurrent writes to stdout produce interleaved or lost log lines.

sintetizador-newave already solved this problem with a `QueueHandler` + listener-process pattern (see `/home/rogerio/git/sintetizador-newave/app/utils/log.py`). This ticket ports that pattern to sintetizador-dessem, adapting it to the dessem codebase's simpler structure (no `version`, no `file_repository` setting in the newave sense).

### Relation to Epic

This is the foundational ticket of Epic 06 (Parallelism & Logging). The multiprocessing-safe logger must be in place before the CLI option (ticket-024) or ProcessPoolExecutor integration (ticket-025) can function correctly, because worker processes need a way to safely emit log messages back to the main process.

### Current State

- `app/utils/log.py`: 25-line `Log` class with `Singleton` metaclass. Has `configure_logging(diretorio)` class method that creates a `StreamHandler` on the "main" logger, and a `log()` class method returning the logger instance.
- `app/utils/singleton.py`: Standard Singleton metaclass (10 lines). Not multiprocessing-safe (each spawned process gets its own `_instances` dict).
- `main.py`: Calls `Log.configure_logging(BASEDIR)` before `app()`.
- `app/app.py`: Each Click command accesses `Log.log()` for pre/post logging.
- The orchestrator at `app/services/synthesis/operation/orchestrator.py` sets `cls.logger = logging.getLogger("main")` at the top of `synthetize()`.

## Specification

### Requirements

1. Replace the `Log` class body in `app/utils/log.py` with the multiprocessing-safe QueueHandler pattern from sintetizador-newave, adapted for dessem.
2. The new `Log` class must provide:
   - `logging_process(q)`: classmethod that runs in a separate `Process`, consuming `LogRecord` objects from a `multiprocessing.Manager().Queue` and dispatching them to a `StreamHandler` on stdout.
   - `configure_queue_logger()`: classmethod that sets up the stdout `StreamHandler` on the root logger (used inside the listener process).
   - `configure_main_logger(q)`: classmethod that returns a `logging.Logger` named "main" with a `QueueHandler` attached, for use in the main process.
   - `configure_process_logger(q, variable)`: classmethod that returns a `logging.Logger` named `"worker-{variable}"` with a `QueueHandler`, for use in child processes.
   - `start_logging_process(q)`: classmethod that spawns the listener `Process`.
   - `terminate_logging_process()`: classmethod that terminates the listener `Process`.
3. Remove the old `configure_logging(diretorio)` and `log()` methods entirely.
4. Update `main.py` to remove the `Log.configure_logging(BASEDIR)` call (logging is now initialized per-command in `app.py`, not globally).
5. The `Singleton` metaclass in `app/utils/singleton.py` remains unchanged -- it is still used by `Settings` and other classes. The `Log` class keeps the `Singleton` metaclass (same as newave).

### Inputs/Props

- `q: multiprocessing.managers.BaseProxy` (a managed Queue proxy from `multiprocessing.Manager().Queue(-1)`)

### Outputs/Behavior

- Log messages from any process (main or worker) are enqueued via `QueueHandler` into the managed queue.
- The listener process dequeues and writes them to stdout with format `"%(asctime)s %(levelname)s: %(message)s"`.
- The listener process polls with `time.sleep(0.1)` and handles `IOError(EPIPE)` gracefully.
- `terminate_logging_process()` calls `.terminate()` on the listener process.

### Error Handling

- `IOError` with `errno.EPIPE` in the listener process prints "EPIPE" and continues looping.
- If `q.get()` returns `None`, the listener process returns (sentinel-based shutdown).

## Acceptance Criteria

- [ ] Given the file `app/utils/log.py`, when inspected, then it contains the methods `logging_process`, `configure_queue_logger`, `configure_main_logger`, `configure_process_logger`, `start_logging_process`, and `terminate_logging_process`, and does NOT contain `configure_logging` or the `log` method.
- [ ] Given the file `main.py`, when inspected, then the line `Log.configure_logging(BASEDIR)` is absent and the file still calls `app()`.
- [ ] Given `app/utils/log.py`, when `Log.configure_main_logger(q)` is called with a managed queue, then it returns a `logging.Logger` instance named "main" with exactly one `QueueHandler` attached.
- [ ] Given `app/utils/log.py`, when `Log.configure_process_logger(q, "CMG_SIN")` is called, then it returns a `logging.Logger` named `"worker-CMG_SIN"` with exactly one `QueueHandler`.
- [ ] Given the full test suite, when `python -m pytest tests/` is run, then all 82 existing tests pass (tests do not use the logging infrastructure directly).

## Implementation Guide

### Suggested Approach

1. Rewrite `app/utils/log.py` following the newave implementation at `/home/rogerio/git/sintetizador-newave/app/utils/log.py`. Key differences from newave:
   - The newave `configure_process_logger` takes `(q, variable, member)` -- dessem does not have the `member` concept (DESSEM is deterministic, no scenario members). Simplify the signature to `(q, variable)` and name the logger `"worker-{variable}"`.
   - Keep `from __future__ import annotations` for cleaner type hints.
   - Use `multiprocessing.queues.Queue as MPQueue` for type hints on the queue parameter, matching newave.
2. Edit `main.py` to remove the `Log.configure_logging(BASEDIR)` call and the `from app.utils.log import Log` import. The file should only set env vars and call `app()`.
3. Do NOT modify `app/app.py` in this ticket -- the CLI command changes happen in ticket-024. The commands will temporarily lose their logging until ticket-024 wires the queue into each command. This is acceptable because tickets 023-025 are deployed together.

### Key Files to Modify

- `app/utils/log.py` (rewrite)
- `main.py` (remove `Log.configure_logging` call and import)

### Patterns to Follow

- Match the newave `Log` class structure exactly (same method names, same `Singleton` metaclass, same `listener: Optional[Process]` class variable).
- Use `from __future__ import annotations` at the top.
- Use `sys.stdout` explicitly in the `StreamHandler` constructor.
- Root logger level `DEBUG` in `configure_queue_logger`; named logger level `INFO` in `configure_main_logger` and `configure_process_logger`.

### Pitfalls to Avoid

- Do NOT change `app/utils/singleton.py` -- it is correct as-is and used by `Settings`.
- Do NOT use `multiprocessing.Queue` directly -- use `multiprocessing.Manager().Queue(-1)` (the managed variant). The `Log` class itself does not create the queue; it receives it as a parameter. The queue creation happens in `app.py` (ticket-024).
- Do NOT remove the `from app.utils.log import Log` import from `app/app.py` in this ticket -- it is still used there and will be updated in ticket-024.

## Testing Requirements

### Unit Tests

No new unit tests required. The logging infrastructure is integration-level and will be tested end-to-end in ticket-025. The existing 82 tests must continue to pass.

### Integration Tests

None in this ticket. Integration testing of the full queue-based logging happens after ticket-024 wires the queue through the CLI.

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-022-migrate-operation-synthesis-to-polars.md
- **Blocks**: ticket-024-add-processadores-cli-option.md

## Effort Estimate

**Points**: 1
**Confidence**: High
