# ticket-024 Add --processadores CLI Option

## Context

### Background

With the multiprocessing-safe logger in place (ticket-023), each Click command must now be updated to: (a) create a managed queue, (b) start the logging listener process, (c) configure a main logger via QueueHandler, (d) pass the process count through to the synthesis layer, and (e) terminate the listener on exit. Additionally, a `--processadores` CLI option must be added to the `operacao` and `completa` commands (the only ones that will use ProcessPoolExecutor in ticket-025).

sintetizador-newave's `app/app.py` shows the established pattern: every command creates a `Manager().Queue(-1)`, calls `Log.start_logging_process(q)`, gets a logger from `Log.configure_main_logger(q)`, sets `os.environ["PROCESSADORES"]`, passes `q` to the `factory("FS", os.curdir, q)` UnitOfWork, and terminates the listener at the end. This ticket ports that pattern to dessem.

### Relation to Epic

This is the second ticket of Epic 06. It connects the logger (ticket-023) to the CLI and prepares the plumbing for ProcessPoolExecutor integration (ticket-025) by making the process count available through `Settings().processors` and passing the managed queue through the UnitOfWork.

### Current State

- `app/app.py`: Five Click commands (`sistema`, `operacao`, `execucao`, `limpeza`, `completa`). Each uses `Log.log()` for logging, creates a `factory("FS", os.curdir)` UnitOfWork (no queue parameter), and does not have a `--processadores` option.
- `app/domain/commands.py`: Four dataclasses (`SynthetizeSystem`, `SynthetizeExecution`, `SynthetizeScenario`, `SynthetizeOperation`), each with a `variables: List[str]` field.
- `app/services/unitofwork.py`: `FSUnitOfWork.__init__(self, directory)` takes only a directory. `factory(kind, *args, **kwargs)` passes through.
- `app/model/settings.py`: No `processors` attribute.
- `main.py` (after ticket-023): Sets env vars and calls `app()`, no logging initialization.

## Specification

### Requirements

1. **Add `processors` to `Settings`**: Add `self.processors: str | int = getenv("PROCESSADORES", 1)` to `app/model/settings.py`, matching newave's `Settings`.

2. **Add queue parameter to UnitOfWork**: Modify `AbstractUnitOfWork.__init__` and `FSUnitOfWork.__init__` to accept a `q: Any` parameter and store it as `self._queue`. Add a `queue` property. Modify `factory` to pass through. This matches newave's UnitOfWork pattern.

3. **Remove `chdir` from FSUnitOfWork**: The newave `FSUnitOfWork` does NOT `chdir` -- it stores the path and uses it directly. The dessem `FSUnitOfWork` currently calls `chdir(self._path)` in `__enter__` and `chdir(self._current_path)` in `__exit__`. This `chdir` pattern is incompatible with multiprocessing because `chdir` affects the entire process. Remove the `chdir` calls. Store `self._path = str(Path(directory).resolve())` as a string (matching newave). Remove `self._current_path`.

4. **Update `app/app.py`**: Rewrite each command following the newave pattern:
   - Create `m = Manager()`, `q: Any = m.Queue(-1)`
   - Call `Log.start_logging_process(q)`
   - Get `logger = Log.configure_main_logger(q)`
   - Use `logger.info(...)` instead of `Log.log().info(...)`
   - Set `os.environ["PROCESSADORES"] = str(processadores)` in `operacao` and `completa`
   - Pass `q` to `factory("FS", os.curdir, q)`
   - Add `time.sleep(1.0)` before `Log.terminate_logging_process()` to let the queue drain
   - Add `--processadores` option (default=1) to `operacao` and `completa` commands only

5. **Commands that do NOT get `--processadores`**: `sistema`, `execucao`, `limpeza`. These commands still get the queue-based logging pattern but without the processadores option (they always run single-process). For `limpeza`, no logging is needed (it just calls `handlers.clean()`), so it remains unchanged.

### Inputs/Props

- `--processadores`: integer CLI option, default `1`, help text `"numero de processadores para paralelizar"`

### Outputs/Behavior

- Running `sintetizador-dessem operacao --processadores 4 CMG_SIN` sets `os.environ["PROCESSADORES"] = "4"` and the synthesis layer can read `int(Settings().processors)` to get `4`.
- All log messages flow through the managed queue to the listener process.
- The `limpeza` command remains unchanged (no logging, no queue).

### Error Handling

- If `--processadores` receives a non-integer or value < 1, Click's built-in validation handles it (the option is typed as `int` with `default=1`).

## Acceptance Criteria

- [ ] Given `app/model/settings.py`, when inspected, then it contains `self.processors: str | int = getenv("PROCESSADORES", 1)`.
- [ ] Given `app/services/unitofwork.py`, when inspected, then `AbstractUnitOfWork.__init__` accepts a `q: Any` parameter, stores it as `self._queue`, and exposes a `queue` property; `FSUnitOfWork.__init__` accepts `(self, directory: str, q: Any)` and does NOT call `chdir`.
- [ ] Given the Click command `operacao` in `app/app.py`, when invoked with `--processadores 4`, then `os.environ["PROCESSADORES"]` is set to `"4"` and the command runs successfully.
- [ ] Given the Click command `completa` in `app/app.py`, when invoked with `--processadores 2`, then `os.environ["PROCESSADORES"]` is set to `"2"`.
- [ ] Given the Click commands `sistema` and `execucao` in `app/app.py`, when inspected, then they do NOT have a `--processadores` option but DO use the queue-based logging pattern (Manager, Queue, Log.start_logging_process, Log.configure_main_logger, Log.terminate_logging_process).
- [ ] Given the full test suite, when `python -m pytest tests/` is run, then all 82 existing tests pass.

## Implementation Guide

### Suggested Approach

1. **Edit `app/model/settings.py`**: Add the `processors` attribute. Add `from os import getenv` if not already present (it is already imported).

2. **Edit `app/services/unitofwork.py`**:
   - Add `from typing import Any` to imports.
   - Change `AbstractUnitOfWork.__init__(self)` to `AbstractUnitOfWork.__init__(self, q: Any)`. Store `self._queue = q`. Add `@property` for `queue`.
   - Change `FSUnitOfWork.__init__(self, directory: str)` to `FSUnitOfWork.__init__(self, directory: str, q: Any)`. Call `super().__init__(q)`. Store path as `self._path = str(Path(directory).resolve())`. Remove `self._current_path`. Change `__enter__` to not call `chdir`. Change `__exit__` to not call `chdir`. Set `self._files = None` and `self._exporter = None` in `__exit__` (matching newave).
   - In `__create_repository`, use `Path(self._path)` instead of `self._path` where needed.

3. **Edit `app/app.py`**:
   - Add imports: `import time`, `from multiprocessing import Manager`, `from typing import Any, Tuple`.
   - For each command except `limpeza`: create manager/queue, start listener, configure logger, use `factory("FS", os.curdir, q)`, sleep + terminate.
   - Add `@click.option("--processadores", default=1, help="numero de processadores para paralelizar")` to `operacao` and `completa`.
   - Set `os.environ["PROCESSADORES"] = str(processadores)` in `operacao` and `completa`.

4. **Edit `main.py`** (if not already done in ticket-023): Ensure it does NOT call `Log.configure_logging`.

### Key Files to Modify

- `app/model/settings.py` (add `processors` attribute)
- `app/services/unitofwork.py` (add queue parameter, remove chdir)
- `app/app.py` (rewrite commands with queue-based logging, add --processadores)

### Patterns to Follow

- Follow the newave `app/app.py` pattern exactly for the queue/logging lifecycle in each command.
- Follow the newave `app/services/unitofwork.py` for the queue parameter and no-chdir pattern.
- Use `Tuple[str, ...]` type hints for Click variadic arguments (matching newave).

### Pitfalls to Avoid

- Do NOT add `--processadores` to `sistema` or `execucao` -- only `operacao` and `completa` benefit from parallelism (DESSEM has no scenario synthesis like newave's `cenarios`).
- Do NOT forget `time.sleep(1.0)` before `Log.terminate_logging_process()` -- without it, queued messages may be lost.
- Do NOT modify the handler functions in `app/services/handlers.py` -- they receive `uow` and work unchanged.
- The `chdir` removal may affect tests that rely on the working directory being changed. Check test fixtures -- the existing tests use `AbstractTestOperationSynthetizer` which creates its own UnitOfWork with a test directory. These tests will need their `factory` calls updated to pass a queue (use `None` or a mock queue in tests).

## Testing Requirements

### Unit Tests

- Update all existing test files that call `factory("FS", ...)` or instantiate `FSUnitOfWork(...)` to pass an additional `q=None` parameter. The queue is only used by the logging system; passing `None` is safe for tests that do not exercise multiprocess logging.

### Integration Tests

None in this ticket.

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-023-add-multiprocessing-logger.md
- **Blocks**: ticket-025-integrate-process-pool-executor.md

## Effort Estimate

**Points**: 2
**Confidence**: High
