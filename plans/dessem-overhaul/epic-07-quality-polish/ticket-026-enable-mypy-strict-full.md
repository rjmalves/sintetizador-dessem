# ticket-026 Enable Full mypy Strict Mode Compliance

## Context

### Background

Epics 01-06 established `mypy --strict` in `pyproject.toml` and added overrides for third-party libraries (idessem, cfinterface, dateutil) and for `app.adapters.repository.files` (which suffers from idessem's return-type limitations). However, across 6 epics of rapid development, 114 mypy errors have accumulated in 27 files. These must be resolved to zero before the codebase can be considered release-quality.

### Relation to Epic

This is the first ticket in epic-07 (Quality & Polish). It establishes a clean mypy baseline that ticket-027 (constants/annotation audit) and ticket-028 (documentation) build upon. No new features or refactors -- purely adding type annotations and fixing type errors.

### Current State

- `uv run mypy ./app` reports 114 errors across 27 files (55 files checked total)
- `pyproject.toml` has `strict = true` with overrides for `idessem.*`, `cfinterface.*`, `dateutil.*`, and `app.adapters.repository.files`
- Error categories:
  - 17 stale `# type: ignore` comments (`unused-ignore`)
  - 33 missing type annotations (`no-untyped-def`: return types, parameter types)
  - 32 bare generic types (`type-arg`: `list`, `dict`, `Callable` without parameters)
  - 17 returning Any from typed functions (`no-any-return`)
  - 7 calls to untyped `Settings()` (`no-untyped-call`)
  - 4 argument type mismatches (`arg-type`)
  - 2 assignment type mismatches in `app/app.py` (`assignment`)
  - 1 call to unknown function type in `app/utils/operations.py` (`operator`)
  - 1 missing attribute `date_arrays` on `_temporal` (`attr-defined`)

## Specification

### Requirements

1. Fix all 114 mypy errors so `uv run mypy ./app` exits with 0 errors
2. Add return type annotations to all functions currently missing them
3. Add parameter type annotations to all functions currently missing them
4. Replace bare `list`, `dict`, `Callable` with parameterized versions (`list[str]`, `dict[str, Any]`, `Callable[[...], ...]`)
5. Remove all stale `# type: ignore` comments that are no longer needed
6. Fix the `Settings.__init__` method to have a proper return type (`-> None`)
7. Fix `app/app.py` variable reuse bug where `command` is reassigned with incompatible types
8. Fix or remove the `Deck.date_arrays` method that references a non-existent `_temporal.date_arrays`
9. Add proper `cast()` or explicit return statements to fix `no-any-return` errors
10. Keep the existing `# type: ignore` comments on `import pandas as pd` lines (pandas stubs are not installed and `ignore_missing_imports = true` already covers this, so these comments are now unused -- remove them)

### Out of Scope

- Refactoring constants into dataclasses or NamedTuples (ticket-027)
- Removing the `PANDAS_GROUPING_ENGINE` / `STRING_DF_TYPE` constants (ticket-027)
- Documentation changes (ticket-028)
- Adding new mypy overrides for additional modules
- Changing any runtime behavior -- all changes are annotation-only or stale-comment removal

### Error Handling

No runtime behavior changes. All fixes are type-annotation additions, stale comment removals, and minor variable scoping fixes.

## Acceptance Criteria

- [ ] Given the current codebase, when running `uv run mypy ./app`, then the exit code is 0 and stdout contains "Success: no issues found"
- [ ] Given the file `app/utils/singleton.py`, when inspected, then `_instances` has type `dict[type, Any]` and `__call__` has full parameter and return type annotations
- [ ] Given the file `app/model/settings.py`, when inspected, then `Settings.__init__` has return type `-> None`
- [ ] Given the file `app/app.py`, when inspected, then the `completa` function uses separate variable names for each command type (no type-incompatible reassignment)
- [ ] Given the full test suite, when running `uv run pytest`, then all 82 tests pass

## Implementation Guide

### Suggested Approach

Work through the errors file-by-file in dependency order (utils first, then model, then adapters, then services, then app.py):

**Phase A -- Remove stale `# type: ignore` comments (17 errors):**
Files: `app/utils/tz.py`, `app/internal/constants.py`, `app/adapters/repository/export.py`, `app/services/deck/accessors.py`, `app/services/deck/temporal.py`, `app/services/deck/thermal.py`, `app/services/deck/system.py`, `app/services/deck/entities.py`, `app/services/deck/hydro.py`. Remove the `# type: ignore` from pandas/numpy imports since `ignore_missing_imports = true` in pyproject.toml makes them unnecessary.

**Phase B -- Add missing type annotations (33 errors):**

- `app/utils/singleton.py`: `_instances: dict[type, Any] = {}`, `def __call__(cls, *args: Any, **kwargs: Any) -> Any:`
- `app/utils/regex.py`: Add `-> list[str]` return type
- `app/utils/fs.py`: Add `-> None` to `__enter__`, add `-> None` and typed params to `__exit__`
- `app/utils/timing.py`: Add `-> "time_and_log"` to `__enter__`, add typed params and `-> None` to `__exit__`
- `app/utils/encoding.py`: Add `-> None` return type
- `app/model/settings.py`: Add `-> None` to `__init__`
- `app/model/operation/spatialresolution.py`: Add `-> str` to `__repr__`
- `app/adapters/repository/export.py`: Add `-> bool` return types to `synthetize_df` in ABC and CSV, add `-> AbstractExportRepository` to `factory`
- `app/adapters/repository/files.py`: Add typed params to `_validate_data` and `factory`
- `app/services/handlers.py`: Add return types (`-> None`) to all handler functions
- `app/services/synthesis/operation/orchestrator.py`: Add `-> None` return types to `clear_cache`, `_log`, `synthetize`
- `app/app.py`: Add `-> None` to `app()` function

**Phase C -- Add type parameters to generics (32 errors):**

- `app/utils/operations.py`: `list[str]` for `grouping_columns` and `extract_columns`
- `app/services/deck/temporal.py`: `dict[str, int]` for return types of `block_map` and `stage_block_map`, `list[str]` for `blocks`
- `app/services/synthesis/operation/pipeline.py`: Add full type parameters to all `Callable` and `list` annotations
- `app/services/synthesis/operation/orchestrator.py`: `list[str]` for `_default_args` return, `list[OperationSynthesis]` for class attributes

**Phase D -- Fix `no-any-return` errors (17 errors):**
For functions returning `pd.DataFrame` or `pl.DataFrame` from idessem/pandas calls that mypy sees as `Any`, use `cast()` from typing:

- `app/services/deck/hydro.py`: Wrap returns in `cast(pl.DataFrame, ...)`
- `app/services/deck/thermal.py`: Same pattern
- `app/services/deck/bounds.py`: Same pattern
- `app/services/synthesis/operation/pipeline.py`: Same pattern
- `app/adapters/repository/export.py`: Wrap `synthetize_df` returns

**Phase E -- Fix `no-untyped-call` to Settings (7 errors):**
Once `Settings.__init__` gets `-> None`, the `no-untyped-call` errors for `Settings()` will resolve automatically.

**Phase F -- Fix structural type errors (4 errors):**

- `app/app.py` lines 170-173: Use separate variable names (`sys_cmd`, `op_cmd`, `exec_cmd`) instead of reusing `command`
- `app/adapters/repository/files.py` line 165: Guard `Settings().installdir` with `assert ... is not None` or use a fallback
- `app/services/deck/deck.py`: Remove or stub `date_arrays` method (it references non-existent `_temporal.date_arrays`)

**Phase G -- Fix remaining errors:**

- `app/utils/operations.py` line 27: The `agg_fn` retrieved from `operation_map` has unknown type; add explicit annotation `agg_fn: Callable[[pl.Expr], pl.Expr]`

### Key Files to Modify

1. `app/utils/singleton.py`
2. `app/utils/regex.py`
3. `app/utils/fs.py`
4. `app/utils/timing.py`
5. `app/utils/encoding.py`
6. `app/utils/tz.py`
7. `app/utils/operations.py`
8. `app/model/settings.py`
9. `app/model/operation/spatialresolution.py`
10. `app/adapters/repository/export.py`
11. `app/adapters/repository/files.py`
12. `app/services/handlers.py`
13. `app/services/unitofwork.py`
14. `app/services/deck/accessors.py`
15. `app/services/deck/temporal.py`
16. `app/services/deck/thermal.py`
17. `app/services/deck/system.py`
18. `app/services/deck/entities.py`
19. `app/services/deck/hydro.py`
20. `app/services/deck/deck.py`
21. `app/services/deck/bounds.py`
22. `app/services/synthesis/operation/pipeline.py`
23. `app/services/synthesis/operation/orchestrator.py`
24. `app/services/synthesis/execution.py`
25. `app/services/synthesis/system.py`
26. `app/app.py`
27. `app/internal/constants.py`

### Patterns to Follow

- Use `from __future__ import annotations` only if needed for forward references; otherwise use string quotes for forward refs
- Use built-in generics (`list[str]`, `dict[str, int]`) rather than `typing.List`, `typing.Dict` (Python >= 3.11)
- Use `X | None` instead of `Optional[X]`
- Use `cast(TargetType, expr)` for values returned by untyped third-party code
- Keep `# type: ignore[error-code]` comments only where genuinely needed (e.g., idessem interactions already covered by overrides)

### Pitfalls to Avoid

- Do NOT add `# type: ignore` as a blanket fix -- each error must be resolved with proper annotations or casts
- Do NOT change the `app.adapters.repository.files` mypy overrides -- they exist because idessem returns base types
- Do NOT modify runtime behavior -- only add annotations, casts, and remove stale comments
- The `Deck.date_arrays` method references `_temporal.date_arrays` which does not exist; investigate whether it's dead code before removing
- `Settings().installdir` returns `str | None` from `getenv()` -- the `Path()` call needs a guard, not a type ignore

## Testing Requirements

### Unit Tests

- All 82 existing tests must continue to pass unchanged

### Integration Tests

- `uv run mypy ./app` must exit with code 0

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-025-integrate-process-pool-executor.md
- **Blocks**: ticket-027-type-annotations-and-constants-audit.md

## Effort Estimate

**Points**: 3
**Confidence**: High
