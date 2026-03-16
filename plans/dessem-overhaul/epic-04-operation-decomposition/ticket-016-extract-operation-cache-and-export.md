# ticket-016 Extract Operation Cache and Export Modules

## Context

### Background

After ticket-015 moved the monolithic `operation.py` into `operation/_monolith.py`, this ticket extracts the cache and export logic into dedicated submodules. This follows the sintetizador-newave pattern where `cache.py` contains functions for reading/writing the synthesis cache and `export.py` contains functions for metadata export, statistics accumulation, and scenario data export. The functions become module-level functions that accept `cls` (the `OperationSynthetizer` class) as the first parameter, using `TYPE_CHECKING` for the type hint to avoid circular imports.

### Relation to Epic

This is the second ticket in Epic 04. It performs the first extraction from `_monolith.py`, creating `cache.py` and `export.py`. After this ticket, the orchestrator class in `_monolith.py` will delegate cache and export operations to these new modules via thin one-line class methods.

### Current State

- `app/services/synthesis/operation/_monolith.py` contains the full `OperationSynthetizer` class (871 lines).
- Cache-related methods: `clear_cache` (lines 54-60), `_get_from_cache` (lines 529-543), `__get_from_cache_if_exists` (lines 576-584), `__store_in_cache_if_needed` (lines 587-599).
- Cache-related class attributes: `SYNTHESIS_TO_CACHE`, `CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, `SYNTHESIS_STATS` (lines 42-51).
- Export-related methods: `_export_metadata` (lines 677-713), `_add_synthesis_stats` (lines 716-726), `_export_scenario_synthesis` (lines 729-754), `_export_stats` (lines 757-777).
- Statistics accumulation (`SYNTHESIS_STATS` dict, `_add_synthesis_stats`) is closely related to export since stats are only consumed by `_export_stats`. It belongs in `export.py`.

## Specification

### Requirements

1. Create `app/services/synthesis/operation/cache.py` with module-level functions extracted from the cache methods of `OperationSynthetizer`.
2. Create `app/services/synthesis/operation/export.py` with module-level functions extracted from the export methods of `OperationSynthetizer`.
3. Replace the extracted method bodies in `_monolith.py` with one-line delegations to the new module functions.
4. Class-level state attributes (`CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, `SYNTHESIS_STATS`, `SYNTHESIS_TO_CACHE`) remain on the `OperationSynthetizer` class -- the new module functions receive `cls` as a parameter to access them.
5. All existing tests pass without modification.

### Inputs/Props

- `_monolith.py` containing the `OperationSynthetizer` class with cache and export methods.

### Outputs/Behavior

- `cache.py` exposes: `get_from_cache(cls, s)`, `get_from_cache_if_exists(cls, s)`, `store_in_cache_if_needed(cls, s, df)`.
- `export.py` exposes: `export_metadata(cls, success_synthesis, uow)`, `add_synthesis_stats(cls, s, df)`, `export_scenario_synthesis(cls, s, df, uow)`, `export_stats(cls, uow)`.
- `_monolith.py` methods become thin delegations: e.g., `cls._get_from_cache(s)` calls `cache.get_from_cache(cls, s)`.

### Error Handling

- All error handling logic (RuntimeError on cache miss, try/except in export) is preserved in the extracted functions. No error handling changes.

## Acceptance Criteria

- [ ] Given the package exists, when `app/services/synthesis/operation/cache.py` is created, then it contains three public functions: `get_from_cache`, `get_from_cache_if_exists`, `store_in_cache_if_needed`.
- [ ] Given the package exists, when `app/services/synthesis/operation/export.py` is created, then it contains four public functions: `export_metadata`, `add_synthesis_stats`, `export_scenario_synthesis`, `export_stats`.
- [ ] Given `cache.py` functions exist, when each function's first parameter is inspected, then it is typed as `type[OperationSynthetizer]` using a `TYPE_CHECKING` guard.
- [ ] Given `_monolith.py` is updated, when inspecting the `_get_from_cache` method body, then it is a single-line delegation: `return cache.get_from_cache(cls, s)`.
- [ ] Given all changes are applied, when running `python -m pytest tests/app/services/synthesis/test_operation.py -x`, then all tests pass.

## Implementation Guide

### Suggested Approach

1. **Create `cache.py`**: Extract the following methods as module-level functions, each taking `cls` as the first parameter:
   - `_get_from_cache` -> `get_from_cache(cls, s)`
   - `__get_from_cache_if_exists` -> `get_from_cache_if_exists(cls, s)`
   - `__store_in_cache_if_needed` -> `store_in_cache_if_needed(cls, s, df)`

   Use `TYPE_CHECKING` guard for the `OperationSynthetizer` type hint:

   ```python
   from typing import TYPE_CHECKING
   import pandas as pd
   from app.model.operation.operationsynthesis import OperationSynthesis
   from app.utils.timing import time_and_log

   if TYPE_CHECKING:
       from app.services.synthesis.operation._monolith import OperationSynthetizer
   ```

2. **Create `export.py`**: Extract the following methods as module-level functions:
   - `_export_metadata` -> `export_metadata(cls, success_synthesis, uow)`
   - `_add_synthesis_stats` -> `add_synthesis_stats(cls, s, df)`
   - `_export_scenario_synthesis` -> `export_scenario_synthesis(cls, s, df, uow)`
   - `_export_stats` -> `export_stats(cls, uow)`

   Note: `export_scenario_synthesis` calls `store_in_cache_if_needed` and `add_synthesis_stats` internally. Import `store_in_cache_if_needed` from `cache.py` at function level (inside the function body) to avoid circular imports, following newave's pattern.

3. **Update `_monolith.py`**: Add imports at the top:

   ```python
   from app.services.synthesis.operation import cache as _cache_mod
   from app.services.synthesis.operation import export as _export_mod
   ```

   Replace each extracted method body with a one-line delegation. Example:

   ```python
   @classmethod
   def _get_from_cache(cls, s: OperationSynthesis) -> pd.DataFrame:
       return _cache_mod.get_from_cache(cls, s)
   ```

   The `clear_cache` method stays in `_monolith.py` since it is a simple 3-line method that directly clears class-level dicts.

4. **Run tests** to confirm all pass.

### Key Files to Modify

- `app/services/synthesis/operation/cache.py` -- create (new file)
- `app/services/synthesis/operation/export.py` -- create (new file)
- `app/services/synthesis/operation/_monolith.py` -- replace method bodies with delegations

### Patterns to Follow

- Follow sintetizador-newave's `operation/cache.py` and `operation/export.py` exactly: module-level functions with `cls: "type[OperationSynthetizer]"` as the first parameter.
- Use `TYPE_CHECKING` guard to avoid circular imports, with the string-form type annotation `"type[OperationSynthetizer]"`.
- Keep `pd.DataFrame` (not polars) -- Polars migration is Epic 5.

### Pitfalls to Avoid

- Do NOT move class-level state attributes (`CACHED_SYNTHESIS`, etc.) out of the class. They must remain on `OperationSynthetizer` for backward compatibility.
- Do NOT change the `clear_cache` method -- it is simple enough to stay in the class.
- Do NOT import `_monolith` at module level in `cache.py` or `export.py` -- use `TYPE_CHECKING` guard to prevent circular imports.
- Do NOT modify test files -- the existing tests must pass as-is.

## Testing Requirements

### Unit Tests

- No new tests needed. The extraction is a pure refactor; existing tests validate correctness.

### Integration Tests

- Run `python -m pytest tests/app/services/synthesis/test_operation.py -x` to confirm all ~30 operation synthesis tests pass after extraction.

### E2E Tests

- Not applicable.

## Dependencies

- **Blocked By**: ticket-015-create-operation-synthesis-package.md
- **Blocks**: ticket-017-extract-resolution-modules.md

## Effort Estimate

**Points**: 2
**Confidence**: High
