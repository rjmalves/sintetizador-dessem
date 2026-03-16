# ticket-018 Create Operation Orchestrator and Pipeline

## Context

### Background

After tickets 016 and 017 extracted cache, export, resolution, and pipeline logic from `_monolith.py`, the remaining methods are the orchestration logic: the main synthesis loop (`synthetize`), single-variable synthesis (`_synthetize_single_variable`), variable preprocessing (`_preprocess_synthesis_variables`, `_process_variable_arguments`, `_filter_valid_variables`, `_match_wildcards`, `_default_args`, `_add_synthesis_dependencies`), and class-level state declarations. This ticket moves these remaining methods into `orchestrator.py`, removes `_monolith.py`, and updates `__init__.py` to import from `orchestrator.py`.

### Relation to Epic

This is the final ticket in Epic 04. It completes the decomposition by replacing `_monolith.py` with `orchestrator.py`, resulting in the target package structure: `__init__.py`, `orchestrator.py`, `cache.py`, `export.py`, `resolution.py`, `pipeline.py`. After this ticket, `_monolith.py` no longer exists, and the operation package is fully decomposed.

### Current State

After tickets 016 and 017, `_monolith.py` contains:

- Class-level state: `logger`, `DEFAULT_OPERATION_SYNTHESIS_ARGS`, `SYNTHESIS_TO_CACHE`, `CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, `SYNTHESIS_STATS`
- Class methods that are one-line delegations to `_cache_mod`, `_export_mod`, `_pipeline_mod`, `_resolution_mod`
- Orchestration methods that still contain real logic:
  - `synthetize(cls, variables, uow)` -- main entry point
  - `_synthetize_single_variable(cls, s, uow)` -- single variable synthesis loop
  - `_preprocess_synthesis_variables(cls, variables, uow)` -- preprocessing
  - `_process_variable_arguments(cls, args)` -- argument parsing
  - `_filter_valid_variables(cls, variables, uow)` -- validation
  - `_match_wildcards(cls, variables)` -- wildcard matching
  - `_default_args(cls)` -- default arguments
  - `_add_synthesis_dependencies(cls, synthesis)` -- dependency resolution
  - `clear_cache(cls)` -- cache clearing
  - `_log(cls, msg, level)` -- logging

## Specification

### Requirements

1. Create `app/services/synthesis/operation/orchestrator.py` containing the `OperationSynthetizer` class with class-level state, orchestration methods, and thin delegations to cache/export/pipeline/resolution modules.
2. Delete `app/services/synthesis/operation/_monolith.py`.
3. Update `app/services/synthesis/operation/__init__.py` to import from `orchestrator` instead of `_monolith`.
4. Verify each module in the package is <= 300 lines.
5. All existing tests pass without modification.

### Inputs/Props

- `_monolith.py` after tickets 016 and 017 have extracted cache, export, resolution, and pipeline logic.

### Outputs/Behavior

- `orchestrator.py` contains the `OperationSynthetizer` class with:
  - Class-level state declarations
  - `synthetize`, `_synthetize_single_variable`, `_preprocess_synthesis_variables` (real logic)
  - `_log`, `clear_cache`, `_default_args`, `_match_wildcards`, `_process_variable_arguments`, `_filter_valid_variables`, `_add_synthesis_dependencies` (real logic)
  - One-line delegations for cache/export/pipeline/resolution methods
- `__init__.py` imports `OperationSynthetizer` from `orchestrator`.
- `_monolith.py` no longer exists.

### Error Handling

- All error handling from `_synthetize_single_variable` (try/except with print_exc and logging) and `_preprocess_synthesis_variables` (try/except returning empty list) is preserved in `orchestrator.py`.

## Acceptance Criteria

- [ ] Given all prior tickets are completed, when `app/services/synthesis/operation/orchestrator.py` is created, then it contains the `OperationSynthetizer` class with methods `synthetize`, `_synthetize_single_variable`, `_preprocess_synthesis_variables`, `clear_cache`, `_log`.
- [ ] Given `orchestrator.py` exists, when `app/services/synthesis/operation/_monolith.py` is checked, then the file does not exist.
- [ ] Given `__init__.py` is updated, when inspecting its content, then it reads `from app.services.synthesis.operation.orchestrator import OperationSynthetizer`.
- [ ] Given the package is complete, when running `wc -l` on each module, then every `.py` file in `app/services/synthesis/operation/` has <= 300 lines.
- [ ] Given all changes are applied, when running `python -m pytest tests/app/services/synthesis/test_operation.py -x`, then all tests pass.

## Implementation Guide

### Suggested Approach

1. **Rename `_monolith.py` to `orchestrator.py`**: Use `git mv` to preserve history:

   ```bash
   git mv app/services/synthesis/operation/_monolith.py app/services/synthesis/operation/orchestrator.py
   ```

2. **Clean up `orchestrator.py`**: At this point the file already has all delegations in place from tickets 016-017. Review it to ensure:
   - All imports reference the correct submodules (`_cache_mod`, `_export_mod`, `_pipeline_mod`, `_resolution_mod`).
   - No dead imports remain (imports that were only needed by methods now living in other modules).
   - Remove any methods that are pure delegations and are never called from outside the class (private methods that only other extracted methods called). These can be removed since the extracted modules call each other directly.

3. **Update `__init__.py`**:

   ```python
   from app.services.synthesis.operation.orchestrator import OperationSynthetizer

   __all__ = ["OperationSynthetizer"]
   ```

4. **Verify line counts**: Run `wc -l app/services/synthesis/operation/*.py` and confirm each file <= 300 lines. If `orchestrator.py` exceeds 300 lines, identify delegation-only methods that can be removed (the submodules call each other directly, so delegation methods that are only called internally can be dropped).

5. **Update `TYPE_CHECKING` imports**: In `cache.py`, `export.py`, and `pipeline.py`, update the `TYPE_CHECKING` import from `_monolith` to `orchestrator`:

   ```python
   if TYPE_CHECKING:
       from app.services.synthesis.operation.orchestrator import OperationSynthetizer
   ```

6. **Run tests** to confirm all pass.

### Key Files to Modify

- `app/services/synthesis/operation/_monolith.py` -- rename to `orchestrator.py`
- `app/services/synthesis/operation/orchestrator.py` -- clean up (remove dead code/imports)
- `app/services/synthesis/operation/__init__.py` -- update import path
- `app/services/synthesis/operation/cache.py` -- update TYPE_CHECKING import
- `app/services/synthesis/operation/export.py` -- update TYPE_CHECKING import
- `app/services/synthesis/operation/pipeline.py` -- update TYPE_CHECKING import (if used)

### Patterns to Follow

- Follow sintetizador-newave's `operation/orchestrator.py`: the class retains state and orchestration logic, delegates everything else to submodules.
- Use `git mv` to preserve file history.
- Keep the `OperationSynthetizer` class name unchanged for backward compatibility.

### Pitfalls to Avoid

- Do NOT remove delegation methods that are called from test code or external callers. Only remove internal-only delegations.
- Do NOT forget to update `TYPE_CHECKING` imports in all submodules from `_monolith` to `orchestrator` -- failing to do this causes import errors when type checking.
- Do NOT modify the class's public API (`synthetize`, `clear_cache`) -- these are called by `handlers.py` and tests.
- Do NOT leave `_monolith.py` in place alongside `orchestrator.py` -- the file must be removed/renamed.

## Testing Requirements

### Unit Tests

- No new tests needed. This is a pure structural refactor.

### Integration Tests

- Run `python -m pytest tests/app/services/synthesis/test_operation.py -x` to confirm all ~30 operation synthesis tests pass.
- Run `python -m pytest tests/app/services/test_handlers.py -x` to confirm handler imports work.
- Run `python -m pytest tests/ -x` to confirm no other tests are broken.

### E2E Tests

- Not applicable.

## Dependencies

- **Blocked By**: ticket-017-extract-resolution-modules.md
- **Blocks**: ticket-019-add-polars-dependency.md (Epic 5)

## Effort Estimate

**Points**: 2
**Confidence**: High
