# ticket-015 Create Operation Synthesis Package Structure

## Context

### Background

Epic 03 successfully decomposed `app/services/deck/deck.py` (1884 lines) into a package with 7 focused submodules using the sintetizador-newave pattern. The same decomposition pattern must now be applied to `app/services/synthesis/operation.py` (871 lines). This ticket creates the package structure and moves the existing monolithic code into it as a temporary `_monolith.py` module, establishing the foundation for subsequent extraction tickets.

### Relation to Epic

This is the first ticket in Epic 04 (Operation Decomposition). It creates the `operation/` package directory and `__init__.py` that re-exports `OperationSynthetizer`, ensuring that all existing callers (notably `app/services/handlers.py` and `tests/app/services/synthesis/test_operation.py`) continue to work without modification. Subsequent tickets (016-018) will extract submodules from the temporary `_monolith.py`.

### Current State

- `app/services/synthesis/operation.py` is a single 871-line file containing the `OperationSynthetizer` class with all cache, export, resolution, pipeline, and orchestration logic.
- `app/services/handlers.py` imports `from app.services.synthesis.operation import OperationSynthetizer` (line 7).
- `tests/app/services/synthesis/test_operation.py` imports `from app.services.synthesis.operation import OperationSynthetizer` (line 24).
- No `app/services/synthesis/operation/` directory exists.

## Specification

### Requirements

1. Create directory `app/services/synthesis/operation/`.
2. Move `app/services/synthesis/operation.py` to `app/services/synthesis/operation/_monolith.py` (preserving all 871 lines exactly).
3. Create `app/services/synthesis/operation/__init__.py` that re-exports `OperationSynthetizer` from `_monolith.py`.
4. All existing imports remain valid without changes to any caller.

### Inputs/Props

- The existing `app/services/synthesis/operation.py` file (871 lines).

### Outputs/Behavior

- `app/services/synthesis/operation/` is a Python package.
- `from app.services.synthesis.operation import OperationSynthetizer` resolves to the same class.
- All existing tests pass with zero changes to test code.

### Error Handling

- If `operation.py` cannot be moved (e.g., directory creation fails), the operation should be aborted and the original file preserved. No partial state should be left.

## Acceptance Criteria

- [ ] Given `app/services/synthesis/operation.py` exists, when the ticket is implemented, then `app/services/synthesis/operation/` is a directory containing `__init__.py` and `_monolith.py`.
- [ ] Given the package is created, when running `python -c "from app.services.synthesis.operation import OperationSynthetizer; print(OperationSynthetizer)"`, then the class is imported without errors.
- [ ] Given `app/services/handlers.py` line 7 reads `from app.services.synthesis.operation import OperationSynthetizer`, when the package exists, then no import changes are needed in handlers.py.
- [ ] Given the package is created, when running `python -m pytest tests/app/services/synthesis/test_operation.py -x`, then all tests pass.
- [ ] Given `_monolith.py` is created, when comparing its content to the original `operation.py`, then the file content is byte-identical (only the filename changed).

## Implementation Guide

### Suggested Approach

1. Create the directory `app/services/synthesis/operation/`.
2. Move (git mv) `app/services/synthesis/operation.py` to `app/services/synthesis/operation/_monolith.py`.
3. Create `app/services/synthesis/operation/__init__.py` with content:

   ```python
   from app.services.synthesis.operation._monolith import OperationSynthetizer

   __all__ = ["OperationSynthetizer"]
   ```

4. Run the full operation test suite to confirm nothing broke.

### Key Files to Modify

- `app/services/synthesis/operation.py` -- rename/move to `app/services/synthesis/operation/_monolith.py`
- `app/services/synthesis/operation/__init__.py` -- create (new file)

### Patterns to Follow

- Follow the same pattern used in Epic 03 when `app/services/deck/deck.py` was converted to a package. The `__init__.py` re-exports the public API.
- Use `git mv` instead of manual copy+delete to preserve git history.

### Pitfalls to Avoid

- Do NOT modify the content of `_monolith.py` -- it must remain identical to the original `operation.py`. Content extraction happens in tickets 016-018.
- Do NOT change any import statements in `handlers.py` or test files. The `__init__.py` re-export ensures backward compatibility.
- Do NOT create empty submodule files yet (cache.py, export.py, etc.). Those are created in subsequent tickets.

## Testing Requirements

### Unit Tests

- No new tests needed. Existing tests in `tests/app/services/synthesis/test_operation.py` validate that the import path and class behavior are unchanged.

### Integration Tests

- Run `python -m pytest tests/app/services/synthesis/test_operation.py -x` to confirm all ~30 operation synthesis tests pass.
- Run `python -m pytest tests/app/services/test_handlers.py -x` to confirm handler imports work.

### E2E Tests

- Not applicable.

## Dependencies

- **Blocked By**: ticket-014-reduce-deck-to-facade.md
- **Blocks**: ticket-016-extract-operation-cache-and-export.md

## Effort Estimate

**Points**: 1
**Confidence**: High
