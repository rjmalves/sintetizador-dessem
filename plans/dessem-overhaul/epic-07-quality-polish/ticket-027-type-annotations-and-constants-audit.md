# ticket-027 Audit Type Annotations and Constants

## Context

### Background

After ticket-026 achieves zero mypy errors, the codebase is type-safe but still contains legacy patterns from the pandas era: `typing.List`, `typing.Dict`, `typing.Optional` imports; `PANDAS_GROUPING_ENGINE` and `STRING_DF_TYPE` constants that are no longer used after the Polars migration; and bare `dict`/`list` return types in several Deck facade methods. This ticket modernizes the annotation style and cleans up dead constants.

### Relation to Epic

Second ticket in epic-07. Builds on the mypy-clean baseline from ticket-026 to ensure all annotations follow modern Python 3.11+ conventions and that `app/internal/constants.py` has no dead code from the pandas era.

### Current State

After ticket-026, all files type-check cleanly. However:

- Many files still use `from typing import List, Dict, Optional, Tuple, Callable, Type` instead of built-in generics and `X | None` union syntax
- `app/internal/constants.py` lines 130-138 contain `PANDAS_GROUPING_ENGINE` and `STRING_DF_TYPE` which depend on pandas and numba -- these are unused after the Polars migration in epic-05
- `app/domain/commands.py` uses `typing.List` instead of `list`
- Several Deck facade methods in `app/services/deck/deck.py` return `pd.DataFrame` when the underlying modules now return `pl.DataFrame`
- `app/services/synthesis/execution.py` and `app/services/synthesis/system.py` still use `typing.List`, `typing.Optional`, `typing.Callable`

## Specification

### Requirements

1. Replace all `typing.List` with `list`, `typing.Dict` with `dict`, `typing.Optional[X]` with `X | None`, `typing.Tuple` with `tuple`, `typing.Type` with `type` across all files in `app/`
2. Remove unused `from typing import ...` entries after migration (keep only `Any`, `TypeVar`, `TYPE_CHECKING`, `Callable` if still needed for complex signatures, `cast`)
3. Remove `PANDAS_GROUPING_ENGINE` and `STRING_DF_TYPE` from `app/internal/constants.py`, along with the `import pandas` and `find_spec` at the bottom of that file
4. Verify no remaining code references `PANDAS_GROUPING_ENGINE` or `STRING_DF_TYPE`
5. Update return type annotations in `app/services/deck/deck.py` facade methods: methods that delegate to submodules returning `pl.DataFrame` should declare `-> pl.DataFrame` (not `-> pd.DataFrame`)
6. Ensure `uv run mypy ./app` still passes with 0 errors after all changes
7. Ensure `uv run ruff check ./app` passes

### Out of Scope

- Refactoring constants into dataclasses or NamedTuples (the current module-level string constants pattern works well and matches newave)
- Adding new type annotations to functions that already have complete annotations
- Changing runtime behavior
- Documentation updates (ticket-028)

### Error Handling

No runtime behavior changes. Pure refactoring of annotations and removal of dead code.

## Acceptance Criteria

- [ ] Given any Python file in `app/`, when searching for `from typing import List` or `from typing import Dict` or `from typing import Optional`, then zero matches are found (verified by `grep -r "from typing import.*\b(List|Dict|Optional|Tuple|Type)\b" app/` returning empty)
- [ ] Given `app/internal/constants.py`, when inspected, then it contains no reference to `PANDAS_GROUPING_ENGINE`, `STRING_DF_TYPE`, `find_spec`, or `import pandas`
- [ ] Given a codebase-wide search for `PANDAS_GROUPING_ENGINE` or `STRING_DF_TYPE`, when run via `grep -r`, then zero matches are found in `app/` (only plan files may reference them)
- [ ] Given `uv run mypy ./app`, when executed, then it exits with code 0
- [ ] Given `uv run ruff check ./app`, when executed, then it exits with code 0

## Implementation Guide

### Suggested Approach

**Step 1: Remove dead constants from `app/internal/constants.py`**
Remove lines 130-138 (the `import pandas`, `find_spec`, `PANDAS_GROUPING_ENGINE`, `STRING_DF_TYPE` block). Also remove the `from importlib.util import find_spec` import at line 1. Search all files for references to ensure nothing still uses them.

**Step 2: Modernize typing imports across all files**
For each file in `app/`:

1. Replace `List[X]` with `list[X]`
2. Replace `Dict[K, V]` with `dict[K, V]`
3. Replace `Optional[X]` with `X | None`
4. Replace `Tuple[X, ...]` with `tuple[X, ...]`
5. Replace `Type[X]` with `type[X]`
6. Remove the now-unused imports from `typing`

Key files requiring changes:

- `app/domain/commands.py`: `List[str]` -> `list[str]`
- `app/services/synthesis/execution.py`: `List`, `Optional`, `Callable`
- `app/services/synthesis/system.py`: `Optional`, `Callable`
- `app/services/deck/accessors.py`: `Dict`, `Optional`, `Type`, `TypeVar`
- `app/services/deck/temporal.py`: `Dict`
- `app/services/deck/thermal.py`: `Dict`, `List`, `Optional`
- `app/services/deck/hydro.py`: `Dict`
- `app/services/deck/bounds.py`: `Callable`, `Dict`, `Optional`, `TypeVar`
- `app/services/deck/deck.py`: `Dict`, `Optional`
- `app/services/unitofwork.py`: `Any`, `Dict`, `Optional`, `Type`
- `app/adapters/repository/export.py`: `Type`
- `app/adapters/repository/files.py`: `Type`, `TypeVar`
- `app/utils/regex.py`: `List`
- `app/utils/timing.py`: `Optional`
- `app/services/synthesis/operation/orchestrator.py`: `List`, `TypeVar`
- `docs/source/conf.py`: `List` (not under `app/` but should be cleaned too)

**Step 3: Update Deck facade return types**
In `app/services/deck/deck.py`, update methods that now return `pl.DataFrame` from their submodules but are annotated as `pd.DataFrame`:

- `stages_durations`, `blocks_durations` -> `pl.DataFrame`
- `pdo_eco_usih`, `pdo_hidr`, `pdo_oper_tviag_calha` -> `pl.DataFrame`
- `pdo_sist`, `pdo_eolica`, `pdo_inter` -> `pl.DataFrame`
- `pdo_oper_uct`, `pdo_oper_term` -> `pl.DataFrame`
- And all the `pdo_hidr_*`, `pdo_sist_*`, `pdo_eolica_*`, `pdo_inter_*`, `pdo_oper_term_ute` methods
- `hydro_generation_bounds`, `stored_volume_bounds`, `thermal_generation_bounds` -> `pl.DataFrame`
- `eer_submarket_map`, `hydro_eer_map`, `hydro_eer_submarket_map`, `hydro_initial_volumes`, `thermals`, `submarkets` -> `pl.DataFrame`
- `thermal_costs` -> `pl.DataFrame`
- `block_map` -> `dict[str, int]`
- `stage_block_map` -> `dict[int, int]`

**Step 4: Run validation**

- `uv run mypy ./app` -- zero errors
- `uv run ruff check ./app` -- zero errors
- `uv run pytest` -- all tests pass

### Key Files to Modify

1. `app/internal/constants.py` -- remove dead constants
2. `app/domain/commands.py` -- modernize imports
3. `app/services/deck/deck.py` -- fix return types + modernize imports
4. `app/services/deck/accessors.py` -- modernize imports
5. `app/services/deck/temporal.py` -- modernize imports
6. `app/services/deck/thermal.py` -- modernize imports
7. `app/services/deck/hydro.py` -- modernize imports
8. `app/services/deck/bounds.py` -- modernize imports
9. `app/services/synthesis/execution.py` -- modernize imports
10. `app/services/synthesis/system.py` -- modernize imports
11. `app/services/synthesis/operation/orchestrator.py` -- modernize imports
12. `app/services/unitofwork.py` -- modernize imports
13. `app/adapters/repository/export.py` -- modernize imports
14. `app/adapters/repository/files.py` -- modernize imports
15. `app/utils/regex.py` -- modernize imports
16. `app/utils/timing.py` -- modernize imports

### Patterns to Follow

- Use built-in generics: `list[str]`, `dict[str, int]`, `tuple[str, ...]`, `type[Foo]`
- Use union syntax: `X | None` instead of `Optional[X]`
- Keep `Any` and `TypeVar` from typing (no built-in equivalent)
- Keep `Callable` from typing (still needed for complex signatures in Python 3.11)
- Keep `TYPE_CHECKING` from typing where used
- Keep `cast` from typing where used

### Pitfalls to Avoid

- Do NOT remove `from typing import Any` or `from typing import TypeVar` -- these have no built-in equivalents
- Do NOT remove `from typing import TYPE_CHECKING` -- it's needed for circular import guards
- Verify `PANDAS_GROUPING_ENGINE` and `STRING_DF_TYPE` are truly unused before removing (search in tests too)
- The `docs/source/conf.py` file uses `List[str]` but is not checked by mypy; still clean it up for consistency
- When changing Deck facade return types from `pd.DataFrame` to `pl.DataFrame`, verify the callers (system.py, execution.py) that call `.to_pandas()` on the result still work

## Testing Requirements

### Unit Tests

- All 82 existing tests must continue to pass unchanged

### Integration Tests

- `uv run mypy ./app` exits with code 0
- `uv run ruff check ./app` exits with code 0

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-026-enable-mypy-strict-full.md
- **Blocks**: ticket-028-documentation-and-final-polish.md

## Effort Estimate

**Points**: 2
**Confidence**: High
