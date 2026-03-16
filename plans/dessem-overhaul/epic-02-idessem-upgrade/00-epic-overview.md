# Epic 02: idessem Upgrade & API Adaptation

## Goal

Upgrade the idessem dependency from >= 1.0.0 to >= 1.1.0, which pulls in cfinterface >= 1.9.0 (breaking change from <= 1.8.3), pandas >= 3.0.0, and Python >= 3.11. Adapt all call sites to the new API, add mypy overrides for idessem/cfinterface type stubs, and verify all existing tests pass.

## Scope

- Update `pyproject.toml` to require `idessem >= 1.1.0` and `pandas >= 3.0.0`
- Add mypy overrides for idessem and cfinterface modules (`.read()` classmethods now return base types)
- Add mypy override for `app.adapters.repository.files` to suppress false-positive assignment/return-value errors
- Verify all file reading in `files.py` works with new idessem API
- Run full test suite to confirm no regressions

## Out of Scope

- Refactoring files.py structure (Epic 3)
- Adding Polars (Epic 5)
- Strict mypy beyond overrides (Epic 7)

## Tickets

1. ticket-005-upgrade-idessem-dependency.md
2. ticket-006-add-mypy-overrides-for-idessem.md
3. ticket-007-verify-files-repository-compatibility.md

## Success Criteria

- `uv sync` resolves with idessem >= 1.1.0 and cfinterface >= 1.9.0
- `uv run mypy ./app` passes with the new overrides
- `uv run pytest ./tests` passes with zero failures
- All `.read()` call sites in files.py work correctly at runtime
