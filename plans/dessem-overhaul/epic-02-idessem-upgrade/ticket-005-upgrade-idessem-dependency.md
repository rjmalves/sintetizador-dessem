# ticket-005 Upgrade idessem Dependency to >= 1.1.0

## Context

### Background

The idessem library has been modernized on the `feat/infra-docs-overhaul` branch with cfinterface >= 1.9.0, Python >= 3.11, pandas >= 3.0.0, and strict type annotations. sintetizador-dessem currently pins `idessem >= 1.0.0` which uses cfinterface <= 1.8.3. The new cfinterface version changes how `.read()` classmethods return types (they return base types like `SectionFile`, `BlockFile`, `RegisterFile` instead of `Self`), which affects mypy type checking.

### Relation to Epic

First ticket in Epic 2. Updates the dependency constraint. Subsequent tickets add mypy overrides and verify runtime compatibility.

### Current State

`pyproject.toml` dependencies:

```
dependencies = [
    "click>=8.1.8",
    "idessem>=1.0.0",
    "pyarrow>=19.0.0",
]
```

idessem on `feat/infra-docs-overhaul` branch at `/home/rogerio/git/idessem` requires:

- `cfinterface>=1.9.0`
- `numpy>=2.2.1`
- `pandas>=3.0.0`
- `requires-python = ">= 3.11"`

## Specification

### Requirements

1. Update `pyproject.toml` dependencies to:
   - `idessem>=1.1.0` (or git reference to the feature branch if 1.1.0 is not yet published)
   - Add `pandas>=3.0.0` as explicit dependency (currently implicit via idessem)
2. Run `uv lock` to regenerate `uv.lock` with new dependency graph
3. Run `uv sync --all-extras --dev` to verify installation succeeds
4. Verify idessem imports work by running `uv run python -c "from idessem.dessem.dessemarq import DessemArq; print('OK')"`

### Inputs/Props

- idessem repository: `/home/rogerio/git/idessem` (branch `feat/infra-docs-overhaul`)
- idessem pyproject.toml: `/home/rogerio/git/idessem/pyproject.toml`

### Outputs/Behavior

- `uv sync` resolves all dependencies without conflicts
- idessem classes can be imported
- pandas >= 3.0.0 is available

### Error Handling

- If idessem 1.1.0 is not published to PyPI, use a git dependency: `idessem @ git+https://github.com/rjmalves/idessem.git@feat/infra-docs-overhaul`
- If dependency resolution fails, check for numpy/pandas version conflicts and adjust constraints

## Acceptance Criteria

- [ ] Given `pyproject.toml`, when reading the dependencies list, then `idessem>=1.1.0` (or a git reference to the feature branch) is present
- [ ] Given `pyproject.toml`, when reading the dependencies list, then `pandas>=3.0.0` is present as an explicit dependency
- [ ] Given a fresh virtual environment, when running `uv sync --all-extras --dev`, then the command exits with code 0
- [ ] Given the installed environment, when running `uv run python -c "import idessem; import cfinterface; print(cfinterface.__version__)"`, then cfinterface version is >= 1.9.0

## Implementation Guide

### Suggested Approach

1. Edit `pyproject.toml` to update the `idessem` version constraint
2. Add `pandas>=3.0.0` to the dependencies list
3. Run `uv lock` to regenerate the lock file
4. Run `uv sync --all-extras --dev`
5. Verify imports with a quick Python script

### Key Files to Modify

- `pyproject.toml`
- `uv.lock` (regenerated)

### Patterns to Follow

Match sintetizador-newave's dependency declaration style where `pandas` is listed as explicit dependency even though it's transitively required by inewave/idessem.

### Pitfalls to Avoid

- Do NOT remove `pyarrow>=19.0.0` from dependencies
- Do NOT add polars yet (Epic 5)
- If using a git reference for idessem, document it clearly in a comment in pyproject.toml so it can be replaced with a version constraint once published

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` to check for any immediate breakage.

### Integration Tests

Verify all idessem class imports used in `app/adapters/repository/files.py` still work.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-001-update-pyproject-and-python-version.md
- **Blocks**: ticket-006-add-mypy-overrides-for-idessem.md, ticket-007-verify-files-repository-compatibility.md

## Effort Estimate

**Points**: 1
**Confidence**: Medium (depends on idessem 1.1.0 availability on PyPI)
