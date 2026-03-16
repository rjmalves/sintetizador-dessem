# ticket-001 Update pyproject.toml and Python Version

## Context

### Background

sintetizador-dessem currently requires Python >= 3.10 and has minimal tooling configuration in `pyproject.toml`. sintetizador-newave has been modernized to require Python >= 3.11 with ruff lint rules, mypy strict config, and updated classifiers. This ticket aligns the project configuration.

### Relation to Epic

First ticket in Epic 1 (Infrastructure & Dependency Modernization). It establishes the foundational project configuration that all subsequent tickets build upon.

### Current State

`pyproject.toml` at `/home/rogerio/git/sintetizador-dessem/pyproject.toml`:

- `requires-python = ">= 3.10"`
- Classifiers list only Python 3.10
- `[tool.ruff]` has only `line-length = 80`
- No `[tool.ruff.lint]`, `[tool.ruff.lint.isort]`, or `[tool.mypy]` sections
- Dev dependencies: pytest, pytest-cov, ruff, mypy, sphinx-rtd-theme, sphinx-gallery, sphinx, numpydoc, plotly, matplotlib
- No `pre-commit` or `types-python-dateutil` in dev dependencies
- No `furo` theme (uses sphinx-rtd-theme)

## Specification

### Requirements

1. Update `requires-python` to `">= 3.11"`
2. Update classifiers to list Python 3.11, 3.12, 3.13, 3.14
3. Add `[tool.ruff.lint]` section with `select = ["E", "F", "W", "I"]` and `ignore = ["E501"]`
4. Add `[tool.ruff.lint.isort]` section with `known-first-party = ["app"]`
5. Add `[tool.mypy]` section with `strict = true`, `warn_return_any = true`, `warn_unused_configs = true`, `ignore_missing_imports = true`
6. Update dev dependencies: replace `sphinx-rtd-theme` with `furo`, add `pre-commit`, add `types-python-dateutil`
7. Add `app/py.typed` empty marker file (PEP 561)

### Inputs/Props

- Reference: sintetizador-newave `pyproject.toml` at `/home/rogerio/git/sintetizador-newave/pyproject.toml`

### Outputs/Behavior

- `pyproject.toml` matches newave's configuration structure
- `app/py.typed` exists as empty file
- `uv sync --all-extras --dev` succeeds

### Error Handling

- If `uv sync` fails due to dependency conflicts, resolve by adjusting version constraints (do not pin to exact versions)

## Acceptance Criteria

- [ ] Given `pyproject.toml`, when reading the `requires-python` field, then it equals `">= 3.11"`
- [ ] Given `pyproject.toml`, when reading the classifiers, then Python 3.11, 3.12, 3.13, 3.14 are listed and 3.10 is absent
- [ ] Given `pyproject.toml`, when reading `[tool.ruff.lint]`, then `select = ["E", "F", "W", "I"]` and `ignore = ["E501"]` are present
- [ ] Given `pyproject.toml`, when reading `[tool.mypy]`, then `strict = true` is set
- [ ] Given the file `app/py.typed`, when checking its existence, then it is an empty file at that path

## Implementation Guide

### Suggested Approach

1. Edit `pyproject.toml`:
   - Change `requires-python` from `">= 3.10"` to `">= 3.11"`
   - Replace the classifiers list to include 3.11-3.14
   - Add ruff lint and isort sections after the existing `[tool.ruff]` section
   - Add mypy section at the end
   - Update `[project.optional-dependencies] dev` list
2. Create empty `app/py.typed` file
3. Run `uv sync --all-extras --dev` to verify resolution
4. Run `uv run ruff check ./app` to verify ruff config works

### Key Files to Modify

- `pyproject.toml`
- `app/py.typed` (create new)

### Patterns to Follow

Match sintetizador-newave's `pyproject.toml` structure exactly for the tooling sections.

### Pitfalls to Avoid

- Do NOT add mypy overrides for idessem/cfinterface yet (that is ticket-006 in Epic 2)
- Do NOT change runtime dependencies (idessem, pyarrow, click) - only dev dependencies
- Do NOT add polars dependency yet (Epic 5)

## Testing Requirements

### Unit Tests

No code changes to test; verify `uv sync` succeeds.

### Integration Tests

Run `uv run ruff check ./app` and `uv run ruff format --check ./app` to verify ruff configuration. Note: mypy may fail at this point due to strict mode -- that is expected and will be addressed in Epic 2 and Epic 7.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: None
- **Blocks**: ticket-002-modernize-ci-workflows.md, ticket-003-add-precommit-and-tooling.md

## Effort Estimate

**Points**: 1
**Confidence**: High
