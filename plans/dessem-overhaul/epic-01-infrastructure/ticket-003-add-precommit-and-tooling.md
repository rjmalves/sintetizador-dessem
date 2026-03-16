# ticket-003 Add Pre-commit Hooks and Tooling

## Context

### Background

sintetizador-dessem has no pre-commit configuration. sintetizador-newave uses a `.pre-commit-config.yaml` with ruff (check + format) and a local mypy hook via `uv run`. This ensures code quality checks run before every commit.

### Relation to Epic

Third ticket in Epic 1. Depends on ticket-001 for the ruff and mypy configuration in `pyproject.toml`.

### Current State

No `.pre-commit-config.yaml` exists in the repository. No `uv.lock` file exists.

## Specification

### Requirements

1. Create `.pre-commit-config.yaml` with:
   - Ruff pre-commit hook (check with `--fix` + format) from `https://github.com/astral-sh/ruff-pre-commit`
   - Local mypy hook using `uv run mypy` with `language: system`, `types: [python]`, `pass_filenames: false`, `args: [./app]`
2. Generate `uv.lock` by running `uv lock`
3. Verify hooks work by running `uv run pre-commit run --all-files`

### Inputs/Props

- Reference: sintetizador-newave `.pre-commit-config.yaml` at `/home/rogerio/git/sintetizador-newave/.pre-commit-config.yaml`

### Outputs/Behavior

- `.pre-commit-config.yaml` exists at repository root
- `uv.lock` exists at repository root
- Running `uv run pre-commit run --all-files` executes ruff and mypy hooks

### Error Handling

- mypy hook may report errors in strict mode before Epic 2/7 -- this is expected. The hook should still be configured; developers can skip it during transition with `--no-verify` if needed.

## Acceptance Criteria

- [ ] Given the file `.pre-commit-config.yaml`, when parsing its YAML content, then it contains a repo entry for `ruff-pre-commit` with hooks `ruff` (with `--fix` arg) and `ruff-format`
- [ ] Given the file `.pre-commit-config.yaml`, when parsing its YAML content, then it contains a local hook with `id: mypy` using `entry: uv run mypy`
- [ ] Given the repository root, when listing files, then `uv.lock` exists and is valid (parseable by uv)
- [ ] Given the pre-commit configuration, when running `uv run pre-commit run --all-files`, then ruff check and ruff format hooks execute (mypy may report errors)

## Implementation Guide

### Suggested Approach

1. Create `.pre-commit-config.yaml` matching newave's structure. Use a recent ruff-pre-commit rev (check latest at the repo).
2. Run `uv lock` to generate `uv.lock`
3. Run `uv run pre-commit install` to install hooks
4. Run `uv run pre-commit run --all-files` to verify

### Key Files to Modify

- `.pre-commit-config.yaml` (create new)
- `uv.lock` (generated)

### Patterns to Follow

Match `/home/rogerio/git/sintetizador-newave/.pre-commit-config.yaml` exactly.

### Pitfalls to Avoid

- Do NOT pin ruff-pre-commit to an old version -- use a recent rev
- The mypy hook must use `pass_filenames: false` so it runs on the entire `./app` directory, not individual files
- Do NOT commit `.pre-commit-config.yaml` without verifying `uv run pre-commit run --all-files` executes the ruff hooks cleanly

## Testing Requirements

### Unit Tests

N/A

### Integration Tests

Run `uv run pre-commit run --all-files` and verify ruff hooks pass without errors.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-001-update-pyproject-and-python-version.md
- **Blocks**: None

## Effort Estimate

**Points**: 1
**Confidence**: High
