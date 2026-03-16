# ticket-002 Modernize CI Workflows

## Context

### Background

The current `.github/workflows/main.yml` runs a single `test` job that combines pytest, mypy, ruff, and sphinx in sequential steps with a Python 3.10-3.12 matrix. sintetizador-newave has been restructured into separate `lint`, `typecheck`, `test`, and `docs` jobs running in parallel, with a Python 3.11-3.14 matrix for tests only.

### Relation to Epic

Second ticket in Epic 1. Depends on ticket-001 for the updated Python version and ruff/mypy config in `pyproject.toml`.

### Current State

`.github/workflows/main.yml` at `/home/rogerio/git/sintetizador-dessem/.github/workflows/main.yml`:

- Single `test` job with matrix: ["3.10", "3.11", "3.12"]
- Uses `astral-sh/setup-uv@v3`
- Steps: checkout, install uv, install python, uv sync, pytest with coverage, codecov, mypy, ruff check, sphinx-build
- All steps run sequentially in each matrix entry

## Specification

### Requirements

1. Replace single `test` job with four independent jobs: `lint`, `typecheck`, `test`, `docs`
2. `lint` job: Python 3.12, runs `uv run ruff check ./app` and `uv run ruff format --check ./app`
3. `typecheck` job: Python 3.12, runs `uv run mypy ./app`
4. `test` job: matrix ["3.11", "3.12", "3.13", "3.14"] with `fail-fast: false`, runs pytest with coverage + codecov
5. `docs` job: Python 3.12, runs `uv run sphinx-build -W -b html docs/source docs/build`
6. Update `astral-sh/setup-uv` to `@v7` with `enable-cache: true`
7. Remove Python 3.10 from all matrices

### Inputs/Props

- Reference: sintetizador-newave `.github/workflows/main.yml` at `/home/rogerio/git/sintetizador-newave/.github/workflows/main.yml`

### Outputs/Behavior

- CI runs 4 parallel jobs on push/PR to main
- Test matrix covers Python 3.11-3.14
- Lint and typecheck run on a single Python version (3.12) for speed

### Error Handling

- `fail-fast: false` on test matrix to see all failures
- Sphinx uses `-W` flag to treat warnings as errors

## Acceptance Criteria

- [ ] Given `.github/workflows/main.yml`, when parsing the YAML, then exactly four job keys exist: `lint`, `typecheck`, `test`, `docs`
- [ ] Given the `test` job, when reading its matrix, then `python-version` contains `["3.11", "3.12", "3.13", "3.14"]` and `fail-fast` is `false`
- [ ] Given the `lint` job, when reading its steps, then both `ruff check ./app` and `ruff format --check ./app` commands are present
- [ ] Given all jobs, when reading setup-uv action, then the version tag is `@v7` with `enable-cache: true`

## Implementation Guide

### Suggested Approach

1. Rewrite `.github/workflows/main.yml` following the exact structure of newave's `main.yml`
2. Keep the existing codecov integration in the `test` job
3. Preserve the workflow trigger configuration (push + PR to main)

### Key Files to Modify

- `.github/workflows/main.yml`

### Patterns to Follow

Copy the structure from `/home/rogerio/git/sintetizador-newave/.github/workflows/main.yml` exactly, changing only the project name in codecov.

### Pitfalls to Avoid

- Do NOT remove the codecov step from tests
- Do NOT add `needs:` dependencies between jobs (they should run in parallel)
- Ensure the sphinx docs source path matches this project's structure (`docs/source` and `docs/build`)

## Testing Requirements

### Unit Tests

N/A - workflow file changes verified by CI execution.

### Integration Tests

After pushing, verify the GitHub Actions UI shows 4 separate jobs.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-001-update-pyproject-and-python-version.md
- **Blocks**: ticket-004-add-release-workflow-and-docs.md

## Effort Estimate

**Points**: 1
**Confidence**: High
