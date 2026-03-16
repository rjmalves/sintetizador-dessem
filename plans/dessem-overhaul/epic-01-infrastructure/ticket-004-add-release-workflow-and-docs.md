# ticket-004 Add Release Workflow and Contributing Guide

## Context

### Background

sintetizador-dessem lacks a release workflow for automated PyPI publishing and a CONTRIBUTING.md guide. sintetizador-newave has a `release.yml` workflow triggered by GitHub releases that runs lint, typecheck, test, build, and publish steps, plus a `CONTRIBUTING.md` for onboarding contributors.

### Relation to Epic

Final ticket in Epic 1. Depends on ticket-002 for the modernized CI workflow structure.

### Current State

- No `.github/workflows/release.yml` exists
- No `CONTRIBUTING.md` exists
- No `.github/workflows/docs.yml` exists (docs build is part of main.yml)

## Specification

### Requirements

1. Create `.github/workflows/release.yml` that:
   - Triggers on `release: [published]`
   - Runs `lint` job (ruff check + format on Python 3.12)
   - Runs `typecheck` job (mypy on Python 3.12)
   - Runs `test` job (pytest on matrix: ["3.11", "3.12", "3.14"])
   - Runs `build` job (needs lint+typecheck+test, runs `uv build`, uploads dist artifact)
   - Runs `publish` job (needs build, downloads artifact, uses `pypa/gh-action-pypi-publish@release/v1` with `id-token: write`)
2. Create `CONTRIBUTING.md` with:
   - Prerequisites (Python 3.11+, uv)
   - Setup instructions (`uv sync --all-extras --dev`, `uv run pre-commit install`)
   - Development workflow (running tests, linting, type checking)
   - Commit conventions
   - PR process

### Inputs/Props

- Reference: sintetizador-newave `release.yml` at `/home/rogerio/git/sintetizador-newave/.github/workflows/release.yml`

### Outputs/Behavior

- `release.yml` triggers automated PyPI publishing on GitHub release creation
- `CONTRIBUTING.md` provides clear onboarding for contributors

### Error Handling

- Release workflow fails gracefully if lint/typecheck/test fail -- build and publish do not run

## Acceptance Criteria

- [ ] Given `.github/workflows/release.yml`, when parsing the YAML trigger, then it fires on `release: types: [published]`
- [ ] Given `.github/workflows/release.yml`, when reading the `publish` job, then it uses `pypa/gh-action-pypi-publish@release/v1` with `permissions.id-token: write`
- [ ] Given `.github/workflows/release.yml`, when reading the `build` job, then it has `needs: [lint, typecheck, test]`
- [ ] Given `CONTRIBUTING.md`, when reading its content, then it contains sections for Prerequisites, Setup, Development Workflow, and PR Process

## Implementation Guide

### Suggested Approach

1. Copy newave's `release.yml` and adapt project name references
2. Write `CONTRIBUTING.md` referencing the project's specific tooling (uv, ruff, mypy, pytest)
3. Verify YAML syntax with a linter or by reviewing in GitHub's workflow editor

### Key Files to Modify

- `.github/workflows/release.yml` (create new)
- `CONTRIBUTING.md` (create new)

### Patterns to Follow

Match `/home/rogerio/git/sintetizador-newave/.github/workflows/release.yml` structure. The release workflow re-uses the same lint/typecheck/test structure from `main.yml` but with a reduced test matrix.

### Pitfalls to Avoid

- The `publish` job requires the `pypi` environment to be configured in GitHub repository settings
- Do NOT include `docs` job in release workflow (docs are built separately)
- Ensure `permissions.id-token: write` is at the job level, not the workflow level

## Testing Requirements

### Unit Tests

N/A

### Integration Tests

Verify YAML is valid by running a YAML linter. The release workflow can only be fully tested by creating a GitHub release.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-002-modernize-ci-workflows.md
- **Blocks**: None

## Effort Estimate

**Points**: 1
**Confidence**: High
