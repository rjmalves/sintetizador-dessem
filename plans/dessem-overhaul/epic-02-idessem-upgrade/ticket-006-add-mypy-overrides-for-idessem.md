# ticket-006 Add mypy Overrides for idessem and cfinterface

## Context

### Background

With cfinterface >= 1.9.0, the `.read()` classmethods on idessem file classes (DessemArq, Entdados, PdoSist, etc.) return base types (SectionFile, BlockFile, RegisterFile) instead of `Self`. This causes mypy strict mode to report false-positive errors for assignment and return-value throughout `app/adapters/repository/files.py`. sintetizador-newave solved this by adding targeted mypy overrides in `pyproject.toml`.

### Relation to Epic

Second ticket in Epic 2. Depends on ticket-005 for the upgraded idessem. After this ticket, `uv run mypy ./app` should pass cleanly.

### Current State

`pyproject.toml` has `[tool.mypy]` with `strict = true` (from ticket-001) but no `[[tool.mypy.overrides]]` sections. Running `uv run mypy ./app` with the new idessem will produce many errors in `app/adapters/repository/files.py` and on any import from idessem/cfinterface.

## Specification

### Requirements

1. Add `[[tool.mypy.overrides]]` for `module = ["idessem.*"]` with `ignore_errors = true`
2. Add `[[tool.mypy.overrides]]` for `module = ["cfinterface.*"]` with `ignore_errors = true`
3. Add `[[tool.mypy.overrides]]` for `module = ["app.adapters.repository.files"]` with `disable_error_code = ["assignment", "return-value", "attr-defined", "unused-ignore"]`
4. After adding overrides, `uv run mypy ./app` must pass with zero errors (or only expected errors from non-override modules)

### Inputs/Props

- Reference: sintetizador-newave `pyproject.toml` mypy overrides at `/home/rogerio/git/sintetizador-newave/pyproject.toml` lines 81-98

### Outputs/Behavior

- `uv run mypy ./app` exits with code 0

### Error Handling

- If additional modules produce errors that are false positives from cfinterface types, add targeted overrides for those modules specifically (do NOT use blanket `ignore_errors = true` for `app.*`)

## Acceptance Criteria

- [ ] Given `pyproject.toml`, when reading mypy overrides, then `idessem.*` module has `ignore_errors = true`
- [ ] Given `pyproject.toml`, when reading mypy overrides, then `cfinterface.*` module has `ignore_errors = true`
- [ ] Given `pyproject.toml`, when reading mypy overrides, then `app.adapters.repository.files` has `disable_error_code` including `"assignment"` and `"return-value"`
- [ ] Given the project, when running `uv run mypy ./app`, then the exit code is 0

## Implementation Guide

### Suggested Approach

1. Add three `[[tool.mypy.overrides]]` sections to `pyproject.toml` after the existing `[tool.mypy]` section
2. Run `uv run mypy ./app` and check output
3. If additional false positives appear in other modules, add targeted overrides
4. Document each override with a comment explaining why it exists (same as newave)

### Key Files to Modify

- `pyproject.toml`

### Patterns to Follow

Follow newave's pattern of adding a comment above the `files` override explaining the root cause:

```toml
# idessem v1.1+ .read() classmethods return base types (SectionFile,
# BlockFile, RegisterFile) instead of Self, causing many false-positive
# errors throughout this module.
```

### Pitfalls to Avoid

- Do NOT add `ignore_errors = true` to `app.adapters.repository.files` -- use the more precise `disable_error_code` to catch real type errors
- Do NOT add overrides for `dateutil.*` unless it causes errors (it may be needed if `types-python-dateutil` is not sufficient)
- Do NOT suppress errors in domain model files -- those should type-check cleanly

## Testing Requirements

### Unit Tests

Run `uv run mypy ./app` and verify exit code 0.

### Integration Tests

Run `uv run pytest ./tests` to confirm no behavioral regressions.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-005-upgrade-idessem-dependency.md
- **Blocks**: ticket-007-verify-files-repository-compatibility.md

## Effort Estimate

**Points**: 1
**Confidence**: High
