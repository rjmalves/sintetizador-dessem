# ticket-028 Update Documentation and Final Polish

## Context

### Background

The sintetizador-dessem codebase has undergone a major overhaul across 6 epics: infrastructure modernization, idessem upgrade, deck decomposition into submodules, operation synthesis decomposition, Polars migration, and multiprocessing logger integration. The Sphinx documentation still references the old sphinx_rtd_theme and does not reflect the new module structure (deck submodules, operation synthesis package). The epic-07 goal includes switching to the furo theme and ensuring documentation builds cleanly.

### Relation to Epic

Final ticket in epic-07 and the entire plan. After ticket-026 (mypy clean) and ticket-027 (annotations/constants audit), this ticket handles documentation and a final linting pass to deliver a release-quality codebase.

### Current State

- `docs/source/conf.py` uses `sphinx_rtd_theme` with `sphinx_rtd_theme` in the extensions list
- The `furo` package is already in `pyproject.toml` dev dependencies
- `docs/source/referencia/modelo.rst` and `docs/source/referencia/saidas.rst` document the old module structure (single `app.services.deck.deck.Deck` class, no submodules)
- The conf.py imports `from typing import List` and uses `List[str]` for `exclude_patterns`
- `docs/source/conf.py` references `sphinx_rtd_theme` as both an extension and the html_theme
- `ruff check` and `ruff format` currently pass on `app/`
- All 82 tests pass

## Specification

### Requirements

1. Switch Sphinx theme from `sphinx_rtd_theme` to `furo` in `docs/source/conf.py`
2. Remove `sphinx_rtd_theme` from the extensions list (furo does not need to be listed as an extension -- just set `html_theme = "furo"`)
3. Update `html_theme_options` to furo-compatible options (furo does not support rtd-specific options like `collapse_navigation`, `sticky_navigation`, etc.)
4. Update `docs/source/conf.py` to use modern Python: `list[str]` instead of `List[str]`
5. Verify documentation builds without errors using `uv run sphinx-build -W -b html docs/source docs/build`
6. Run a final `uv run ruff format --check ./app` and `uv run ruff check ./app` to confirm the full codebase is clean
7. Run `uv run mypy ./app` to confirm zero errors are maintained after all epic-07 changes
8. Run `uv run pytest` to confirm all tests pass

### Out of Scope

- Rewriting RST documentation content for the new module structure (the autodoc/autosummary will pick up the new modules automatically)
- Adding new Sphinx gallery examples for --processadores
- Writing a developer guide
- Adding new tests
- Performance work

### Error Handling

No runtime behavior changes. Documentation configuration only.

## Acceptance Criteria

- [ ] Given `docs/source/conf.py`, when inspected, then `html_theme` is set to `"furo"` and `sphinx_rtd_theme` does not appear anywhere in the file
- [ ] Given `docs/source/conf.py`, when inspected, then the `exclude_patterns` annotation uses `list[str]` not `List[str]`, and there is no `from typing import List`
- [ ] Given the command `uv run sphinx-build -W -b html docs/source docs/build`, when executed, then it exits with code 0 (zero warnings treated as errors)
- [ ] Given `uv run ruff check ./app && uv run ruff format --check ./app`, when executed, then both exit with code 0
- [ ] Given `uv run mypy ./app`, when executed, then it exits with code 0

## Implementation Guide

### Suggested Approach

**Step 1: Update `docs/source/conf.py`**

1. Remove `"sphinx_rtd_theme"` from the `extensions` list
2. Change `html_theme = "sphinx_rtd_theme"` to `html_theme = "furo"`
3. Replace the `html_theme_options` dict with furo-compatible options:
   ```python
   html_theme_options = {
       "navigation_with_keys": True,
   }
   ```
4. Replace `from typing import List` with nothing, and change `exclude_patterns: List[str] = []` to `exclude_patterns: list[str] = []`
5. Optionally add `"sidebar_hide_name": False` to theme options if desired

**Step 2: Remove sphinx_rtd_theme from dependencies (if present)**
Check `pyproject.toml` -- `sphinx_rtd_theme` does not appear in the dev dependencies currently, so nothing to remove. The `furo` package is already listed.

**Step 3: Build documentation**
Run `uv run sphinx-build -W -b html docs/source docs/build` and fix any warnings. Common issues:

- Missing module references in autosummary/autodoc for the new submodules
- Broken cross-references to renamed modules
- If `sphinx_gallery` examples fail, the `-W` flag will catch it

**Step 4: Final validation sweep**
Run all three quality gates:

- `uv run mypy ./app`
- `uv run ruff check ./app`
- `uv run ruff format --check ./app`
- `uv run pytest`

### Key Files to Modify

1. `docs/source/conf.py` -- theme switch and typing modernization

### Patterns to Follow

- The furo theme configuration is minimal: just set `html_theme = "furo"` and optionally configure `html_theme_options` with furo-specific keys
- Keep intersphinx and numpydoc configuration unchanged
- Keep sphinx_gallery configuration unchanged

### Pitfalls to Avoid

- Do NOT add `"furo"` to the extensions list -- furo is a theme, not an extension
- The `-W` flag in sphinx-build treats warnings as errors; if the build has pre-existing warnings unrelated to this ticket (e.g., missing modules in autodoc), consider whether to fix them or remove `-W`
- Some furo theme options have different names from RTD theme options -- do not copy RTD options verbatim
- The `sphinx_gallery` extension requires plotly; ensure it's available in the dev dependencies (it is: `plotly` is listed)

## Testing Requirements

### Unit Tests

- All 82 existing tests must continue to pass

### Integration Tests

- `uv run sphinx-build -W -b html docs/source docs/build` exits with code 0
- `uv run mypy ./app` exits with code 0
- `uv run ruff check ./app` exits with code 0
- `uv run ruff format --check ./app` exits with code 0

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-027-type-annotations-and-constants-audit.md
- **Blocks**: None

## Effort Estimate

**Points**: 1
**Confidence**: High
