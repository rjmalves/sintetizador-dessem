# ticket-028 Update Documentation and Final Polish

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Update Sphinx documentation configuration to use furo theme, update API docs to reflect the new module structure (deck submodules, operation package), ensure documentation builds without warnings, and do a final formatting/linting pass across the entire codebase.

## Anticipated Scope

- **Files likely to be modified**: `docs/source/conf.py`, `docs/source/*.rst` (API docs), `pyproject.toml` (if sphinx config needed)
- **Key decisions needed**: Whether to generate API docs automatically or maintain manual RST files; whether to document the new module structure in a developer guide
- **Open questions**: Should the Sphinx gallery examples be updated? Are there new examples needed for the --processadores option?

## Dependencies

- **Blocked By**: ticket-027-type-annotations-and-constants-audit.md
- **Blocks**: None

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
