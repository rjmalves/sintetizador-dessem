# ticket-027 Audit Type Annotations and Constants

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Audit all type annotations across the codebase for correctness and completeness. Audit `app/internal/constants.py` to align with newave's typed column definitions pattern. Remove legacy type imports (typing.List, typing.Dict, typing.Optional in favor of built-in generics and X | None syntax).

## Anticipated Scope

- **Files likely to be modified**: `app/internal/constants.py`, all files in `app/services/`, `app/adapters/`, `app/utils/`, `app/model/`
- **Key decisions needed**: Whether to use `StringDtype` from pyarrow or polars for column type definitions; whether to keep the PANDAS_GROUPING_ENGINE logic after Polars migration
- **Open questions**: Should constants be refactored into typed NamedTuple or dataclass patterns?

## Dependencies

- **Blocked By**: ticket-026-enable-mypy-strict-full.md
- **Blocks**: ticket-028-documentation-and-final-polish.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
