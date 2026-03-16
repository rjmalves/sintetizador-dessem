# ticket-015 Create Operation Synthesis Package Structure

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert `app/services/synthesis/operation.py` into an `app/services/synthesis/operation/` package by creating the package directory, `__init__.py` with public exports, and moving the existing code as a starting point. This establishes the package structure that subsequent tickets will decompose into focused submodules.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation.py` (rename/move), `app/services/synthesis/operation/__init__.py` (create), `app/services/handlers.py` (update import)
- **Key decisions needed**: Whether to re-export `OperationSynthetizer` from `__init__.py` or update all callers to use the new module path
- **Open questions**: Should the `_resolve` dispatch dict remain in the orchestrator or be split into resolution modules?

## Dependencies

- **Blocked By**: ticket-014-reduce-deck-to-facade.md
- **Blocks**: ticket-016-extract-operation-cache-and-export.md

## Effort Estimate

**Points**: 1
**Confidence**: Low (will be re-estimated during refinement)
