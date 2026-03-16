# ticket-018 Create Operation Orchestrator and Pipeline

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Create the `orchestrator.py` module containing the main synthesis loop (`synthetize`, `_synthetize_single_variable`, `_preprocess_synthesis_variables`) and the `pipeline.py` module containing the data resolution pipeline (`_resolve_synthesis`, `_post_resolve`, `_resolve_stub`, `_resolve_bounds`). Wire everything together so the operation package is functional and all tests pass.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/orchestrator.py` (create), `app/services/synthesis/operation/pipeline.py` (create), `app/services/synthesis/operation/__init__.py` (update exports), `app/services/handlers.py` (update import if needed)
- **Key decisions needed**: Whether the orchestrator should be a class or module-level functions; whether DeckContext should be used here
- **Open questions**: How to maintain the existing class-level state (logger, cache) across the decomposed modules

## Dependencies

- **Blocked By**: ticket-017-extract-resolution-modules.md
- **Blocks**: ticket-019-add-polars-dependency.md (Epic 5)

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
