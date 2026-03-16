# ticket-022 Migrate Operation Synthesis to Polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert the operation synthesis pipeline (orchestrator, pipeline, resolution modules, cache, export) to work with Polars DataFrames. This includes the post-resolve processing, bounds calculation, statistics calculation, and export calls using `synthetize_pl()`.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/orchestrator.py`, `app/services/synthesis/operation/pipeline.py`, `app/services/synthesis/operation/cache.py`, `app/services/synthesis/operation/export.py`, `app/services/synthesis/operation/resolution_*.py`, `app/services/deck/bounds.py`, `app/utils/operations.py`
- **Key decisions needed**: Whether to convert system.py and execution.py synthesis to Polars as well (they are simpler); how to handle bounds.py which uses numpy and pandas merge
- **Open questions**: Should `calc_statistics` be rewritten in Polars or is it fast enough in Pandas?

## Dependencies

- **Blocked By**: ticket-021-migrate-deck-hot-paths-to-polars.md
- **Blocks**: ticket-023-add-multiprocessing-logger.md (Epic 6)

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
