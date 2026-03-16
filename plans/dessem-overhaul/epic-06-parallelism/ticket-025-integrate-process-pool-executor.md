# ticket-025 Integrate ProcessPoolExecutor for Parallel Synthesis

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Integrate `concurrent.futures.ProcessPoolExecutor` into the operation synthesis orchestrator to parallelize synthesis of independent variables. When `--processadores` is > 1, synthesis variables are distributed across worker processes.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/orchestrator.py`, `app/services/synthesis/operation/cache.py` (multiprocessing-safe cache), `app/services/unitofwork.py` (picklability audit)
- **Key decisions needed**: Whether to use process pool at variable level or at a coarser granularity; how to handle the cache dict across processes (each process gets its own cache, merged at the end?)
- **Open questions**: Can the UnitOfWork and FileRepository be pickled for multiprocessing? Does the cwd-changing behavior in FSUnitOfWork work with spawn?

## Dependencies

- **Blocked By**: ticket-024-add-processadores-cli-option.md
- **Blocks**: ticket-026-enable-mypy-strict-full.md (Epic 7)

## Effort Estimate

**Points**: 4
**Confidence**: Low (will be re-estimated during refinement)
