# ticket-016 Extract Operation Cache and Export Modules

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Extract caching logic (CACHED_SYNTHESIS, ORDERED_SYNTHESIS_ENTITIES, SYNTHESIS_STATS, get/store cache methods) and export logic (\_export_metadata, \_export_scenario_synthesis, \_export_stats, \_add_synthesis_stats) from the OperationSynthetizer class into dedicated `cache.py` and `export.py` modules within the operation package.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/cache.py` (create), `app/services/synthesis/operation/export.py` (create), `app/services/synthesis/operation/orchestrator.py` (or wherever the main class ends up)
- **Key decisions needed**: Whether cache should be class-level dicts (current) or instance-level (to support multiprocessing in Epic 6)
- **Open questions**: Should statistics accumulation (SYNTHESIS_STATS) be part of cache or export?

## Dependencies

- **Blocked By**: ticket-015-create-operation-synthesis-package.md
- **Blocks**: ticket-017-extract-resolution-modules.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
