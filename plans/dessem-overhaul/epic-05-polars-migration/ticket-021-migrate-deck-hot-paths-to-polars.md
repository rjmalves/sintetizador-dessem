# ticket-021 Migrate Deck Hot Paths to Polars

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Convert the most performance-critical DataFrame operations in the deck submodules (hydro.py, system.py, thermal.py, entities.py) from Pandas to Polars. Conversion from Pandas (idessem output) to Polars happens at the accessor boundary; all internal processing uses Polars expressions.

## Anticipated Scope

- **Files likely to be modified**: `app/services/deck/hydro.py`, `app/services/deck/system.py`, `app/services/deck/thermal.py`, `app/services/deck/entities.py`, `app/services/deck/temporal.py`, `app/services/deck/context.py` (switch to pl.DataFrame fields)
- **Key decisions needed**: At what layer to convert Pandas -> Polars (accessor level vs. individual function level); whether DeckContext should store Polars DataFrames
- **Open questions**: How to handle the `fast_group_df` utility (rewrite in Polars or keep Pandas); what to do with numpy operations in bounds calculations

## Dependencies

- **Blocked By**: ticket-020-add-polars-export-repository.md
- **Blocks**: ticket-022-migrate-operation-synthesis-to-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Low (will be re-estimated during refinement)
