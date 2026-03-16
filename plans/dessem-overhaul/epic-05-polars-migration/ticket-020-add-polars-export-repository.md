# ticket-020 Implement Native Polars Parquet Export

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Implement the native Polars Parquet export path in `ParquetExportRepository.synthetize_pl()` that bypasses Pandas by going Polars -> Arrow -> PyArrow table, with UTC enforcement on datetime columns and pandas metadata for round-trip compatibility.

## Anticipated Scope

- **Files likely to be modified**: `app/adapters/repository/export.py`, `app/utils/tz.py` (may need Polars-aware UTC enforcement)
- **Key decisions needed**: Whether to use the same `pq.write_table` options as the Pandas path for schema compatibility
- **Open questions**: How to handle timezone enforcement on Polars DatetimeColumns vs Pandas Series

## Dependencies

- **Blocked By**: ticket-019-add-polars-dependency.md
- **Blocks**: ticket-021-migrate-deck-hot-paths-to-polars.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
