# Epic 05: Polars Migration

## Goal

Migrate hot-path DataFrame operations from Pandas to Polars for improved performance. Add native Polars Parquet export via PyArrow. Keep Pandas at the boundary (idessem returns Pandas DataFrames) but convert to Polars for internal processing.

## Scope

- Add polars >= 1.0.0 dependency
- Add `synthetize_pl()` method to export repositories
- Migrate deck submodule data processing to Polars
- Migrate operation synthesis pipeline to Polars
- Migrate statistics calculation to Polars
- Update bounds calculation to Polars

## Out of Scope

- Changing idessem to return Polars (upstream change)
- Changing the Parquet output schema (must remain compatible)
- Removing Pandas entirely (still needed at idessem boundary)

## Tickets

1. ticket-019-add-polars-dependency.md
2. ticket-020-add-polars-export-repository.md
3. ticket-021-migrate-deck-hot-paths-to-polars.md
4. ticket-022-migrate-operation-synthesis-to-polars.md

## Success Criteria

- Hot-path operations use Polars
- Parquet output files are schema-compatible with current output
- Tests pass with Polars operations
