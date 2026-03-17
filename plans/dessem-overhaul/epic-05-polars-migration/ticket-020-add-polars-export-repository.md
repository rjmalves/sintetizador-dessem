# ticket-020 Implement Native Polars Parquet Export

## Context

### Background

Ticket-019 added the `polars` dependency and established the default `synthetize_pl()` method on `AbstractExportRepository` that converts to pandas before exporting. For Parquet output, this pandas conversion is unnecessary overhead -- Polars can go directly to Arrow and then to a PyArrow table for writing. This ticket implements the native Polars Parquet path on `ParquetExportRepository`, following sintetizador-newave's proven pattern.

### Relation to Epic

This is the second ticket in Epic 05 (Polars Migration). It provides the high-performance Parquet export path that tickets 021 and 022 will use when they switch internal processing to Polars DataFrames.

### Current State

After ticket-019:

- `polars>=1.0.0` is available as a dependency.
- `AbstractExportRepository.synthetize_pl()` exists as a default that converts to pandas.
- `ParquetExportRepository.synthetize_df()` writes via `pa.Table.from_pandas(enforce_utc(df))` with `pq.write_table()` using options: `write_statistics=False`, `flavor="spark"`, `coerce_timestamps="ms"`, `allow_truncated_timestamps=True`.
- `app/utils/tz.py` contains `enforce_utc()` which localizes timezone-naive `datetime64[ns]` columns to UTC on pandas DataFrames. It has no Polars awareness.

The newave reference (`/home/rogerio/git/sintetizador-newave/app/adapters/repository/export.py` lines 56-100) shows the target implementation: UTC enforcement on Polars datetime columns, then `Polars -> Arrow -> pandas -> pa.Table.from_pandas()` to get pandas Parquet metadata for round-trip compatibility, with a fallback to the pandas path on failure.

## Specification

### Requirements

1. Override `synthetize_pl()` on `ParquetExportRepository` with a native Polars implementation.
2. Enforce UTC timezone on all timezone-naive Polars Datetime columns before writing (iterate columns, check `isinstance(dtype, pl.Datetime)` and `dtype.time_zone is None`, then `pl.col(col_name).dt.replace_time_zone("UTC")`).
3. Convert Polars -> Arrow -> pandas -> `pa.Table.from_pandas()` to generate pandas Parquet metadata (this ensures `pd.read_parquet()` restores `datetime64[ns, UTC]` dtype). This is the same approach used in sintetizador-newave.
4. Write with `pq.write_table()` using the same options as `synthetize_df()`: `write_statistics=False`, `flavor="spark"`, `coerce_timestamps="ms"`, `allow_truncated_timestamps=True`.
5. On exception, log a warning and fall back to the pandas path via `self.synthetize_df(df.to_pandas(), filename)`.
6. Replace the module-level `from app.utils.log import Log` logger usage with `logging.getLogger(__name__)` for consistency with the newave pattern.

### Inputs/Props

- `df: pl.DataFrame` -- a Polars DataFrame to export as Parquet.
- `filename: str` -- output filename without extension.

### Outputs/Behavior

- Returns `True` on success.
- Writes a `.parquet` file (not `.parquet.gzip`) to `self.path / (filename + ".parquet")`, matching the existing `synthetize_df()` output path.
- On failure, falls back to the pandas path and returns whatever `synthetize_df()` returns.

### Error Handling

- Wrap the primary write path in `try/except Exception`.
- On exception: log warning with `exc_info=True`, fall back to `self.synthetize_df(df.to_pandas(), filename)`.

## Acceptance Criteria

- [ ] Given the file `app/adapters/repository/export.py`, when inspected, then `ParquetExportRepository` has a `synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool` method that does NOT call `df.to_pandas()` in the primary path (only in the fallback).
- [ ] Given a `pl.DataFrame` with a timezone-naive Datetime column, when `ParquetExportRepository.synthetize_pl()` is called, then the written Parquet file contains the datetime column with UTC timezone, verified by `pd.read_parquet(path).dtypes` showing `datetime64[ns, UTC]`.
- [ ] Given a `pl.DataFrame` with columns `["estagio", "data_inicio", "valor"]` where `data_inicio` is `pl.Datetime("us")`, when `synthetize_pl()` is called, then a `.parquet` file is written at `self.path / (filename + ".parquet")` and the method returns `True`.
- [ ] Given the file `app/adapters/repository/export.py`, when inspected, then a module-level `logger = logging.getLogger(__name__)` is present and used for warning messages (replacing the old `Log.log()` pattern).
- [ ] Given the test suite, when `python -m pytest tests/` is run, then all 82 existing tests pass.

## Implementation Guide

### Suggested Approach

1. Edit `app/adapters/repository/export.py`:
   - Add `import logging` and `import polars as pl` to the imports.
   - Add `logger = logging.getLogger(__name__)` at module level (after imports).
   - Remove `from app.utils.log import Log` (no longer needed -- the `factory()` function's error handling should switch to `logger.error(msg)` instead of `Log.log()`).
   - Add the `synthetize_pl()` override to `ParquetExportRepository`, following sintetizador-newave lines 56-100:

     ```python
     def synthetize_pl(self, df: pl.DataFrame, filename: str) -> bool:
         # Enforce UTC on timezone-naive datetime columns
         for col_name in df.columns:
             dtype = df[col_name].dtype
             if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                 df = df.with_columns(
                     pl.col(col_name).dt.replace_time_zone("UTC")
                 )
         try:
             arrow_table = pa.Table.from_pandas(df.to_arrow().to_pandas())
             pq.write_table(
                 arrow_table,
                 self.path.joinpath(filename + ".parquet"),
                 write_statistics=False,
                 flavor="spark",
                 coerce_timestamps="ms",
                 allow_truncated_timestamps=True,
             )
             return True
         except Exception:
             logger.warning(
                 "synthetize_pl failed for %s; falling back to pandas path",
                 filename,
                 exc_info=True,
             )
             return self.synthetize_df(df.to_pandas(), filename)
     ```

2. Update `factory()` to use `logger.error(msg)` instead of `Log.log()`.
3. Run `python -m pytest tests/` to verify all tests pass.

### Key Files to Modify

- `app/adapters/repository/export.py` (add `synthetize_pl` override to `ParquetExportRepository`, switch logging)

### Patterns to Follow

- Follow sintetizador-newave's `app/adapters/repository/export.py` lines 56-100 exactly for the `synthetize_pl()` implementation.
- Use `logging.getLogger(__name__)` module-level logger (same pattern as newave's export.py line 14).

### Pitfalls to Avoid

- Do NOT skip the `pa.Table.from_pandas()` step -- going directly from Arrow to Parquet without pandas metadata means `pd.read_parquet()` will NOT restore UTC timezone on datetime columns, breaking downstream consumers.
- Do NOT change the `pq.write_table()` options -- they must remain identical to `synthetize_df()` for schema compatibility.
- Do NOT modify `app/utils/tz.py` -- the Polars UTC enforcement is done inline in `synthetize_pl()`, not via a shared utility.

## Out of Scope

- Modifying `CSVExportRepository.synthetize_pl()` -- the default pandas-conversion path is sufficient for CSV.
- Modifying `app/utils/tz.py` for Polars support.
- Calling `synthetize_pl()` from any synthesis code (that happens in tickets 021-022).

## Testing Requirements

### Unit Tests

- No new test files required for this ticket. The Parquet export path will be exercised end-to-end by tickets 021-022. The acceptance criteria can be verified by manual inspection or by a small ad-hoc script.

### Integration Tests

- Run the full test suite (`python -m pytest tests/`) to confirm no regressions from the logger change and new method.

### E2E Tests

- Not applicable for this ticket.

## Dependencies

- **Blocked By**: ticket-019-add-polars-dependency.md
- **Blocks**: ticket-021-migrate-deck-hot-paths-to-polars.md

## Effort Estimate

**Points**: 2
**Confidence**: High
