# ticket-022 Migrate Operation Synthesis to Polars

## Context

### Background

Ticket-021 converted all deck submodule functions to return `pl.DataFrame` instead of `pd.DataFrame`. The operation synthesis pipeline -- orchestrator, pipeline, resolution, cache, export, and bounds -- currently expects and operates on `pd.DataFrame`. This ticket updates the entire operation synthesis pipeline to work with `pl.DataFrame` throughout, and switches the final export call from `synthetize_df()` to `synthetize_pl()` (established in tickets 019-020) for Parquet output.

### Relation to Epic

This is the fourth and final ticket in Epic 05 (Polars Migration). It completes the migration by ensuring the full data path -- from deck data extraction through synthesis resolution, post-processing, bounds calculation, statistics, caching, and export -- operates on Polars DataFrames.

### Current State

After ticket-021:

- All `Deck.*` class methods return `pl.DataFrame`.
- `fast_group_df()` and `calc_statistics()` in `app/utils/operations.py` operate on `pl.DataFrame`.
- `OperationVariableBounds` methods in `app/services/deck/bounds.py` operate on `pl.DataFrame`.

The operation synthesis modules remain on pandas:

- `app/services/synthesis/operation/orchestrator.py` -- `OperationSynthetizer` class with `CACHED_SYNTHESIS: dict[OperationSynthesis, pd.DataFrame]`, `SYNTHESIS_STATS: dict[SpatialResolution, list[pd.DataFrame]]`.
- `app/services/synthesis/operation/pipeline.py` -- `post_resolve_file()` filters columns on `pd.DataFrame`; `post_resolve()` sorts and extracts entities; `resolve_bounds()` delegates to `OperationVariableBounds`; `resolve_synthesis()` chains resolve -> post_resolve -> bounds.
- `app/services/synthesis/operation/resolution.py` -- all `resolve_*` functions call `Deck.*` methods (which now return `pl.DataFrame`) but the function signatures still type-hint `pd.DataFrame`.
- `app/services/synthesis/operation/cache.py` -- stores and retrieves `pd.DataFrame` from class-level cache.
- `app/services/synthesis/operation/export.py` -- calls `uow.export.synthetize_df()`, `calc_statistics()`, `pd.concat()`.

## Specification

### Requirements

1. Update `orchestrator.py` type annotations: `CACHED_SYNTHESIS: dict[OperationSynthesis, pl.DataFrame]` and `SYNTHESIS_STATS: dict[SpatialResolution, list[pl.DataFrame]]`.
2. Update `pipeline.py` functions to operate on `pl.DataFrame`:
   - `post_resolve_file()`: filter columns using `df.select([c for c in df.columns if c in IDENTIFICATION_COLUMNS] + [VALUE_COL])`.
   - `get_unique_column_values_in_order()`: use `df[col].unique().to_list()` (Polars `unique()` returns a Series with `.to_list()`).
   - `post_resolve()`: use `df.sort(columns)` instead of `df.sort_values(columns)`.
   - `resolve_bounds()`: no change needed (OperationVariableBounds already operates on Polars after ticket-021).
3. Update `resolution.py` functions: all `resolve_*` functions already call `Deck.*` methods that return `pl.DataFrame`. Remove `pd.DataFrame` type hints and replace with `pl.DataFrame`. The `post_resolve_file()` call now operates on Polars.
4. Update `cache.py`: change type hints to `pl.DataFrame`. Use `df.clone()` instead of `df.copy()` for cache storage (Polars uses `clone()` for explicit copies).
5. Update `export.py`:
   - `export_scenario_synthesis()`: call `uow.export.synthetize_pl(df, filename)` instead of `uow.export.synthetize_df(df, filename)` for the main data export. The `df[columns]` selection should use `df.select(columns)`.
   - `add_synthesis_stats()`: use `df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))`.
   - `export_stats()`: replace `pd.concat(dfs)` with `pl.concat(dfs)`. Use `df.select([VARIABLE_COL] + columns)`, `df.sort(columns)`, and `df.cast({VARIABLE_COL: pl.Utf8})`.
   - `export_metadata()`: this function builds a metadata DataFrame from scratch. Convert it to build a `pl.DataFrame` directly (or keep it as pandas since it is small and exported once -- use `synthetize_df()` for metadata). The metadata DF is tiny and not a hot path, so pandas is acceptable here.
6. Remove `import pandas as pd` from modules that no longer need it (resolution.py, pipeline.py, cache.py). Keep it in export.py only if metadata still uses pandas.

### Inputs/Props

- All function signatures remain the same (the `cls`, `s`, `uow` pattern).
- DataFrame type changes from `pd.DataFrame` to `pl.DataFrame` throughout.

### Outputs/Behavior

- Synthesis data is exported via `synthetize_pl()` (Polars Parquet path for ParquetExportRepository).
- Metadata is exported via `synthetize_df()` (small DataFrame, pandas is fine).
- Statistics are exported via `synthetize_pl()`.
- Cache stores `pl.DataFrame` objects.
- All column names and output file names remain identical.

### Error Handling

- Same error handling pattern as current code. Exception handling in `_synthetize_single_variable()` remains unchanged.

## Acceptance Criteria

- [ ] Given the file `app/services/synthesis/operation/pipeline.py`, when inspected, then `import polars as pl` is present and `post_resolve_file()` uses `df.select()` instead of pandas column indexing.
- [ ] Given the file `app/services/synthesis/operation/export.py`, when inspected, then `export_scenario_synthesis()` calls `uow.export.synthetize_pl(df, filename)` for the main synthesis data export.
- [ ] Given the file `app/services/synthesis/operation/export.py`, when inspected, then `export_stats()` uses `pl.concat(dfs)` instead of `pd.concat(dfs)`.
- [ ] Given the file `app/services/synthesis/operation/cache.py`, when inspected, then `get_from_cache()` returns `pl.DataFrame` and `store_in_cache_if_needed()` stores via `df.clone()`.
- [ ] Given the test suite, when `python -m pytest tests/` is run, then all 82 tests pass.

## Implementation Guide

### Suggested Approach

**Step 1: `resolution.py`** (leaf module, calls Deck but is called by pipeline)

- Replace `import pandas as pd` with `import polars as pl`.
- All `resolve_*` functions already return what `Deck.*` returns (now `pl.DataFrame`). Update return type hints.
- `post_resolve_file()` is imported from `pipeline` -- it will be updated in step 2.
- `resolve_thermal_submarkets_pdo_sist_sbm()` and `resolve_hydro_submarkets_pdo_sist_sbm()`: replace `df.loc[df[col].isin(list)].reset_index(drop=True)` with `df.filter(pl.col(col).is_in(list))`.

**Step 2: `pipeline.py`**

- Replace `import pandas as pd` with `import polars as pl`.
- `post_resolve_file()`: `cols = [c for c in df.columns if c in IDENTIFICATION_COLUMNS]; return df.select(cols + [VALUE_COL])`.
- `get_unique_column_values_in_order()`: `{col: df[col].unique().sort().to_list() for col in cols}`.
- `post_resolve()`: `df.sort(columns)` instead of `df.sort_values(columns).reset_index(drop=True)`. Polars sort does not have an index to reset.
- `resolve_stub()`: `pl.DataFrame()` instead of `pd.DataFrame()` for empty DataFrame.

**Step 3: `cache.py`**

- Replace `import pandas as pd` with `import polars as pl`.
- `get_from_cache()`: return `res.clone()` instead of `res.copy()`.
- `get_from_cache_if_exists()`: return `pl.DataFrame()` instead of `pd.DataFrame()` for empty case.
- `store_in_cache_if_needed()`: store `df.clone()` instead of `df.copy()`.

**Step 4: `export.py`**

- Add `import polars as pl`.
- `export_scenario_synthesis()`:
  - `df = df.sort(columns)` -- no reset_index needed.
  - `stats_df = calc_statistics(df)` -- already returns `pl.DataFrame` after ticket-021.
  - `df = df.select(s.spatial_resolution.all_synthesis_df_columns)`.
  - `uow.export.synthetize_pl(df, filename)` instead of `uow.export.synthetize_df(df, filename)`.
- `add_synthesis_stats()`: `df = df.with_columns(pl.lit(s.variable.value).alias(VARIABLE_COL))`.
- `export_stats()`:
  - `df = pl.concat(dfs)`.
  - `df = df.select([VARIABLE_COL] + res.all_synthesis_df_columns)`.
  - `df = df.cast({VARIABLE_COL: pl.Utf8})` (replaces `df.astype({VARIABLE_COL: STRING_DF_TYPE})`).
  - `df = df.sort([VARIABLE_COL] + res.sorting_synthesis_df_columns)`.
  - `uow.export.synthetize_pl(df, ...)`.
- `export_metadata()`: keep as pandas since it builds a small DataFrame row-by-row. Use `synthetize_df()` for metadata export (it is not a hot path).

**Step 5: `orchestrator.py`**

- Update type hints for class-level caches.
- Replace `import pandas as pd` with `import polars as pl` (if pandas is still needed for metadata, keep both imports or let export.py handle it).
- The rest of the orchestrator logic (preprocessing variables, iterating synthesis) is type-agnostic and needs no changes.

**Step 6: Update tests**

- Tests that assert on synthesis output DataFrames may need Polars assertions.
- Tests that use `pd.DataFrame` fixtures for mock export may need updating.

### Key Files to Modify

- `app/services/synthesis/operation/resolution.py`
- `app/services/synthesis/operation/pipeline.py`
- `app/services/synthesis/operation/cache.py`
- `app/services/synthesis/operation/export.py`
- `app/services/synthesis/operation/orchestrator.py`
- Test files that assert on operation synthesis outputs

### Patterns to Follow

- **Column selection**: `df.select(cols)` instead of `df[cols]`.
- **Sorting**: `df.sort(cols)` instead of `df.sort_values(cols).reset_index(drop=True)`.
- **Empty DataFrame**: `pl.DataFrame()` instead of `pd.DataFrame()`.
- **Clone**: `df.clone()` instead of `df.copy()` (for explicit copies in cache).
- **Concat**: `pl.concat(dfs)` instead of `pd.concat(dfs, ignore_index=True)`.
- **Filter**: `df.filter(pl.col(col).is_in(values))` instead of `df.loc[df[col].isin(values)]`.
- **With column**: `df.with_columns(pl.lit(val).alias(name))` instead of `df[name] = val`.
- **Cast**: `df.cast({col: pl.Utf8})` instead of `df.astype({col: dtype})`.

### Pitfalls to Avoid

- Do NOT convert the metadata DataFrame to Polars -- it is built row-by-row and exported once. Keep it as pandas with `synthetize_df()`.
- Do NOT change the `SpatialResolution` column lists (`all_synthesis_df_columns`, `sorting_synthesis_df_columns`) -- those are string lists that work with both pandas and Polars.
- `pl.DataFrame.unique()` on a column returns a Series, not a list. Use `.to_list()` to convert.
- `pl.concat` requires all DataFrames to have the same schema. If stats DataFrames have slightly different columns, use `how="diagonal"` or ensure schema consistency.
- The `STRING_DF_TYPE` constant in `app/internal/constants.py` is `pandas.StringDtype(storage="pyarrow")` -- this is pandas-specific. For Polars, use `pl.Utf8` directly. Do NOT import or use `STRING_DF_TYPE` in Polars code paths.
- The `PANDAS_GROUPING_ENGINE` constant is no longer needed after Polars migration. It can be kept in `constants.py` for backward compatibility but should not be referenced from any Polars code path.

## Out of Scope

- Migrating system synthesis (`app/services/synthesis/system.py`) or execution synthesis (`app/services/synthesis/execution.py`) to Polars -- those are simpler, lower-priority, and can be done later.
- Removing pandas as a dependency (it is still needed at the idessem boundary and for metadata export).
- Parallelism (ticket-023 in Epic 06).

## Testing Requirements

### Unit Tests

- Update test assertions that check DataFrame types from the synthesis pipeline.
- Verify that exported Parquet files have identical column names and compatible schemas to pre-migration output.

### Integration Tests

- Run the full test suite (`python -m pytest tests/`). All 82 tests must pass.

### E2E Tests

- If an end-to-end test dataset is available, run the full synthesis pipeline and compare output Parquet file schemas and row counts against a baseline from before the migration.

## Dependencies

- **Blocked By**: ticket-021-migrate-deck-hot-paths-to-polars.md
- **Blocks**: ticket-023-add-multiprocessing-logger.md (Epic 06)

## Effort Estimate

**Points**: 4
**Confidence**: Medium
