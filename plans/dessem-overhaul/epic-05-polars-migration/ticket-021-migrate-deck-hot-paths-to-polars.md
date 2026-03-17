# ticket-021 Migrate Deck Hot Paths to Polars

## Context

### Background

Tickets 019-020 added Polars as a dependency and established the native Polars Parquet export path. The deck submodules (`hydro.py`, `system.py`, `thermal.py`, `entities.py`, `temporal.py`) currently use pandas for all DataFrame operations. The hot-path functions -- `pdo_hidr()`, `pdo_sist()`, `pdo_oper_term()`, `pdo_eolica()`, `pdo_inter()`, and their aggregation variants -- perform heavy groupby, merge, rename, filter, and column arithmetic operations that will benefit from Polars' lazy evaluation and columnar engine.

### Relation to Epic

This is the third ticket in Epic 05. It converts the deck-layer DataFrame processing from pandas to Polars. All deck functions that return DataFrames used by the operation synthesis pipeline must return `pl.DataFrame` after this ticket. The subsequent ticket-022 will update the operation synthesis pipeline to consume these Polars DataFrames and export via `synthetize_pl()`.

### Current State

The deck submodules are organized as follows (after Epic 03 decomposition):

- `app/services/deck/accessors.py` -- raw file getters; return idessem objects (these stay pandas since idessem returns `pd.DataFrame`).
- `app/services/deck/temporal.py` -- stage/block/date calculations. Functions like `stages_durations()`, `add_single_scenario()`, `add_submarket_code()`, `date_arrays()`, `block_map()`, `stage_block_map()`. All return/operate on `pd.DataFrame`.
- `app/services/deck/entities.py` -- entity maps (hydro/EER/submarket/thermal). Functions like `eer_submarket_map()`, `hydro_eer_map()`, `thermals()`, `submarkets()`. All return `pd.DataFrame`.
- `app/services/deck/hydro.py` -- `pdo_hidr()`, `pdo_eco_usih()`, aggregation functions, bounds functions. Heavy use of `pd.merge`, `groupby`, `np.tile`, `np.repeat`, `np.concatenate`.
- `app/services/deck/system.py` -- `pdo_sist()`, `pdo_eolica()`, `pdo_inter()`, aggregation functions. Uses `groupby`, `apply` with `partial(date_arrays)`, `np.repeat`.
- `app/services/deck/thermal.py` -- `pdo_oper_term()`, `pdo_oper_uct()`, `thermal_costs()`, bounds. Uses `groupby`, `np.repeat`, `fast_group_df()`.
- `app/services/deck/bounds.py` -- `OperationVariableBounds` class with merge-based bounds calculation. Uses `pd.merge`, `fast_group_df()`.
- `app/services/deck/context.py` -- `DeckContext` dataclass holding cached DataFrames.
- `app/utils/operations.py` -- `fast_group_df()` and `calc_statistics()`.

Key constraints:

- idessem returns `pd.DataFrame` from `.tabela`, `.discretizacao`, etc. The conversion `pd.DataFrame -> pl.DataFrame` must happen right after idessem data is received.
- The deck functions are cached via `cache: Dict[str, Any]` -- cached values will now be `pl.DataFrame`.
- `np.tile`, `np.repeat`, `np.concatenate` patterns must be replaced with Polars equivalents (`pl.concat`, `join`, `with_columns`).
- The `date_arrays()` function using `df.apply()` row-by-row is a major anti-pattern that must be replaced with a Polars join.

## Specification

### Requirements

1. Convert all deck submodule functions (`hydro.py`, `system.py`, `thermal.py`, `entities.py`, `temporal.py`) to return `pl.DataFrame` instead of `pd.DataFrame`.
2. At the idessem boundary (in each function that reads from `file.tabela`, `file.discretizacao`, etc.), convert `pd.DataFrame` to `pl.DataFrame` via `pl.from_pandas()`.
3. Replace `pd.merge()` calls with `pl.DataFrame.join()`.
4. Replace `groupby().sum()/mean()/max()` with Polars `group_by().agg()`.
5. Replace `np.tile()` / `np.repeat()` patterns for date assignment with Polars `join()` on stage columns.
6. Replace `df.apply(partial(date_arrays, ...), axis=1)` row-by-row iteration with a Polars `join()` against the stages DataFrame.
7. Replace `pd.Timedelta(hours=1)` duration calculations with Polars duration arithmetic: `(col("data_fim") - col("data_inicio")).dt.total_hours()`.
8. Replace `df.rename(columns={...})` with `pl.DataFrame.rename({...})`.
9. Replace `df.loc[condition]` filtering with `pl.DataFrame.filter()`.
10. Replace `df.sort_values()` with `pl.DataFrame.sort()`.
11. Replace `np.concatenate` volume calculations in `hydro.py` `_get_initial_volume()` and `_cast_volumes_to_absolute()` with Polars column operations.
12. Update `DeckContext` dataclass to use `pl.DataFrame` type annotations.
13. Update `fast_group_df()` in `app/utils/operations.py` to operate on `pl.DataFrame`.
14. Replace `df.copy()` returns with direct returns (Polars DataFrames are immutable, no need for copies).
15. The `validate_data()` function in `accessors.py` should continue to validate idessem objects but NOT validate that the result is `pd.DataFrame` when the caller will immediately convert to Polars. Callers should validate the idessem object type, then access `.tabela` and convert.

### Inputs/Props

- All functions maintain the same signatures: `(deck_cls, cache, uow)` or `(col, deck_cls, cache, uow)`.
- Return types change from `pd.DataFrame` to `pl.DataFrame`.

### Outputs/Behavior

- All deck functions return `pl.DataFrame` with the same column names and semantics as before.
- Cache entries store `pl.DataFrame` instead of `pd.DataFrame`.
- The Deck facade class (`app/services/deck/deck.py`) returns `pl.DataFrame` from all its methods.

### Error Handling

- Same error handling pattern as current code (RuntimeError on missing data via `validate_data()`).

## Acceptance Criteria

- [ ] Given the file `app/services/deck/hydro.py`, when inspected, then `import polars as pl` is present and `pdo_hidr()` returns a `pl.DataFrame` (no `pd.DataFrame` operations remain in the function body except at the idessem boundary conversion).
- [ ] Given the file `app/services/deck/system.py`, when inspected, then `pdo_sist()`, `pdo_eolica()`, and `pdo_inter()` return `pl.DataFrame` and do not use `df.apply()` row-by-row iteration.
- [ ] Given the file `app/services/deck/temporal.py`, when inspected, then `stages_durations()` returns `pl.DataFrame` and `add_single_scenario()` operates on `pl.DataFrame`.
- [ ] Given the file `app/services/deck/context.py`, when inspected, then `DeckContext` fields are typed as `pl.DataFrame`.
- [ ] Given the file `app/utils/operations.py`, when inspected, then `fast_group_df()` accepts and returns `pl.DataFrame` using Polars `group_by().agg()`.
- [ ] Given the test suite, when `python -m pytest tests/` is run, then all 82 tests pass (tests may need updates to handle `pl.DataFrame` assertions instead of `pd.DataFrame`).

## Implementation Guide

### Suggested Approach

Work through the modules bottom-up by dependency order:

**Step 1: `temporal.py`** (no internal dependencies)

- Convert `stages_durations()`: after getting `pdo_op.discretizacao` (a pandas DF), convert with `pl.from_pandas()`, then `rename()`.
- Convert `add_single_scenario()`: `df.with_columns(pl.lit(1).alias(SCENARIO_COL))`.
- Convert `add_submarket_code()`: build the map dict as before, then use `df.with_columns(pl.col(submarket_name_col).replace(submarket_map).alias(submarket_code_col_new))`.
- Convert `block_map()`, `stage_block_map()`, `blocks_durations()`: these return dicts or `pl.DataFrame`.
- Replace `date_arrays()` row-by-row function: instead of `df.apply()`, callers should do a `join()` on `STAGE_COL` against `stages_durations()`. Remove `date_arrays()` and update callers in `system.py`.

**Step 2: `entities.py`** (depends on temporal, accessors)

- Convert all entity-map functions. Replace `pd.merge` with `join()`.
- `hydro_initial_volumes()`: replace `np.isnan()` with `pl.col(...).is_null()` or `pl.col(...).is_nan()`.

**Step 3: `hydro.py`** (depends on temporal, entities)

- `pdo_eco_usih()`: convert from pandas at the boundary, filter with `.filter()`.
- `pdo_hidr()`: the most complex function. Replace `np.tile`, `np.repeat`, `np.concatenate` with Polars joins and column operations. Replace the `df.loc[df[STAGE_COL] == 1]` count with `df.filter(pl.col(STAGE_COL) == 1).height`. Use `join()` against `stages_durations()` for date columns.
- `_get_initial_volume()`, `_cast_volumes_to_absolute()`: replace numpy array operations with Polars `with_columns()` expressions. The initial volume concatenation pattern should use `pl.concat([initial_series, shifted_series])`.
- Aggregation functions (`pdo_hidr_eer`, `pdo_hidr_sbm`, `pdo_hidr_sin`): replace `groupby().sum()` with `group_by().agg(pl.col(VALUE_COL).sum())`.
- Bounds functions: replace `pd.merge` with `join()`, `pd.DataFrame({...})` construction with `pl.DataFrame({...})`.

**Step 4: `system.py`** (depends on temporal, entities)

- `pdo_sist()`: replace `df.apply(partial(date_arrays, ...), axis=1)` with a `join()` against `stages_durations()` on `STAGE_COL`.
- `pdo_eolica()`: replace `groupby().sum()` and `np.repeat` with Polars equivalents.
- `pdo_inter()`: same pattern as pdo_sist.
- Aggregation functions: same pattern as hydro aggregations.

**Step 5: `thermal.py`** (depends on temporal, entities)

- `pdo_oper_uct()`, `pdo_oper_term()`: convert at boundary, replace `np.repeat` with join.
- `thermal_costs()`: convert groupby and rename.
- `_group_thermal_bounds_df()`: update to use `fast_group_df()` on Polars.

**Step 6: `context.py`**

- Change `DeckContext` field types from `pd.DataFrame` to `pl.DataFrame`.
- Update `import pandas as pd` to `import polars as pl`.

**Step 7: `app/utils/operations.py`**

- Rewrite `fast_group_df()` to use Polars `group_by().agg()`. The `engine` parameter is no longer needed.
- Rewrite `calc_statistics()` and `_calc_mean()` to use Polars `group_by().agg(pl.col(VALUE_COL).mean())`.

**Step 8: `bounds.py`**

- Rewrite `OperationVariableBounds` methods to use Polars `join()`, `with_columns()`, `filter()`.
- Replace `pd.merge()` with `join()`.
- Replace `np.round()` with `pl.col(...).round(decimals)`.
- Replace `fast_group_df()` calls -- these now operate on Polars.

**Step 9: Update tests**

- Tests that assert on `pd.DataFrame` results from Deck functions need updating to assert on `pl.DataFrame`. The test fixtures may need `pl.from_pandas()` conversions or Polars-native assertions.

### Key Files to Modify

- `app/services/deck/temporal.py`
- `app/services/deck/entities.py`
- `app/services/deck/hydro.py`
- `app/services/deck/system.py`
- `app/services/deck/thermal.py`
- `app/services/deck/context.py`
- `app/services/deck/bounds.py`
- `app/services/deck/accessors.py` (minor -- validate_data stays, but some type checks may need updating)
- `app/utils/operations.py`
- Test files that assert on deck function return types

### Patterns to Follow

- **Boundary conversion**: `df = pl.from_pandas(idessem_obj.tabela)` immediately after getting data from idessem.
- **Renaming**: `df = df.rename({"old_name": "new_name"})` (Polars rename takes a dict, not `columns=` kwarg).
- **Filtering**: `df = df.filter(pl.col("col") == value)` instead of `df.loc[df["col"] == value]`.
- **Groupby**: `df.group_by(cols).agg(pl.col(val_col).sum())` instead of `df.groupby(cols).sum()`.
- **Join**: `df.join(other, on="col", how="left")` instead of `pd.merge(df, other, on="col", how="left")`.
- **Duration**: `(pl.col("end") - pl.col("start")).dt.total_hours()` instead of `/ pd.Timedelta(hours=1)`.
- **No copies**: Polars DataFrames are immutable. Remove all `.copy()` calls on return values.
- **No inplace**: Polars has no `inplace=True`. All operations return new DataFrames.

### Pitfalls to Avoid

- Do NOT try to convert the hydro operative constraints functions (`_get_hydro_flow_operative_constraints` with its nested `iterrows()`) to pure Polars in one shot. The nested loop with cross-referencing is complex. Convert the outer structure to Polars but keep the inner constraint expansion logic using `.to_pandas()` locally for that specific function, converting back to Polars at the end. This function is rarely called and is not a hot path.
- Do NOT change column names -- the output schema must remain identical.
- Do NOT change the caching pattern (`cache: Dict[str, Any]`) -- just store `pl.DataFrame` instead of `pd.DataFrame`.
- Watch for `NaN` vs `null` differences: Polars uses `null` for missing values, not `NaN`. Functions that check `np.isnan()` must use `pl.col(...).is_null()` or `pl.col(...).is_nan()` depending on whether the original data uses float NaN or actual nulls.

## Out of Scope

- Operation synthesis pipeline migration (ticket-022).
- Changes to idessem (it continues to return pandas).
- The `synthetize_pl()` / `synthetize_df()` export methods (those are done in tickets 019-020).
- The hydro operative constraints `iterrows()` loop -- keep it in pandas internally; full Polars rewrite of that function can be a future optimization.

## Testing Requirements

### Unit Tests

- Update existing deck-related tests to assert `pl.DataFrame` types where they currently assert `pd.DataFrame`.
- Verify column names and row counts remain identical after migration.

### Integration Tests

- Run the full test suite (`python -m pytest tests/`). All 82 tests must pass.

### E2E Tests

- Not applicable (end-to-end synthesis flow is covered by ticket-022).

## Dependencies

- **Blocked By**: ticket-020-add-polars-export-repository.md
- **Blocks**: ticket-022-migrate-operation-synthesis-to-polars.md

## Effort Estimate

**Points**: 5
**Confidence**: Medium
