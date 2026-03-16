# ticket-017 Extract Spatial Resolution Modules

## Context

### Background

The `_monolith.py` file contains 13 resolution methods that read data from different DESSEM output files (pdo_sist, pdo_hidr, pdo_eolica, pdo_oper_term, pdo_operacao, pdo_inter, pdo_oper_tviag_calha) and return DataFrames for specific spatial resolutions (SIN, SBM, UHE, UTE, SBP). These methods follow a uniform pattern: call a `Deck.*` accessor, apply `_post_resolve_file`, and return the result. Extracting them into resolution modules reduces `_monolith.py` further and groups related data access logic together.

Unlike sintetizador-newave which has one module per spatial resolution (resolution_sin.py, resolution_sbm.py, etc.) with complex per-entity logic, DESSEM's resolution functions are simpler -- they are thin wrappers around `Deck.*` methods. Rather than creating 5+ tiny modules (most under 30 lines), the DESSEM-specific approach is to create a single `resolution.py` module that groups all resolution functions, plus a `pipeline.py` module containing the dispatch dict `_resolve`, `_post_resolve_file`, `_post_resolve`, `_resolve_synthesis`, `_resolve_bounds`, `_resolve_stub`, and `_stub_mappings`.

### Relation to Epic

This is the third ticket in Epic 04. It extracts the resolution functions and pipeline logic from `_monolith.py`, leaving only the orchestration methods (synthetize, preprocessing, single-variable synthesis) for ticket-018.

### Current State

After ticket-016, `_monolith.py` still contains:

- 13 resolution methods (lines ~286-440): `_resolve_pdo_sist_sbm`, `_resolve_pdo_sist_sin`, `_resolve_pdo_hidr_uhe`, `_resolve_pdo_hidr_eer`, `_resolve_pdo_hidr_sbm`, `_resolve_pdo_hidr_sin`, `_resolve_pdo_eolica_sbm`, `_resolve_pdo_eolica_sin`, `_resolve_pdo_oper_term_ute`, `_resolve_pdo_operacao_costs`, `_resolve_pdo_inter_sbp`, `_resolve_pdo_oper_tviag_calha_uhe`, `_resolve_thermal_submarkets_pdo_sist_sbm`, `_resolve_hydro_submarkets_pdo_sist_sbm`
- Pipeline methods: `_post_resolve_file` (line 277-283), `_post_resolve` (lines 623-661), `_resolve_synthesis` (lines 664-675), `_resolve_bounds` (lines 601-620), `_resolve_stub` (lines 556-573), `_stub_mappings` (lines 546-553)
- Dispatch dict: `_resolve` method (lines 68-274) mapping `(Variable, SpatialResolution)` tuples to resolution lambdas
- Utility methods: `_get_unique_column_values_in_order` (lines 503-510), `_set_ordered_entities` (lines 513-519), `_get_ordered_entities` (lines 522-526)

## Specification

### Requirements

1. Create `app/services/synthesis/operation/resolution.py` containing all 13 resolution functions as module-level functions (without `cls` parameter since they do not access class state -- they only use `Deck.*` and `_post_resolve_file`).
2. Create `app/services/synthesis/operation/pipeline.py` containing:
   - `post_resolve_file(df)` -- column filtering
   - `resolve_dispatch(synthesis)` -- the dispatch dict (currently `_resolve`)
   - `post_resolve(cls, df, s, uow, early_hooks, late_hooks)` -- post-processing
   - `resolve_synthesis(cls, s, uow)` -- full resolution flow
   - `resolve_bounds(cls, s, df, uow)` -- bounds computation
   - `resolve_stub(cls, s, uow)` -- stub resolution
   - `stub_mappings(cls, s)` -- stub dispatch
   - `get_unique_column_values_in_order(df, cols)` -- utility
   - `set_ordered_entities(cls, s, entities)` -- utility
   - `get_ordered_entities(cls, s)` -- utility
3. Replace extracted method bodies in `_monolith.py` with one-line delegations.
4. All existing tests pass without modification.

### Inputs/Props

- `_monolith.py` with cache/export already delegated (from ticket-016).

### Outputs/Behavior

- `resolution.py` contains 13 functions, each taking `(uow, col)` as parameters (plus optionally logger) and returning `pd.DataFrame`.
- `pipeline.py` contains the dispatch dict and pipeline logic.
- `_monolith.py` methods delegate to these modules.

### Error Handling

- All existing error handling is preserved in the extracted functions. The `_resolve_pdo_hidr_uhe` function retains the `~df[VALUE_COL].isna()` filter. The `_resolve_synthesis` method retains the `if df is not None` check.

## Acceptance Criteria

- [ ] Given the package exists, when `app/services/synthesis/operation/resolution.py` is created, then it contains functions `resolve_pdo_sist_sbm`, `resolve_pdo_sist_sin`, `resolve_pdo_hidr_uhe`, `resolve_pdo_hidr_eer`, `resolve_pdo_hidr_sbm`, `resolve_pdo_hidr_sin`, `resolve_pdo_eolica_sbm`, `resolve_pdo_eolica_sin`, `resolve_pdo_oper_term_ute`, `resolve_pdo_operacao_costs`, `resolve_pdo_inter_sbp`, `resolve_pdo_oper_tviag_calha_uhe`, `resolve_thermal_submarkets_pdo_sist_sbm`, and `resolve_hydro_submarkets_pdo_sist_sbm`.
- [ ] Given the package exists, when `app/services/synthesis/operation/pipeline.py` is created, then it contains functions `post_resolve_file`, `resolve_dispatch`, `post_resolve`, `resolve_synthesis`, `resolve_bounds`, `resolve_stub`, `stub_mappings`, `get_unique_column_values_in_order`, `set_ordered_entities`, `get_ordered_entities`.
- [ ] Given `pipeline.py` exists, when `resolve_dispatch((Variable.CUSTO_OPERACAO, SpatialResolution.SISTEMA_INTERLIGADO))` is called, then it returns a callable that, when invoked with `uow`, returns a DataFrame.
- [ ] Given all changes are applied, when running `python -m pytest tests/app/services/synthesis/test_operation.py -x`, then all tests pass.

## Implementation Guide

### Suggested Approach

1. **Create `resolution.py`**: Extract each `_resolve_pdo_*` method as a module-level function. Since these methods do not access class state (they use `Deck.*` and `_post_resolve_file`), they do NOT need `cls`. They do need access to a logger and to `_post_resolve_file`, so:
   - Import `post_resolve_file` from `pipeline.py` (or define it locally if needed to avoid circular deps).
   - Accept an optional `logger` parameter or import the `time_and_log` context manager.
   - The two submarket-filtering methods (`_resolve_thermal_submarkets_pdo_sist_sbm` and `_resolve_hydro_submarkets_pdo_sist_sbm`) call `_resolve_pdo_sist_sbm` internally, so they should call the local `resolve_pdo_sist_sbm` function.

   Since `_post_resolve_file` is a simple utility (3 lines), place it in `pipeline.py` and import it in `resolution.py`.

   Example function:

   ```python
   def resolve_pdo_sist_sbm(
       uow: AbstractUnitOfWork, col: str, logger: logging.Logger | None = None
   ) -> pd.DataFrame:
       with time_and_log(
           message_root="Tempo para obtencao dos dados do pdo_sist para SBM",
           logger=logger,
       ):
           df = Deck.pdo_sist_sbm(col, uow)
           return post_resolve_file(df)
   ```

2. **Create `pipeline.py`**: Extract the dispatch dict and pipeline methods. The `resolve_dispatch` function replaces the class method `_resolve`. The lambdas in the dispatch dict now call `resolution.*` functions instead of `cls._resolve_*`:

   ```python
   def resolve_dispatch(
       synthesis: tuple[Variable, SpatialResolution],
       logger: logging.Logger | None = None,
   ) -> Callable:
       _rules = {
           (Variable.CUSTO_OPERACAO, SpatialResolution.SISTEMA_INTERLIGADO):
               lambda uow: resolution.resolve_pdo_operacao_costs(uow, "custo_presente", logger),
           # ... etc
       }
       return _rules[synthesis]
   ```

   Also extract: `post_resolve_file`, `post_resolve`, `resolve_synthesis`, `resolve_bounds`, `resolve_stub`, `stub_mappings`, and the ordered-entity helpers. The pipeline functions that access class state (`ORDERED_SYNTHESIS_ENTITIES`, etc.) take `cls` as the first parameter.

3. **Update `_monolith.py`**: Replace each extracted method with a one-line delegation:

   ```python
   from app.services.synthesis.operation import pipeline as _pipeline_mod
   from app.services.synthesis.operation import resolution as _resolution_mod

   @classmethod
   def _resolve(cls, synthesis):
       return _pipeline_mod.resolve_dispatch(synthesis, cls.logger)

   @classmethod
   def _resolve_pdo_sist_sbm(cls, uow, col):
       return _resolution_mod.resolve_pdo_sist_sbm(uow, col, cls.logger)
   ```

4. **Run tests** to confirm all pass.

### Key Files to Modify

- `app/services/synthesis/operation/resolution.py` -- create (new file)
- `app/services/synthesis/operation/pipeline.py` -- create (new file)
- `app/services/synthesis/operation/_monolith.py` -- replace method bodies with delegations

### Patterns to Follow

- Follow newave's pattern of module-level functions. Functions that access class state receive `cls` as the first parameter; functions that do not (pure data access like resolution functions) omit `cls`.
- Use `TYPE_CHECKING` guard for `OperationSynthetizer` type hints in `pipeline.py`.
- Keep `_post_resolve_file` in `pipeline.py` since it is a shared utility used by all resolution functions.

### Pitfalls to Avoid

- Do NOT create separate files per spatial resolution (resolution_sin.py, resolution_sbm.py, etc.) -- DESSEM's resolution functions are too simple (3-5 lines each) to warrant individual files. A single `resolution.py` is cleaner.
- Do NOT change the dispatch dict keys or values -- the `(Variable, SpatialResolution)` tuples must remain identical.
- Do NOT add DeckContext or ProcessPoolExecutor parameters -- those are Epic 5/6 concerns.
- Be careful with the lambda closures in the dispatch dict: ensure `logger` is captured correctly (pass it as a parameter, not as a closure over a mutable class attribute).

## Testing Requirements

### Unit Tests

- No new tests needed. The extraction is a pure refactor.

### Integration Tests

- Run `python -m pytest tests/app/services/synthesis/test_operation.py -x` to confirm all ~30 operation synthesis tests pass.

### E2E Tests

- Not applicable.

## Dependencies

- **Blocked By**: ticket-016-extract-operation-cache-and-export.md
- **Blocks**: ticket-018-create-operation-orchestrator.md

## Effort Estimate

**Points**: 3
**Confidence**: High
