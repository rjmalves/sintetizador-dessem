# ticket-012 Extract Hydro Module from Deck

## Context

### Background

The `Deck` class contains a large block of hydro-related data extraction methods that deal with pdo_hidr, operuh, dadvaz, and hydro bounds calculations. These should be extracted to a dedicated `hydro.py` module.

### Relation to Epic

Fifth ticket in Epic 3. Extracts the largest domain-specific block from deck.py.

### Current State

Hydro methods in `deck.py`:

- `pdo_hidr` (line 327, ~130 lines) - reads and processes PDO_HIDR file, includes nested helpers `_get_initial_volume` and `_cast_volumes_to_absolute`
- `hydro_inflows` (line 790) - extracts hydro inflow data
- `pdo_hidr_hydro` (line 1074) - UHE-level hydro data extraction
- `pdo_hidr_eer` (line 1100) - EER-level hydro aggregation
- `pdo_hidr_sbm` (line 1132) - submarket-level hydro aggregation
- `pdo_hidr_sin` (line 1163) - SIN-level hydro aggregation
- `pdo_oper_tviag_calha` (line 549) - travel time channel data
- `pdo_oper_tviag_calha_hydro` (line 1193) - UHE-level channel data
- `pdo_eco_usih` (line 698) - hydro economic data
- All hydro bounds methods (lines 1485-1884): `hydro_generation_bounds`, `stored_volume_bounds`, `hydro_turbined_flow_bounds`, `hydro_outflow_bounds`, `hydro_spilled_flow_bounds`, and their internal helpers

## Specification

### Requirements

1. Create `app/services/deck/hydro.py` with:
   - `pdo_hidr(cls, cache, uow) -> pd.DataFrame` (including nested helpers)
   - `hydro_inflows(cls, cache, uow) -> pd.DataFrame`
   - `pdo_hidr_hydro(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_hidr_eer(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_hidr_sbm(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_hidr_sin(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_oper_tviag_calha(cls, cache, uow) -> pd.DataFrame`
   - `pdo_oper_tviag_calha_hydro(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_eco_usih(cls, cache, uow) -> pd.DataFrame`
   - Hydro bounds methods: `hydro_generation_bounds`, `stored_volume_bounds`, `hydro_turbined_flow_bounds`, `hydro_outflow_bounds`, `hydro_spilled_flow_bounds` and all their internal helpers
2. Update `deck.py` to delegate hydro methods to `hydro.py`
3. Preserve all existing method signatures on the Deck facade

### Inputs/Props

- Source: `app/services/deck/deck.py` lines 327-453, 549-616, 698-718, 790-809, 1074-1193, 1485-1884

### Outputs/Behavior

- `hydro.py` is the largest submodule (~700-800 lines), containing all hydro data extraction and bounds logic
- `deck.py` delegates all hydro methods to `hydro.py`

### Error Handling

- Preserve all existing error handling and logging

## Acceptance Criteria

- [ ] Given `app/services/deck/hydro.py`, when listing its functions, then `pdo_hidr`, `pdo_hidr_hydro`, `hydro_generation_bounds`, `stored_volume_bounds`, `hydro_turbined_flow_bounds` are present
- [ ] Given `deck.py`, when reading the `pdo_hidr_hydro` method, then it delegates to `hydro.pdo_hidr_hydro`
- [ ] Given the project, when running `uv run pytest ./tests`, then all tests pass
- [ ] Given `deck.py`, when counting its lines after this extraction, then it has decreased by at least 500 lines compared to before this ticket

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/hydro.py`
2. Start with simpler methods (hydro_inflows, pdo_eco_usih) and move to complex ones (pdo_hidr with nested helpers, bounds methods)
3. The nested functions `_get_initial_volume` and `_cast_volumes_to_absolute` inside `pdo_hidr` become regular functions in `hydro.py`
4. The complex bounds methods (lines 1561-1884) with `__hydro_operative_constraints_*` helpers should be moved together
5. Update deck.py to delegate after each batch
6. Run tests frequently

### Key Files to Modify

- `app/services/deck/hydro.py` (create)
- `app/services/deck/deck.py` (reduce significantly)

### Patterns to Follow

Follow the accessor pattern: functions accept `(cls, cache, uow)` for cached operations.

### Pitfalls to Avoid

- The `pdo_hidr` method has nested function definitions -- extract them as module-level functions
- The bounds methods reference `_get_hydro_flow_operative_constraints` which has deeply nested helpers -- move the entire block together
- `_group_thermal_bounds_df` (line 1408) belongs in thermal.py, not hydro.py -- do NOT move it here
- Hydro bounds methods depend on entities (hydro_eer_submarket_map) -- use imports from entities or Deck facade

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Verify `Deck.pdo_hidr_hydro("geracao", uow)` and `Deck.hydro_generation_bounds(uow)` return correct results.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-011-extract-entities-module.md
- **Blocks**: ticket-013-extract-thermal-and-system-modules.md

## Effort Estimate

**Points**: 3
**Confidence**: High
