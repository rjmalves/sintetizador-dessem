# ticket-013 Extract Thermal and System Modules from Deck

## Context

### Background

After extracting accessors, temporal, entities, and hydro modules, the remaining deck.py methods deal with thermal data, system-level data (pdo_sist, pdo_operacao, pdo_inter, pdo_eolica), and thermal bounds. These should be split into `thermal.py` and `system.py` modules.

### Relation to Epic

Sixth ticket in Epic 3. Extracts the remaining domain-specific methods.

### Current State

Remaining domain methods in `deck.py` after tickets 009-012:

- **Thermal**: `pdo_oper_uct` (line 616), `pdo_oper_term` (line 640), `pdo_oper_term_ute` (line 1300), `thermal_costs` (line 1364), `_group_thermal_bounds_df` (line 1408), `thermal_generation_bounds` (line 1448)
- **System**: `pdo_sist` (line 292), `pdo_eolica` (line 454), `pdo_inter` (line 503), `pdo_operacao` (line 686), `pdo_sist_sbm` (line 1020), `pdo_sist_sin` (line 1044), `pdo_eolica_sbm` (line 1221), `pdo_eolica_sin` (line 1245), `pdo_inter_sbp` (line 1275), `pdo_operacao_costs` (line 1327)

## Specification

### Requirements

1. Create `app/services/deck/thermal.py` with:
   - `pdo_oper_uct(cls, cache, uow) -> pd.DataFrame`
   - `pdo_oper_term(cls, cache, uow) -> pd.DataFrame`
   - `pdo_oper_term_ute(col, cls, cache, uow) -> pd.DataFrame`
   - `thermal_costs(cls, cache, uow) -> pd.DataFrame`
   - `_group_thermal_bounds_df(...)` helper
   - `thermal_generation_bounds(cls, cache, uow) -> pd.DataFrame`
2. Create `app/services/deck/system.py` with:
   - `pdo_sist(cls, cache, uow) -> pd.DataFrame`
   - `pdo_eolica(cls, cache, uow) -> pd.DataFrame`
   - `pdo_inter(cls, cache, uow) -> pd.DataFrame`
   - `pdo_operacao(cls, cache, uow) -> PdoOperacao`
   - `pdo_sist_sbm(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_sist_sin(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_eolica_sbm(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_eolica_sin(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_inter_sbp(col, cls, cache, uow) -> pd.DataFrame`
   - `pdo_operacao_costs(col, cls, cache, uow) -> pd.DataFrame`
3. Update `deck.py` to delegate all remaining domain methods
4. After this ticket, `deck.py` should contain only the Deck class with delegation methods and the `DECK_DATA_CACHING` dict

### Inputs/Props

- Source: `app/services/deck/deck.py` remaining domain methods

### Outputs/Behavior

- `thermal.py` contains all thermal-related data extraction and bounds
- `system.py` contains all system-level data extraction (pdo_sist, pdo_eolica, pdo_inter, pdo_operacao)
- `deck.py` is reduced to a thin facade

### Error Handling

- Preserve all existing error handling

## Acceptance Criteria

- [ ] Given `app/services/deck/thermal.py`, when listing its functions, then `pdo_oper_term_ute`, `thermal_costs`, and `thermal_generation_bounds` are present
- [ ] Given `app/services/deck/system.py`, when listing its functions, then `pdo_sist_sbm`, `pdo_sist_sin`, `pdo_eolica_sbm`, `pdo_inter_sbp`, and `pdo_operacao_costs` are present
- [ ] Given the project, when running `uv run pytest ./tests`, then all tests pass
- [ ] Given `deck.py`, when searching for function bodies longer than 5 lines, then none exist (all methods are single-line delegations)

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/thermal.py` -- start with simpler methods (pdo_oper_term, pdo_oper_uct)
2. Create `app/services/deck/system.py` -- start with pdo_sist, pdo_eolica
3. Move the aggregation methods (pdo_sist_sbm, pdo_sist_sin, etc.) into the appropriate module
4. Update deck.py delegation
5. Run tests after each batch

### Key Files to Modify

- `app/services/deck/thermal.py` (create)
- `app/services/deck/system.py` (create)
- `app/services/deck/deck.py` (reduce)

### Patterns to Follow

Same accessor pattern as previous tickets. Functions accept `(cls, cache, uow)`.

### Pitfalls to Avoid

- `pdo_sist` (line 292) uses `_add_single_scenario` and `_add_submarket_code` helpers -- ensure they are importable from temporal.py or a shared helpers module
- `pdo_eolica` depends on `pdo_sist` for submarket name resolution -- ensure system.py can reference its own methods
- `pdo_oper_term` has a complex block mapping pattern that uses `stage_block_map` -- import from temporal

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Verify all `Deck.pdo_sist_sbm()`, `Deck.pdo_oper_term_ute()`, etc. still work correctly.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-012-extract-hydro-module.md
- **Blocks**: ticket-014-reduce-deck-to-facade.md

## Effort Estimate

**Points**: 3
**Confidence**: High
