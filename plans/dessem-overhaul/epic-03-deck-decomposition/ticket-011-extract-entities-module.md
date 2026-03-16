# ticket-011 Extract Entities Module from Deck

## Context

### Background

The `Deck` class contains entity mapping methods that extract and organize DESSEM entities (hydro plants, thermal plants, submarkets, EERs) from input files. These should be extracted to a dedicated `entities.py` module.

### Relation to Epic

Fourth ticket in Epic 3. Extracts entity mapping logic.

### Current State

Entity methods in `deck.py`:

- `eer_submarket_map` (line 851) - EER to submarket mapping
- `hydro_eer_map` (line 886) - hydro to EER mapping
- `hydro_eer_submarket_map` (line 906) - hydro to EER to submarket mapping
- `hydro_initial_volumes` (line 922) - initial volume data
- `thermals` (line 955) - thermal plant list
- `submarkets` (line 994) - submarket list

## Specification

### Requirements

1. Create `app/services/deck/entities.py` with functions:
   - `eer_submarket_map(cls, cache, uow) -> pd.DataFrame`
   - `hydro_eer_map(cls, cache, uow) -> pd.DataFrame`
   - `hydro_eer_submarket_map(cls, cache, uow) -> pd.DataFrame`
   - `hydro_initial_volumes(cls, cache, uow) -> pd.DataFrame`
   - `thermals(cls, cache, uow) -> pd.DataFrame`
   - `submarkets(cls, cache, uow) -> pd.DataFrame`
2. Update `deck.py` to delegate entity methods to `entities.py`
3. All callers of `Deck.thermals()`, `Deck.submarkets()`, etc. continue unchanged

### Inputs/Props

- Source: `app/services/deck/deck.py` lines 851-1020

### Outputs/Behavior

- `entities.py` contains all entity extraction logic
- `deck.py` entity methods are one-line delegations

### Error Handling

- Preserve existing RuntimeError on missing data

## Acceptance Criteria

- [ ] Given the file `app/services/deck/entities.py`, when listing its functions, then `eer_submarket_map`, `hydro_eer_map`, `hydro_eer_submarket_map`, `thermals`, and `submarkets` are present
- [ ] Given `deck.py`, when reading the `thermals` method body, then it delegates to `entities.thermals`
- [ ] Given the project, when running `uv run pytest ./tests`, then all tests pass
- [ ] Given `app/services/synthesis/system.py`, when calling `Deck.thermals(uow)`, then the result is identical to before extraction

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/entities.py`
2. Extract each entity method, converting from classmethod to module-level function
3. Each function accepts `(cls, cache, uow)` following the accessor pattern, or just `(uow)` if no caching is needed
4. Update deck.py delegation methods
5. Run tests after each extraction

### Key Files to Modify

- `app/services/deck/entities.py` (create)
- `app/services/deck/deck.py` (reduce)

### Patterns to Follow

Follow newave's entities.py pattern. Functions read from entdados/dadvaz via the accessors, not directly from uow.files.

### Pitfalls to Avoid

- `hydro_eer_submarket_map` depends on `eer_submarket_map` and `hydro_eer_map` -- extract them in the right order
- `thermals` reads from `entdados` -- use accessors to get entdados, not direct file access
- Do NOT extract `hydro_inflows` here (it belongs in hydro.py, ticket-012)

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Verify `Deck.eer_submarket_map(uow)` and `Deck.thermals(uow)` return correct DataFrames.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-010-extract-temporal-module.md
- **Blocks**: ticket-012-extract-hydro-module.md

## Effort Estimate

**Points**: 2
**Confidence**: High
