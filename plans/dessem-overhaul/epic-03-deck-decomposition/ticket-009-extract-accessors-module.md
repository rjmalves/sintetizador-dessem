# ticket-009 Extract Accessors Module from Deck

## Context

### Background

The `Deck` class in `app/services/deck/deck.py` contains 15 `_get_*` methods (lines 60-153) that retrieve file objects from the unit of work, and ~10 cached accessor methods (lines 162-268) that use `_validate_data` and `DECK_DATA_CACHING`. These should be extracted to a dedicated `accessors.py` module following newave's pattern where accessors handle file access with caching.

### Relation to Epic

Second ticket in Epic 3. After this, the file access and caching logic is isolated from domain-specific data extraction.

### Current State

`deck.py` lines 60-268 contain:

- `_get_entdados`, `_get_dessemarq`, `_get_dadvaz`, `_get_log_matriz`, `_get_des_log_relato`, `_get_pdo_sist`, `_get_pdo_inter`, `_get_pdo_hidr`, `_get_pdo_eolica`, `_get_pdo_operacao`, `_get_pdo_oper_uct`, `_get_pdo_oper_term`, `_get_pdo_oper_tviag_calha`, `_get_pdo_eco_usih`, `_get_operuh` (15 getter methods)
- `_validate_data` classmethod
- `entdados`, `dadvaz`, `log_matriz`, `des_log_relato`, `runtimes`, `costs` public classmethods with caching

## Specification

### Requirements

1. Create `app/services/deck/accessors.py` containing:
   - All `_get_*` private methods extracted as module-level functions that accept `(cls, uow)` parameters (or the deck class and cache dict, following newave's pattern)
   - `_validate_data` function
   - Cached accessor functions for `entdados`, `dadvaz`, `log_matriz`, `des_log_relato`
2. Update `deck.py` to import from `accessors.py` and delegate calls
3. All existing Deck method signatures remain unchanged (Deck is still the public API)
4. `DECK_DATA_CACHING` dict stays on the Deck class; accessor functions receive it as parameter

### Inputs/Props

- Source: `app/services/deck/deck.py` lines 54-268
- Reference: `/home/rogerio/git/sintetizador-newave/app/services/deck/accessors.py`

### Outputs/Behavior

- `accessors.py` contains all file access and caching logic
- `deck.py` delegates to `accessors.py` for file access
- All existing callers of `Deck._get_*` and `Deck.entdados()` etc. work without changes

### Error Handling

- Preserve existing error handling (RuntimeError on validation failure, logging)

## Acceptance Criteria

- [ ] Given the file `app/services/deck/accessors.py`, when counting its functions, then it contains at least 15 getter functions and 4 cached accessor functions
- [ ] Given `deck.py`, when searching for `_get_entdados` method body, then it delegates to `accessors._get_entdados` (or similar)
- [ ] Given the project, when running `uv run pytest ./tests`, then all existing tests pass
- [ ] Given the project, when running `uv run python -c "from app.services.deck.deck import Deck"`, then no import errors occur

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/accessors.py`
2. Move the `_get_*` methods from `Deck` class to module-level functions in accessors.py. Each function takes `(uow: AbstractUnitOfWork)` as parameter and uses `with uow:` to access `uow.files.*`
3. Move `_validate_data` to accessors.py as a module-level function
4. Move the cached accessor bodies (entdados, dadvaz, etc.) to accessors.py. They should accept `(cls, cache_dict, uow)` parameters, following newave's pattern
5. Update `deck.py` to import from accessors and delegate. Example:

   ```python
   from app.services.deck import accessors

   class Deck:
       @classmethod
       def entdados(cls, uow):
           return accessors.entdados(cls, cls._c(), uow)
   ```

6. Run tests after each moved function to catch breakage early

### Key Files to Modify

- `app/services/deck/accessors.py` (create)
- `app/services/deck/deck.py` (reduce)

### Patterns to Follow

Follow newave's accessors.py pattern where functions accept `(cls, cache, uow)` and handle caching internally.

### Pitfalls to Avoid

- Do NOT change the public API of `Deck` class -- all classmethods must retain their signatures
- Do NOT move domain-specific methods (pdo_sist_sbm, pdo_hidr_hydro, etc.) -- those go to other modules in later tickets
- Keep imports minimal in accessors.py -- only idessem types and UoW

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Verify `Deck.entdados(uow)` and `Deck.dadvaz(uow)` still work by importing and calling them in a test.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-008-create-deck-context-dataclass.md
- **Blocks**: ticket-010-extract-temporal-module.md

## Effort Estimate

**Points**: 2
**Confidence**: High
