# ticket-014 Reduce Deck to Facade and Update All Imports

## Context

### Background

After tickets 009-013, all domain logic has been extracted to submodules (accessors, temporal, entities, hydro, thermal, system). This ticket cleans up `deck.py` to be a pure facade, ensures all imports across the codebase are correct, and verifies there are no dead code or circular imports.

### Relation to Epic

Final ticket in Epic 3. Polishes the decomposition and ensures everything is wired correctly.

### Current State

After tickets 009-013, `deck.py` should already be mostly delegation methods. This ticket does the final cleanup:

- Remove any remaining helper methods or inline logic
- Ensure consistent import style (all via `from app.services.deck import module`)
- Verify all callers of Deck methods throughout the codebase still work
- Remove unused imports from deck.py

## Specification

### Requirements

1. Audit `deck.py` and remove any remaining non-delegation code
2. Ensure `deck.py` follows a consistent pattern: each method is a one-line delegation to the appropriate submodule
3. Verify all callers of Deck methods across the codebase:
   - `app/services/synthesis/operation.py` (calls pdo_sist_sbm, pdo_hidr_hydro, etc.)
   - `app/services/synthesis/system.py` (calls stages_durations, thermals, etc.)
   - `app/services/synthesis/execution.py` (calls version, title, costs, runtimes)
   - `app/services/deck/bounds.py` (calls thermal_generation_bounds, hydro_generation_bounds, etc.)
4. Remove unused imports from `deck.py`
5. Verify no circular imports exist by importing each submodule independently
6. Verify deck.py is <= 250 lines

### Inputs/Props

- All deck submodules created in tickets 008-013
- All callers of Deck in synthesis and bounds modules

### Outputs/Behavior

- `deck.py` is a clean facade with only delegation methods
- All imports across the codebase work correctly
- No circular imports
- All tests pass

### Error Handling

- N/A (this is a cleanup ticket)

## Acceptance Criteria

- [ ] Given `deck.py`, when counting its lines with `wc -l`, then the count is <= 250
- [ ] Given `deck.py`, when searching for `import pandas` or `import numpy`, then neither is found (the facade should not need these)
- [ ] Given the project, when running `uv run pytest ./tests`, then all tests pass
- [ ] Given the project, when running `uv run python -c "from app.services.deck.deck import Deck; from app.services.deck.accessors import *; from app.services.deck.temporal import *; from app.services.deck.entities import *; from app.services.deck.hydro import *; from app.services.deck.thermal import *; from app.services.deck.system import *"`, then no import errors occur

## Implementation Guide

### Suggested Approach

1. Review deck.py and identify any remaining non-delegation code
2. Move any stragglers to the appropriate submodule
3. Clean up imports in deck.py -- it should only import from its submodules
4. Grep the entire codebase for `from app.services.deck.deck import` to verify all callers use the Deck facade
5. Run the circular import test by importing each submodule
6. Run full test suite

### Key Files to Modify

- `app/services/deck/deck.py` (final cleanup)

### Patterns to Follow

The final deck.py should look like newave's deck.py: a Deck class with `DECK_DATA_CACHING` dict, `_log` method, `_c` cache accessor, and one-line delegation classmethods.

### Pitfalls to Avoid

- Do NOT change the public API -- all existing `Deck.method_name()` calls must continue to work
- Verify bounds.py still works since it imports from Deck directly
- If any test fails, it likely means a method was not properly delegated

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Run `uv run mypy ./app` to verify type checking still passes.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-013-extract-thermal-and-system-modules.md
- **Blocks**: ticket-015-create-operation-synthesis-package.md (Epic 4)

## Effort Estimate

**Points**: 1
**Confidence**: High
