# ticket-010 Extract Temporal Module from Deck

## Context

### Background

The `Deck` class contains stage/date/block calculation methods that deal with temporal aspects of DESSEM data: `stages_durations`, `date_arrays`, `block_map`, `stage_block_map`, `blocks_durations`, and `version`/`title`. These should be extracted to a `temporal.py` module.

### Relation to Epic

Third ticket in Epic 3. Extracts temporal logic into its own module.

### Current State

Temporal methods in `deck.py`:

- `stages_durations` (line 718) - builds stage start/end date DataFrame
- `date_arrays` (line 739) - extracts start/end date arrays
- `block_map` (line 809) - maps blocks to durations
- `stage_block_map` (line 825) - maps stages to blocks
- `blocks_durations` (line 841) - block duration DataFrame
- `version` (line 753) - extracts DESSEM version string
- `title` (line 771) - extracts case title
- `_add_single_scenario` (line 268) - helper to add scenario column
- `_add_submarket_code` (line 273) - helper to add submarket code column

## Specification

### Requirements

1. Create `app/services/deck/temporal.py` with the following functions extracted from Deck:
   - `stages_durations(uow) -> pd.DataFrame`
   - `date_arrays(uow) -> tuple`
   - `block_map(uow) -> dict`
   - `stage_block_map(uow) -> dict`
   - `blocks_durations(uow) -> pd.DataFrame`
   - `version(uow) -> str`
   - `title(uow) -> str`
2. Move helper methods `_add_single_scenario` and `_add_submarket_code` to a shared location (either temporal.py or a new helpers module -- they are used by multiple domain modules)
3. Update `deck.py` to delegate temporal methods to `temporal.py`
4. All existing callers (system.py, execution.py, bounds.py) continue to call `Deck.stages_durations()` etc. unchanged

### Inputs/Props

- Source: `app/services/deck/deck.py` lines 268-293, 718-790, 809-851

### Outputs/Behavior

- `temporal.py` contains all stage/date/block logic
- `deck.py` temporal methods are one-line delegations

### Error Handling

- Preserve existing error handling (RuntimeError on missing data)

## Acceptance Criteria

- [ ] Given the file `app/services/deck/temporal.py`, when listing its public functions, then `stages_durations`, `blocks_durations`, `block_map`, `stage_block_map`, `version`, and `title` are present
- [ ] Given `deck.py`, when reading the `stages_durations` method body, then it delegates to `temporal.stages_durations`
- [ ] Given the project, when running `uv run pytest ./tests`, then all tests pass
- [ ] Given the project, when running `uv run python -c "from app.services.deck.temporal import stages_durations"`, then no import error occurs

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/temporal.py`
2. Extract methods one at a time, updating deck.py to delegate after each extraction
3. Run tests after each method extraction to catch breakage immediately
4. For `_add_single_scenario` and `_add_submarket_code`, keep them as module-level functions in temporal.py (or a separate helpers.py) since they are DataFrame transformation utilities used across multiple methods

### Key Files to Modify

- `app/services/deck/temporal.py` (create)
- `app/services/deck/deck.py` (reduce)

### Patterns to Follow

Functions accept `(cls_or_cache, uow)` or just `(uow)` following the accessor pattern.

### Pitfalls to Avoid

- The `stages_durations` method reads from `entdados` via `Deck.entdados(uow)` -- use a lazy import or pass the accessor function to avoid circular imports
- `_add_single_scenario` and `_add_submarket_code` are used by `pdo_sist`, `pdo_hidr`, etc. -- if moved to temporal.py, ensure those methods can import them
- Do NOT extract methods that belong to other domains (hydro, thermal, system)

## Testing Requirements

### Unit Tests

Run `uv run pytest ./tests` -- all tests must pass.

### Integration Tests

Verify `Deck.stages_durations(uow)` returns the same result as before.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-009-extract-accessors-module.md
- **Blocks**: ticket-011-extract-entities-module.md

## Effort Estimate

**Points**: 2
**Confidence**: High
