# ticket-008 Create DeckContext Dataclass

## Context

### Background

sintetizador-newave introduced a `DeckContext` dataclass that bundles frequently-accessed deck state (block lengths, scenario counts, date arrays, entity maps) into a single object, reducing repeated file reads and providing a clean interface for synthesis modules. sintetizador-dessem needs a similar pattern adapted for DESSEM's domain.

### Relation to Epic

First ticket in Epic 3. Creates the DeckContext foundation that subsequent decomposition tickets will populate and use.

### Current State

No `context.py` exists in `app/services/deck/`. The `Deck` class at `app/services/deck/deck.py` uses `DECK_DATA_CACHING: Dict[str, Any] = {}` for ad-hoc caching. Frequently-accessed data includes stage durations, block mappings, submarket lists, EER maps, and hydro-EER-submarket maps.

## Specification

### Requirements

1. Create `app/services/deck/__init__.py` (empty, package marker) if it does not already exist
2. Create `app/services/deck/context.py` with a `DeckContext` dataclass containing:
   - `stages_durations: pd.DataFrame` - stage start/end dates
   - `blocks_durations: pd.DataFrame` - block durations per stage
   - `eer_submarket_map: pd.DataFrame` - EER to submarket mapping
   - `hydro_eer_submarket_map: pd.DataFrame` - hydro plant to EER to submarket mapping
   - `submarkets: pd.DataFrame` - submarket code/name list
   - `thermals: pd.DataFrame` - thermal plant list with submarket codes
3. Add `__post_init__` validation that raises `ValueError` if any field is None
4. Add `from_deck` classmethod that constructs the context from the existing `Deck` class methods

### Inputs/Props

- Reference: `/home/rogerio/git/sintetizador-newave/app/services/deck/context.py`

### Outputs/Behavior

- `DeckContext.from_deck(uow)` returns a populated context object
- All fields are validated non-None at construction time

### Error Handling

- `__post_init__` raises `ValueError` with the field name if any field is None

## Acceptance Criteria

- [ ] Given the file `app/services/deck/context.py`, when importing `DeckContext`, then it is a `dataclass` with fields `stages_durations`, `blocks_durations`, `eer_submarket_map`, `hydro_eer_submarket_map`, `submarkets`, `thermals`
- [ ] Given a `DeckContext` constructed with a None field, when `__post_init__` runs, then a `ValueError` is raised naming the None field
- [ ] Given the file `app/services/deck/__init__.py`, when checking its existence, then it exists (may be empty)
- [ ] Given the project, when running `uv run pytest ./tests`, then all existing tests still pass

## Implementation Guide

### Suggested Approach

1. Create `app/services/deck/__init__.py` (empty file)
2. Create `app/services/deck/context.py`:

   ```python
   from __future__ import annotations
   from dataclasses import dataclass
   from typing import TYPE_CHECKING
   import pandas as pd

   if TYPE_CHECKING:
       from app.services.unitofwork import AbstractUnitOfWork

   @dataclass
   class DeckContext:
       stages_durations: pd.DataFrame
       blocks_durations: pd.DataFrame
       eer_submarket_map: pd.DataFrame
       hydro_eer_submarket_map: pd.DataFrame
       submarkets: pd.DataFrame
       thermals: pd.DataFrame

       def __post_init__(self) -> None:
           for field_name, value in self.__dict__.items():
               if value is None:
                   raise ValueError(
                       f"DeckContext field '{field_name}' must not be None"
                   )

       @classmethod
       def from_deck(cls, uow: "AbstractUnitOfWork") -> "DeckContext":
           from app.services.deck.deck import Deck
           return cls(
               stages_durations=Deck.stages_durations(uow),
               blocks_durations=Deck.blocks_durations(uow),
               eer_submarket_map=Deck.eer_submarket_map(uow),
               hydro_eer_submarket_map=Deck.hydro_eer_submarket_map(uow),
               submarkets=Deck.submarkets(uow),
               thermals=Deck.thermals(uow),
           )
   ```

3. Verify no circular imports by running `uv run python -c "from app.services.deck.context import DeckContext"`
4. Run `uv run pytest ./tests`

### Key Files to Modify

- `app/services/deck/__init__.py` (create)
- `app/services/deck/context.py` (create)

### Patterns to Follow

Follow newave's `context.py` pattern but use `pd.DataFrame` types (Polars migration is Epic 5).

### Pitfalls to Avoid

- Use `from __future__ import annotations` and `TYPE_CHECKING` to avoid circular imports with `AbstractUnitOfWork`
- Do NOT use Polars types yet (that's Epic 5)
- Do NOT modify the existing `deck.py` in this ticket
- The `from_deck` method uses lazy import of `Deck` to avoid circular dependency

## Testing Requirements

### Unit Tests

Existing tests must pass. No new tests required for the dataclass itself (it's a simple data container).

### Integration Tests

Verify import: `uv run python -c "from app.services.deck.context import DeckContext; print('OK')"`

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-007-verify-files-repository-compatibility.md
- **Blocks**: ticket-009-extract-accessors-module.md

## Effort Estimate

**Points**: 1
**Confidence**: High
