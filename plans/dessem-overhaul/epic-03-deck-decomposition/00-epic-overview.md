# Epic 03: Code Decomposition - Deck Service

## Goal

Decompose the monolithic `app/services/deck/deck.py` (1884 lines) into focused submodules following sintetizador-newave's pattern. The Deck class becomes a thin facade that delegates to domain-specific modules: accessors (file access + caching), temporal (stage/date calculations), hydro (hydro data extraction), thermal (thermal data), system (system-level data), entities (entity mappings), readers (file reading orchestration), and context (DeckContext dataclass).

## Scope

- Create `app/services/deck/__init__.py` (package marker)
- Create `app/services/deck/context.py` with DeckContext dataclass
- Create `app/services/deck/accessors.py` for file access with caching (lines 60-198)
- Create `app/services/deck/temporal.py` for stage/date/block calculations (lines 718-851)
- Create `app/services/deck/entities.py` for entity mappings: hydros, thermals, submarkets, EERs (lines 851-1020)
- Create `app/services/deck/hydro.py` for hydro data extraction from pdo_hidr (lines 327-453, 1074-1193)
- Create `app/services/deck/thermal.py` for thermal data (lines 640-686, 1300-1327, 1364-1448)
- Create `app/services/deck/system.py` for system-level data: pdo_sist, pdo_operacao, pdo_inter, pdo_eolica (lines 292-327, 454-616, 1020-1074, 1221-1300, 1327-1364)
- Create `app/services/deck/readers.py` for file reading orchestration (pdo_eco_usih, operuh, etc.)
- Reduce `deck.py` to a ~200-line facade delegating to submodules
- Update all imports across the codebase (synthesis modules, bounds, handlers)
- All existing tests must continue to pass

## Out of Scope

- Splitting operation.py (Epic 4)
- Polars migration of deck methods (Epic 5)
- Modifying bounds.py logic (stays as-is, just updated imports)
- Changing the public API of the Deck class (methods remain classmethod-based)

## Tickets

1. ticket-008-create-deck-context-dataclass.md
2. ticket-009-extract-accessors-module.md
3. ticket-010-extract-temporal-module.md
4. ticket-011-extract-entities-module.md
5. ticket-012-extract-hydro-module.md
6. ticket-013-extract-thermal-and-system-modules.md
7. ticket-014-reduce-deck-to-facade.md

## Success Criteria

- deck.py is <= 250 lines and contains only delegation methods
- Each submodule has a single clear responsibility
- All existing tests pass with `uv run pytest ./tests`
- No circular imports between submodules
- All imports across the codebase updated (grep for old import paths returns zero hits)
