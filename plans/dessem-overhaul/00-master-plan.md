# Master Plan: sintetizador-dessem Overhaul

## Executive Summary

Modernize sintetizador-dessem to match sintetizador-newave's refactored architecture. This involves upgrading infrastructure (Python 3.11+, CI/CD, pre-commit), adapting to the new idessem API (cfinterface >= 1.9.0), decomposing the monolithic deck.py (1884 lines) and operation.py (871 lines) into focused submodules, migrating hot-path DataFrame operations from Pandas to Polars, adding multiprocessing support, and enforcing strict type safety with mypy.

## Goals & Non-Goals

### Goals

- Align project infrastructure with sintetizador-newave (Python 3.11+, CI matrix, pre-commit, release workflow)
- Upgrade idessem dependency to >= 1.1.0 (cfinterface >= 1.9.0) and adapt to API changes
- Decompose deck.py into ~8 focused submodules following newave's pattern
- Decompose operation.py into a package with orchestrator, pipeline, cache, resolution modules
- Migrate hot-path Pandas operations to Polars with native Parquet export
- Add multiprocessing support with safe logging and `--processadores` CLI option
- Enforce mypy strict mode with full type annotations

### Non-Goals

- Changing the CLI interface semantics (commands remain: sistema, operacao, execucao, completa, limpeza)
- Adding new synthesis variables or spatial resolutions
- Rewriting tests from scratch (tests will be adapted incrementally)
- Changing the domain model (operation/system/execution synthesis remain)
- Migrating away from Click CLI framework

## Architecture Overview

### Current State

- Python >= 3.10, single CI job combining tests + mypy + ruff + sphinx
- idessem >= 1.0.0 with cfinterface <= 1.8.3
- Monolithic deck.py (1884 lines) with mixed responsibilities: file access, data extraction, bounds calculation, entity mapping
- Monolithic operation.py (871 lines) with synthesis orchestration, data resolution, caching, export, and statistics all in one class
- All DataFrame operations use Pandas
- No multiprocessing support
- No pre-commit hooks, no release workflow, no py.typed marker

### Target State

- Python >= 3.11, separate CI jobs (lint, typecheck, test, docs), Python 3.11-3.14 matrix
- idessem >= 1.1.0 with cfinterface >= 1.9.0
- deck/ package with: deck.py (facade ~200 lines), accessors.py, temporal.py, hydro.py, thermal.py, system.py, entities.py, readers.py, context.py
- operation/ package with: orchestrator.py, pipeline.py, cache.py, export.py, bounds.py, resolution_sin.py, resolution_sbm.py, resolution_uhe.py, resolution_ute.py, resolution_sbp.py
- Hot-path operations on Polars with native Parquet export via PyArrow
- ProcessPoolExecutor with multiprocessing-safe QueueHandler logger
- mypy strict mode, pre-commit hooks, release.yml, CONTRIBUTING.md, py.typed

### Key Design Decisions

1. **Follow newave's decomposition patterns exactly** for consistency across the sister applications
2. **Keep Pandas at the boundary** (export compatibility, idessem returns Pandas DataFrames) but use Polars for internal hot-path operations
3. **DeckContext dataclass** to bundle frequently-accessed deck state, reducing repeated file reads
4. **Accessors pattern** for file access caching (same as newave) rather than the current `__read_*` boolean flags in files.py
5. **bounds.py stays in deck/** (not moved to operation/) since it's domain logic tied to DESSEM deck data

## Technical Approach

### Tech Stack

- Python >= 3.11
- idessem >= 1.1.0 (cfinterface >= 1.9.0, pandas >= 3.0.0)
- polars >= 1.0.0
- pandas >= 3.0.0
- pyarrow >= 19.0.0
- click >= 8.1.8
- Build: hatchling
- Tooling: ruff, mypy (strict), pre-commit, uv

### Component/Module Breakdown

```
app/
  services/
    deck/
      __init__.py
      deck.py          # Facade (thin delegation layer)
      accessors.py     # File access with caching
      temporal.py      # Stage/date/block calculations
      hydro.py         # Hydro data extraction (pdo_hidr, operuh, dadvaz)
      thermal.py       # Thermal data extraction (pdo_oper_term, pdo_oper_uct)
      system.py        # System-level data (pdo_sist, pdo_operacao, pdo_inter)
      entities.py      # Entity mappings (hydros, thermals, submarkets, EERs)
      readers.py       # File reading orchestration (pdo_eolica, pdo_eco_usih)
      context.py       # DeckContext dataclass
      bounds.py        # Operation variable bounds (existing, may be refactored)
    synthesis/
      operation/
        __init__.py
        orchestrator.py   # Main synthesis orchestration loop
        pipeline.py       # Data resolution pipeline
        cache.py          # Synthesis caching logic
        export.py         # Metadata and statistics export
        bounds.py         # Bounds delegation (thin wrapper)
        resolution_sin.py # SIN spatial resolution
        resolution_sbm.py # Submarket resolution
        resolution_uhe.py # Hydro plant resolution
        resolution_ute.py # Thermal plant resolution
        resolution_sbp.py # Submarket pair resolution
      system.py          # (existing, adapted)
      execution.py       # (existing, adapted)
```

### Data Flow

1. CLI command -> handler -> Synthetizer.synthetize()
2. Synthetizer creates DeckContext from Deck facade
3. For each synthesis variable: resolve data via pipeline -> apply bounds -> export
4. Export writes Polars DataFrame via native PyArrow path

### Testing Strategy

- Existing tests adapted incrementally (import path changes)
- Each decomposition ticket verifies `uv run pytest` passes
- mypy strict compliance verified per-epic
- Integration tests verify end-to-end synthesis output parity

## Phases & Milestones

| Epic | Name                                      | Duration  | Detail Level |
| ---- | ----------------------------------------- | --------- | ------------ |
| 1    | Infrastructure & Dependency Modernization | 1-2 weeks | Detailed     |
| 2    | idessem Upgrade & API Adaptation          | 1 week    | Detailed     |
| 3    | Code Decomposition - Deck Service         | 2-3 weeks | Detailed     |
| 4    | Code Decomposition - Operation Synthesis  | 2-3 weeks | Outline      |
| 5    | Polars Migration                          | 2-3 weeks | Outline      |
| 6    | Parallelism & Logging                     | 1-2 weeks | Outline      |
| 7    | Quality & Polish                          | 1 week    | Outline      |

## Risk Analysis

| Risk                                              | Impact | Mitigation                                                             |
| ------------------------------------------------- | ------ | ---------------------------------------------------------------------- |
| idessem API breaks at runtime (cfinterface 1.9.0) | High   | Epic 2 includes comprehensive runtime verification with test data      |
| Deck decomposition breaks existing tests          | Medium | Each decomposition ticket runs full test suite; incremental approach   |
| Polars/Pandas interop issues at boundary          | Medium | Keep Pandas at idessem boundary; explicit conversion points            |
| Multiprocessing breaks singleton patterns         | Medium | Review Singleton usage in Epic 6; use spawn method                     |
| idessem 1.1.0 not yet published                   | High   | Work against feat/infra-docs-overhaul branch; pin to git ref if needed |

## Success Metrics

- All existing tests pass after each epic
- mypy strict mode passes with zero errors (after Epic 7)
- CI runs in separate lint/typecheck/test/docs jobs
- deck.py reduced from 1884 lines to ~200 lines (facade only)
- operation.py reduced from 871 lines to ~0 (replaced by package)
- Synthesis output files are byte-identical (Parquet schema compatible)
