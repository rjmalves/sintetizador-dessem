# Epic 04: Code Decomposition - Operation Synthesis

## Goal

Decompose the monolithic `app/services/synthesis/operation.py` (871 lines) into a package with focused submodules following sintetizador-newave's operation synthesis pattern: orchestrator, pipeline, cache, export, bounds, and spatial resolution modules.

## Scope

- Convert `operation.py` into `operation/` package
- Create orchestrator.py (main synthesis loop)
- Create pipeline.py (data resolution pipeline)
- Create cache.py (synthesis caching logic)
- Create export.py (metadata and statistics export)
- Create bounds.py (thin wrapper for bounds delegation)
- Create resolution modules: resolution_sin.py, resolution_sbm.py, resolution_uhe.py, resolution_ute.py, resolution_sbp.py

## Out of Scope

- Modifying the Deck facade API (completed in Epic 3)
- Polars migration (Epic 5)
- Parallelism (Epic 6)

## Tickets

1. ticket-015-create-operation-synthesis-package.md
2. ticket-016-extract-operation-cache-and-export.md
3. ticket-017-extract-resolution-modules.md
4. ticket-018-create-operation-orchestrator.md

## Success Criteria

- `operation.py` no longer exists (replaced by `operation/` package)
- All existing operation synthesis tests pass
- Each module has <= 300 lines
