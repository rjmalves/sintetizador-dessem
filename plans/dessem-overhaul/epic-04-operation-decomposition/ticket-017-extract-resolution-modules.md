# ticket-017 Extract Spatial Resolution Modules

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Extract the `_resolve_*` methods from OperationSynthetizer that handle different spatial resolutions into dedicated modules: `resolution_sin.py` (SIN-level), `resolution_sbm.py` (submarket-level), `resolution_uhe.py` (UHE-level), `resolution_ute.py` (UTE-level), `resolution_sbp.py` (submarket pair). Each module encapsulates the data resolution logic for its spatial resolution.

## Anticipated Scope

- **Files likely to be modified**: `app/services/synthesis/operation/resolution_sin.py`, `resolution_sbm.py`, `resolution_uhe.py`, `resolution_ute.py`, `resolution_sbp.py` (all create), main orchestrator module (update)
- **Key decisions needed**: Whether resolution functions should be standalone functions or classes; how the `_resolve` dispatch dict maps to resolution modules
- **Open questions**: How to handle resolutions that share logic (e.g., `_resolve_pdo_sist_sbm` and `_resolve_pdo_sist_sin` share post-processing)

## Dependencies

- **Blocked By**: ticket-016-extract-operation-cache-and-export.md
- **Blocks**: ticket-018-create-operation-orchestrator.md

## Effort Estimate

**Points**: 3
**Confidence**: Low (will be re-estimated during refinement)
