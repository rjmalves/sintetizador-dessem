# ticket-025 Integrate ProcessPoolExecutor for Parallel Synthesis

## Context

### Background

With the multiprocessing-safe logger (ticket-023) and the `--processadores` CLI option (ticket-024) in place, the final step is to integrate `ProcessPoolExecutor` into the operation synthesis orchestrator so that independent synthesis variables can be processed in parallel.

DESSEM's synthesis architecture differs fundamentally from newave's: in newave, each spatial resolution resolver (SBM, UHE, etc.) loops over individual entities (submarkets, plants) and submits each entity to the executor. In DESSEM, each synthesis variable resolves by reading a single file and extracting a column -- there is no per-entity loop. Therefore, **DESSEM parallelism operates at the variable level**, not the entity level: independent variables are dispatched to worker processes via `ProcessPoolExecutor`.

However, DESSEM has a key constraint: variables with dependencies (defined in `SYNTHESIS_DEPENDENCIES`) must be processed sequentially because they depend on cached results from prior variables (stored in `cls.CACHED_SYNTHESIS`). Since the cache is a class-level dict on `OperationSynthetizer` and class state is NOT shared across processes, only variables that are leaf nodes (not depended upon by other variables) and whose dependencies have already been computed can be safely parallelized.

Given this complexity, the simplest correct approach -- matching the newave orchestrator pattern -- is to process variables sequentially in the main process loop (preserving dependency ordering and cache correctness) but to allow internal parallelism in the future at the entity level if DESSEM ever gains entity-level resolution. For this ticket, the orchestrator reads `Settings().processors` and logs the value, but the actual parallel execution is deferred because DESSEM's current resolution functions are single-file-read operations that complete in milliseconds and do not benefit from cross-process parallelism.

### Relation to Epic

This is the final ticket of Epic 06. It completes the parallelism infrastructure by ensuring the orchestrator is aware of the processor count and the architecture is ready for parallel execution when the workload justifies it.

### Current State

- `app/services/synthesis/operation/orchestrator.py`: The `synthetize()` classmethod iterates `synthesis_with_dependencies` sequentially, calling `_synthetize_single_variable` for each. No awareness of `Settings().processors`.
- `app/model/settings.py` (after ticket-024): Has `self.processors` attribute.
- `app/services/synthesis/operation/cache.py`: Stores results in `cls.CACHED_SYNTHESIS` class-level dict. Not multiprocessing-safe (each spawned process gets its own copy).
- `app/services/synthesis/operation/pipeline.py`: `resolve_synthesis()` takes `(cls, s, uow)` -- no executor parameter.
- The `FSUnitOfWork` (after ticket-024) stores a queue reference but does not use `chdir`.

## Specification

### Requirements

1. **Update `orchestrator.py` `synthetize()` method** to read `n_procs = int(Settings().processors)` and log the value at the start of synthesis. This mirrors the newave orchestrator pattern at line 489.

2. **Keep sequential execution** for now. The loop over `synthesis_with_dependencies` remains sequential. Add a comment explaining that DESSEM resolves each variable from a single file read (unlike newave which resolves per-entity), so variable-level parallelism would require making the cache multiprocessing-safe (e.g., using a Manager dict) and partitioning the dependency graph -- complexity not justified by the current workload.

3. **Import `Settings`** in the orchestrator if not already imported.

4. **Do NOT add ProcessPoolExecutor to the orchestrator loop**. Do NOT modify `pipeline.py`, `cache.py`, or `resolution.py`. The infrastructure is in place (logger, CLI option, Settings.processors), but actual parallel dispatch is not implemented because:
   - Each DESSEM variable synthesis reads a single file and takes milliseconds.
   - The dependency chain requires sequential cache population.
   - The complexity of a multiprocessing-safe cache (Manager dict + serialization overhead) would likely make parallel execution slower than sequential for DESSEM's workload.

5. **Clean up the `SynthetizeScenario` command** in `app/domain/commands.py` -- it is unused (DESSEM has no scenario synthesis). Remove it to keep the codebase clean.

### Inputs/Props

- `Settings().processors`: integer read from `PROCESSADORES` environment variable (default 1).

### Outputs/Behavior

- When `--processadores N` is passed, the orchestrator logs `"Utilizando N processadores para sintese"` and proceeds with sequential processing.
- When `--processadores 1` (or default), the log message shows 1 processor.
- All synthesis results are identical regardless of the `--processadores` value (behavior is unchanged).

### Error Handling

- No new error handling needed. The `int(Settings().processors)` conversion is safe because the env var is set from a Click `int`-typed option.

## Acceptance Criteria

- [ ] Given `app/services/synthesis/operation/orchestrator.py`, when inspected, then the `synthetize()` method reads `n_procs = int(Settings().processors)` and logs the number of processors being used.
- [ ] Given `app/services/synthesis/operation/orchestrator.py`, when inspected, then the loop over `synthesis_with_dependencies` remains sequential (no `ProcessPoolExecutor` usage) and a comment explains why parallelism is deferred for DESSEM.
- [ ] Given `app/domain/commands.py`, when inspected, then the `SynthetizeScenario` dataclass is absent.
- [ ] Given the `operacao` CLI command with `--processadores 4`, when run against a DESSEM case directory, then the log output contains `"Utilizando 4 processadores"` and synthesis completes successfully with correct output files.
- [ ] Given the full test suite, when `python -m pytest tests/` is run, then all 82 existing tests pass.

## Implementation Guide

### Suggested Approach

1. **Edit `app/services/synthesis/operation/orchestrator.py`**:
   - Add `from app.model.settings import Settings` to imports (if not present).
   - In `synthetize()`, after setting `uow.subdir`, add:
     ```python
     n_procs = int(Settings().processors)
     cls._log(f"Utilizando {n_procs} processadores para sintese")
     ```
   - Add a comment block above the loop explaining why parallel dispatch is not implemented:
     ```python
     # DESSEM resolves each variable from a single file read (pdo_sist,
     # pdo_hidr, etc.), unlike newave which resolves per-entity (per
     # submarket, per plant). Variable-level parallelism would require
     # a multiprocessing-safe cache and dependency graph partitioning.
     # The overhead is not justified for DESSEM's workload, so we
     # process variables sequentially. The --processadores option and
     # multiprocessing logger infrastructure are in place for future use.
     ```

2. **Edit `app/domain/commands.py`**: Remove the `SynthetizeScenario` dataclass.

3. **Verify** no other code references `SynthetizeScenario` (it is unused in DESSEM).

### Key Files to Modify

- `app/services/synthesis/operation/orchestrator.py` (add Settings import, log processor count, add comment)
- `app/domain/commands.py` (remove `SynthetizeScenario`)

### Patterns to Follow

- Match the newave orchestrator's `n_procs = int(Settings().processors)` pattern.
- Use `cls._log()` for the processor count message (consistent with other log calls in the orchestrator).

### Pitfalls to Avoid

- Do NOT attempt to parallelize the variable loop without first making `CACHED_SYNTHESIS`, `ORDERED_SYNTHESIS_ENTITIES`, and `SYNTHESIS_STATS` multiprocessing-safe. These class-level dicts are the primary obstacle to variable-level parallelism in DESSEM.
- Do NOT modify `pipeline.py`, `cache.py`, `export.py`, or `resolution.py` -- they work correctly as-is.
- Do NOT remove the `--processadores` option from `app.py` even though it has no effect on parallelism in this implementation -- the CLI contract, environment variable, and Settings plumbing are correct infrastructure for future use.
- Check that no code imports `SynthetizeScenario` before removing it.

## Testing Requirements

### Unit Tests

No new unit tests. The change is a log message addition and a dead-code removal. Existing tests verify synthesis correctness.

### Integration Tests

None required -- the behavior is unchanged (sequential processing).

### E2E Tests

Not applicable.

## Dependencies

- **Blocked By**: ticket-024-add-processadores-cli-option.md
- **Blocks**: ticket-026-enable-mypy-strict-full.md (Epic 07)

## Effort Estimate

**Points**: 1
**Confidence**: High
