# ticket-024 Add --processadores CLI Option

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Add a `--processadores` CLI option to the Click commands (operacao, completa) that controls the number of worker processes for parallel synthesis. Default to 1 (sequential). Wire the option through the command dataclasses and handlers to the synthesis orchestrator.

## Anticipated Scope

- **Files likely to be modified**: `app/app.py`, `app/domain/commands.py`, `app/services/handlers.py`
- **Key decisions needed**: Whether to add the option to all commands or only `operacao` and `completa`; whether to use `os.cpu_count()` as default or 1
- **Open questions**: How to pass the process count to the synthesis orchestrator (via command dataclass, environment variable, or direct parameter)?

## Dependencies

- **Blocked By**: ticket-023-add-multiprocessing-logger.md
- **Blocks**: ticket-025-integrate-process-pool-executor.md

## Effort Estimate

**Points**: 1
**Confidence**: Low (will be re-estimated during refinement)
