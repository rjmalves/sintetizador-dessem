# ticket-023 Add Multiprocessing-Safe Logger

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Replace the current logging setup with a multiprocessing-safe logger using Python's `logging.handlers.QueueHandler` and `QueueListener` pattern. This ensures log messages from child processes are safely forwarded to the main process for output, preventing interleaved or lost log messages.

## Anticipated Scope

- **Files likely to be modified**: `app/utils/log.py`, `main.py`, `app/utils/singleton.py` (audit for multiprocessing safety)
- **Key decisions needed**: Whether to use `multiprocessing.Queue` or `multiprocessing.Manager().Queue()`; whether the singleton Log class should be replaced entirely
- **Open questions**: How does the existing `Log.configure_logging` interact with child processes? Does the singleton pattern work with `spawn` method?

## Dependencies

- **Blocked By**: ticket-022-migrate-operation-synthesis-to-polars.md
- **Blocks**: ticket-024-add-processadores-cli-option.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
