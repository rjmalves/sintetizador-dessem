# Epic 06: Parallelism & Logging

## Goal

Add multiprocessing support to sintetizador-dessem with a multiprocessing-safe logger using QueueHandler pattern and ProcessPoolExecutor for parallel synthesis. Add `--processadores` CLI option.

## Scope

- Multiprocessing-safe logger with QueueHandler + spawn method
- `--processadores` CLI option
- ProcessPoolExecutor integration in operation synthesis
- Singleton pattern audit for multiprocessing safety

## Out of Scope

- Changing the synthesis algorithm
- Distributed computing (only local multiprocessing)
- Thread-based parallelism

## Tickets

1. ticket-023-add-multiprocessing-logger.md
2. ticket-024-add-processadores-cli-option.md
3. ticket-025-integrate-process-pool-executor.md

## Success Criteria

- `--processadores N` option works with N > 1
- Logging works correctly with multiple processes
- No data corruption or race conditions
