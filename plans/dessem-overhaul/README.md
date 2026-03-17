# sintetizador-dessem Overhaul Plan

Modernize sintetizador-dessem to match sintetizador-newave's refactored architecture. Covers infrastructure, dependency upgrade, code decomposition, Polars migration, parallelism, and quality improvements.

## Tech Stack

- Python >= 3.11
- idessem >= 1.1.0 (cfinterface >= 1.9.0)
- polars >= 1.0.0, pandas >= 3.0.0, pyarrow >= 19.0.0
- click >= 8.1.8
- Build: hatchling, uv
- Tooling: ruff, mypy (strict), pre-commit

## Epics

| Epic | Name                                      | Tickets | Detail Level |
| ---- | ----------------------------------------- | ------- | ------------ |
| 01   | Infrastructure & Dependency Modernization | 4       | Detailed     |
| 02   | idessem Upgrade & API Adaptation          | 3       | Detailed     |
| 03   | Code Decomposition - Deck Service         | 7       | Detailed     |
| 04   | Code Decomposition - Operation Synthesis  | 4       | Refined      |
| 05   | Polars Migration                          | 4       | Refined      |
| 06   | Parallelism & Logging                     | 3       | Outline      |
| 07   | Quality & Polish                          | 3       | Outline      |

## Progress

| Ticket     | Title                                          | Epic    | Status    | Detail Level | Readiness | Quality | Badge      |
| ---------- | ---------------------------------------------- | ------- | --------- | ------------ | --------- | ------- | ---------- |
| ticket-001 | Update pyproject.toml and Python version       | epic-01 | completed | Detailed     | 1.00      | 0.85    | ACCEPTABLE |
| ticket-002 | Modernize CI workflows                         | epic-01 | completed | Detailed     | 1.00      | 0.95    | EXCELLENT  |
| ticket-003 | Add pre-commit hooks and tooling               | epic-01 | completed | Detailed     | 1.00      | 0.90    | EXCELLENT  |
| ticket-004 | Add release workflow and contributing guide    | epic-01 | completed | Detailed     | 1.00      | 0.92    | EXCELLENT  |
| ticket-005 | Upgrade idessem dependency to >= 1.1.0         | epic-02 | completed | Detailed     | 1.00      | 0.95    | EXCELLENT  |
| ticket-006 | Add mypy overrides for idessem and cfinterface | epic-02 | completed | Detailed     | 1.00      | 0.85    | ACCEPTABLE |
| ticket-007 | Verify files repository compatibility          | epic-02 | completed | Detailed     | 1.00      | 0.90    | EXCELLENT  |
| ticket-008 | Create DeckContext dataclass                   | epic-03 | completed | Detailed     | 1.00      | 0.95    | EXCELLENT  |
| ticket-009 | Extract accessors module                       | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-010 | Extract temporal module                        | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-011 | Extract entities module                        | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-012 | Extract hydro module                           | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-013 | Extract thermal and system modules             | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-014 | Reduce deck to facade                          | epic-03 | completed | Detailed     | 1.00      | 0.93    | EXCELLENT  |
| ticket-015 | Create operation synthesis package             | epic-04 | completed | Refined      | 1.00      | 0.95    | EXCELLENT  |
| ticket-016 | Extract operation cache and export             | epic-04 | completed | Refined      | 1.00      | 0.95    | EXCELLENT  |
| ticket-017 | Extract spatial resolution modules             | epic-04 | completed | Refined      | 1.00      | 0.95    | EXCELLENT  |
| ticket-018 | Create operation orchestrator and pipeline     | epic-04 | completed | Refined      | 0.98      | 0.95    | EXCELLENT  |
| ticket-019 | Add Polars dependency and export method        | epic-05 | completed | Refined      | 1.00      | 0.95    | EXCELLENT  |
| ticket-020 | Implement native Polars Parquet export         | epic-05 | completed | Refined      | 1.00      | 0.95    | EXCELLENT  |
| ticket-021 | Migrate deck hot paths to Polars               | epic-05 | completed | Refined      | 0.94      | 0.93    | EXCELLENT  |
| ticket-022 | Migrate operation synthesis to Polars          | epic-05 | completed | Refined      | 0.98      | 0.93    | EXCELLENT  |
| ticket-023 | Add multiprocessing-safe logger                | epic-06 | pending   | Outline      | --        | --      | --         |
| ticket-024 | Add --processadores CLI option                 | epic-06 | pending   | Outline      | --        | --      | --         |
| ticket-025 | Integrate ProcessPoolExecutor                  | epic-06 | pending   | Outline      | --        | --      | --         |
| ticket-026 | Enable full mypy strict mode compliance        | epic-07 | pending   | Outline      | --        | --      | --         |
| ticket-027 | Audit type annotations and constants           | epic-07 | pending   | Outline      | --        | --      | --         |
| ticket-028 | Update documentation and final polish          | epic-07 | pending   | Outline      | --        | --      | --         |
