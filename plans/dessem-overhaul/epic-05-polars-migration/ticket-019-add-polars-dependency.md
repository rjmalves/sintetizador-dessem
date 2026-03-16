# ticket-019 Add Polars Dependency and Export Method

> **[OUTLINE]** This ticket requires refinement before execution.
> It will be refined with learnings from earlier epics.

## Objective

Add `polars >= 1.0.0` as a project dependency in `pyproject.toml` and add the `synthetize_pl()` method to the export repositories (ParquetExportRepository, CSVExportRepository, TestExportRepository) following the pattern established in sintetizador-newave.

## Anticipated Scope

- **Files likely to be modified**: `pyproject.toml`, `app/adapters/repository/export.py`, `uv.lock`
- **Key decisions needed**: Whether CSVExportRepository should have a native Polars path or just convert to Pandas
- **Open questions**: Should `synthetize_pl()` be added to AbstractExportRepository as a default implementation that converts to Pandas?

## Dependencies

- **Blocked By**: ticket-018-create-operation-orchestrator.md
- **Blocks**: ticket-020-add-polars-export-repository.md

## Effort Estimate

**Points**: 2
**Confidence**: Low (will be re-estimated during refinement)
