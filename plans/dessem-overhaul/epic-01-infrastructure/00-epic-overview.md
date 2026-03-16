# Epic 01: Infrastructure & Dependency Modernization

## Goal

Align sintetizador-dessem's project infrastructure with sintetizador-newave's modernized setup. This includes upgrading the minimum Python version, modernizing CI/CD pipelines, adding pre-commit hooks, dependency locking, release workflows, and developer documentation.

## Scope

- Update `pyproject.toml`: Python >= 3.11, classifiers, ruff config, mypy config
- Add `uv.lock` for reproducible builds
- Add `.pre-commit-config.yaml` with ruff + mypy hooks
- Add `app/py.typed` marker (PEP 561)
- Restructure CI into separate lint, typecheck, test (3.11-3.14), docs jobs
- Add `release.yml` workflow for PyPI publishing
- Add `CONTRIBUTING.md` guide
- Update dev dependencies (pre-commit, furo, types-python-dateutil)

## Out of Scope

- Changing runtime dependencies (idessem upgrade is Epic 2)
- Code changes to production files (except pyproject.toml and **init**.py classifiers)
- Adding polars dependency (Epic 5)

## Tickets

1. ticket-001-update-pyproject-and-python-version.md
2. ticket-002-modernize-ci-workflows.md
3. ticket-003-add-precommit-and-tooling.md
4. ticket-004-add-release-workflow-and-docs.md

## Success Criteria

- `uv sync --all-extras --dev` succeeds on Python 3.11+
- CI pipeline has separate lint, typecheck, test, docs jobs
- Pre-commit hooks run ruff check, ruff format, and mypy
- `py.typed` marker exists
- `uv.lock` is committed
