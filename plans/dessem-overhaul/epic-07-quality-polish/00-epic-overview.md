# Epic 07: Quality & Polish

## Goal

Final quality pass: ensure mypy strict mode passes across the entire codebase, complete type annotation audit, align ruff configuration, audit constants, and update documentation.

## Scope

- mypy strict mode compliance across all modules
- Full type annotations audit
- Constants.py alignment with typed column definitions
- Documentation updates (Sphinx config for furo theme, API docs)
- Final ruff formatting pass

## Out of Scope

- New features
- Performance optimization
- Additional synthesis variables

## Tickets

1. ticket-026-enable-mypy-strict-full.md
2. ticket-027-type-annotations-and-constants-audit.md
3. ticket-028-documentation-and-final-polish.md

## Success Criteria

- `uv run mypy ./app` passes with zero errors in strict mode
- `uv run ruff check ./app` and `uv run ruff format --check ./app` pass
- All tests pass
- Documentation builds without warnings
