# ticket-007 Verify Files Repository Compatibility with New idessem

## Context

### Background

After upgrading idessem to >= 1.1.0 and adding mypy overrides, we need to verify that all file reading operations in `RawFilesRepository` work correctly at runtime with the new cfinterface >= 1.9.0 API. The `.read()` classmethods may have subtle behavioral changes beyond the return type signature change.

### Relation to Epic

Final ticket in Epic 2. Depends on ticket-005 (dependency upgrade) and ticket-006 (mypy overrides). This is a verification/adaptation ticket.

### Current State

`app/adapters/repository/files.py` contains `RawFilesRepository` with 15 lazy-loaded file readers using the pattern:

```python
self.__entdados = Entdados.read(path)
```

Each uses the idessem `.read()` classmethod which, under cfinterface >= 1.9.0, returns a base type. The runtime behavior should be the same, but property access patterns may differ.

File types read: DessemArq, Entdados, Dadvaz, PdoSist, PdoInter, PdoHidr, PdoEolica, PdoOperacao, PdoOperUct, PdoOperTerm, PdoOperTviagCalha, PdoEcoUsih, Operuh, DesLogRelato, LogMatriz.

## Specification

### Requirements

1. Run the full test suite with the new idessem and verify all tests pass
2. If any test fails due to API changes, adapt the code in `files.py` to work with the new idessem API
3. Specifically verify the `PdoEcoUsih` double-read pattern (lines 403-425 of files.py) still works -- it reads the file, extracts a version, sets the version, then re-reads
4. Verify the `ENCODING` class attribute pattern (lines 28-43) still works with the new cfinterface

### Inputs/Props

- `app/adapters/repository/files.py` (all 15 reader methods)
- Test files in `tests/app/adapters/repository/test_files.py`
- Test data in `tests/mocks/`

### Outputs/Behavior

- All tests pass with zero failures
- All 15 file type readers work correctly at runtime
- No behavioral regressions in file parsing

### Error Handling

- If a specific file type's `.read()` method fails, check the idessem changelog for API changes and adapt accordingly
- If the PdoEcoUsih version-then-re-read pattern breaks, refactor to use the new idessem API for version detection

## Acceptance Criteria

- [ ] Given the test suite, when running `uv run pytest ./tests -v`, then all tests pass with exit code 0
- [ ] Given `RawFilesRepository`, when calling `get_entdados()` with test data, then it returns an `Entdados` instance (not None, not a base type)
- [ ] Given `RawFilesRepository`, when calling `get_pdo_eco_usih()` with test data, then the version detection and re-read pattern succeeds without exceptions
- [ ] Given `RawFilesRepository.__init__`, when constructing with a valid DESSEM directory, then `DessemArq.read(path)` succeeds and `self.__dessemarq` is populated

## Implementation Guide

### Suggested Approach

1. Run `uv run pytest ./tests -v` and collect all failures
2. For each failure, determine if it's an idessem API change or a test data issue
3. Common fixes needed:
   - `.read()` may need explicit type cast: `cast(Entdados, Entdados.read(path))`
   - Property access on the result may need adaptation
   - pandas >= 3.0.0 deprecations (e.g., `DataFrame.append` removed, use `pd.concat`)
4. Fix each issue in `files.py` and re-run tests
5. If test mock data is incompatible with new idessem, update the mock data

### Key Files to Modify

- `app/adapters/repository/files.py` (adapt if needed)
- Test files in `tests/` (adapt if needed)

### Patterns to Follow

For type casting at `.read()` call sites, follow newave's pattern -- the mypy overrides handle the type mismatch, so explicit casts are not needed. If runtime behavior changes, add defensive checks.

### Pitfalls to Avoid

- Do NOT refactor the lazy-loading pattern (that's Epic 3 scope)
- Do NOT change the public API of `AbstractFilesRepository` or `RawFilesRepository`
- If tests fail due to missing test data, note the gap but do not block -- add a comment for future test data generation

## Testing Requirements

### Unit Tests

Run full test suite: `uv run pytest ./tests -v`

### Integration Tests

If test data for all 15 file types is not available in `tests/mocks/`, document which readers could not be verified and add a tracking comment.

### E2E Tests

N/A

## Dependencies

- **Blocked By**: ticket-005-upgrade-idessem-dependency.md, ticket-006-add-mypy-overrides-for-idessem.md
- **Blocks**: ticket-008-create-deck-context-dataclass.md (Epic 3)

## Effort Estimate

**Points**: 2
**Confidence**: Medium (depends on extent of idessem API changes at runtime)
