# Tests

Integration tests for sql-identity-resolution.

## Test Fixtures

| Fixture | Description | Expected Outcome |
|---------|-------------|------------------|
| `test_two_entities_same_email.sql` | Two entities share email | Same cluster |
| `test_chain_three_entities.sql` | A-B via email, B-C via phone | All same cluster |
| `test_disjoint_graphs.sql` | Two separate groups | Two clusters |
| `test_case_insensitive_email.sql` | UPPER/lower email | Same cluster |

## Running Tests

### Databricks

1. **Setup**: Run `tests/run_tests.py` notebook to create test data
2. **Execute**: Run `IDR_Run.py` with `RUN_MODE=FULL`
3. **Assert**: Return to `run_tests.py` and run the `run_assertions()` cell
4. **Review**: Check pass/fail status

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CATALOG` | `main` | Unity Catalog name |
| `TEST_SCHEMA` | `idr_test` | Schema for test tables |
| `CLEAN_UP` | `true` | Drop test tables after run |

## Test Design Principles

1. **Deterministic**: Same inputs always produce same outputs
2. **Isolated**: Each test uses distinct entity keys (`test_source_*`)
3. **Fast**: Small datasets (2-10 entities per test)
4. **Comprehensive**: Cover edges, clustering, and golden profile

## Adding New Tests

1. Create a new fixture in `tests/fixtures/` with:
   - Setup SQL to create test tables
   - Comments explaining expected outcomes
   - Assertion queries

2. Add assertions to `run_tests.py`

3. Update this README
