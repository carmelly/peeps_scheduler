# Test Architecture Guide

This document explains the test fixture architecture for the peeps_scheduler test suite. All fixtures follow optimized patterns to maintain test isolation while maximizing performance.

## Fixture Architecture

### Fixture Scope Hierarchy

**1. Session-Scoped (schema_sql):**

- Loaded once per test session
- Schema SQL is expensive to load/parse
- Shared by all tests

**2. Module-Scoped (db_connection):**

- Single in-memory database per test module
- Schema is created once (fast, reused)
- Tests use transaction rollback for isolation (test_db fixture)
- ~5-7x faster than creating new DB per test
- Never commit changes - all auto-rolled back

**3. Function-Scoped (test_db, test_db_path):**

- Isolated view of db_connection (via transaction savepoint)
- Each test runs in its own transaction (BEGIN...ROLLBACK)
- Database state is clean for next test
- Modifications never leak between tests

## Database Testing Patterns

### For Direct Connection Tests (Unit Tests)

Use the `test_db` fixture directly:

```python
def test_member_insertion(test_db):
    cursor = test_db.cursor()
    cursor.execute("INSERT INTO peeps (id, full_name, ...) VALUES (1, 'Test', ...)")
    cursor.execute("SELECT * FROM peeps WHERE id = 1")
    assert cursor.fetchone() is not None
```

**Characteristics:**

- Provides clean database for each test
- Transaction rollback ensures isolation
- Fast: ~0.005s per test (no schema re-init)

### For CLI Tests (Subprocess Tests)

Use test files with session-scoped template DB (test_import_period_data_cli.py pattern):

```python
@pytest.fixture(scope='session')
def template_db_file():
    # Create template DB file once per session
    db_path = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_path)
    conn.executescript(reordered_sql)
    conn.close()
    yield db_path
    os.unlink(db_path)

@pytest.fixture
def temp_test_db(template_db_file):
    # Copy template for each test (60x faster than re-creating schema)
    db_path = tempfile.mktemp(suffix='.db')
    shutil.copy2(template_db_file, db_path)
    yield db_path
    os.unlink(db_path)
```

**Characteristics:**

- CLI requires actual file paths (can't use :memory:)
- Template DB pattern: create once (expensive), copy many times (cheap)
- Fast: ~0.005s per test (file copy, not schema execution)

**⚠️ NEVER use `:memory:` for subprocess tests** - subprocesses can't access parent process memory

## Performance Baseline

Test suite: 406 tests passing, 2 skipped

- **test_db_import.py**: 82 tests in ~0.5s = 0.006s/test ✅
- **test_validate.py**: 26 tests in ~0.2s = 0.008s/test ✅
- **test_validation_cli.py**: 24 tests in ~0.1s = 0.004s/test ✅
- **test_import_period_data_cli.py**: 30 tests in ~3.3s = 0.11s/test (includes CLI overhead)
- **Full suite**: 6.27s total (75.3% faster than baseline)

## Parametrization Patterns

Use `@pytest.mark.parametrize` to reduce duplicate test functions:

```python
@pytest.mark.parametrize("scenario,field,old_value,new_value", [
    ("active_changed", "active", True, False),
    ("index_changed", "index_position", 0, 5),
])
def test_field_change_detection(scenario, field, old_value, new_value):
    # Single test body handles all parameter combinations
    # Reduces 4 functions to 1
```

**Benefits:**

- Reduces code duplication (3-4 similar tests → 1 parametrized)
- Maintains identical coverage
- Easier maintenance (single test body)
- Clearer intent (parameters show scenarios)

## Architecture Decisions

### Why Module-Scoped db_connection + Transaction Rollback?

**Traditional approach:**

- Per-test fixture: Create new DB, apply schema, test, teardown
- Cost: Schema creation ~0.3s per test × 400 tests = 120s overhead
- Total: 25+ seconds for full suite

**Optimized approach:**

- Module fixture: Create DB once, apply schema once
- Per-test: BEGIN transaction, test, ROLLBACK (changes discarded)
- Cost: Schema creation once per module (~50ms), rollback ~1ms per test
- Total: 6.27 seconds for full suite (73% faster)

### Why Consolidate Schema Parsing?

The `_parse_and_reorder_schema()` function in conftest.py is the single source of truth for schema parsing. It handles reordering CREATE INDEX after CREATE TABLE (schema file bug).

All test files import and use this function:

- conftest.py (in-memory fixture)
- test_import_period_data_cli.py (file-based fixture)
- Prevents divergence if schema handling needs updates

### Why Separate In-Memory and File-Based Patterns?

**In-Memory (test_db):**

- Fastest: ~0.005s per test
- Use for: Direct unit/integration tests
- Limitation: Can't pass path to subprocess

**File-Based (template DB):**

- Fast enough: ~0.005s per test (with template pattern)
- Use for: CLI tests requiring actual file paths
- Pattern: Session-scoped template + function-scoped copy
- Avoids: Per-test schema execution (60x slower)

## Gotchas to Avoid

### ❌ Never create new `:memory:` database per test

- Schema execution: 0.3s per test
- 400 tests: 120s overhead (test suite too slow)
- Fix: Use module-scoped db_connection with rollback pattern

### ❌ Never commit changes in test_db fixture

- Modifications pollute next test
- Transaction rollback won't work after commit
- Fix: Use test_db fixture which handles rollback automatically

### ❌ Never use `:memory:` with subprocess tests

- Subprocess can't access parent process memory
- Result: Tests hang or fail silently
- Fix: Use file-based template DB pattern (temp file path)

### ❌ Never duplicate schema parsing logic

- Creates risk of divergence
- Makes maintenance harder
- Fix: Use `_parse_and_reorder_schema()` from conftest everywhere

## How to Use This Guide

### For test writers

1. Direct DB tests → Use `test_db` fixture
2. CLI tests → Use session-scoped template DB pattern
3. Multiple scenarios → Use `@pytest.mark.parametrize`

### For future optimizations

1. Check Performance Baseline section (6.27s goal)
2. Run `pytest tests/ -v --durations=10` after changes
3. If adding new tests, follow existing patterns
4. Avoid the gotchas listed above

## Implementation Reference

The implementation of these patterns is in `tests/conftest.py`:

- `schema_sql` fixture (session-scoped): Loads schema once
- `db_connection` fixture (module-scoped): Creates in-memory DB with schema
- `test_db` fixture (function-scoped): Provides transaction-isolated DB view
- `_parse_and_reorder_schema()` function: Single source of truth for schema parsing
