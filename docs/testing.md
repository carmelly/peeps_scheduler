# Test Suite Documentation

Comprehensive guide to the test suite structure, fixtures, conventions, and best practices.

## Directory Structure

```text
tests/
  conftest.py                      # Core fixtures (test_db, peep_factory, event_factory)

  fixtures/                        # Shared test data (used by CLI AND db tests)
    __init__.py
    data_specs.py                  # EventSpec, ResponseSpec, MemberSpec dataclasses
    conftest.py                    # File builder fixtures

  db/
    __init__.py
    conftest.py                    # DB fixtures, test_period_data (builder-based)
    helpers.py                     # Query/assertion helpers
    test_snapshots.py              # SnapshotGenerator unit tests

    import/                        # Import-related tests (db/import_period_data.py)
      __init__.py
      conftest.py                  # importer_factory, imported_period
      test_members.py              # MemberCollector tests
      test_responses.py            # Response import tests
      test_events.py               # Event creation tests
      test_assignments.py          # Assignment import tests
      test_attendance.py           # Attendance import tests
      test_cancellations.py        # Cancelled events/availability
      test_partnerships.py         # Partnership import tests
      test_snapshots.py            # Snapshot calculation integration tests
      test_integration.py          # Period/event status, full import flow
      test_cli.py                  # CLI main() tests (mocked importer/collector)

    validation/                    # Validation tests (db/validate.py)
      __init__.py
      conftest.py
```

## Fixture Hierarchy

Pytest uses the "closest" fixture when multiple fixtures share the same name. This allows fixture shadowing at different directory levels.

```text
tests/conftest.py
  - test_db              # In-memory SQLite with schema
  - schema_sql           # Parsed and reordered schema SQL
  - peep_factory         # Factory for creating Peep objects
  - event_factory        # Factory for creating Event objects

tests/fixtures/data_specs.py
  - EventSpec            # Dataclass for event test data
  - ResponseSpec         # Dataclass for response test data
  - MemberSpec           # Dataclass for member test data
  - AttendanceSpec       # Dataclass for attendance test data

tests/fixtures/conftest.py (file builder fixtures)
  - results_json_builder      # Builds results.json from EventSpec list
  - responses_csv_builder     # Builds responses.csv from ResponseSpec list
  - members_csv_builder       # Builds members.csv from MemberSpec list
  - attendance_json_builder   # Builds actual_attendance.json from AttendanceSpec list
  - cancellations_json_builder
  - partnerships_json_builder

tests/db/conftest.py
  - test_period_data(period_name="2025-02", num_members=10, num_events=3)
                         # Creates complete test period using builders

tests/db/import/conftest.py
  - ImporterContext (NamedTuple)   # Return type: importer, cursor, period_data, peep_id_mapping, period_id
  - importer_factory               # Factory fixture - call with parameters:
                                   #   importer_factory(period_name='2025-02', num_members=10,
                                   #                    create_period=True, skip_snapshots=True)
                                   # Returns: ImporterContext
  - imported_period                # Fully imported period with events and attendance
```

## Data Specs

Data specs are dataclasses that define test data. They are consumed by file builders to create test files.

### EventSpec

```python
from tests.fixtures.data_specs import EventSpec

event = EventSpec(
    date="2025-02-07 17:00",           # Required: YYYY-MM-DD HH:MM format
    duration_minutes=120,               # Default: 120
    attendees=[                         # List of (id, name, role) tuples
        (1, "Alice", "leader"),
        (2, "Bob", "follower")
    ],
    alternates=[                        # Optional alternates
        (3, "Carol", "leader")
    ]
)
```

### ResponseSpec

```python
from tests.fixtures.data_specs import ResponseSpec

response = ResponseSpec(
    email="alice@test.com",             # Required
    name="Alice",                       # Required
    role="leader",                      # Default: "leader"
    availability=[                      # List of availability strings
        "Friday February 7th - 5pm to 7pm",
        "Friday February 14th - 5pm to 7pm"
    ],
    max_sessions=2,                     # Default: 2
    min_interval_days=0,                # Default: 0
    secondary_role="I only want to be scheduled in my primary role",  # Default
)
```

### MemberSpec

```python
from tests.fixtures.data_specs import MemberSpec

member = MemberSpec(
    csv_id=1,                           # Required: ID in CSV
    name="Alice",                       # Required
    email="alice@test.com",             # Auto-generated if None
    role="leader",                      # Default: "leader"
    active=True,                        # Default: True
    priority=1,                         # Default: 1
)
```

### AttendanceSpec

```python
from tests.fixtures.data_specs import AttendanceSpec

attendance = AttendanceSpec(
    date="2025-02-07 17:00",            # Required: YYYY-MM-DD HH:MM format
    duration_minutes=120,               # Default: 120
    attendees=[                         # List of (id, name, role) tuples
        (1, "Alice", "leader"),
        (2, "Bob", "follower")
    ]
)
```

## File Builders

File builders are fixtures that create test data files from specs. They accept `period_dir` as the first argument.

**CRITICAL:** File builders must accept `period_dir` as first argument so they write to the same directory as the importer context. They should NOT call `test_period_data()` internally - that would create a different temp directory.

### Usage Pattern

```python
def test_something(importer_factory, results_json_builder):
    ctx = importer_factory()
    period_dir = Path(ctx.period_data['period_dir'])

    # Use builder to create/overwrite file
    results_json_builder(period_dir, [
        EventSpec(date="2025-02-07 17:00", attendees=[(1, "Alice", "leader")])
    ])

    # Now run import - reads from same directory
    ctx.importer.import_assignments()
```

### Available Builders

| Builder | Input | Output File |
| --------- | ------- | ------------- |
| `results_json_builder` | `List[EventSpec]` | `results.json` |
| `responses_csv_builder` | `List[ResponseSpec]` | `responses.csv` |
| `members_csv_builder` | `List[MemberSpec]` | `members.csv` |
| `attendance_json_builder` | `List[AttendanceSpec]` | `actual_attendance.json` |
| `cancellations_json_builder` | `dict` | `cancellations.json` |
| `partnerships_json_builder` | `dict` | `partnerships.json` |

## DB Assertion Helpers

Located in `tests/db/helpers.py`. Import as needed:

```python
from tests.db.helpers import assert_event_count, assert_row_count
```

### Query Helpers

| Helper | Description |
| -------- | ------------- |
| `get_table_count(cursor, table, where=None)` | Count rows in table |
| `get_period_id(cursor, period_name)` | Get period ID by name |
| `get_event_by_datetime(cursor, period_id, datetime_str)` | Get event by datetime |

### Assertion Helpers

| Helper | Description |
| -------- | ------------- |
| `assert_row_count(cursor, table, expected, where=None)` | Assert row count |
| `assert_period_status(cursor, period_id, expected_status)` | Assert period status |
| `assert_event_status(cursor, event_id, expected_status)` | Assert event status |
| `assert_event_count(cursor, period_id, expected)` | Assert event count for period |
| `assert_assignment_count(cursor, event_id, expected, assignment_type=None)` | Assert assignments |
| `assert_attendance_count(cursor, event_id, expected)` | Assert attendance records |
| `assert_response_count(cursor, period_id, expected)` | Assert response count |
| `assert_snapshot_count(cursor, period_id, expected)` | Assert snapshot count |

## Fixtures: When to Use What

| Call | Use When | Example |
| ---- | -------- | ------- |
| `importer_factory()` | Testing individual import methods (period exists) | Testing cancelled events aren't imported |
| `importer_factory(create_period=False)` | Testing full `import_period()` flow | Testing period/event status determination |
| `importer_factory(skip_snapshots=False)` | Testing snapshot generation | Verifying snapshot records created |
| `importer_factory(period_name='2025-03')` | Testing with different period | Multi-period tests |
| `imported_period` fixture | Testing post-import state | Validation queries against imported data |
| `*_builder` fixtures | Customizing files before import | Adding cancellations.json before import |
| Data specs | Defining custom test data for builders | Creating event with specific attendees |

### Decision Tree

1. **Need to test `import_period()` directly?** Use `importer_factory(create_period=False)`
2. **Need to test individual import methods?** Use `importer_factory()` + builders
3. **Need to test snapshots?** Use `importer_factory(skip_snapshots=False)`
4. **Need multiple periods?** Call `importer_factory()` multiple times with different `period_name`
5. **Need fully imported data?** Use `imported_period` fixture
6. **Testing MemberCollector directly?** Use `test_db` + `test_period_data` + `members_csv_builder`

## Conventions

### Test Structure (AAA Pattern)

Use whitespace to separate Arrange, Act, Assert. Do NOT use comments like `# Arrange`.

```python
def test_imports_assignments_from_results_json(importer_factory, results_json_builder):
    """PeriodImporter imports assignments from results.json."""
    ctx = importer_factory()
    period_dir = Path(ctx.period_data['period_dir'])

    response_mapping = ctx.importer.import_responses()
    ctx.importer.create_events(response_mapping)
    results_json_builder(period_dir, [
        EventSpec(date="2025-02-07 17:00", attendees=[(1, "Member 1", "leader")])
    ])

    imported_count = ctx.importer.import_assignments()

    assert imported_count == 1
    assert_assignment_count(ctx.cursor, event_id=1, expected=1)
```

### Naming Conventions

**Test files:**

- `test_<feature>.py` - e.g., `test_responses.py`, `test_assignments.py`

**Test classes:**

- `Test<Feature>` - e.g., `TestResponseImport`, `TestAssignmentImport`
- Use `@pytest.mark.db` for database tests

**Test functions:**

- `test_<action>_<expected_outcome>` - e.g., `test_imports_all_responses`
- `test_<scenario>_<expected_behavior>` - e.g., `test_missing_email_raises_error`

### Parameterization

Use `@pytest.mark.parametrize` to reduce duplicate tests:

```python
@pytest.mark.parametrize("secondary_role,expected_switch_pref", [
    pytest.param(
        "I only want to be scheduled in my primary role", 1,
        id="primary_only"
    ),
    pytest.param(
        "I'm happy to dance my secondary role if it lets me attend", 2,
        id="switch_if_primary_full"
    ),
])
def test_parses_switch_preference(importer_factory, secondary_role, expected_switch_pref):
    # Test implementation
    ...
```

### Markers

| Marker | Description |
| -------- | ------------- |
| `@pytest.mark.db` | Database integration tests |
| `@pytest.mark.integration` | Full integration tests |
| `@pytest.mark.slow` | Slow-running tests |
| `@pytest.mark.revisit("Issue #NNN: description")` | Tests documenting current behavior that should change |

## Writing New Tests

### Adding a New Import Test

1. Choose the right factory call:
   - `importer_factory()` for most import tests
   - `importer_factory(skip_snapshots=False)` for snapshot tests
   - `importer_factory(create_period=False)` for import_period() tests

2. Use builders to set up test data:

   ```python
   def test_new_feature(importer_factory, results_json_builder):
       ctx = importer_factory()
       period_dir = Path(ctx.period_data['period_dir'])
       results_json_builder(period_dir, [EventSpec(...)])
   ```

3. Use assertion helpers for verification:

   ```python
   from tests.db.helpers import assert_event_count
   assert_event_count(cursor, period_id, expected=3)
   ```

### Testing Error Cases

For malformed data that builders can't produce, write JSON/CSV directly:

```python
def test_invalid_json_structure(importer_factory):
    ctx = importer_factory()
    period_dir = Path(ctx.period_data['period_dir'])

    # Builder can't create invalid structure, so write directly
    invalid_data = {"invalid_key": "not_an_array"}
    with open(period_dir / "partnerships.json", "w") as f:
        json.dump(invalid_data, f)

    with pytest.raises(ValueError, match="must be a list"):
        ctx.importer.import_partnerships()
```

### Testing CLI main()

CLI tests use mocked classes instead of `importer_factory`. This tests main()'s branching logic without running actual imports:

```python
from unittest.mock import patch, MagicMock

@pytest.mark.db
class TestMainCLI:
    def test_main_period_flag(self, tmp_path):
        with patch('db.import_period_data.PeriodImporter') as MockImporter:
            with patch('db.import_period_data.MemberCollector') as MockCollector:
                mock_collector = MagicMock()
                mock_collector.peep_id_mapping = {}
                MockCollector.return_value = mock_collector

                main(['--period', '2025-02'])

                MockImporter.assert_called_once()
                assert MockImporter.call_args.kwargs['period_name'] == '2025-02'

    def test_main_skip_snapshots_flag(self, tmp_path):
        with patch('db.import_period_data.PeriodImporter') as MockImporter:
            with patch('db.import_period_data.MemberCollector') as MockCollector:
                MockCollector.return_value.peep_id_mapping = {}

                main(['--period', '2025-02', '--skip-snapshots'])

                assert MockImporter.call_args.kwargs['skip_snapshots'] is True
```

This approach:

- Tests main()'s orchestration logic (flag parsing, arg passing)
- Runs fast (no actual imports)
- Covers --period, --all, --dry-run, --skip-snapshots flags

### Adding New Fixtures

1. **Shared across all tests** - Add to `tests/conftest.py`
2. **Shared file builders** - Add to `tests/fixtures/conftest.py`
3. **DB-specific** - Add to `tests/db/conftest.py`
4. **Import-specific** - Add to `tests/db/import/conftest.py`

## Running Tests

### Common Commands

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=db --cov-report=term-missing

# Run specific test file
pytest tests/db/import/test_responses.py

# Run specific test class
pytest tests/db/import/test_responses.py::TestResponseImport

# Run specific test
pytest tests/db/import/test_responses.py::TestResponseImport::test_imports_all_responses

# Run tests matching pattern
pytest -k "response"

# Run only db-marked tests
pytest -m db

# Run with verbose output
pytest -v

# Run last failed tests
pytest --lf
```

### Coverage Requirements

- All new code must have tests
- Coverage should not decrease after changes
- Run coverage check before committing:
  
  ```bash
  pytest --cov=db --cov-report=term-missing --cov-fail-under=80
  ```

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Solution |
| -------------- | --------- | ---------- |
| Manual file creation | Duplicates builder logic | Use `*_builder` fixtures |
| Manual MemberCollector/PeriodImporter | Duplicates fixture setup | Use `importer_factory` |
| **Direct `cursor.execute()` for queries** | Duplicates helper logic, inconsistent | Use helpers from `tests/db/helpers.py` |
| Shared mutable state | Tests interfere with each other | Use function-scoped fixtures |
| Testing implementation details | Brittle tests | Test observable behavior |
| Copy-paste test variations | Maintenance burden | Use `@pytest.mark.parametrize` |
| `# Arrange`, `# Act`, `# Assert` comments | Unnecessary noise | Use whitespace separation |

**CRITICAL: Never use `cursor.execute()` directly for assertions.** Always use the most specific helper function available:

```python
# WRONG - raw SQL
cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (period_id,))
count = cursor.fetchone()[0]
assert count == 3

# RIGHT - use helper
assert_event_count(ctx.cursor, period_id, expected=3)

# WRONG - raw SQL for single value
cursor.execute("SELECT status FROM events WHERE id = ?", (event_id,))
status = cursor.fetchone()[0]
assert status == 'completed'

# RIGHT - use helper
assert_event_status(ctx.cursor, event_id, 'completed')
```

If no helper exists for your use case, add one to `tests/db/helpers.py` first.

**CRITICAL: Always use builders with data specs for test data files.** Never write JSON/CSV directly except when testing malformed data that specs cannot produce:

```python
# WRONG - writing JSON directly
with open(period_dir / 'results.json', 'w') as f:
    json.dump({'valid_events': [...]}, f)

# RIGHT - use builder with spec
results_json_builder(period_dir, [
    EventSpec(date='2025-02-07 17:00', attendees=[(1, 'Alice', 'leader')])
])

# WRONG - writing CSV directly
with open(period_dir / 'responses.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=[...])
    writer.writerow({...})

# RIGHT - use builder with spec
responses_csv_builder(period_dir, [
    ResponseSpec(email='alice@test.com', name='Alice', availability=[...])
])

# EXCEPTION - malformed data that specs can't produce (for error handling tests)
invalid_data = {"invalid_key": "not_an_array"}  # Intentionally wrong structure
with open(period_dir / "partnerships.json", "w") as f:
    json.dump(invalid_data, f)
```

### Anti-Pattern Code Examples (DO NOT DO)

```python
# WRONG: Manually creating files
with open(some_path / 'results.json', 'w') as f:
    json.dump({...}, f)

# WRONG: Instantiating collectors/importers directly
collector = MemberCollector(...)
importer = PeriodImporter(...)

# WRONG: Using builders without period_dir (writes to wrong directory)
results_json_builder(events)  # Missing period_dir!
```

## Before/After Example

Shows the value of using fixtures vs manual setup.

### Before (40 lines of boilerplate)

```python
def test_response_email_not_in_peeps_table(self, test_db, test_period_data):
    """Test that response with unknown email raises ValueError."""
    period_data = next(test_period_data(period_name='2025-02', num_members=5))
    cursor = test_db.cursor()

    collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
    collector.scan_all_periods()
    collector.insert_members_to_db(cursor)

    importer = PeriodImporter(
        period_name='2025-02',
        processed_data_path=Path(period_data['temp_dir']),
        peep_id_mapping=collector.peep_id_mapping,
        cursor=cursor,
        verbose=False,
        skip_snapshots=True
    )
    importer.create_schedule_period()

    # Create response with unknown email
    responses_path = Path(period_data['period_dir']) / 'responses.csv'
    with open(responses_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', ...])
        writer.writeheader()
        writer.writerow({
            'Timestamp': '2/1/2025 10:00:00',
            'Email Address': 'unknown@example.com',
            ...
        })

    with pytest.raises(ValueError, match=r"Email mismatch.*does not match any member"):
        importer.import_responses()
```

### After (15 lines, focused on what matters)

```python
def test_response_email_not_in_peeps_table(self, importer_factory, responses_csv_builder):
    """Test that response with unknown email raises ValueError."""
    ctx = importer_factory()
    period_dir = Path(ctx.period_data['period_dir'])

    responses_csv_builder(period_dir, [
        ResponseSpec(
            email='unknown@example.com',  # Not in peeps table
            name='Unknown Person',
            role='leader',
            availability=['Friday February 7th - 5pm to 7pm'],
        )
    ])

    with pytest.raises(ValueError, match=r"Email mismatch.*does not match any member"):
        ctx.importer.import_responses()
```

## Quality Gate

**BLOCKING requirements - must pass before any PR:**

- [ ] All tests pass (zero failures)
- [ ] Coverage >= baseline (run `pytest --cov` before and after)
- [ ] No regressions in tested behavior

**If any test fails:**

1. STOP - do not proceed
2. Fix the failing test before continuing
3. Re-run to confirm green
