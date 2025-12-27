"""Test fixtures and configuration for peeps_scheduler test suite.

For comprehensive test architecture documentation, fixture patterns, and best practices,
see docs/test-architecture.md

This module implements:
- Session-scoped schema loading (_parse_and_reorder_schema, schema_sql)
- Module-scoped in-memory database with transaction rollback (db_connection)
- Function-scoped test database fixtures (test_db, test_db_path)
- Test data factories (peep_factory, event_factory, test_period_data)
"""

import datetime
import sqlite3
from pathlib import Path
import pytest
from peeps_scheduler.constants import SCHEMA_PATH
from peeps_scheduler.models import Event, Peep, Role, SwitchPreference
from tests.fixtures.conftest import (
    members_csv_builder,
    responses_csv_builder,
    results_json_builder,
    attendance_json_builder,
    cancellations_json_builder,
    partnerships_json_builder,
)

def _parse_and_reorder_schema(schema_sql):
    """Parse schema SQL and reorder statements (CREATE TABLE before CREATE INDEX).

    The schema file has CREATE INDEX statements before CREATE TABLE statements,
    which is invalid. This function reorders them for correct execution.

    Args:
        schema_sql: Raw schema SQL text

    Returns:
        Reordered SQL string with CREATE TABLE first, then CREATE INDEX
    """
    lines = schema_sql.split('\n')
    index_statements = []
    other_statements = []
    current_statement = []

    for line in lines:
        current_statement.append(line)
        # SQL statements end with semicolon
        if line.strip().endswith(';'):
            statement = '\n'.join(current_statement)
            if statement.strip().upper().startswith('CREATE INDEX'):
                index_statements.append(statement)
            else:
                other_statements.append(statement)
            current_statement = []

    # Reconstruct schema with CREATE TABLE first, then CREATE INDEX
    return '\n'.join(other_statements + index_statements)


@pytest.fixture(scope='session')
def schema_sql():
    """Load schema once per test session."""

    schema_path = Path(SCHEMA_PATH)
    return schema_path.read_text()


@pytest.fixture(scope='module')
def db_connection(schema_sql):
    """Shared database connection with schema applied once per module.

    This module-scoped fixture creates a single in-memory database connection
    per test module. Individual tests use transaction rollback (test_db fixture)
    to maintain isolation while sharing the same schema and base setup.

    Benefits:
    - Schema is created once per module instead of per test
    - Transaction rollback pattern provides test isolation without re-initialization
    - ~5-7x faster than creating new DB per test
    """
    conn = sqlite3.connect(':memory:')
    reordered_sql = _parse_and_reorder_schema(schema_sql)
    conn.executescript(reordered_sql)

    yield conn

    conn.close()


@pytest.fixture
def test_db(db_connection):
    """Isolated test database using transaction rollback for test isolation.

    Each test gets a savepoint at the start. All database modifications
    are rolled back at the end of the test, leaving the database clean
    for the next test. This provides test isolation without re-creating
    the schema repeatedly.

    Usage:
        def test_something(test_db):
            test_db.execute("INSERT INTO peeps ...")
            # Modifications are automatically rolled back after test
    """
    # Start transaction for this test
    db_connection.execute('BEGIN')
    yield db_connection
    # Rollback all changes from this test
    db_connection.rollback()


@pytest.fixture
def test_db_path(schema_sql):
    """Create an isolated in-memory test database and return its path.

    Note: Returns ':memory:' URI instead of file path for in-memory databases.
    Use test_db fixture if you need a connection object instead.
    """
    conn = sqlite3.connect(':memory:')
    reordered_sql = _parse_and_reorder_schema(schema_sql)
    conn.executescript(reordered_sql)

    yield ':memory:'

    # Cleanup
    conn.close()


@pytest.fixture
def peep_factory():
    """Factory for creating test peeps with sensible defaults."""
    def _create(id=1, role=Role.LEADER, **kwargs):
        defaults = {
            'full_name': f'TestPeep{id}',
            'display_name': f'TestPeep{id}',
            'email': f'peep{id}@test.com',
            'availability': [1],
            'event_limit': 2,
            'priority': 0,
            'responded': True,
            'switch_pref': SwitchPreference.PRIMARY_ONLY,
            'index': 0,
            'total_attended': 0,
            'min_interval_days': 0,
            'active': True,
            'date_joined': '2025-01-01'
        }
        defaults.update(kwargs)
        return Peep(id=id, role=role, **defaults)
    return _create


@pytest.fixture
def event_factory():
    """Factory for creating test events with sensible defaults."""
    def _create(id=1, duration_minutes=120, **kwargs):
        defaults = {
            'date': datetime.datetime(2025, 1, 15, 18, 0)
        }
        defaults.update(kwargs)
        return Event(id=id, duration_minutes=duration_minutes, **defaults)
    return _create


@pytest.fixture
def test_period_data():
	"""Generate complete test period data for import testing."""
	import csv
	import json
	import shutil
	import tempfile

	def _create(period_name="2025-02", num_members=10, num_events=3):
		"""Create test CSV/JSON files for a period in a temporary directory."""
		# Create temporary directory for period data
		temp_dir = tempfile.mkdtemp()
		period_dir = Path(temp_dir) / period_name
		period_dir.mkdir(parents=True)

		# Generate test members (match production format)
		members = []
		for i in range(1, num_members + 1):
			members.append(
				{
					"id": i,
					"Name": f"Test Member {i}",
					"Display Name": f"Member{i}",
					"Email Address": f"member{i}@test.com",
					"Role": "leader" if i % 2 == 1 else "follower",
					"Index": 0,
					"Priority": i,
					"Total Attended": 0,
					"Active": "TRUE",
					"Date Joined": "1/1/2025",  # Production format: M/D/YYYY
				}
			)

		# Write members.csv
		with open(period_dir / "members.csv", "w", newline="") as f:
			writer = csv.DictWriter(
				f,
				fieldnames=[
					"id",
					"Name",
					"Display Name",
					"Email Address",
					"Role",
					"Index",
					"Priority",
					"Total Attended",
					"Active",
					"Date Joined",
				],
			)
			writer.writeheader()
			writer.writerows(members)

		# Generate test responses with date strings for auto-derive event mode
		responses = []
		event_dates = [
			"Friday February 7th - 5pm to 7pm",
			"Friday February 14th - 5pm to 7pm",
			"Friday February 21st - 5pm to 7pm",
		][:num_events]

		for i in range(1, min(num_members, 8) + 1):  # First 8 members respond
			# Use actual date strings with time ranges for auto-derive mode
			availability_str = ", ".join(event_dates)
			responses.append(
				{
					"Timestamp": "2/1/2025 10:00:00",
					"Email Address": f"member{i}@test.com",
					"Name": f"Test Member {i}",
					"Primary Role": "leader" if i % 2 == 1 else "follower",
					"Secondary Role": "I only want to be scheduled in my primary role",
					"Max Sessions": 2,
					"Availability": availability_str,
					"Event Duration": "",
					"Session Spacing Preference": "",
					"Min Interval Days": 0,
					"Partnership Preference": "",
					"Questions or Comments": "",
				}
			)

		# Write responses.csv (match production format with all columns)
		with open(period_dir / "responses.csv", "w", newline="") as f:
			writer = csv.DictWriter(
				f,
				fieldnames=[
					"Timestamp",
					"Email Address",
					"Name",
					"Primary Role",
					"Secondary Role",
					"Max Sessions",
					"Availability",
					"Event Duration",
					"Session Spacing Preference",
					"Min Interval Days",
					"Partnership Preference",
					"Questions or Comments",
				],
			)
			writer.writeheader()
			writer.writerows(responses)

		# Note: Events will be auto-derived from availability strings by extract_events()

		# Generate test assignments (results.json) - PRODUCTION FORMAT
		results_data = {
			"valid_events": [],
			"peeps": [],
			"num_unique_attendees": min(num_members, 8),
			"priority_fulfilled": 0,
			"system_weight": 0,
		}

		base_date = datetime.datetime(2025, 2, 7, 17, 0)  # Feb 7, 5pm
		for event_idx in range(num_events):
			event_date = base_date + datetime.timedelta(days=7 * event_idx)
			results_data["valid_events"].append(
				{
					"id": event_idx,
					"date": event_date.strftime("%Y-%m-%d %H:%M"),
					"duration_minutes": 120,
					"attendees": [
						{"id": 1, "name": "Member1", "role": "leader"},
						{"id": 2, "name": "Member2", "role": "follower"},
					],
					"alternates": [{"id": 3, "name": "Member3", "role": "leader"}],
					"leaders_string": "Leaders(2): Member1, Member3",
					"followers_string": "Followers(1): Member2",
				}
			)

		# Write results.json
		with open(period_dir / "results.json", "w") as f:
			json.dump(results_data, f, indent=2)

		# Write output.json (scheduler's snapshot of input data)
		# This should mirror the results structure but represents the scheduler's input state
		output_data = {
			"events": results_data["valid_events"],  # Same events as results
			"peeps": results_data["peeps"],
			"members": members,  # Include member data snapshot
		}
		with open(period_dir / "output.json", "w") as f:
			json.dump(output_data, f, indent=2)

		# Generate test attendance (actual_attendance.json) - PRODUCTION FORMAT
		attendance_data = {"valid_events": []}

		for event_idx in range(num_events):
			event_date = base_date + datetime.timedelta(days=7 * event_idx)
			attendance_data["valid_events"].append(
				{
					"id": event_idx,
					"date": event_date.strftime("%Y-%m-%d %H:%M"),
					"duration_minutes": 120,
					"attendees": [
						{"id": 1, "name": "Member1", "role": "leader"},
						{"id": 2, "name": "Member2", "role": "follower"},
					],
					"alternates": [],
					"leaders_string": "Leaders(1): Member1",
					"followers_string": "Followers(1): Member2",
				}
			)

		# Write actual_attendance.json
		with open(period_dir / "actual_attendance.json", "w") as f:
			json.dump(attendance_data, f, indent=2)

		# Write empty notes.json
		with open(period_dir / "notes.json", "w") as f:
			json.dump([], f, indent=2)

		# Generate cancellations.json (optional - for cancellations feature)
		cancellations_data = {
			"cancelled_events": [],
			"cancelled_availability": [],
			"notes": "Test cancellations data",
		}
		with open(period_dir / "cancellations.json", "w") as f:
			json.dump(cancellations_data, f, indent=2)

		# Generate partnerships.json (optional - for partnerships feature)
		partnerships_data = {}
		with open(period_dir / "partnerships.json", "w") as f:
			json.dump(partnerships_data, f, indent=2)

		yield {
			"temp_dir": temp_dir,
			"period_dir": period_dir,
			"period_name": period_name,
			"num_members": num_members,
			"num_events": num_events,
		}

		# Cleanup
		shutil.rmtree(temp_dir)

	return _create


@pytest.fixture
def prepared_importer(test_db, test_period_data):
    """Fixture providing fully prepared importer with members and period already set up.

    This fixture consolidates common setup code used across multiple import tests,
    reducing duplication and improving readability.

    Returns a dictionary with:
    - 'importer': PeriodImporter instance with period created
    - 'cursor': Database cursor for assertions
    - 'period_data': Test period data (temp_dir, period_dir, etc.)
    - 'peep_id_mapping': Mapping of email to peep ID

    Note: Uses default period_name='2025-02'. Tests needing different periods
    should set up their own importer instead of using this fixture.
    """
    from pathlib import Path
    from peeps_scheduler.db.import_period_data import MemberCollector, PeriodImporter

    # Setup test period data
    period_data = next(test_period_data(period_name="2025-02", num_members=10))

    # Get database cursor
    cursor = test_db.cursor()

    # Collect and insert members
    collector = MemberCollector(processed_data_path=Path(period_data["temp_dir"]), verbose=False)
    collector.scan_all_periods()
    collector.insert_members_to_db(cursor)

    # Create and initialize importer
    importer = PeriodImporter(
        period_name="2025-02",
        processed_data_path=Path(period_data["temp_dir"]),
        peep_id_mapping=collector.peep_id_mapping,
        cursor=cursor,
        verbose=False,
        skip_snapshots=True,
    )
    importer.create_schedule_period()

    return {
        "importer": importer,
        "cursor": cursor,
        "period_data": period_data,
        "peep_id_mapping": collector.peep_id_mapping,
    }


def pytest_sessionfinish(session, exitstatus):
    """Clean up test logs after test session completes."""
    try:
        from peeps_scheduler.logging_config import cleanup_test_logs

        cleanup_test_logs()
    except ImportError:
        # Logging config may not be available in all environments
        pass
