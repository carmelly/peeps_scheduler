"""
Comprehensive test suite for db/validate.py CLI validation tool.

Tests cover:
- --validate-members: Validate all members against CSV
- --validate-period: Comprehensive period validation (responses, assignments, attendance, snapshots)
- --show-period: Display period summary counts
- --list-periods: List all periods chronologically
- CLI integration: help, error handling, database paths
"""

import pytest
import subprocess
import sys
import sqlite3
import tempfile
import csv
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from file_io import normalize_email


# =============================================================================
# TEST DATA FIXTURES
# =============================================================================

@pytest.fixture(scope='module')
def test_data_dir():
    """Create temporary directory with test CSV/JSON files."""
    temp_dir = Path(tempfile.mkdtemp())
    processed_dir = temp_dir / 'processed' / '2020-01'
    processed_dir.mkdir(parents=True)

    # Create members.csv (using production format: lowercase roles)
    members_data = [
        {'id': '1', 'Name': 'Alice Leader', 'Display Name': 'Alice', 'Email Address': 'alice@example.com',
         'Role': 'leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '5', 'Index': '0', 'Total Attended': '1'},
        {'id': '2', 'Name': 'Bob Follower', 'Display Name': 'Bob', 'Email Address': 'bob@example.com',
         'Role': 'follower', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '3', 'Index': '1', 'Total Attended': '2'},
        {'id': '3', 'Name': 'Carol Both', 'Display Name': 'Carol', 'Email Address': 'carol@example.com',
         'Role': 'leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '2', 'Index': '2', 'Total Attended': '1'},
    ]
    with open(processed_dir / 'members.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
        writer.writeheader()
        writer.writerows(members_data)

    # Create responses.csv
    responses_data = [
        {'Email Address': 'alice@example.com', 'Name': 'Alice Leader', 'Primary Role': 'Leader',
         'Max Sessions': '2', 'Min Interval Days': '7', 'Timestamp': '2020-01-01 10:00:00',
         'Availability': 'Thursday January 7th - 5pm to 7pm, Thursday January 14th - 5pm to 7pm'},
        {'Email Address': 'bob@example.com', 'Name': 'Bob Follower', 'Primary Role': 'Follower',
         'Max Sessions': '1', 'Min Interval Days': '14', 'Timestamp': '2020-01-01 11:00:00',
         'Availability': ''},
    ]
    with open(processed_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
        writer.writeheader()
        writer.writerows(responses_data)

    # Create results.json (using production format: datetime with time, id, duration, lowercase roles)
    results_data = {
        'valid_events': [
            {'id': 1, 'date': '2020-01-07 17:00', 'duration_minutes': 120,
             'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}],
             'alternates': [{'id': 3, 'role': 'leader'}]},
            {'id': 2, 'date': '2020-01-14 17:00', 'duration_minutes': 120,
             'attendees': [{'id': 2, 'role': 'follower'}, {'id': 3, 'role': 'leader'}],
             'alternates': []}
        ],
        'downgraded_events': []
    }
    with open(processed_dir / 'results.json', 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2)

    # Create actual_attendance.json (using production format: dict with valid_events, datetime with time, lowercase roles)
    attendance_data = {
        'valid_events': [
            {'id': 1, 'date': '2020-01-07 17:00', 'duration_minutes': 120,
             'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}]},
            {'id': 2, 'date': '2020-01-14 17:00', 'duration_minutes': 120,
             'attendees': [{'id': 2, 'role': 'follower'}, {'id': 3, 'role': 'leader'}]}
        ]
    }
    with open(processed_dir / 'actual_attendance.json', 'w', encoding='utf-8') as f:
        json.dump(attendance_data, f, indent=2)

    yield temp_dir / 'processed'

    import shutil
    shutil.rmtree(temp_dir)


@pytest.fixture(scope='module')
def test_db(test_data_dir, tmp_path_factory):
    """Create test database matching test CSV/JSON files."""
    project_root = Path(__file__).parent.parent

    # Load and prepare schema
    with open(project_root / 'db' / 'schema.sql', 'r') as f:
        schema_sql = f.read()

    lines = [line for line in schema_sql.split('\n') if 'sqlite_sequence' not in line.lower()]

    # Separate CREATE INDEX from CREATE TABLE
    index_statements = []
    other_statements = []
    current_statement = []

    for line in lines:
        current_statement.append(line)
        if line.strip().endswith(';'):
            statement = '\n'.join(current_statement)
            if statement.strip().upper().startswith('CREATE INDEX'):
                index_statements.append(statement)
            else:
                other_statements.append(statement)
            current_statement = []

    schema_sql = '\n'.join(other_statements + index_statements)

    # Create database
    db_path = tmp_path_factory.mktemp("db") / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(schema_sql)
    cursor = conn.cursor()

    # Insert test data
    test_data = [
        # Members
        ('''INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
         [(1, 'Alice Leader', 'Alice', normalize_email('alice@example.com'), 'leader', '2024-01-01', 1),
          (2, 'Bob Follower', 'Bob', normalize_email('bob@example.com'), 'follower', '2024-01-01', 1),
          (3, 'Carol Both', 'Carol', normalize_email('carol@example.com'), 'leader', '2024-01-01', 1)]),

        # Period
        ('''INSERT INTO schedule_periods (id, period_name, display_name, start_date, end_date, status)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, '2020-01', 'Test Period', '2020-01-01', '2020-01-31', 'completed')]),

        # Snapshots
        ('''INSERT INTO peep_order_snapshots (peep_id, period_id, priority, index_position, total_attended, active)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, 1, 5, 0, 1, 1), (2, 1, 3, 1, 2, 1), (3, 1, 2, 2, 1, 1)]),

        # Responses
        ('''INSERT INTO responses (id, peep_id, period_id, response_role, max_sessions, min_interval_days, response_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
         [(1, 1, 1, 'leader', 2, 7, '2020-01-01 10:00:00'),
          (2, 2, 1, 'follower', 1, 14, '2020-01-01 11:00:00')]),

        # Events (using ISO 8601 format with T separator to match import_period_data.py)
        ('''INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, 1, 0, '2020-01-07T17:00:00', 120, 'completed'),
          (2, 1, 1, '2020-01-14T17:00:00', 120, 'completed')]),

        # Assignments
        ('''INSERT INTO event_assignments (id, event_id, peep_id, assigned_role, assignment_type, assignment_order)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, 1, 1, 'leader', 'attendee', 1), (2, 1, 2, 'follower', 'attendee', 2),
          (3, 1, 3, 'leader', 'alternate', 3), (4, 2, 2, 'follower', 'attendee', 1),
          (5, 2, 3, 'leader', 'attendee', 2)]),

        # Attendance
        ('''INSERT INTO event_attendance
            (id, event_id, peep_id, event_assignment_id, expected_role, expected_type, actual_role,
             attendance_status, participation_mode, last_minute_cancel, check_in_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
         [(1, 1, 1, 1, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2020-01-07 17:00:00'),
          (2, 1, 2, 2, 'follower', 'attendee', 'follower', 'attended', 'scheduled', 0, '2020-01-07 17:00:00'),
          (3, 2, 2, 4, 'follower', 'attendee', 'follower', 'attended', 'scheduled', 0, '2020-01-14 17:00:00'),
          (4, 2, 3, 5, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2020-01-14 17:00:00')])
    ]

    for query, rows in test_data:
        cursor.executemany(query, rows)

    conn.commit()
    conn.close()

    test_db.db_path = str(db_path)
    test_db.data_dir = str(test_data_dir)
    yield test_db


@pytest.fixture
def mutable_test_db(test_db):
    """Create mutable copy of test database."""
    import os
    fd, copy_path = tempfile.mkstemp(suffix='.db')

    import shutil
    shutil.copy2(test_db.db_path, copy_path)
    yield copy_path

    os.close(fd)
    os.unlink(copy_path)


@pytest.fixture
def empty_test_db(tmp_path):
    """Create empty test database with schema only."""
    project_root = Path(__file__).parent.parent
    with open(project_root / 'db' / 'schema.sql', 'r') as f:
        schema_sql = f.read()

    lines = [line for line in schema_sql.split('\n') if 'sqlite_sequence' not in line.lower()]
    index_statements = []
    other_statements = []
    current_statement = []

    for line in lines:
        current_statement.append(line)
        if line.strip().endswith(';'):
            statement = '\n'.join(current_statement)
            if statement.strip().upper().startswith('CREATE INDEX'):
                index_statements.append(statement)
            else:
                other_statements.append(statement)
            current_statement = []

    schema_sql = '\n'.join(other_statements + index_statements)

    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(schema_sql)
    conn.close()

    return str(db_path)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def run_validate_cmd(command, db_path, data_dir=None):
    """Helper to run validate.py command and return result."""
    project_root = Path(__file__).parent.parent
    cmd = [sys.executable, 'db/validate.py'] + command + ['--db', db_path]
    if data_dir:
        cmd += ['--data-dir', data_dir]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=project_root)


# =============================================================================
# TESTS: --validate-members
# =============================================================================

class TestValidateMembersCommand:
    """Tests for --validate-members command."""

    def test_validate_members_success(self, test_db):
        """Test that validation passes when DB matches CSV."""
        result = run_validate_cmd(['--validate-members'], test_db.db_path, test_db.data_dir)
        assert result.returncode == 0, f"Validation should pass: {result.stdout}\n{result.stderr}"
        assert 'PASSED' in result.stdout

    def test_validate_members_detects_missing_member(self, mutable_test_db, test_db):
        """Test that validation detects member in CSV but not in DB."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("DELETE FROM peeps WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-members'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'not in DB' in result.stdout or 'FAILED' in result.stdout

    def test_validate_members_detects_field_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects field mismatches."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE peeps SET full_name = 'Wrong Name' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-members'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_members_email_case_insensitive(self, mutable_test_db, tmp_path):
        """Test that email validation is case-insensitive.

        Bug: validate.py line 186 builds csv_emails set WITHOUT normalizing,
        but DB stores normalized (lowercase) emails. This causes false positives
        where members are reported as "Member in DB but not in CSV" when the
        email differs only in case.

        Example:
        - CSV has: "Alice@Example.com" (mixed case)
        - DB stores: "alice@example.com" (normalized)
        - Bug: Reports "Member in DB but not in CSV" because case doesn't match

        This test:
        1. Keeps the DB with normalized emails (alice@example.com, etc.)
        2. Creates a CSV with MIXED-CASE emails (Alice@Example.com, etc.)
        3. Expects validation to PASS because email comparison should be case-insensitive
        """
        # Create a period directory with mixed-case email in members.csv
        period_dir = tmp_path / '2020-01'
        period_dir.mkdir(parents=True)

        # Create members.csv with MIXED-CASE emails
        # The DB has normalized lowercase emails, but CSV will have mixed case
        members_data = [
            {'id': '1', 'Name': 'Alice Leader', 'Display Name': 'Alice',
             'Email Address': 'Alice@Example.com',  # Mixed case (not normalized)
             'Role': 'Leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
             'Priority': '5', 'Index': '0', 'Total Attended': '1'},
            {'id': '2', 'Name': 'Bob Follower', 'Display Name': 'Bob',
             'Email Address': 'BOB@EXAMPLE.COM',  # All uppercase (not normalized)
             'Role': 'Follower', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
             'Priority': '3', 'Index': '1', 'Total Attended': '2'},
            {'id': '3', 'Name': 'Carol Both', 'Display Name': 'Carol',
             'Email Address': 'carol@example.com',  # Lowercase (normalized)
             'Role': 'Leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
             'Priority': '2', 'Index': '2', 'Total Attended': '1'},
        ]
        with open(period_dir / 'members.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # The DB has normalized emails:
        # - alice@example.com (normalized from any case variation)
        # - bob@example.com (normalized from any case variation)
        # - carol@example.com
        #
        # The CSV has:
        # - Alice@Example.com (mixed case - NOT normalized by bug at line 186)
        # - BOB@EXAMPLE.COM (uppercase - NOT normalized by bug at line 186)
        # - carol@example.com (matches DB)
        #
        # Due to bug at line 186, csv_emails set will be:
        # {"Alice@Example.com", "BOB@EXAMPLE.COM", "carol@example.com"}
        #
        # When checking DB members:
        # - db_member['email'] = 'alice@example.com' NOT IN csv_emails -> ERROR
        # - db_member['email'] = 'bob@example.com' NOT IN csv_emails -> ERROR
        # - db_member['email'] = 'carol@example.com' IN csv_emails -> OK

        result = run_validate_cmd(['--validate-members'], mutable_test_db, str(tmp_path))

        # FIXED: Validation PASSES despite email case differences
        # because CSV emails are now normalized before comparison (line 186)
        assert result.returncode == 0, (
            f"Validation should pass despite email case differences. Got:\n{result.stdout}\n{result.stderr}"
        )
        assert 'PASSED' in result.stdout, (
            f"Expected validation to PASS with case-insensitive email matching. Got:\n{result.stdout}"
        )


# =============================================================================
# TESTS: --validate-period
# =============================================================================

class TestValidatePeriodCommand:
    """Tests for --validate-period <name> command."""

    def test_validate_period_success(self, test_db):
        """Test that comprehensive period validation passes when data matches."""
        result = run_validate_cmd(['--validate-period', '2020-01'], test_db.db_path, test_db.data_dir)
        assert result.returncode == 0, f"Validation should pass: {result.stdout}\n{result.stderr}"
        assert 'PASSED' in result.stdout

    def test_validate_period_detects_response_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB responses don't match CSV."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE responses SET max_sessions = 999 WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_detects_assignment_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB assignments don't match results.json."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE event_assignments SET assigned_role = 'follower' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_detects_attendance_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB attendance doesn't match actual_attendance.json."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE event_attendance SET actual_role = 'follower' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_attendance_with_production_json_structure(self, mutable_test_db, test_db, tmp_path):
        """Test that attendance validation detects mismatches with production JSON structure.

        This test exposes the bug where validate.py line 419 uses .get('events', [])
        but production actual_attendance.json uses the key 'valid_events', not 'events'.
        Without the fix, validation will silently PASS even though it never validated anything.

        The test:
        1. Sets up a period with mismatched attendance data (wrong role in DB)
        2. Creates actual_attendance.json using production structure with 'valid_events' key
        3. Verifies validation FAILS and detects the role mismatch
        """
        # Create a period directory with production-style actual_attendance.json
        period_dir = tmp_path / '2020-01'
        period_dir.mkdir(parents=True)

        # Production structure: uses 'valid_events' key, not 'events'
        attendance_data = {
            'valid_events': [
                {'date': '2020-01-07 17:00', 'attendees': [
                    {'id': 1, 'role': 'leader'},
                    {'id': 2, 'role': 'follower'}
                ]},
                {'date': '2020-01-14 17:00', 'attendees': [
                    {'id': 2, 'role': 'follower'},
                    {'id': 3, 'role': 'leader'}
                ]}
            ]
        }
        with open(period_dir / 'actual_attendance.json', 'w', encoding='utf-8') as f:
            json.dump(attendance_data, f, indent=2)

        # Update DB to have a WRONG role for peep 1 at event 1
        # Expected: leader, DB should have: follower (mismatch)
        conn = sqlite3.connect(mutable_test_db)
        conn.execute(
            "UPDATE event_attendance SET actual_role = 'follower' WHERE event_id = 1 AND peep_id = 1"
        )
        conn.commit()
        conn.close()

        # Run validation with the production-style actual_attendance.json
        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, str(tmp_path))

        # The validation SHOULD FAIL because DB has 'follower' but JSON expects 'leader'
        assert result.returncode != 0, (
            f"Validation should FAIL due to role mismatch, but got: {result.stdout}\n{result.stderr}"
        )
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout, (
            f"Expected mismatch/FAILED message, got: {result.stdout}"
        )

    def test_validate_period_detects_snapshot_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB snapshots don't match actual attendance."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE peep_order_snapshots SET total_attended = 999 WHERE peep_id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'total_attended' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_not_found(self, test_db):
        """Test that validation handles non-existent period."""
        result = run_validate_cmd(['--validate-period', '9999-99'], test_db.db_path, test_db.data_dir)
        assert result.returncode != 0
        assert 'not found' in (result.stderr or result.stdout).lower()

    def test_validate_period_handles_datetime_format_mismatch(self, mutable_test_db, test_db, tmp_path):
        """Test that validation passes when DB uses ISO 8601 format and JSON uses space separator.

        This test verifies the datetime format normalization:
        - DB stores events with ISO 8601 format: '2020-01-07T17:00:00'
        - results.json uses space separator format: '2020-01-07 17:00'
        - Validation should normalize both formats and find matching events
        """
        # Update the mutable_test_db to use ISO 8601 format for events
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE events SET event_datetime = '2020-01-07T17:00:00' WHERE id = 1")
        conn.execute("UPDATE events SET event_datetime = '2020-01-14T17:00:00' WHERE id = 2")
        conn.commit()
        conn.close()

        # Create a period directory matching test_db period name with results.json using space-separated datetime
        period_dir = tmp_path / '2020-01'
        period_dir.mkdir(parents=True)

        # Create minimal results.json with space-separated datetime format
        results_data = {
            'valid_events': [
                {'date': '2020-01-07 17:00', 'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}],
                 'alternates': [{'id': 3, 'role': 'leader'}]},
                {'date': '2020-01-14 17:00', 'attendees': [{'id': 2, 'role': 'follower'}, {'id': 3, 'role': 'leader'}],
                 'alternates': []}
            ],
            'downgraded_events': []
        }
        with open(period_dir / 'results.json', 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2)

        # Create minimal actual_attendance.json with space-separated datetime format
        attendance_data = [
            {'date': '2020-01-07 17:00', 'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}]},
            {'date': '2020-01-14 17:00', 'attendees': [{'id': 2, 'role': 'follower'}, {'id': 3, 'role': 'leader'}]}
        ]
        with open(period_dir / 'actual_attendance.json', 'w', encoding='utf-8') as f:
            json.dump(attendance_data, f, indent=2)

        # Verify validation passes with format mismatch
        # The DB has ISO 8601 format ('2020-01-07T17:00:00')
        # The JSON has space-separated format ('2020-01-07 17:00')
        result = run_validate_cmd(['--validate-period', '2020-01'], mutable_test_db, str(tmp_path))
        assert result.returncode == 0, f"Validation should pass despite datetime format mismatch: {result.stdout}\n{result.stderr}"
        assert 'PASSED' in result.stdout

    def test_validate_events_checks_proposed_events(self, mutable_test_db, test_db, tmp_path):
        """Test that validate_events() checks ALL events in database against responses.csv availability data.

        Bug: validate_period only validates events that appear in results.json (scheduled) or
        actual_attendance.json (completed). Proposed events from responses.csv are never validated.

        Expected behavior: validate_events() should check ALL events in database against
        responses.csv availability data by extracting events from Availability column.

        This test:
        1. Creates a period with 2 events in responses.csv availability
        2. Adds a 3rd EXTRA event to database that doesn't match responses.csv
        3. Expects validate_events() to detect the mismatch (extra event in DB)
        4. Should FAIL because validate_events() doesn't exist yet
        """
        from db.validate import validate_events

        # Create period directory with responses.csv containing 2 events
        period_dir = tmp_path / '2020-01'
        period_dir.mkdir(parents=True)

        # Create responses.csv with availability for 2 events only
        # Events will be auto-derived from Availability column
        responses_data = [
            {'Timestamp': '2020-01-01 10:00:00',
             'Email Address': 'alice@example.com',
             'Name': 'Alice Leader',
             'Primary Role': 'Leader',
             'Max Sessions': '2',
             'Min Interval Days': '7',
             'Secondary Role': 'I only want to be scheduled in my primary role',
             'Availability': 'Friday February 7th - 5pm to 7pm, Friday February 14th - 5pm to 7pm'},
        ]
        with open(period_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # Add a 3rd event to the database that doesn't match responses.csv
        # DB already has 2 events (Feb 7, Feb 14), we'll add Feb 21
        conn = sqlite3.connect(mutable_test_db)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (3, 1, 2, '2020-01-21T17:00:00', 120, 'proposed'))
        conn.commit()

        # Get period_id
        cursor.execute("SELECT id FROM schedule_periods WHERE period_name = '2020-01'")
        period_id = cursor.fetchone()[0]

        # Read responses.csv for validate_events
        with open(period_dir / 'responses.csv', 'r', encoding='utf-8') as f:
            import csv as csv_module
            responses_csv = list(csv_module.DictReader(f))

        # Call validate_events() - this should detect the extra event
        # Note: period_name needs year extraction, use '2020-01' format
        issues = validate_events(cursor, period_id, '2020-01', responses_csv)

        conn.close()

        # Should detect mismatch: 3 events in DB vs 2 in responses.csv
        assert len(issues) > 0, "validate_events() should detect extra event in database"
        assert any('in db but not in responses.csv' in issue.lower()
                   for issue in issues), f"Expected extra event issue, got: {issues}"


# =============================================================================
# TESTS: --show-period
# =============================================================================

class TestShowPeriodCommand:
    """Tests for --show-period <name> command."""

    def test_show_period_displays_basic_info(self, test_db):
        """Test that show-period displays period information."""
        result = run_validate_cmd(['--show-period', '2020-01'], test_db.db_path)
        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert '2020-01' in result.stdout.lower()
        assert '2020-01-01' in result.stdout

    def test_show_period_displays_counts(self, test_db):
        """Test that show-period displays counts for all record types."""
        result = run_validate_cmd(['--show-period', '2020-01'], test_db.db_path)
        assert result.returncode == 0
        output = result.stdout.lower()
        assert all(word in output for word in ['event', 'response', 'assignment', 'attendance', 'snapshot'])

    def test_show_period_not_found(self, test_db):
        """Test that show-period handles non-existent period."""
        result = run_validate_cmd(['--show-period', '9999-99'], test_db.db_path)
        assert result.returncode != 0
        assert 'not found' in (result.stderr or result.stdout).lower()


# =============================================================================
# TESTS: --list-periods
# =============================================================================

class TestListPeriodsCommand:
    """Tests for --list-periods command."""

    def test_list_periods_shows_all_periods(self, test_db):
        """Test that list-periods shows all periods."""
        result = run_validate_cmd(['--list-periods'], test_db.db_path)
        assert result.returncode == 0
        assert '2020-01' in result.stdout.lower()

    def test_list_periods_shows_dates(self, test_db):
        """Test that list-periods shows dates."""
        result = run_validate_cmd(['--list-periods'], test_db.db_path)
        assert result.returncode == 0
        assert '2020-01-01' in result.stdout or '2020-01-28' in result.stdout

    def test_list_periods_empty_database(self, empty_test_db):
        """Test that list-periods handles empty database."""
        result = run_validate_cmd(['--list-periods'], empty_test_db)
        assert result.returncode == 0


# =============================================================================
# TESTS: CLI Integration
# =============================================================================

class TestCLIIntegration:
    """Integration tests for CLI."""

    def test_cli_requires_command(self, empty_test_db):
        """Test that CLI requires a command."""
        result = run_validate_cmd([], empty_test_db)
        assert result.returncode != 0 or 'usage' in result.stdout.lower()

    def test_cli_help_flag(self):
        """Test that --help works."""
        result = subprocess.run(
            [sys.executable, 'db/validate.py', '--help'],
            capture_output=True, text=True,
            cwd=Path(__file__).parent.parent
        )
        assert result.returncode == 0
        output = result.stdout.lower()
        assert 'usage' in output or 'help' in output
        assert 'validate-members' in output or 'validate-period' in output

    def test_cli_handles_missing_database(self):
        """Test that CLI handles missing database gracefully."""
        result = run_validate_cmd(['--list-periods'], 'E:\\nonexistent\\db.db')
        assert result.returncode != 0
        assert 'not found' in (result.stderr or result.stdout).lower()
