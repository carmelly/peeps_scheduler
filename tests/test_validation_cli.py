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
    processed_dir = temp_dir / 'processed' / 'test-period'
    processed_dir.mkdir(parents=True)

    # Create members.csv
    members_data = [
        {'id': '1', 'Name': 'Alice Leader', 'Display Name': 'Alice', 'Email Address': 'alice@example.com',
         'Role': 'Leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '5', 'Index': '0', 'Total Attended': '1'},
        {'id': '2', 'Name': 'Bob Follower', 'Display Name': 'Bob', 'Email Address': 'bob@example.com',
         'Role': 'Follower', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '3', 'Index': '1', 'Total Attended': '2'},
        {'id': '3', 'Name': 'Carol Both', 'Display Name': 'Carol', 'Email Address': 'carol@example.com',
         'Role': 'Leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE',
         'Priority': '2', 'Index': '2', 'Total Attended': '1'},
    ]
    with open(processed_dir / 'members.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
        writer.writeheader()
        writer.writerows(members_data)

    # Create responses.csv
    responses_data = [
        {'Email Address': 'alice@example.com', 'Name': 'Alice Leader', 'Primary Role': 'Leader',
         'Max Sessions': '2', 'Min Interval Days': '7', 'Timestamp': '2025-02-01 10:00:00'},
        {'Email Address': 'bob@example.com', 'Name': 'Bob Follower', 'Primary Role': 'Follower',
         'Max Sessions': '1', 'Min Interval Days': '14', 'Timestamp': '2025-02-01 11:00:00'},
    ]
    with open(processed_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
        writer.writeheader()
        writer.writerows(responses_data)

    # Create results.json
    results_data = {
        'valid_events': [
            {'date': '2025-02-07', 'attendees': [{'id': 1, 'role': 'Leader'}, {'id': 2, 'role': 'Follower'}],
             'alternates': [{'id': 3, 'role': 'Leader'}]},
            {'date': '2025-02-14', 'attendees': [{'id': 2, 'role': 'Follower'}, {'id': 3, 'role': 'Leader'}],
             'alternates': []}
        ],
        'downgraded_events': []
    }
    with open(processed_dir / 'results.json', 'w', encoding='utf-8') as f:
        json.dump(results_data, f, indent=2)

    # Create actual_attendance.json
    attendance_data = [
        {'date': '2025-02-07', 'attendees': [{'id': 1, 'role': 'Leader'}, {'id': 2, 'role': 'Follower'}]},
        {'date': '2025-02-14', 'attendees': [{'id': 2, 'role': 'Follower'}, {'id': 3, 'role': 'Leader'}]}
    ]
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
         [(1, 'test-period', 'Test Period', '2025-02-01', '2025-02-28', 'completed')]),

        # Snapshots
        ('''INSERT INTO peep_order_snapshots (peep_id, period_id, priority, index_position, total_attended, active)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, 1, 5, 0, 1, 1), (2, 1, 3, 1, 2, 1), (3, 1, 2, 2, 1, 1)]),

        # Responses
        ('''INSERT INTO responses (id, peep_id, period_id, response_role, max_sessions, min_interval_days, response_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
         [(1, 1, 1, 'leader', 2, 7, '2025-02-01 10:00:00'),
          (2, 2, 1, 'follower', 1, 14, '2025-02-01 11:00:00')]),

        # Events
        ('''INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)''',
         [(1, 1, 0, '2025-02-07 17:00:00', 120, 'completed'),
          (2, 1, 1, '2025-02-14 17:00:00', 120, 'completed')]),

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
         [(1, 1, 1, 1, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2025-02-07 17:00:00'),
          (2, 1, 2, 2, 'follower', 'attendee', 'follower', 'attended', 'scheduled', 0, '2025-02-07 17:00:00'),
          (3, 2, 2, 4, 'follower', 'attendee', 'follower', 'attended', 'scheduled', 0, '2025-02-14 17:00:00'),
          (4, 2, 3, 5, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2025-02-14 17:00:00')])
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


# =============================================================================
# TESTS: --validate-period
# =============================================================================

class TestValidatePeriodCommand:
    """Tests for --validate-period <name> command."""

    def test_validate_period_success(self, test_db):
        """Test that comprehensive period validation passes when data matches."""
        result = run_validate_cmd(['--validate-period', 'test-period'], test_db.db_path, test_db.data_dir)
        assert result.returncode == 0, f"Validation should pass: {result.stdout}\n{result.stderr}"
        assert 'PASSED' in result.stdout

    def test_validate_period_detects_response_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB responses don't match CSV."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE responses SET max_sessions = 999 WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', 'test-period'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_detects_assignment_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB assignments don't match results.json."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE event_assignments SET assigned_role = 'follower' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', 'test-period'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_detects_attendance_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB attendance doesn't match actual_attendance.json."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE event_attendance SET actual_role = 'follower' WHERE id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', 'test-period'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'mismatch' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_detects_snapshot_mismatch(self, mutable_test_db, test_db):
        """Test that validation detects when DB snapshots don't match actual attendance."""
        conn = sqlite3.connect(mutable_test_db)
        conn.execute("UPDATE peep_order_snapshots SET total_attended = 999 WHERE peep_id = 1")
        conn.commit()
        conn.close()

        result = run_validate_cmd(['--validate-period', 'test-period'], mutable_test_db, test_db.data_dir)
        assert result.returncode != 0
        assert 'total_attended' in result.stdout.lower() or 'FAILED' in result.stdout

    def test_validate_period_not_found(self, test_db):
        """Test that validation handles non-existent period."""
        result = run_validate_cmd(['--validate-period', '9999-99'], test_db.db_path, test_db.data_dir)
        assert result.returncode != 0
        assert 'not found' in (result.stderr or result.stdout).lower()


# =============================================================================
# TESTS: --show-period
# =============================================================================

class TestShowPeriodCommand:
    """Tests for --show-period <name> command."""

    def test_show_period_displays_basic_info(self, test_db):
        """Test that show-period displays period information."""
        result = run_validate_cmd(['--show-period', 'test-period'], test_db.db_path)
        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert 'test-period' in result.stdout.lower()
        assert '2025-02-01' in result.stdout

    def test_show_period_displays_counts(self, test_db):
        """Test that show-period displays counts for all record types."""
        result = run_validate_cmd(['--show-period', 'test-period'], test_db.db_path)
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
        assert 'test-period' in result.stdout.lower()

    def test_list_periods_shows_dates(self, test_db):
        """Test that list-periods shows dates."""
        result = run_validate_cmd(['--list-periods'], test_db.db_path)
        assert result.returncode == 0
        assert '2025-02-01' in result.stdout or '2025-02-28' in result.stdout

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
