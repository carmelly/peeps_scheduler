"""
Test suite for validator bugs in db/validate.py.

Tests focus on validating the validation logic itself - ensuring validators
check data against the correct source files and skip validation when appropriate.
"""

import pytest
import sqlite3
import tempfile
import csv
import json
import shutil
from pathlib import Path
from datetime import datetime


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def test_db_with_period():
    """Create test database with a period and basic data."""
    # Create temporary database
    db_fd, db_path = tempfile.mkstemp(suffix='.db')

    # Load schema
    project_root = Path(__file__).parent.parent
    with open(project_root / 'db' / 'schema.sql', 'r') as f:
        schema_sql = f.read()

    # Filter and reorder schema statements
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

    # Create database
    conn = sqlite3.connect(db_path)
    conn.executescript(schema_sql)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Insert test data
    # Members
    cursor.execute("""
        INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (1, 'Alice Leader', 'Alice', 'alice@example.com', 'leader', '2024-01-01', 1))
    cursor.execute("""
        INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (2, 'Bob Follower', 'Bob', 'bob@example.com', 'follower', '2024-01-01', 1))

    # Period
    cursor.execute("""
        INSERT INTO schedule_periods (id, period_name, display_name, start_date, end_date, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (1, '2025-02', 'Feb 2025', '2025-02-01', '2025-02-28', 'scheduled'))

    conn.commit()

    yield {'conn': conn, 'cursor': cursor, 'db_path': db_path, 'period_id': 1, 'period_name': '2025-02'}

    # Cleanup
    conn.close()
    import os
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def test_period_dir(tmp_path):
    """Create temporary directory with test period data files."""
    period_dir = tmp_path / '2025-02'
    period_dir.mkdir(parents=True)

    yield period_dir

    # Cleanup happens automatically with tmp_path


# =============================================================================
# TESTS: validate_events() should check duration against status-appropriate source
# =============================================================================

class TestValidateEventsChecksStatusAppropriateSource:
    """
    Tests for validate_events() duration checking logic.

    Current behavior (bug):
    - Always checks DB event durations against responses.csv (proposed durations)
    - Causes false failures when events have different durations after scheduling

    Expected behavior:
    - Check duration against status-appropriate source:
      - status='proposed' -> responses.csv
      - status='scheduled' -> results.json
      - status='completed' -> actual_attendance.json
    """

    def test_scheduled_event_duration_checked_against_results_json(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that scheduled events check duration against results.json, not responses.csv.

        Scenario:
        1. Event proposed at 120 min (responses.csv)
        2. Event scheduled at 90 min (results.json - downgraded)
        3. DB has 90 min with status='scheduled' (correct - matches results.json)
        4. Validator should NOT report mismatch

        This test SHOULD FAIL initially because validate_events() currently checks
        all events against responses.csv, regardless of status.
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create responses.csv with event proposed at 120 minutes
        responses_data = [
            {
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'  # 2 hours = 120 min
            }
        ]
        with open(test_period_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # Create results.json with event scheduled at 90 minutes (downgraded)
        results_data = {
            'valid_events': [],
            'downgraded_events': [
                {
                    'id': 0,
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 90,  # Downgraded to 90 min
                    'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}],
                    'alternates': []
                }
            ]
        }
        with open(test_period_dir / 'results.json', 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2)

        # Create actual_attendance.json with event completed at 90 minutes
        attendance_data = {
            'valid_events': [
                {
                    'id': 0,
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 90,  # Actual duration was 90 min
                    'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}]
                }
            ]
        }
        with open(test_period_dir / 'actual_attendance.json', 'w', encoding='utf-8') as f:
            json.dump(attendance_data, f, indent=2)

        # Insert event into DB with scheduled status and 90 min duration (correct)
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 90, 'scheduled'))
        conn.commit()

        # Read responses.csv for validation
        with open(test_period_dir / 'responses.csv', 'r', encoding='utf-8') as f:
            responses_csv = list(csv.DictReader(f))

        # Call validate_events()
        from db.validate import validate_events
        issues = validate_events(cursor, period_id, period_name, responses_csv, test_period_dir)

        # Should PASS (no issues) because:
        # - Event status is 'scheduled' -> should check against results.json (90 min)
        # - DB has 90 min (matches results.json)
        # - Should NOT check against responses.csv (120 min)
        assert len(issues) == 0, (
            f"validate_events() should NOT report duration mismatch for scheduled event. "
            f"Event was proposed at 120 min but scheduled at 90 min (downgrade). "
            f"DB has 90 min (correct), but validator checks against responses.csv (120 min). "
            f"Issues found: {issues}"
        )

    def test_completed_event_duration_checked_against_actual_attendance_json(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that completed events check duration against actual_attendance.json, not responses.csv.

        Scenario:
        1. Event proposed at 120 min (responses.csv)
        2. Event scheduled at 90 min (results.json)
        3. Event completed at 90 min (actual_attendance.json)
        4. DB has 90 min with status='completed' (correct)
        5. Validator should check against actual_attendance.json (90 min), not responses.csv (120 min)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create responses.csv with event proposed at 120 minutes
        responses_data = [
            {
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'  # 2 hours = 120 min
            }
        ]
        with open(test_period_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # Create actual_attendance.json with event completed at 90 minutes
        attendance_data = {
            'valid_events': [
                {
                    'id': 0,
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 90,  # Actual duration was 90 min
                    'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}]
                }
            ]
        }
        with open(test_period_dir / 'actual_attendance.json', 'w', encoding='utf-8') as f:
            json.dump(attendance_data, f, indent=2)

        # Insert event into DB with completed status and 90 min duration (correct)
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 90, 'completed'))
        conn.commit()

        # Read responses.csv for validation
        with open(test_period_dir / 'responses.csv', 'r', encoding='utf-8') as f:
            responses_csv = list(csv.DictReader(f))

        # Call validate_events()
        from db.validate import validate_events
        issues = validate_events(cursor, period_id, period_name, responses_csv, test_period_dir)

        # Should PASS (no issues) because:
        # - Event status is 'completed' -> should check against actual_attendance.json (90 min)
        # - DB has 90 min (matches actual_attendance.json)
        # - Should NOT check against responses.csv (120 min)
        assert len(issues) == 0, (
            f"validate_events() should NOT report duration mismatch for completed event. "
            f"Event was proposed at 120 min but completed at 90 min. "
            f"DB has 90 min (correct), but validator checks against responses.csv (120 min). "
            f"Issues found: {issues}"
        )

    def test_proposed_event_duration_still_checked_against_responses_csv(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that proposed events continue to check duration against responses.csv (correct behavior).

        This test ensures the fix doesn't break proposed event validation.
        Proposed events should still be checked against responses.csv.
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create responses.csv with event proposed at 120 minutes
        responses_data = [
            {
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'  # 2 hours = 120 min
            }
        ]
        with open(test_period_dir / 'responses.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # Insert event into DB with proposed status and WRONG duration (90 min instead of 120)
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 90, 'proposed'))
        conn.commit()

        # Read responses.csv for validation
        with open(test_period_dir / 'responses.csv', 'r', encoding='utf-8') as f:
            responses_csv = list(csv.DictReader(f))

        # Call validate_events()
        from db.validate import validate_events
        issues = validate_events(cursor, period_id, period_name, responses_csv, test_period_dir)

        # Proposed events SHOULD still be checked against responses.csv
        # This should FAIL validation (find issues) because DB has 90 but CSV has 120
        assert len(issues) > 0, (
            f"validate_events() SHOULD report duration mismatch for proposed event. "
            f"DB has 90 min but responses.csv has 120 min. This is correct behavior."
        )
        assert any('duration mismatch' in issue.lower() for issue in issues), (
            f"Expected duration mismatch issue, got: {issues}"
        )


# =============================================================================
# TESTS: validate_period_snapshots() should skip validation when no attendance exists
# =============================================================================

class TestValidatePeriodSnapshotsSkipsValidationWithoutAttendance:
    """
    Tests for validate_period_snapshots() logic when no attendance exists.

    Current behavior (bug):
    - Expects snapshots for all active members regardless of attendance existence
    - Causes false failures for future periods with no attendance

    Expected behavior:
    - Skip snapshot validation when period has no attendance data
    - Snapshots derive from attendance, so no attendance = no snapshots is correct
    """

    def test_snapshots_not_required_when_period_has_no_attendance(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that validate_period_snapshots() skips validation when no attendance exists.

        Scenario:
        1. Future period (2026-01) with 0 attendance records
        2. DB correctly has no snapshots (snapshots derive from attendance)
        3. Validator should NOT report missing snapshots

        This test SHOULD FAIL initially because validate_period_snapshots() currently
        expects snapshots for all active members, regardless of whether the period
        has any attendance data.
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']

        # Create a FUTURE period with no attendance
        cursor.execute("""
            INSERT INTO schedule_periods (id, period_name, display_name, start_date, end_date, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, '2026-01', 'Jan 2026', '2026-01-01', '2026-01-31', 'draft'))
        future_period_id = 2

        # Create events for the future period (but no attendance)
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, future_period_id, 0, '2026-01-10T17:00:00', 120, 'proposed'))

        conn.commit()

        # Verify no attendance records exist for this period
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (future_period_id,))
        attendance_count = cursor.fetchone()['count']
        assert attendance_count == 0, "Test setup: future period should have 0 attendance records"

        # Verify no snapshots exist for this period (correct)
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM peep_order_snapshots
            WHERE period_id = ?
        """, (future_period_id,))
        snapshot_count = cursor.fetchone()['count']
        assert snapshot_count == 0, "Test setup: future period should have 0 snapshots"

        # Create members.csv for validation (active members exist)
        members_data = [
            {
                'id': '1',
                'Name': 'Alice Leader',
                'Display Name': 'Alice',
                'Email Address': 'alice@example.com',
                'Role': 'leader',
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',
                'Priority': '5',
                'Index': '0',
                'Total Attended': '0'
            },
            {
                'id': '2',
                'Name': 'Bob Follower',
                'Display Name': 'Bob',
                'Email Address': 'bob@example.com',
                'Role': 'follower',
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',
                'Priority': '3',
                'Index': '1',
                'Total Attended': '0'
            }
        ]
        with open(test_period_dir / 'members.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # Read members.csv for validation
        with open(test_period_dir / 'members.csv', 'r', encoding='utf-8') as f:
            members_csv = list(csv.DictReader(f))

        # Call validate_period_snapshots()
        from db.validate import validate_period_snapshots
        issues = validate_period_snapshots(cursor, future_period_id, members_csv)

        # Should PASS (no issues) because:
        # - Period has no attendance records (future period)
        # - DB correctly has no snapshots (snapshots derive from attendance)
        # - Validator should skip snapshot validation when no attendance exists
        assert len(issues) == 0, (
            f"validate_period_snapshots() should NOT report missing snapshots when period has no attendance. "
            f"Future periods have no attendance yet, so no snapshots is correct. "
            f"Issues found: {issues}"
        )

    def test_snapshots_still_validated_when_period_has_attendance(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that snapshot validation still works when attendance exists (correct behavior).

        This test ensures the fix doesn't break snapshot validation for periods with attendance.
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create an event for the period
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'completed'))

        # Create attendance records (period has attendance)
        cursor.execute("""
            INSERT INTO event_attendance
            (id, event_id, peep_id, event_assignment_id, expected_role, expected_type, actual_role,
             attendance_status, participation_mode, last_minute_cancel, check_in_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, 1, 1, None, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2025-02-07 17:00:00'))

        conn.commit()

        # Verify attendance exists
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        attendance_count = cursor.fetchone()['count']
        assert attendance_count > 0, "Test setup: period should have attendance records"

        # Verify no snapshots exist (missing snapshots - this is an error)
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM peep_order_snapshots
            WHERE period_id = ?
        """, (period_id,))
        snapshot_count = cursor.fetchone()['count']
        assert snapshot_count == 0, "Test setup: period should have 0 snapshots (error state)"

        # Create members.csv for validation
        members_data = [
            {
                'id': '1',
                'Name': 'Alice Leader',
                'Display Name': 'Alice',
                'Email Address': 'alice@example.com',
                'Role': 'leader',
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',
                'Priority': '5',
                'Index': '0',
                'Total Attended': '1'
            },
            {
                'id': '2',
                'Name': 'Bob Follower',
                'Display Name': 'Bob',
                'Email Address': 'bob@example.com',
                'Role': 'follower',
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',
                'Priority': '3',
                'Index': '1',
                'Total Attended': '0'
            }
        ]
        with open(test_period_dir / 'members.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # Read members.csv for validation
        with open(test_period_dir / 'members.csv', 'r', encoding='utf-8') as f:
            members_csv = list(csv.DictReader(f))

        # Call validate_period_snapshots()
        from db.validate import validate_period_snapshots
        issues = validate_period_snapshots(cursor, period_id, members_csv)

        # When attendance EXISTS, validator SHOULD report missing snapshots
        # This should FAIL validation (find issues)
        assert len(issues) > 0, (
            f"validate_period_snapshots() SHOULD report missing snapshots when attendance exists. "
            f"This is correct behavior - snapshots are required when attendance data is present."
        )
        assert any('snapshot missing' in issue.lower() for issue in issues), (
            f"Expected missing snapshot issue, got: {issues}"
        )
