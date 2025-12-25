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


# =============================================================================
# TESTS: validate_period_cancellations()
# =============================================================================

class TestValidatePeriodCancellations:
    """
    Tests for validate_period_cancellations() function.

    Validates that cancelled events from cancellations.json have status='cancelled' in DB.
    """

    def test_cancelled_events_exist_with_correct_status(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that cancelled events with status='cancelled' pass validation.

        Scenario:
        1. Create event with status='cancelled' in DB
        2. Add same event to cancelled_events in cancellations.json
        3. Validation should pass (no issues)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Insert cancelled event into DB
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'cancelled'))
        conn.commit()

        # Create cancellations.json with cancelled_events
        cancellations_data = {
            'cancelled_events': ['Friday February 7th - 5pm to 7pm'],
            'cancelled_availability': []
        }
        with open(test_period_dir / 'cancellations.json', 'w', encoding='utf-8') as f:
            json.dump(cancellations_data, f, indent=2)

        # Call validate_period_cancellations()
        from db.validate import validate_period_cancellations
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_cancellations() should pass when cancelled event exists "
            f"in DB with status='cancelled'. Issues found: {issues}"
        )

    def test_cancelled_event_not_found_in_db(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that cancelled event not found in DB is reported as an issue.

        Scenario:
        1. Add event to cancelled_events in cancellations.json
        2. Event does NOT exist in DB
        3. Validation should report an issue
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Don't insert any events

        # Create cancellations.json with cancelled_events
        cancellations_data = {
            'cancelled_events': ['Friday February 7th - 5pm to 7pm'],
            'cancelled_availability': []
        }

        # Call validate_period_cancellations()
        from db.validate import validate_period_cancellations
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should FAIL (report issue)
        assert len(issues) > 0, (
            f"validate_period_cancellations() should report when cancelled event "
            f"is not found in DB"
        )
        assert any('not found' in issue.lower() for issue in issues), (
            f"Expected 'not found' issue, got: {issues}"
        )

    def test_cancelled_event_has_wrong_status(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that cancelled event with wrong status is reported as an issue.

        Scenario:
        1. Create event with status='scheduled' in DB (wrong status)
        2. Add same event to cancelled_events in cancellations.json
        3. Validation should report that status should be 'cancelled'
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Insert event with WRONG status (scheduled instead of cancelled)
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'scheduled'))
        conn.commit()

        # Create cancellations.json with this event marked as cancelled
        cancellations_data = {
            'cancelled_events': ['Friday February 7th - 5pm to 7pm'],
            'cancelled_availability': []
        }

        # Call validate_period_cancellations()
        from db.validate import validate_period_cancellations
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should FAIL (report issue)
        assert len(issues) > 0, (
            f"validate_period_cancellations() should report when cancelled event "
            f"has wrong status"
        )
        assert any('status' in issue.lower() and 'cancelled' in issue.lower() for issue in issues), (
            f"Expected status mismatch issue mentioning 'cancelled', got: {issues}"
        )

    def test_empty_cancelled_events_list_passes(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that empty cancelled_events list passes validation.

        Scenario:
        1. No events to validate
        2. cancelled_events is empty list
        3. Validation should pass (no issues)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create cancellations.json with empty cancelled_events
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': []
        }

        # Call validate_period_cancellations()
        from db.validate import validate_period_cancellations
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_cancellations() should pass with empty cancelled_events list"
        )

    def test_multiple_cancelled_events_mixed_states(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test validation with multiple cancelled events in mixed states.

        Scenario:
        1. Create 3 events: one correct (cancelled), one wrong (scheduled), one missing
        2. Add all 3 to cancelled_events in cancellations.json
        3. Validation should report 2 issues (wrong status + not found)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Insert first event with CORRECT status
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'cancelled'))

        # Insert second event with WRONG status
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, period_id, 1, '2025-02-14T17:00:00', 120, 'scheduled'))

        # Don't insert third event (missing)

        conn.commit()

        # Create cancellations.json with all 3 events
        cancellations_data = {
            'cancelled_events': [
                'Friday February 7th - 5pm to 7pm',      # Correct: cancelled
                'Friday February 14th - 5pm to 7pm',     # Wrong: scheduled
                'Friday February 21st - 5pm to 7pm'      # Missing from DB
            ],
            'cancelled_availability': []
        }

        # Call validate_period_cancellations()
        from db.validate import validate_period_cancellations
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should report 2 issues (wrong status + not found)
        assert len(issues) == 2, (
            f"Expected 2 issues (wrong status + not found), got {len(issues)}: {issues}"
        )


# =============================================================================
# TESTS: validate_period_cancelled_availability()
# =============================================================================

class TestValidatePeriodCancelledAvailability:
    """
    Tests for validate_period_cancelled_availability() function.

    Validates that cancelled availability records were removed from event_availability table.
    """

    def test_cancelled_availability_successfully_removed(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that cancelled availability records were correctly removed from DB.

        Scenario:
        1. Create event and response (but NO event_availability record - it was removed)
        2. Add availability to cancelled_availability in cancellations.json
        3. Validation should pass (no event_availability found = correct)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create event
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'proposed'))

        # Create response for alice
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 1, 'leader', 2, 7))

        # Don't create event_availability - it was cancelled/removed

        conn.commit()

        # Create cancellations.json with cancelled_availability
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': [
                {
                    'email': 'alice@example.com',
                    'events': ['Friday February 7th - 5pm to 7pm']
                }
            ]
        }

        # Call validate_period_cancelled_availability()
        from db.validate import validate_period_cancelled_availability
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should PASS (no issues) - event_availability was correctly removed
        assert len(issues) == 0, (
            f"validate_period_cancelled_availability() should pass when "
            f"cancelled availability was correctly removed. Issues found: {issues}"
        )

    def test_cancelled_availability_still_exists_in_db(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that cancelled availability still existing in DB is reported as issue.

        Scenario:
        1. Create event, response, AND event_availability record
        2. Event_availability was NOT removed (error state)
        3. Add to cancelled_availability in cancellations.json
        4. Validation should report that availability still exists
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create event
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'proposed'))

        # Create response for alice
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 1, 'leader', 2, 7))

        # Create event_availability (this was NOT removed - error state)
        cursor.execute("""
            INSERT INTO event_availability (id, event_id, response_id)
            VALUES (?, ?, ?)
        """, (1, 1, 1))

        conn.commit()

        # Create cancellations.json marking this as cancelled availability
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': [
                {
                    'email': 'alice@example.com',
                    'events': ['Friday February 7th - 5pm to 7pm']
                }
            ]
        }

        # Call validate_period_cancelled_availability()
        from db.validate import validate_period_cancelled_availability
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should FAIL (report issue)
        assert len(issues) > 0, (
            f"validate_period_cancelled_availability() should report when "
            f"cancelled availability still exists in DB"
        )
        assert any('not removed' in issue.lower() or 'still has availability' in issue.lower() for issue in issues), (
            f"Expected 'not removed' issue, got: {issues}"
        )

    def test_unknown_email_in_cancelled_availability_skips_gracefully(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that unknown email in cancelled_availability is skipped gracefully.

        Scenario:
        1. Email in cancelled_availability does NOT exist in DB
        2. Validation should skip (not report as error)
        3. Function should continue with other entries
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create one valid event and response
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'proposed'))

        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 1, 'leader', 2, 7))

        conn.commit()

        # Create cancellations.json with unknown email
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': [
                {
                    'email': 'unknown@example.com',  # This email doesn't exist in DB
                    'events': ['Friday February 7th - 5pm to 7pm']
                }
            ]
        }

        # Call validate_period_cancelled_availability()
        from db.validate import validate_period_cancelled_availability
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should PASS (gracefully skip unknown email)
        assert len(issues) == 0, (
            f"validate_period_cancelled_availability() should skip unknown emails gracefully. "
            f"Issues found: {issues}"
        )

    def test_empty_cancelled_availability_list_passes(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that empty cancelled_availability list passes validation.

        Scenario:
        1. No availability to validate
        2. cancelled_availability is empty list
        3. Validation should pass (no issues)
        """
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create cancellations.json with empty cancelled_availability
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': []
        }

        # Call validate_period_cancelled_availability()
        from db.validate import validate_period_cancelled_availability
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_cancelled_availability() should pass with empty cancelled_availability list"
        )

    def test_multiple_cancelled_availability_entries_mixed_states(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test validation with multiple cancelled availability entries in mixed states.

        Scenario:
        1. Two valid entries: one correctly removed, one still exists
        2. One invalid entry with unknown email
        3. Validation should report 1 issue (still exists) and skip unknown email
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Create two events
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 0, '2025-02-07T17:00:00', 120, 'proposed'))
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, period_id, 1, '2025-02-14T17:00:00', 120, 'proposed'))

        # Create responses
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, period_id, 1, 'leader', 2, 7))
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, period_id, 2, 'follower', 2, 7))

        # Create event_availability for second event (NOT removed - error state)
        cursor.execute("""
            INSERT INTO event_availability (id, event_id, response_id)
            VALUES (?, ?, ?)
        """, (1, 2, 2))

        conn.commit()

        # Create cancellations.json
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': [
                {
                    'email': 'alice@example.com',
                    'events': ['Friday February 7th - 5pm to 7pm']      # Correctly removed
                },
                {
                    'email': 'bob@example.com',
                    'events': ['Friday February 14th - 5pm to 7pm']     # Still exists (error)
                },
                {
                    'email': 'unknown@example.com',
                    'events': ['Friday February 21st - 5pm to 7pm']     # Unknown email (skip)
                }
            ]
        }

        # Call validate_period_cancelled_availability()
        from db.validate import validate_period_cancelled_availability
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should report 1 issue (still exists) and skip unknown email
        assert len(issues) == 1, (
            f"Expected 1 issue (still exists), got {len(issues)}: {issues}"
        )


# =============================================================================
# TESTS: validate_period_partnerships()
# =============================================================================

class TestValidatePeriodPartnerships:
    """
    Tests for validate_period_partnerships() function.

    Validates that partnerships from partnerships.json are correctly stored in partnership_requests table.
    """

    def test_partnerships_exist_in_db(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that partnerships existing in DB pass validation.

        Scenario:
        1. Create partnership_requests records in DB
        2. Add same partnerships to partnerships.json
        3. Validation should pass (no issues)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create partnership_requests records
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 2))
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 2, 1))
        conn.commit()

        # Create partnerships.json (unwrapped format)
        partnerships_data = {
            '1': [2],
            '2': [1]
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_partnerships() should pass when partnerships exist in DB. "
            f"Issues found: {issues}"
        )

    def test_partnership_not_found_in_db(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that partnership not found in DB is reported as an issue.

        Scenario:
        1. Add partnership to partnerships.json
        2. Partnership does NOT exist in DB
        3. Validation should report an issue
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Don't create any partnership_requests

        # Create partnerships.json
        partnerships_data = {
            '1': [2]  # Partnership 1 -> 2 doesn't exist
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should FAIL (report issue)
        assert len(issues) > 0, (
            f"validate_period_partnerships() should report when partnership "
            f"is not found in DB"
        )
        assert any('not found' in issue.lower() for issue in issues), (
            f"Expected 'not found' issue, got: {issues}"
        )

    def test_invalid_requester_id_format(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that invalid requester ID format is reported as an issue.

        Scenario:
        1. Add partnership with non-numeric requester ID to partnerships.json
        2. Validation should report format error
        """
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        # Create partnerships.json with invalid requester ID
        partnerships_data = {
            'invalid_id': [1, 2]  # Non-numeric requester ID
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should FAIL (report format issue)
        assert len(issues) > 0, (
            f"validate_period_partnerships() should report when requester ID is invalid"
        )
        assert any('invalid' in issue.lower() and 'requester' in issue.lower() for issue in issues), (
            f"Expected invalid requester ID issue, got: {issues}"
        )

    def test_invalid_partner_id_format(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that invalid partner ID format is reported as an issue.

        Scenario:
        1. Add partnership with non-numeric partner ID to partnerships.json
        2. Validation should report format error
        """
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        # Create partnerships.json with invalid partner ID
        partnerships_data = {
            '1': ['invalid_id', 2]  # Non-numeric partner ID
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should FAIL (report format issue)
        assert len(issues) > 0, (
            f"validate_period_partnerships() should report when partner ID is invalid"
        )
        assert any('invalid' in issue.lower() and 'partner' in issue.lower() for issue in issues), (
            f"Expected invalid partner ID issue, got: {issues}"
        )

    def test_wrapped_json_format(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that wrapped JSON format {"partnerships": {...}} is handled correctly.

        Scenario:
        1. Create partnerships in DB
        2. Use wrapped format in partnerships.json
        3. Validation should work with both wrapped and unwrapped formats
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create partnership_requests record
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 2))
        conn.commit()

        # Create partnerships.json in WRAPPED format
        partnerships_data = {
            'partnerships': {
                '1': [2]
            }
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_partnerships() should work with wrapped format. "
            f"Issues found: {issues}"
        )

    def test_unwrapped_json_format(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that unwrapped JSON format {...} is handled correctly.

        Scenario:
        1. Create partnerships in DB
        2. Use unwrapped format in partnerships.json
        3. Validation should work with both wrapped and unwrapped formats
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create partnership_requests record
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 2))
        conn.commit()

        # Create partnerships.json in UNWRAPPED format
        partnerships_data = {
            '1': [2]
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_partnerships() should work with unwrapped format. "
            f"Issues found: {issues}"
        )

    def test_empty_partnerships_passes(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test that empty partnerships dict passes validation.

        Scenario:
        1. No partnerships to validate
        2. partnerships.json is empty dict
        3. Validation should pass (no issues)
        """
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        # Create empty partnerships.json
        partnerships_data = {}

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (no issues)
        assert len(issues) == 0, (
            f"validate_period_partnerships() should pass with empty partnerships dict"
        )

    def test_multiple_partners_per_requester(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test validation with multiple partners per requester.

        Scenario:
        1. Create requester with 3 partners in DB
        2. Add all to partnerships.json
        3. Validation should pass (all found)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create multiple partnership_requests for requester 1
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 2))
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 3))
        # Create 5th peep for this test (already have 1, 2)
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (5, 'Charlie Partner', 'Charlie', 'charlie@example.com', 'leader', '2024-01-01', 1))
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 5))
        conn.commit()

        # Create partnerships.json with multiple partners
        partnerships_data = {
            '1': [2, 3, 5]
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (all partnerships found)
        assert len(issues) == 0, (
            f"validate_period_partnerships() should pass with multiple partners. "
            f"Issues found: {issues}"
        )

    def test_multiple_partnerships_mixed_states(
        self, test_db_with_period, test_period_dir
    ):
        """
        Test validation with multiple partnerships in mixed states.

        Scenario:
        1. Create 2 partnerships in DB, 1 missing
        2. Validation should report 1 issue (missing)
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Create 2 valid partnerships
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 1, 2))
        cursor.execute("""
            INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
            VALUES (?, ?, ?)
        """, (period_id, 2, 1))
        # Don't create 3rd partnership (1 -> 3)
        conn.commit()

        # Create partnerships.json with 3 partnerships
        partnerships_data = {
            '1': [2, 3],  # 3 doesn't exist in DB
            '2': [1]
        }

        # Call validate_period_partnerships()
        from db.validate import validate_period_partnerships
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should report 1 issue (1 -> 3 not found)
        assert len(issues) == 1, (
            f"Expected 1 issue (not found), got {len(issues)}: {issues}"
        )
        assert any('1' in issue and '3' in issue for issue in issues), (
            f"Expected issue about partnership 1->3, got: {issues}"
        )
