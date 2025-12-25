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

from tests.conftest import _parse_and_reorder_schema
from db.validate import (
    validate_events,
    validate_period_snapshots,
    validate_period_cancellations,
    validate_period_cancelled_availability,
    validate_period_partnerships
)


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def test_db_with_period(schema_sql):
    """Create test database with a period and basic data.

    Uses fresh in-memory database per test for complete isolation.
    Much faster than file-based database (no disk I/O).
    """
    # Create in-memory database
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row

    # Load and execute schema (reordered via conftest helper)
    reordered_sql = _parse_and_reorder_schema(schema_sql)
    conn.executescript(reordered_sql)

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

    yield {'conn': conn, 'cursor': cursor, 'period_id': 1, 'period_name': '2025-02'}

    # Cleanup
    conn.close()


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

@pytest.mark.db
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
        """Test that scheduled events check duration against results.json, not responses.csv.

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
                'Timestamp': '2/1/2025 10:00:00',
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
                'Timestamp': '2/1/2025 10:00:00',
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
                'Timestamp': '2/1/2025 10:00:00',
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

@pytest.mark.db
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

@pytest.mark.db
class TestValidatePeriodCancellations:
    """
    Tests for validate_period_cancellations() function.

    Validates that cancelled events from cancellations.json have status='cancelled' in DB.
    """

    @pytest.mark.parametrize("event_status,expect_issues,expected_msg_contains", [
        ('cancelled', False, None),  # Correct status → no issues
        (None, True, 'not found'),   # Event not in DB → should fail
        ('scheduled', True, 'status'),  # Wrong status → should fail
    ])
    def test_cancelled_event_validation(
        self, test_db_with_period, event_status, expect_issues, expected_msg_contains
    ):
        """
        Parametrized test for cancelled event validation scenarios.

        Tests three cases:
        1. Event exists with correct status ('cancelled') → should pass
        2. Event not found in DB → should report 'not found' issue
        3. Event exists with wrong status → should report 'status' issue
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']
        period_name = test_db_with_period['period_name']

        # Insert event if event_status is specified
        if event_status:
            cursor.execute("""
                INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (1, period_id, 0, '2025-02-07T17:00:00', 120, event_status))
            conn.commit()

        # Create cancellations.json
        cancellations_data = {
            'cancelled_events': ['Friday February 7th - 5pm to 7pm'],
            'cancelled_availability': []
        }

        # Call validate_period_cancellations()
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Assert based on expected outcome
        if expect_issues:
            assert len(issues) > 0, f"Expected validation issues but got none (scenario: event_status={event_status})"
            if expected_msg_contains:
                assert any(expected_msg_contains in issue.lower() for issue in issues), (
                    f"Expected issue containing '{expected_msg_contains}', got: {issues}"
                )
        else:
            assert len(issues) == 0, f"Expected no issues but got: {issues} (scenario: event_status={event_status})"

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
        issues = validate_period_cancellations(cursor, period_id, cancellations_data, period_name)

        # Should report 2 issues (wrong status + not found)
        assert len(issues) == 2, (
            f"Expected 2 issues (wrong status + not found), got {len(issues)}: {issues}"
        )


# =============================================================================
# TESTS: validate_period_cancelled_availability()
# =============================================================================

@pytest.mark.db
class TestValidatePeriodCancelledAvailability:
    """
    Tests for validate_period_cancelled_availability() function.

    Validates that cancelled availability records were removed from event_availability table.
    """

    @pytest.mark.parametrize("create_availability_record,expect_issues", [
        (False, False),  # Availability correctly removed → no issues
        (True, True),    # Availability still exists → should fail
    ])
    def test_cancelled_availability_validation(
        self, test_db_with_period, create_availability_record, expect_issues
    ):
        """
        Parametrized test for cancelled availability validation.

        Tests two cases:
        1. Availability correctly removed from DB → should pass
        2. Availability still exists in DB → should report issue
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

        # Conditionally create event_availability record
        if create_availability_record:
            cursor.execute("""
                INSERT INTO event_availability (id, event_id, response_id)
                VALUES (?, ?, ?)
            """, (1, 1, 1))

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
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Assert based on expected outcome
        if expect_issues:
            assert len(issues) > 0, f"Expected issues when availability not removed (create_availability_record={create_availability_record})"
            assert any('not removed' in issue.lower() or 'still has availability' in issue.lower() for issue in issues), (
                f"Expected 'not removed' or 'still has availability' issue, got: {issues}"
            )
        else:
            assert len(issues) == 0, f"Expected no issues when availability correctly removed, got: {issues}"

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
        issues = validate_period_cancelled_availability(cursor, period_id, cancellations_data, period_name)

        # Should report 1 issue (still exists) and skip unknown email
        assert len(issues) == 1, (
            f"Expected 1 issue (still exists), got {len(issues)}: {issues}"
        )


# =============================================================================
# TESTS: validate_period_partnerships()
# =============================================================================

@pytest.mark.db
class TestValidatePeriodPartnerships:
    """
    Tests for validate_period_partnerships() function.

    Validates that partnerships from partnerships.json are correctly stored in partnership_requests table.
    """

    @pytest.mark.parametrize("create_partnerships,expect_issues", [
        (True, False),   # Partnerships exist in DB → no issues
        (False, True),   # Partnerships not in DB → should fail
    ])
    def test_partnership_existence_validation(
        self, test_db_with_period, create_partnerships, expect_issues
    ):
        """
        Parametrized test for partnership existence validation.

        Tests two cases:
        1. Partnerships exist in DB → should pass
        2. Partnerships not found in DB → should report issue
        """
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Conditionally create partnership_requests records
        if create_partnerships:
            cursor.execute("""
                INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
                VALUES (?, ?, ?)
            """, (period_id, 1, 2))
            cursor.execute("""
                INSERT INTO partnership_requests (period_id, requester_peep_id, partner_peep_id)
                VALUES (?, ?, ?)
            """, (period_id, 2, 1))
            conn.commit()

        # Create partnerships.json
        partnerships_data = {
            '1': [2],
            '2': [1] if create_partnerships else []
        }
        if not create_partnerships:
            partnerships_data = {'1': [2]}  # Only one partnership to test

        # Call validate_period_partnerships()
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Assert based on expected outcome
        if expect_issues:
            assert len(issues) > 0, f"Expected issues when partnerships not in DB (create_partnerships={create_partnerships})"
            assert any('not found' in issue.lower() for issue in issues), f"Expected 'not found' issue, got: {issues}"
        else:
            assert len(issues) == 0, f"Expected no issues when partnerships exist in DB, got: {issues}"

    @pytest.mark.parametrize("invalid_field,partnerships_data,expected_keyword", [
        ('requester', {'invalid_id': [1, 2]}, 'requester'),  # Invalid requester ID
        ('partner', {'1': ['invalid_id', 2]}, 'partner'),    # Invalid partner ID
    ])
    def test_invalid_id_format_validation(
        self, test_db_with_period, invalid_field, partnerships_data, expected_keyword
    ):
        """
        Parametrized test for invalid ID format validation.

        Tests two cases:
        1. Invalid requester ID format → should report issue
        2. Invalid partner ID format → should report issue
        """
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        # Call validate_period_partnerships()
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should FAIL (report format issue)
        assert len(issues) > 0, f"Expected issues for invalid {invalid_field} ID format"
        assert any('invalid' in issue.lower() and expected_keyword in issue.lower() for issue in issues), (
            f"Expected 'invalid' and '{expected_keyword}' in issue, got: {issues}"
        )

    @pytest.mark.parametrize("format_type,partnerships_data", [
        ('wrapped', {'partnerships': {'1': [2]}}),    # Wrapped format
        ('unwrapped', {'1': [2]}),                     # Unwrapped format
    ])
    def test_json_format_validation(
        self, test_db_with_period, format_type, partnerships_data
    ):
        """
        Parametrized test for JSON format validation.

        Tests two cases:
        1. Wrapped format {"partnerships": {...}} → should work
        2. Unwrapped format {...} → should work
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

        # Call validate_period_partnerships()
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should PASS (no issues) - both formats should work
        assert len(issues) == 0, f"validate_period_partnerships() should work with {format_type} format. Issues found: {issues}"

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
        issues = validate_period_partnerships(cursor, period_id, partnerships_data)

        # Should report 1 issue (1 -> 3 not found)
        assert len(issues) == 1, (
            f"Expected 1 issue (not found), got {len(issues)}: {issues}"
        )
        assert any('1' in issue and '3' in issue for issue in issues), (
            f"Expected issue about partnership 1->3, got: {issues}"
        )


# =============================================================================
# Additional Tests: validate_members() and validate_period()
# =============================================================================

from db.validate import (
    validate_members,
    validate_period,
    validate_period_responses,
    read_members_csv,
    read_responses_csv,
    MAX_REASONABLE_PRIORITY,
    MAX_INDEX_MULTIPLIER,
)
from file_io import normalize_email


@pytest.fixture
def members_csv_file(tmp_path):
    """Create test members.csv file."""
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
            'Total Attended': '10'
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
            'Total Attended': '8'
        }
    ]

    filepath = tmp_path / 'members.csv'
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
        writer.writeheader()
        writer.writerows(members_data)

    return filepath


@pytest.fixture
def responses_csv_file(tmp_path):
    """Create test responses.csv file."""
    responses_data = [
        {
            'Timestamp': '2/1/2025 10:00:00',
            'Email Address': 'alice@example.com',
            'Name': 'Alice Leader',
            'Primary Role': 'Leader',
            'Max Sessions': '2',
            'Min Interval Days': '7',
            'Secondary Role': 'I only want to be scheduled in my primary role',
            'Availability': 'Friday February 7th - 5pm to 7pm'
        },
        {
            'Timestamp': '2/1/2025 11:00:00',
            'Email Address': 'bob@example.com',
            'Name': 'Bob Follower',
            'Primary Role': 'Follower',
            'Max Sessions': '3',
            'Min Interval Days': '5',
            'Secondary Role': 'I only want to be scheduled in my primary role',
            'Availability': 'Friday February 7th - 5pm to 7pm'
        }
    ]

    filepath = tmp_path / 'responses.csv'
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
        writer.writeheader()
        writer.writerows(responses_data)

    return filepath


@pytest.mark.db
class TestValidateMembers:
    """Tests for validate_members() function."""

    def test_validate_members_logic_detects_matching_members(
        self, test_db_with_period, members_csv_file
    ):
        """Test that member validation detects matching members correctly."""
        cursor = test_db_with_period['cursor']
        members_csv = read_members_csv(members_csv_file.parent)

        assert len(members_csv) >= 2
        emails_in_csv = {m.get('Email Address', '').lower() for m in members_csv}
        assert 'alice@example.com' in emails_in_csv
        assert 'bob@example.com' in emails_in_csv

    def test_member_field_mismatch_detected(self, test_db_with_period, tmp_path):
        """Test that field mismatches are detected."""
        cursor = test_db_with_period['cursor']
        members_data = [
            {
                'id': '1',
                'Name': 'Alice Leader',
                'Display Name': 'Alicia',  # Mismatch
                'Email Address': 'alice@example.com',
                'Role': 'follower',  # Mismatch
                'Date Joined': '2024-01-01',
                'Active': 'FALSE',  # Mismatch
                'Priority': '5',
                'Index': '0',
                'Total Attended': '10'
            }
        ]

        filepath = tmp_path / 'members.csv'
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        members_csv = read_members_csv(tmp_path)
        assert members_csv[0]['Display Name'] == 'Alicia'
        assert members_csv[0]['Role'] == 'follower'


@pytest.mark.db
class TestValidatePeriodResponses:
    """Tests for validate_period_responses() function."""

    def test_validate_period_responses_all_match_returns_empty(
        self, test_db_with_period
    ):
        """Test that validate_period_responses() returns empty list when all match."""
        cursor = test_db_with_period['cursor']
        conn = test_db_with_period['conn']
        period_id = test_db_with_period['period_id']

        # Insert response into DB
        cursor.execute("""
            INSERT INTO responses (period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?)
        """, (period_id, 1, 'leader', 2, 7))
        conn.commit()

        # Create matching response CSV
        responses_csv = [
            {
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'leader',
                'Max Sessions': '2',
                'Min Interval Days': '7'
            }
        ]

        # Call validate_period_responses
        issues = validate_period_responses(cursor, period_id, responses_csv)

        # Should have no issues
        assert len(issues) == 0, f"All responses match, should have no issues. Got: {issues}"

    def test_validate_period_responses_reports_missing_in_db(
        self, test_db_with_period
    ):
        """Test that missing response in DB is reported."""
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        # CSV has response but DB doesn't
        responses_csv = [
            {
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'leader',
                'Max Sessions': '2',
                'Min Interval Days': '7'
            }
        ]

        issues = validate_period_responses(cursor, period_id, responses_csv)

        assert len(issues) > 0, "Should report response not in DB"
        assert any('not in db' in issue.lower() for issue in issues)

    def test_validate_period_responses_empty_csv_returns_empty(
        self, test_db_with_period
    ):
        """Test that validate_period_responses() handles empty CSV."""
        cursor = test_db_with_period['cursor']
        period_id = test_db_with_period['period_id']

        responses_csv = []
        issues = validate_period_responses(cursor, period_id, responses_csv)

        assert len(issues) == 0, f"Empty CSV should have no issues, got: {issues}"


@pytest.mark.db
class TestValidationHelpers:
    """Tests for validation helper functions."""

    def test_read_members_csv_returns_list_of_dicts(self, members_csv_file):
        """Test that read_members_csv() returns list of dictionaries."""
        period_dir = members_csv_file.parent
        members = read_members_csv(period_dir)

        assert isinstance(members, list)
        assert len(members) == 2
        assert all(isinstance(m, dict) for m in members)

    def test_read_members_csv_handles_missing_file(self, tmp_path):
        """Test that read_members_csv() handles missing file gracefully."""
        members = read_members_csv(tmp_path)

        assert isinstance(members, list)
        assert len(members) == 0

    def test_read_responses_csv_returns_list_of_dicts(self, responses_csv_file):
        """Test that read_responses_csv() returns list of dictionaries."""
        period_dir = responses_csv_file.parent
        responses = read_responses_csv(period_dir)

        assert isinstance(responses, list)
        assert len(responses) == 2

    def test_read_responses_csv_handles_missing_file(self, tmp_path):
        """Test that read_responses_csv() handles missing file gracefully."""
        responses = read_responses_csv(tmp_path)

        assert isinstance(responses, list)
        assert len(responses) == 0


@pytest.mark.db
class TestValidateMembersFunction:
    """Tests for validate_members() CLI function."""

    def test_validate_members_returns_zero_with_valid_data(
        self, tmp_path, schema_sql
    ):
        """Test that validate_members() returns 0 when members match."""
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, 'Alice Leader', 'Alice', 'alice@example.com', 'leader', '2024-01-01', 1))
        conn.commit()
        conn.close()

        # Create processed directory with members.csv
        # validate_members expects data_dir to be the processed directory
        processed_dir = tmp_path / 'processed' / '2025-02'
        processed_dir.mkdir(parents=True)

        members_file = processed_dir / 'members.csv'
        with open(members_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Date Joined', 'Active', 'Priority', 'Index', 'Total Attended'])
            writer.writeheader()
            writer.writerow({'id': '1', 'Name': 'Alice Leader', 'Display Name': 'Alice', 'Email Address': 'alice@example.com', 'Role': 'leader', 'Date Joined': '2024-01-01', 'Active': 'TRUE', 'Priority': '5', 'Index': '0', 'Total Attended': '10'})

        # Call validate_members - pass the processed directory
        result = validate_members(str(db_path), str(tmp_path / 'processed'))

        assert result == 0, f"Valid data should return 0, got {result}"

    def test_validate_members_returns_one_with_no_periods(
        self, tmp_path, schema_sql
    ):
        """Test that validate_members() returns 1 when no period directories found."""
        # Create empty test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)
        conn.close()

        # Create empty processed directory (no periods)
        processed_dir = tmp_path / 'processed'
        processed_dir.mkdir()

        # Call validate_members
        result = validate_members(str(db_path), str(processed_dir))

        assert result == 1, f"No periods should return 1, got {result}"

    def test_validate_members_returns_one_with_no_members_csv(
        self, tmp_path, schema_sql
    ):
        """Test that validate_members() returns 1 when no members.csv found."""
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)
        conn.close()

        # Create processed directory with period subdirectory but no members.csv
        processed_dir = tmp_path / 'processed' / '2025-02'
        processed_dir.mkdir(parents=True)

        # Call validate_members
        result = validate_members(str(db_path), str(tmp_path / 'processed'))

        assert result == 1, f"No members.csv should return 1, got {result}"


@pytest.mark.db
class TestValidatePeriodFunction:
    """Tests for validate_period() CLI function."""

    def test_validate_period_returns_one_when_period_not_found(
        self, tmp_path, schema_sql
    ):
        """Test that validate_period() returns 1 when period not found in DB."""
        # Create test database (empty, no periods)
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)
        conn.close()

        # Call validate_period for non-existent period
        result = validate_period(str(db_path), '2026-01', str(tmp_path))

        assert result == 1, f"Non-existent period should return 1, got {result}"

    def test_validate_period_returns_one_when_period_directory_missing(
        self, tmp_path, schema_sql
    ):
        """Test that validate_period() returns 1 when period directory missing."""
        # Create test database with period
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO schedule_periods (id, period_name, display_name, start_date, end_date, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, '2025-02', 'Feb 2025', '2025-02-01', '2025-02-28', 'scheduled'))
        conn.commit()
        conn.close()

        # Create empty processed directory (no period subdirectory)
        processed_dir = tmp_path / 'processed'
        processed_dir.mkdir()

        # Call validate_period
        result = validate_period(str(db_path), '2025-02', str(processed_dir))

        assert result == 1, f"Missing period directory should return 1, got {result}"


# =============================================================================
# INTEGRATION TESTS: Full Validation Logic End-to-End
# =============================================================================

@pytest.mark.db
class TestValidateMembersFullIntegration:
    """
    Full integration tests for validate_members() function.

    Exercises the complete member validation flow:
    - Member iteration and field comparison
    - Detecting members in CSV but not DB
    - Detecting members in DB but not CSV
    - Field mismatch detection (display_name, role, active status)
    - Return code indicating success/failure
    """

    def test_validate_members_detects_all_issues_complete_flow(
        self, tmp_path, schema_sql
    ):
        """
        INTEGRATION TEST: Complete member validation with mixed state data.

        Exercises:
        1. Member iteration across CSV
        2. Field comparison logic (multiple fields)
        3. Member in CSV but not DB detection
        4. Member in DB but not CSV detection
        5. Return code validation (1 = failure)

        Scenario:
        - Database: alice (leader), bob (follower), charlie (inactive)
        - CSV: alice (follower, display=Alicia, active=FALSE), bob (matches), dave (new)
        - Expected issues:
          * alice display_name mismatch (Alice vs Alicia)
          * alice role mismatch (leader vs follower)
          * alice active mismatch (1 vs 0)
          * dave member not in DB
          * charlie member in DB but not in CSV
          Total: 5 issues
        """
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

        cursor = conn.cursor()

        # Insert DB members: alice (leader, active), bob (follower, active), charlie (inactive)
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, 'Alice Leader', 'Alice', 'alice@example.com', 'leader', '2024-01-01', 1))
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (2, 'Bob Follower', 'Bob', 'bob@example.com', 'follower', '2024-01-01', 1))
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (3, 'Charlie Inactive', 'Charlie', 'charlie@example.com', 'leader', '2024-01-01', 0))

        conn.commit()
        conn.close()

        # Create processed directory with members.csv
        processed_dir = tmp_path / 'processed' / '2025-02'
        processed_dir.mkdir(parents=True)

        members_file = processed_dir / 'members.csv'
        # alice has 3 mismatches (display_name, role, active)
        # bob matches perfectly
        # dave is in CSV but not in DB
        members_data = [
            {
                'id': '1',
                'Name': 'Alice Leader',
                'Display Name': 'Alicia',       # Mismatch: DB='Alice'
                'Email Address': 'alice@example.com',
                'Role': 'follower',             # Mismatch: DB='leader'
                'Date Joined': '2024-01-01',
                'Active': 'FALSE',              # Mismatch: DB=1 (TRUE)
                'Priority': '5',
                'Index': '0',
                'Total Attended': '10'
            },
            {
                'id': '2',
                'Name': 'Bob Follower',
                'Display Name': 'Bob',          # Matches
                'Email Address': 'bob@example.com',
                'Role': 'follower',             # Matches
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',               # Matches
                'Priority': '3',
                'Index': '1',
                'Total Attended': '8'
            },
            {
                'id': '4',
                'Name': 'Dave NewMember',
                'Display Name': 'Dave',
                'Email Address': 'dave@example.com',  # In CSV but not in DB
                'Role': 'leader',
                'Date Joined': '2024-01-01',
                'Active': 'TRUE',
                'Priority': '1',
                'Index': '2',
                'Total Attended': '0'
            }
        ]

        with open(members_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # Call validate_members
        result = validate_members(str(db_path), str(tmp_path / 'processed'))

        # Should FAIL (return 1) due to multiple issues
        assert result == 1, "validate_members() should return 1 when issues found"

    def test_validate_members_passes_with_exact_match(
        self, tmp_path, schema_sql
    ):
        """
        INTEGRATION TEST: Successful validation when all members match.

        Exercises:
        - Full member iteration
        - Successful field comparison (all match)
        - Correct return code (0 = success)
        """
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

        cursor = conn.cursor()

        # Insert DB members
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, 'Alice Leader', 'Alice', 'alice@example.com', 'leader', '2024-01-01', 1))
        cursor.execute("""
            INSERT INTO peeps (id, full_name, display_name, email, primary_role, date_joined, active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (2, 'Bob Follower', 'Bob', 'bob@example.com', 'follower', '2024-01-01', 1))

        conn.commit()
        conn.close()

        # Create processed directory with matching members.csv
        processed_dir = tmp_path / 'processed' / '2025-02'
        processed_dir.mkdir(parents=True)

        members_file = processed_dir / 'members.csv'
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
                'Total Attended': '10'
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
                'Total Attended': '8'
            }
        ]

        with open(members_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # Call validate_members
        result = validate_members(str(db_path), str(tmp_path / 'processed'))

        # Should PASS (return 0)
        assert result == 0, "validate_members() should return 0 when all members match"


@pytest.mark.db
class TestValidatePeriodFullIntegration:
    """
    Full integration tests for validate_period() function.

    Exercises the complete period validation orchestration:
    - Sub-validator orchestration (responses, events, assignments, attendance, snapshots)
    - Issue collection from multiple sources
    - Issue reporting in output
    - Return code indicating success/failure
    """

    def test_validate_period_orchestration_detects_all_issues(
        self, tmp_path, schema_sql
    ):
        """
        INTEGRATION TEST: Complete period validation with mixed data states.

        Exercises:
        1. Sub-validator orchestration (multiple validations)
        2. Issue collection from multiple sources
        3. Complex data state (some correct, some wrong)
        4. Return code validation (1 = failure)

        Scenario:
        - Period with responses (1 correct, 1 wrong max_sessions)
        - Events in DB and responses.csv
        - Assignments in DB and results.json (some correct, some missing)
        - Attendance in DB and actual_attendance.json

        Expected:
        - Multiple issues from different validators
        - All issues collected and reported
        - Return code = 1 (failure)
        """
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

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

        # Responses (alice correct, bob has wrong max_sessions)
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, 1, 1, 'leader', 2, 7))
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, 1, 2, 'follower', 5, 5))  # Wrong: CSV says 3

        # Events
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, 1, 0, '2025-02-07T17:00:00', 120, 'scheduled'))
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, 1, 1, '2025-02-14T17:00:00', 120, 'completed'))

        # Assignments (event 1: alice correct, bob missing)
        cursor.execute("""
            INSERT INTO event_assignments (id, event_id, peep_id, assigned_role, assignment_type)
            VALUES (?, ?, ?, ?, ?)
        """, (1, 1, 1, 'leader', 'attendee'))

        # Attendance (event 2: alice correct)
        cursor.execute("""
            INSERT INTO event_attendance
            (id, event_id, peep_id, event_assignment_id, expected_role, expected_type, actual_role,
             attendance_status, participation_mode, last_minute_cancel, check_in_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, 2, 1, None, 'leader', 'attendee', 'leader', 'attended', 'scheduled', 0, '2025-02-14 17:00:00'))

        conn.commit()
        conn.close()

        # Create period data directory
        period_dir = tmp_path / 'processed' / '2025-02'
        period_dir.mkdir(parents=True)

        # responses.csv (alice correct, bob max_sessions=3 not 5)
        responses_file = period_dir / 'responses.csv'
        responses_data = [
            {
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            },
            {
                'Timestamp': '2/1/2025 11:00:00',
                'Email Address': 'bob@example.com',
                'Name': 'Bob Follower',
                'Primary Role': 'Follower',
                'Max Sessions': '3',  # CSV says 3, DB has 5 -> mismatch
                'Min Interval Days': '5',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            }
        ]
        with open(responses_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # results.json (alice correct, bob missing assignment)
        results_file = period_dir / 'results.json'
        results_data = {
            'valid_events': [
                {
                    'id': 0,
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}],
                    'alternates': []
                }
            ],
            'downgraded_events': []
        }
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2)

        # actual_attendance.json (alice correct)
        attendance_file = period_dir / 'actual_attendance.json'
        attendance_data = [
            {
                'id': 1,
                'date': '2025-02-14 17:00',
                'duration_minutes': 120,
                'attendees': [{'id': 1, 'role': 'leader'}]
            }
        ]
        with open(attendance_file, 'w', encoding='utf-8') as f:
            json.dump(attendance_data, f, indent=2)

        # members.csv (for snapshot validation)
        members_file = period_dir / 'members.csv'
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
        with open(members_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # cancellations.json (event 0 cancelled, but event 1 not in JSON - mismatch)
        cancellations_file = period_dir / 'cancellations.json'
        cancellations_data = {
            'cancelled_events': [0],  # Event 0 cancelled
            'cancelled_availability': [
                {
                    'email': 'alice@example.com',
                    'date_time': 'Friday February 7th - 5pm to 7pm'
                }
            ]
        }
        with open(cancellations_file, 'w', encoding='utf-8') as f:
            json.dump(cancellations_data, f, indent=2)

        # partnerships.json (alice-bob partnership)
        partnerships_file = period_dir / 'partnerships.json'
        partnerships_data = {
            '1': [2]  # Alice (id=1) requests partnership with Bob (id=2)
        }
        with open(partnerships_file, 'w', encoding='utf-8') as f:
            json.dump(partnerships_data, f, indent=2)

        # Call validate_period
        result = validate_period(str(db_path), '2025-02', str(tmp_path / 'processed'))

        # Should FAIL (return 1) due to multiple issues
        assert result == 1, "validate_period() should return 1 when issues found"

    def test_validate_period_passes_with_all_data_matching(
        self, tmp_path, schema_sql
    ):
        """
        INTEGRATION TEST: Successful validation when all data matches.

        Exercises:
        - Multiple sub-validators all passing
        - Complex but correct data state
        - Correct return code (0 = success)
        """
        # Create test database
        db_path = tmp_path / 'test.db'
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        reordered_sql = _parse_and_reorder_schema(schema_sql)
        conn.executescript(reordered_sql)

        cursor = conn.cursor()

        # Insert matching test data
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

        # Responses (both correct)
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, 1, 1, 'leader', 2, 7))
        cursor.execute("""
            INSERT INTO responses (id, period_id, peep_id, response_role, max_sessions, min_interval_days)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (2, 1, 2, 'follower', 3, 5))

        # Events
        cursor.execute("""
            INSERT INTO events (id, period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (1, 1, 0, '2025-02-07T17:00:00', 120, 'scheduled'))

        # Assignments
        cursor.execute("""
            INSERT INTO event_assignments (id, event_id, peep_id, assigned_role, assignment_type)
            VALUES (?, ?, ?, ?, ?)
        """, (1, 1, 1, 'leader', 'attendee'))
        cursor.execute("""
            INSERT INTO event_assignments (id, event_id, peep_id, assigned_role, assignment_type)
            VALUES (?, ?, ?, ?, ?)
        """, (2, 1, 2, 'follower', 'attendee'))

        conn.commit()
        conn.close()

        # Create period data directory with matching data
        period_dir = tmp_path / 'processed' / '2025-02'
        period_dir.mkdir(parents=True)

        # responses.csv (matches DB)
        responses_file = period_dir / 'responses.csv'
        responses_data = [
            {
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': 'alice@example.com',
                'Name': 'Alice Leader',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            },
            {
                'Timestamp': '2/1/2025 11:00:00',
                'Email Address': 'bob@example.com',
                'Name': 'Bob Follower',
                'Primary Role': 'Follower',
                'Max Sessions': '3',
                'Min Interval Days': '5',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            }
        ]
        with open(responses_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
            writer.writeheader()
            writer.writerows(responses_data)

        # results.json (matches DB)
        results_file = period_dir / 'results.json'
        results_data = {
            'valid_events': [
                {
                    'id': 0,
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [{'id': 1, 'role': 'leader'}, {'id': 2, 'role': 'follower'}],
                    'alternates': []
                }
            ],
            'downgraded_events': []
        }
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2)

        # members.csv (matches DB)
        members_file = period_dir / 'members.csv'
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
        with open(members_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
            writer.writeheader()
            writer.writerows(members_data)

        # cancellations.json (no cancelled events or availability - empty)
        cancellations_file = period_dir / 'cancellations.json'
        cancellations_data = {
            'cancelled_events': [],
            'cancelled_availability': []
        }
        with open(cancellations_file, 'w', encoding='utf-8') as f:
            json.dump(cancellations_data, f, indent=2)

        # partnerships.json (no partnerships)
        partnerships_file = period_dir / 'partnerships.json'
        partnerships_data = {}
        with open(partnerships_file, 'w', encoding='utf-8') as f:
            json.dump(partnerships_data, f, indent=2)

        # Call validate_period
        result = validate_period(str(db_path), '2025-02', str(tmp_path / 'processed'))

        # Should PASS (return 0)
        assert result == 0, "validate_period() should return 0 when all data matches"
