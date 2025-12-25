"""Tests for attendance import functionality (PeriodImporter.import_attendance).

Tests cover:
- Attendance import from actual_attendance.json
- Participation modes (scheduled, volunteer_fill, alternate_promoted)
- Error handling for missing events/members
- Duplicate attendance detection
"""

import json
from pathlib import Path
import pytest
from tests.db.helpers import assert_attendance_count
from tests.fixtures.data_specs import AttendanceSpec


@pytest.mark.db
class TestAttendanceImport:
    """Tests for attendance import and handling."""

    def test_imports_attendance_for_scheduled_members(self, importer_factory):
        """PeriodImporter imports attendance for members with assignments (scheduled mode)."""
        ctx = importer_factory()

        # Import responses and events
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Use default assignments from test_period_data (members 1 and 2 for each of 3 events)
        ctx.importer.import_assignments()

        # Use default attendance from test_period_data (members 1 and 2 attended all events)
        imported_count = ctx.importer.import_attendance()

        # Default test data has 3 events with 2 attendees each = 6 total
        assert imported_count == 6

        # Verify all attendance records have participation_mode = 'scheduled'
        ctx.cursor.execute("""
            SELECT ea.participation_mode, ea.attendance_status
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
            ORDER BY ea.event_id, ea.peep_id
        """, (ctx.period_id,))

        attendance_records = ctx.cursor.fetchall()
        assert len(attendance_records) == 6, "Should have 6 total attendance records (2 per event × 3 events)"
        for record in attendance_records:
            assert record[0] == 'scheduled', "Should have 'scheduled' participation mode"
            assert record[1] == 'attended', "Should have 'attended' status"

    def test_imports_attendance_for_unscheduled_members_volunteer_fill(self, importer_factory, attendance_json_builder):
        """PeriodImporter marks unscheduled members with volunteer_fill participation mode."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        from tests.db.helpers import get_single_value
        event_id = get_single_value(ctx.cursor, 'events', 'id', f'period_id = {ctx.period_id}')
        event_datetime = get_single_value(ctx.cursor, 'events', 'event_datetime', f'period_id = {ctx.period_id}')
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # No assignments imported - member 3 volunteers
        attendance_json_builder(period_dir, [
            AttendanceSpec(
                date=event_datetime_str,
                attendees=[(3, "Test Member 3", "leader")]  # No prior assignment
            )
        ])

        imported_count = ctx.importer.import_attendance()

        assert imported_count == 1
        assert_attendance_count(ctx.cursor, event_id, expected=1)

        # Verify participation_mode = 'volunteer_fill'
        from tests.db.helpers import get_attendance_participation_mode, get_single_value
        participation_mode = get_attendance_participation_mode(ctx.cursor, event_id, 3)
        assert participation_mode == 'volunteer_fill', "Should have 'volunteer_fill' participation mode"

        assignment_id = get_single_value(ctx.cursor, 'event_attendance', 'event_assignment_id',
                                        f'event_id = {event_id} AND peep_id = 3')
        assert assignment_id is None, "Should have no assignment_id"

    def test_imports_attendance_for_alternate_promoted(self, importer_factory, attendance_json_builder):
        """PeriodImporter marks alternates who attended with alternate_promoted mode."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        from tests.db.helpers import get_single_value
        event_id = get_single_value(ctx.cursor, 'events', 'id', f'period_id = {ctx.period_id}')
        event_datetime = get_single_value(ctx.cursor, 'events', 'event_datetime', f'period_id = {ctx.period_id}')
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Use default assignments which have member 3 as alternate
        ctx.importer.import_assignments()

        # Override attendance: alternate member 3 attended
        attendance_json_builder(period_dir, [
            AttendanceSpec(
                date=event_datetime_str,
                attendees=[(3, "Test Member 3", "leader")]  # Alternate attended
            )
        ])

        imported_count = ctx.importer.import_attendance()

        assert imported_count == 1
        assert_attendance_count(ctx.cursor, event_id, expected=1)

        # Verify participation_mode = 'alternate_promoted'
        from tests.db.helpers import get_attendance_participation_mode, get_single_value
        participation_mode = get_attendance_participation_mode(ctx.cursor, event_id, 3)
        assert participation_mode == 'alternate_promoted', "Should have 'alternate_promoted' participation mode"

        expected_type = get_single_value(ctx.cursor, 'event_attendance', 'expected_type',
                                         f'event_id = {event_id} AND peep_id = 3')
        assert expected_type == 'alternate', "Expected type should be 'alternate'"

    def test_raises_error_for_unknown_member(self, importer_factory, attendance_json_builder):
        """PeriodImporter raises ValueError when attendance references non-existent member."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        from tests.db.helpers import get_single_value
        event_datetime = get_single_value(ctx.cursor, 'events', 'event_datetime', f'period_id = {ctx.period_id}')
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Create attendance with unknown member ID
        attendance_json_builder(period_dir, [
            AttendanceSpec(
                date=event_datetime_str,
                attendees=[(999, "Non-Existent Member", "leader")]  # ID doesn't exist
            )
        ])

        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*unknown member"):
            ctx.importer.import_attendance()

    def test_raises_error_for_unknown_event(self, importer_factory, attendance_json_builder):
        """PeriodImporter raises ValueError when attendance references non-existent event."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create attendance with non-existent event date
        from tests.db.helpers import get_single_value
        attendance_json_builder(period_dir, [
            AttendanceSpec(
                date="2025-02-99 17:00",  # Invalid date
                attendees=[(1, "Test Member 1", "leader")]
            )
        ])

        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*which does not exist"):
            ctx.importer.import_attendance()

    def test_duplicate_attendance_skipped_by_constraint(self, importer_factory):
        """Duplicate attendance records (same member, same event) are prevented by database constraint."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        ctx.cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ? LIMIT 1", (ctx.period_id,))
        event_id, event_datetime = ctx.cursor.fetchone()
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Create attendance with duplicate by manually writing JSON (builder doesn't support duplicates)
        attendance_path = period_dir / 'actual_attendance.json'
        attendance_data = {
            "valid_events": [
                {
                    "date": event_datetime_str,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"},
                        {"id": 1, "name": "Test Member 1", "role": "follower"}  # Duplicate
                    ]
                }
            ]
        }
        with open(attendance_path, 'w') as f:
            json.dump(attendance_data, f)

        imported_count = ctx.importer.import_attendance()

        assert imported_count == 1
        assert_attendance_count(ctx.cursor, event_id, expected=1)

    def test_no_event_availability_created_for_empty_response_availability(
        self, importer_factory, responses_csv_builder
    ):
        """PeriodImporter skips event_availability creation when response has empty availability."""
        from tests.fixtures.data_specs import ResponseSpec
        from tests.db.helpers import assert_response_count, get_single_value, get_table_count

        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Create responses with mixed availability (one empty, one with events)
        responses = [
            ResponseSpec(
                email="member1@test.com",
                name="Test Member 1",
                role="leader",
                availability=[]  # Empty availability
            ),
            ResponseSpec(
                email="member2@test.com",
                name="Test Member 2",
                role="follower",
                availability=["Friday February 7th - 5pm to 7pm"]
            )
        ]
        responses_csv_builder(period_dir, responses)

        # Import responses, create events, and create event_availability
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)
        ctx.importer.create_event_availability(response_mapping)

        # Verify both responses exist
        assert_response_count(ctx.cursor, ctx.period_id, expected=2)

        # Get response IDs
        response1_id = get_single_value(ctx.cursor, 'responses', 'id',
                                       f'period_id = {ctx.period_id} AND peep_id = 1')
        response2_id = get_single_value(ctx.cursor, 'responses', 'id',
                                       f'period_id = {ctx.period_id} AND peep_id = 2')

        # Member 1: 0 event_availability records (empty availability)
        member1_avail_count = get_table_count(
            ctx.cursor, 'event_availability', where=f'response_id = {response1_id}'
        )
        assert member1_avail_count == 0, "Member 1 should have 0 event_availability (empty availability)"

        # Member 2: 1 event_availability record
        member2_avail_count = get_table_count(
            ctx.cursor, 'event_availability', where=f'response_id = {response2_id}'
        )
        assert member2_avail_count == 1, "Member 2 should have 1 event_availability"
