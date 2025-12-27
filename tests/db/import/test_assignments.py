"""Tests for assignment import functionality (PeriodImporter.import_assignments).

Tests cover:
- Assignment import from results.json
- Attendee and alternate handling
- Assignment ordering and positioning
- Error handling for missing members
- Duplicate assignment detection
- Event creation from results.json if not exists
"""

import json
from pathlib import Path
import pytest
from tests.db.helpers import (
    assert_assignment_count,
    assert_event_count,
    assert_peep_assignment_count,
    get_assignment_count_by_type,
    get_single_value,
)
from tests.fixtures.data_specs import EventSpec


@pytest.mark.db
class TestAssignmentImport:
    """Tests for assignment import and handling."""

    def test_imports_assignments_from_results_json(self, importer_factory, results_json_builder):
        """PeriodImporter imports assignments from results.json."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # First import responses and events to establish database state
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Get actual event from database to use correct datetime
        event_id = get_single_value(ctx.cursor, "events", "id", f"period_id = {ctx.period_id}")
        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        assert event_id is not None

        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]  # Convert to "YYYY-MM-DD HH:MM"

        # Create results.json with assignments
        results_json_builder(period_dir, [
            EventSpec(
                date=event_datetime_str,
                attendees=[(1, "Test Member 1", "leader"), (2, "Test Member 2", "follower")]
            )
        ])

        # Import assignments
        imported_count = ctx.importer.import_assignments()

        assert imported_count == 2
        assert_assignment_count(ctx.cursor, event_id, expected=2)

    def test_imports_attendees_and_alternates(self, importer_factory, results_json_builder):
        """PeriodImporter imports both attendees and alternates correctly."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        event_id = get_single_value(ctx.cursor, "events", "id", f"period_id = {ctx.period_id}")
        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        results_json_builder(
            period_dir,
            [
                EventSpec(
                    date=event_datetime_str,
                    attendees=[(1, "Test Member 1", "leader"), (2, "Test Member 2", "follower")],
                    alternates=[(3, "Test Member 3", "leader"), (4, "Test Member 4", "follower")],
                )
            ],
        )

        imported_count = ctx.importer.import_assignments()

        assert imported_count == 4

        # Verify assignment types
        attendee_count = get_assignment_count_by_type(ctx.cursor, event_id, "attendee")
        alternate_count = get_assignment_count_by_type(ctx.cursor, event_id, "alternate")
        assert attendee_count == 2, "Should have 2 attendees"
        assert alternate_count == 2, "Should have 2 alternates"

    def test_preserves_assignment_order_within_event(self, importer_factory, results_json_builder):
        """Assignment order matches discovery order in results.json attendees array."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        event_id = get_single_value(ctx.cursor, "events", "id", f"period_id = {ctx.period_id}")
        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Create event with specific member order
        results_json_builder(period_dir, [
            EventSpec(
                date=event_datetime_str,
                attendees=[(1, "Member 1", "leader"), (2, "Member 2", "follower"), (3, "Member 3", "leader")]
            )
        ])

        ctx.importer.import_assignments()

        # Multi-row query for order verification
        ctx.cursor.execute("""
            SELECT peep_id, assignment_order
            FROM event_assignments
            WHERE event_id = ? AND assignment_type = 'attendee'
            ORDER BY assignment_order
        """, (event_id,))

        assignments = ctx.cursor.fetchall()
        assert len(assignments) == 3
        assert assignments[0] == (1, 0), "Member 1 should have order 0"
        assert assignments[1] == (2, 1), "Member 2 should have order 1"
        assert assignments[2] == (3, 2), "Member 3 should have order 2"

    def test_preserves_alternate_position_order(self, importer_factory, results_json_builder):
        """Alternate position matches discovery order in results.json alternates array."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        event_id = get_single_value(ctx.cursor, "events", "id", f"period_id = {ctx.period_id}")
        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        results_json_builder(
            period_dir,
            [
                EventSpec(
                    date=event_datetime_str,
                    attendees=[(1, "Member 1", "leader")],
                    alternates=[
                        (3, "Member 3", "leader"),
                        (4, "Member 4", "follower"),
                        (5, "Member 5", "leader"),
                    ],
                )
            ],
        )

        ctx.importer.import_assignments()

        # Multi-row query for alternate position verification
        ctx.cursor.execute(
            """
            SELECT peep_id, alternate_position
            FROM event_assignments
            WHERE event_id = ? AND assignment_type = 'alternate'
            ORDER BY alternate_position
        """,
            (event_id,),
        )

        alternates = ctx.cursor.fetchall()
        assert len(alternates) == 3
        assert alternates[0] == (3, 0), "Member 3 should have position 0"
        assert alternates[1] == (4, 1), "Member 4 should have position 1"
        assert alternates[2] == (5, 2), "Member 5 should have position 2"

    def test_creates_event_from_results_if_not_exists(self, importer_factory, results_json_builder):
        """PeriodImporter creates event if it appears in results.json but not in database."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Only import responses and create availability-derived events
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create results.json with an event that doesn't exist in database yet
        results_json_builder(
            period_dir,
            [EventSpec(date="2025-02-25 17:00", attendees=[(1, "Test Member 1", "leader")])],
        )

        imported_count = ctx.importer.import_assignments()

        assert imported_count == 1

        # Verify event was created (should have 4 total: 3 from responses + 1 new)
        assert_event_count(ctx.cursor, ctx.period_id, 4)

    def test_raises_error_for_unknown_member(self, importer_factory, results_json_builder):
        """PeriodImporter raises ValueError when assignment references non-existent member."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Create results.json with unknown member ID
        results_json_builder(period_dir, [
            EventSpec(
                date=event_datetime_str,
                attendees=[(999, "Non-Existent Member", "leader")]  # ID doesn't exist
            )
        ])

        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*unknown member"):
            ctx.importer.import_assignments()

    def test_duplicate_assignment_skipped_by_constraint(self, importer_factory):
        """Duplicate assignments (same member, same event) are prevented by database constraint."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        event_datetime = get_single_value(
            ctx.cursor, "events", "event_datetime", f"period_id = {ctx.period_id}"
        )
        event_datetime_str = event_datetime.replace("T", " ").rsplit(":", 1)[0]

        # Create results.json with duplicate assignments (same member twice)
        period_dir_path = Path(period_dir)
        results_path = period_dir_path / 'results.json'
        results_data = {
            "valid_events": [
                {
                    "date": event_datetime_str,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"},
                        {"id": 1, "name": "Test Member 1", "role": "follower"}  # Duplicate
                    ],
                    "alternates": []
                }
            ]
        }
        with open(results_path, 'w') as f:
            json.dump(results_data, f)

        # Import assignments
        imported_count = ctx.importer.import_assignments()

        # Should only import 1 (second one caught by constraint)
        assert imported_count == 1, f"Should import 1 assignment (duplicate skipped), got {imported_count}"

        # Verify only 1 assignment in database for this member
        assert_peep_assignment_count(ctx.cursor, peep_id=1, expected=1)

