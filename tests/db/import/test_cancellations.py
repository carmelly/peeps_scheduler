"""Tests for cancelled events and availability (cancellations.json).

Tests cover:
- Cancelled event processing
- Cancelled availability processing
- Error handling for invalid data
- Backward compatibility with missing files
"""

import json
from datetime import datetime
from pathlib import Path
import pytest
from peeps_scheduler.db.import_period_data import (
    import_cancelled_availability,
    import_cancelled_events,
)
from peeps_scheduler.file_io import format_event_date
from tests.db.helpers import assert_event_count, assert_row_count, get_single_value, get_table_count


@pytest.mark.db
class TestEventCancellations:
    """Tests for importing cancelled events from cancellations.json."""

    def test_import_cancelled_events_processes_file(self, importer_factory, cancellations_json_builder):
        """PeriodImporter processes cancellations.json without errors."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses and events first
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create cancellations with empty list (valid data)
        cancellations_json_builder(
            period_dir,
            {"cancelled_events": [], "cancelled_availability": []},
        )

        # Import cancelled events should not raise
        cancellations_path = period_dir / "cancellations.json"
        try:
            import_cancelled_events(cancellations_path, ctx.period_id, ctx.cursor)
        except Exception as e:
            pytest.fail(f"import_cancelled_events should not raise: {e}")

    def test_cancelled_events_backward_compatible_missing_file(self, importer_factory):
        """PeriodImporter handles missing cancellations.json gracefully."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Remove cancellations.json to test backward compatibility
        (period_dir / "cancellations.json").unlink(missing_ok=True)

        # Import period - should handle missing file gracefully
        ctx.importer.import_period()

        # Verify no events are cancelled
        assert_event_count(ctx.cursor, ctx.importer.period_id, 0, status='cancelled')

    def test_cancelled_events_empty_list_handling(self, importer_factory, cancellations_json_builder):
        """PeriodImporter handles empty cancelled_events list."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses and events
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create cancellations with empty list
        cancellations_json_builder(
            period_dir,
            {"cancelled_events": [], "cancelled_availability": []},
        )

        # Import cancelled events
        cancellations_path = period_dir / "cancellations.json"
        import_cancelled_events(cancellations_path, ctx.period_id, ctx.cursor)

        # Verify no events are cancelled
        assert_event_count(ctx.cursor, ctx.period_id, 0, status='cancelled')

    def test_cancelled_events_validates_invalid_event_strings(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter raises ValueError for invalid event strings."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses and events
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create cancellations with invalid event string
        cancellations_json_builder(
            period_dir,
            {
                "cancelled_events": ["Invalid Event String That Doesn't Match Anything"],
                "cancelled_availability": [],
            },
        )

        # Import should raise ValueError
        cancellations_path = period_dir / "cancellations.json"
        with pytest.raises(ValueError, match="(?s).*cancelled_events.*does not match"):
            import_cancelled_events(cancellations_path, ctx.period_id, ctx.cursor)

    def test_imports_cancelled_events_via_json_file(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter calls import_cancelled_events_from_json during import_period."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Create cancellations.json with a cancelled event
        first_event_str = "Friday February 7th - 5pm to 7pm"
        cancellations_json_builder(
            period_dir,
            {"cancelled_events": [first_event_str], "cancelled_availability": []},
        )

        # Import period - calls import_cancelled_events_from_json internally
        ctx.importer.import_period()

        # Verify import_period completed (this exercises the cancellation code path)
        assert_event_count(ctx.cursor, ctx.importer.period_id, 3)


@pytest.mark.db
class TestCancelledAvailability:
    """Tests for importing cancelled availability from cancellations.json."""

    def test_cancelled_availability_processes_file(self, importer_factory, cancellations_json_builder):
        """PeriodImporter processes cancelled_availability without errors."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import everything
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create cancellations with empty list
        cancellations_json_builder(
            period_dir,
            {"cancelled_events": [], "cancelled_availability": []},
        )

        # Import cancelled availability should not raise
        cancellations_path = period_dir / "cancellations.json"
        try:
            import_cancelled_availability(cancellations_path, ctx.period_id, ctx.cursor)
        except Exception as e:
            pytest.fail(f"import_cancelled_availability should not raise: {e}")

    def test_cancelled_availability_duplicate_email_raises(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter raises error for duplicate email entries in cancelled_availability."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import everything
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Get peep email and two events
        peep_id = get_single_value(ctx.cursor, 'responses', 'peep_id', f'period_id = {ctx.period_id}')
        peep_email = get_single_value(ctx.cursor, 'peeps', 'email', f'id = {peep_id}')

        # Multi-row query for event date extraction
        ctx.cursor.execute(
            "SELECT id, event_datetime FROM events WHERE period_id = ? ORDER BY event_datetime LIMIT 2",
            (ctx.period_id,),
        )
        events = ctx.cursor.fetchall()

        event_strings = []

        for event_id, event_datetime in events:
            dt = datetime.fromisoformat(event_datetime)
            event_string = format_event_date(dt, duration_minutes=120)
            event_strings.append(event_string)

        # Create cancellations with duplicate emails
        cancellations_data = {
            "cancelled_events": [],
            "cancelled_availability": [
                {"email": peep_email, "events": [event_strings[0]]},
                {"email": peep_email, "events": [event_strings[1]]},
            ],
        }
        cancellations_path = period_dir / "cancellations.json"
        with open(cancellations_path, "w") as f:
            json.dump(cancellations_data, f)

        # Import should raise ValueError for duplicate emails
        with pytest.raises(ValueError, match=r"duplicate cancelled_availability"):
            import_cancelled_availability(cancellations_path, ctx.period_id, ctx.cursor)

    def test_cancelled_availability_missing_section_raises(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter raises error when cancelled_availability section is missing."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import everything
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Create cancellations without cancelled_availability section
        cancellations_data = {"cancelled_events": []}
        cancellations_path = period_dir / "cancellations.json"
        with open(cancellations_path, "w") as f:
            json.dump(cancellations_data, f)

        # Import should raise ValueError
        with pytest.raises(
            ValueError, match="cancellations.json missing 'cancelled_availability'"
        ):
            import_cancelled_availability(cancellations_path, ctx.period_id, ctx.cursor)

    def test_cancelled_availability_empty_list_handling(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter handles empty cancelled_availability list."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import everything
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Get initial count
        initial_count = get_table_count(ctx.cursor, 'event_availability')

        # Create cancellations with empty list
        cancellations_json_builder(
            period_dir,
            {"cancelled_events": [], "cancelled_availability": []},
        )

        # Import cancelled availability
        cancellations_path = period_dir / "cancellations.json"
        import_cancelled_availability(cancellations_path, ctx.period_id, ctx.cursor)

        # Verify count is unchanged
        final_count = get_table_count(ctx.cursor, 'event_availability')
        assert (
            final_count == initial_count
        ), "Event availability records should be unchanged with empty list"

    @pytest.mark.revisit("Issue #037: Unknown email in cancelled_availability should raise error")
    def test_cancelled_availability_unknown_email(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter currently handles unknown email gracefully with warning.

        REVISIT: This test documents current behavior (graceful handling with warning).
        Future behavior: should raise ValueError for unknown email instead of silently skipping.
        """
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Import period
        ctx.importer.import_period()

        # Create cancellations with email that doesn't exist in peeps
        cancellations_json_builder(
            period_dir,
            {
                "cancelled_events": [],
                "cancelled_availability": [
                    {
                        "email": "nonexistent@invalid.com",  # Email not in peeps
                        "events": ["Friday February 7th - 5pm to 7pm"]
                    }
                ]
            }
        )

        # Current behavior: handles gracefully (warning, no error)
        # Future behavior: should raise ValueError
        cancellations_path = period_dir / "cancellations.json"
        removed = import_cancelled_availability(cancellations_path, ctx.importer.period_id, ctx.cursor)

        # No records should be removed (email not found)
        assert removed == 0, "Should skip unknown email without error"

    @pytest.mark.revisit("Issue #037: Unknown email should raise error")
    def test_cancelled_availability_standalone_unknown_email(
        self, importer_factory, cancellations_json_builder
    ):
        """Standalone import_cancelled_availability handles unknown email gracefully.

        REVISIT: This test documents current behavior for standalone function (graceful handling).
        Future behavior: should raise ValueError for unknown email instead of silently skipping.
        """
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Import period
        ctx.importer.import_period()

        # Create cancellations with email that doesn't exist
        cancellations_json_builder(
            period_dir,
            {
                "cancelled_events": [],
                "cancelled_availability": [
                    {
                        "email": "unknown@invalid.com",  # Email not in peeps
                        "events": ["Friday February 7th - 5pm to 7pm"]
                    }
                ]
            }
        )

        # Current behavior: standalone function handles gracefully (warning, no error)
        # Future behavior: should raise ValueError
        cancellations_path = period_dir / "cancellations.json"
        removed = import_cancelled_availability(cancellations_path, ctx.importer.period_id, ctx.cursor)

        # No records should be removed (email not found)
        assert removed == 0, "Should skip unknown email without error"

    def test_cancelled_availability_removes_event_availability_records(
        self, importer_factory, cancellations_json_builder
    ):
        """PeriodImporter removes event_availability records for cancelled_availability entries."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        ctx.importer.import_period()
        period_id = ctx.importer.period_id

        response_id = get_single_value(ctx.cursor, 'responses', 'id', f'period_id = {period_id}')
        peep_id = get_single_value(ctx.cursor, 'responses', 'peep_id', f'id = {response_id}')
        peep_email = get_single_value(ctx.cursor, 'peeps', 'email', f'id = {peep_id}')
        event_datetime = get_single_value(ctx.cursor, 'events', 'event_datetime', f'period_id = {period_id}')
        event_id = get_single_value(ctx.cursor, 'events', 'id', f'event_datetime = \'{event_datetime}\'')

        initial_count = get_table_count(
            ctx.cursor,
            'event_availability',
            where=f'response_id = {response_id} AND event_id = {event_id}'
        )
        assert initial_count > 0, "Event availability should exist before cancellation"

        dt = datetime.fromisoformat(event_datetime)
        event_string = format_event_date(dt, duration_minutes=120)

        cancellations_json_builder(
            period_dir,
            {
                "cancelled_events": [],
                "cancelled_availability": [
                    {"email": peep_email, "events": [event_string]}
                ]
            }
        )

        cancellations_path = period_dir / "cancellations.json"
        import_cancelled_availability(cancellations_path, period_id, ctx.cursor)

        assert_row_count(
            ctx.cursor,
            'event_availability',
            expected=0,
            where=f'response_id = {response_id} AND event_id = {event_id}'
        )
