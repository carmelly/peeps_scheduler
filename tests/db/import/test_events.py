"""Tests for event creation and derivation from availability strings.

Tests cover:
- Event auto-derivation from availability strings
- Event deduplication
- Date format validation
- Backward compatibility with old format
- Event duration precedence handling
"""

import csv
from pathlib import Path
import pytest
from tests.db.helpers import assert_event_count
from tests.fixtures.data_specs import ResponseSpec


@pytest.mark.db
class TestEventCreation:
    """Tests for event creation and derivation from availability strings."""

    def test_auto_derives_events_from_availability(self, importer_factory):
        """PeriodImporter auto-derives events from availability strings."""
        ctx = importer_factory()

        # Period already created by importer_factory, just import the data
        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        assert_event_count(ctx.cursor, ctx.period_id, expected=3)

    @pytest.mark.revisit("Issue #032: Reject duplicate events in single response availability")
    def test_deduplicates_duplicate_event_dates(self, importer_factory, responses_csv_builder):
        """PeriodImporter currently allows duplicate events in one person's availability.

        REVISIT: This test documents current behavior (allows duplicates).
        See issue #032 for planned behavior change: duplicate events in a single response should raise error.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Member lists same event twice in their availability
        responses = [
            ResponseSpec(
                email="member1@test.com",
                name="Test Member 1",
                role="leader",
                availability=["Friday February 7th - 5pm to 7pm", "Friday February 7th - 5pm to 7pm"]
            ),
            ResponseSpec(
                email="member2@test.com",
                name="Test Member 2",
                role="follower",
                availability=["Friday February 7th - 5pm to 7pm"]
            )
        ]
        responses_csv_builder(period_dir, responses)

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        assert_event_count(ctx.cursor, ctx.period_id, expected=1)

    def test_raises_error_for_invalid_event_date_format(self, importer_factory):
        """PeriodImporter raises ValueError for invalid date format in availability."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        responses_path = period_dir / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role',
                'Secondary Role', 'Max Sessions', 'Availability',
                'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'leader',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Availability': 'Invalid Date Format, Another Invalid Date',
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Partnership Preference': '',
                'Questions or Comments': ''
            })

        response_mapping = ctx.importer.import_responses()
        with pytest.raises(ValueError, match="time data.*Invalid Date Format.*does not match format"):
            ctx.importer.create_events(response_mapping)

    def test_old_format_dates_default_to_120_minutes(self, importer_factory, responses_csv_builder):
        """Old format availability strings (no time range) default to 120 minute duration."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        responses = [ResponseSpec(
            email="member1@test.com",
            name="Test Member 1",
            role="leader",
            availability=["Friday February 7th - 5pm"]
        )]
        responses_csv_builder(period_dir, responses)

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Verify event was created
        assert_event_count(ctx.cursor, ctx.period_id, expected=1)

        # Verify duration
        from tests.db.helpers import get_single_value
        duration = get_single_value(ctx.cursor, 'events', 'duration_minutes', f'period_id = {ctx.period_id}')
        assert duration == 120, "Old format should default to 120 minutes"

    def test_event_duration_row_takes_precedence_over_availability_default(self, importer_factory):
        """Event: row duration takes precedence over availability string defaults.

        Bug scenario: Event: row specifies 90 minutes but availability uses old format (no time range).
        The database event should use Event: row value (90 minutes), not availability default (120 minutes).
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        responses_path = period_dir / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role',
                'Secondary Role', 'Max Sessions', 'Availability',
                'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ])
            writer.writeheader()
            # Event: row with 90 minute duration
            writer.writerow({
                'Timestamp': '',
                'Email Address': '',
                'Name': 'Event: Friday February 7 - 5pm',
                'Primary Role': '',
                'Secondary Role': '',
                'Max Sessions': '',
                'Availability': '',
                'Event Duration': '90',
                'Session Spacing Preference': '',
                'Min Interval Days': '',
                'Partnership Preference': '',
                'Questions or Comments': ''
            })
            # Regular response referencing same event with old format
            writer.writerow({
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'leader',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Max Sessions': '2',
                'Availability': 'Friday February 7 - 5pm',
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Min Interval Days': '7',
                'Partnership Preference': '',
                'Questions or Comments': ''
            })

        response_mapping = ctx.importer.import_responses()
        ctx.importer.create_events(response_mapping)

        # Verify event was created
        assert_event_count(ctx.cursor, ctx.period_id, expected=1)

        # Verify duration precedence
        from tests.db.helpers import get_single_value
        duration = get_single_value(ctx.cursor, 'events', 'duration_minutes', f'period_id = {ctx.period_id}')
        event_datetime = get_single_value(ctx.cursor, 'events', 'event_datetime', f'period_id = {ctx.period_id}')

        assert duration == 90, (
            f"Event: row duration (90 min) should take precedence over old format default (120 min), "
            f"but got {duration} minutes for event {event_datetime}"
        )
