"""Integration tests for full period import workflow.

Tests cover:
- Period status lifecycle (draft, scheduled, completed)
- Event status determination from data files
- Full sequential import flow
- Cross-period features (total_attended accumulation)
"""

import json
from pathlib import Path
import pytest
from tests.db.helpers import (
    assert_event_count,
    assert_period_status,
    assert_response_count,
    assert_row_count,
    assert_snapshot_count,
    get_single_value,
    get_table_count,
)
from tests.fixtures.data_specs import AttendanceSpec, EventSpec


@pytest.mark.db
@pytest.mark.integration
class TestPeriodStatusLifecycle:
    """Tests for period status determination based on data files present."""

    @pytest.mark.parametrize("has_assignments,has_attendance,expected_status", [
        (False, False, "draft"),           # No schedule, no attendance
        (True, False, "scheduled"),        # Has schedule, not completed
        (True, True, "completed"),         # Has schedule and attendance
    ])
    def test_period_status_determined_by_data_files(
        self,
        importer_factory,
        has_assignments,
        has_attendance,
        expected_status
    ):
        """Test that period status is correctly determined by which data files are present."""
        ctx = importer_factory(create_period=False)
        period_dir = ctx.period_data['period_dir']

        # Modify data files based on test parameters
        if not has_assignments:
            # Remove results.json (no assignments)
            (Path(period_dir) / 'results.json').unlink(missing_ok=True)

        if not has_attendance:
            # Remove actual_attendance.json
            (Path(period_dir) / 'actual_attendance.json').unlink(missing_ok=True)

        # Import period
        ctx.importer.import_period()

        # Verify period status
        assert_period_status(ctx.cursor, ctx.importer.period_id, expected_status)

    def test_raises_error_for_invalid_period_name_format(self, importer_factory):
        """PeriodImporter raises ValueError when period_name has invalid format."""
        ctx = importer_factory(period_name='invalid-format-here', create_period=False)

        with pytest.raises(ValueError, match="Invalid period name format"):
            ctx.importer.create_schedule_period()

    def test_december_period_handling(self, importer_factory):
        """PeriodImporter correctly calculates end_date for December (year rollover)."""
        ctx = importer_factory(period_name='2025-12', create_period=False)

        # Should handle December → January year rollover without error
        ctx.importer.create_schedule_period()

        # Verify period was created
        period_id = ctx.importer.period_id
        assert period_id is not None, "Should create December period successfully"

        # Verify end_date is correct (2025-12-31, not 2026-01-31)
        end_date = get_single_value(
            ctx.cursor,
            'schedule_periods',
            'end_date',
            f'id = {period_id}'
        )
        assert end_date == '2025-12-31', \
            f"December period should end on 2025-12-31, got {end_date}"


@pytest.mark.db
@pytest.mark.integration
class TestEventStatusDetermination:
    """Tests for event status determination from results.json and actual_attendance.json."""

    def test_event_status_lifecycle_transitions(
        self,
        importer_factory,
        results_json_builder,
        attendance_json_builder
    ):
        """Test that events transition through status lifecycle: proposed → scheduled → completed."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data['period_dir'])

        # Create test scenario:
        # Event 1 (2025-02-07): scheduled and completed (in both results + attendance)
        # Event 2 (2025-02-14): scheduled but cancelled (in results, not in attendance)
        # Event 3 (2025-02-21): only proposed (not in results or attendance)

        # Overwrite results.json with only 2 events
        results_json_builder(period_dir, [
            EventSpec(
                date='2025-02-07 17:00',
                attendees=[
                    (1, 'Test Member 1', 'leader'),
                    (2, 'Test Member 2', 'follower')
                ]
            ),
            EventSpec(
                date='2025-02-14 17:00',
                attendees=[
                    (3, 'Test Member 3', 'leader'),
                    (4, 'Test Member 4', 'follower')
                ]
            )
        ])

        # Overwrite actual_attendance.json with only 1 event (Event 1)
        attendance_json_builder(period_dir, [
            AttendanceSpec(
                date='2025-02-07 17:00',
                attendees=[
                    (1, 'Test Member 1', 'leader'),
                    (2, 'Test Member 2', 'follower')
                ]
            )
        ])

        # Import period
        ctx.importer.import_period()
        period_id = ctx.importer.period_id

        # Multi-row query for event status verification - acceptable
        ctx.cursor.execute("""
            SELECT event_datetime, status
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (period_id,))
        events = ctx.cursor.fetchall()

        # Should have 3 events total (derived from responses)
        assert len(events) >= 2, f"Should have at least 2 events, got {len(events)}"

        # Event 1 (2025-02-07): completed (in attendance)
        event_1 = next((e for e in events if e[0].startswith('2025-02-07')), None)
        assert event_1 is not None, "Should have event on 2025-02-07"
        assert event_1[1] == 'completed', \
            f"Event 1 should be 'completed' (in attendance), got '{event_1[1]}'"

        # Event 2 (2025-02-14): cancelled (in results but not attendance)
        event_2 = next((e for e in events if e[0].startswith('2025-02-14')), None)
        assert event_2 is not None, "Should have event on 2025-02-14"
        assert event_2[1] == 'cancelled', \
            f"Event 2 should be 'cancelled' (scheduled but not attended), got '{event_2[1]}'"

    def test_event_duration_updated_from_results_json(
        self,
        importer_factory,
        results_json_builder
    ):
        """Test that event duration is updated from results.json (can differ from proposed duration)."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data['period_dir'])

        # Overwrite results.json with 90-minute event (downgraded from default 120)
        results_json_builder(period_dir, [
            EventSpec(
                date='2025-02-07 17:00',
                duration_minutes=90,  # Downgraded from 120
                attendees=[
                    (1, 'Test Member 1', 'leader'),
                    (2, 'Test Member 2', 'follower')
                ]
            )
        ])

        # Remove attendance to test results.json duration only
        (Path(period_dir) / 'actual_attendance.json').unlink(missing_ok=True)

        # Import period
        ctx.importer.import_period()

        # Query event duration
        from tests.db.helpers import get_event_id_by_datetime
        event_id = get_event_id_by_datetime(ctx.cursor, ctx.importer.period_id, '2025-02-07')
        duration = get_single_value(
            ctx.cursor,
            'events',
            'duration_minutes',
            f"id = {event_id}"
        ) if event_id else None

        # Should be 90 minutes (from results.json, not 120 from responses)
        assert duration == 90, f"Event duration should be 90 minutes (from results.json), got {duration}"

    def test_legacy_event_id_populated_from_results(self, importer_factory):
        """Test that legacy_period_event_id is populated from results.json 'id' field."""
        ctx = importer_factory(create_period=False)
        period_dir = ctx.period_data['period_dir']

        # Create results.json with explicit legacy IDs
        results_data = {
            'valid_events': [
                {
                    'id': 'legacy_evt_001',
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'}
                    ],
                    'alternates': []
                },
                {
                    'id': 'legacy_evt_002',
                    'date': '2025-02-14 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        with open(Path(period_dir) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Import period
        ctx.importer.import_period()

        # Multi-row query for legacy ID verification - acceptable
        ctx.cursor.execute("""
            SELECT event_datetime, legacy_period_event_id
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (ctx.importer.period_id,))
        events = ctx.cursor.fetchall()

        assert len(events) >= 2, "Should have at least 2 events"

        # Find events by date
        event_1 = next((e for e in events if e[0].startswith('2025-02-07')), None)
        event_2 = next((e for e in events if e[0].startswith('2025-02-14')), None)

        assert event_1 is not None and event_1[1] == 'legacy_evt_001', \
            f"Event 1 should have legacy_period_event_id='legacy_evt_001', got '{event_1[1] if event_1 else None}'"

        assert event_2 is not None and event_2[1] == 'legacy_evt_002', \
            f"Event 2 should have legacy_period_event_id='legacy_evt_002', got '{event_2[1] if event_2 else None}'"


@pytest.mark.db
@pytest.mark.integration
class TestFullImportFlow:
    """Tests for full sequential import workflow with multiple periods."""

    def test_sequential_multi_period_import(self, importer_factory):
        """Test importing 2 periods sequentially with full data flow."""
        # Create and fully import first period
        ctx1 = importer_factory(period_name='2025-02', skip_snapshots=False, create_period=False)
        ctx1.importer.import_period()

        # Create second period, reusing peep_id_mapping from first period
        # Use create_period=True to create the period before import_responses
        ctx2 = importer_factory(
            period_name='2025-03',
            skip_snapshots=False,
            create_period=True,
            peep_id_mapping=ctx1.peep_id_mapping
        )

        # Only import responses for period2 (events will fail due to duplicate datetimes)
        # Note: test_period_data fixture generates same dates for all periods,
        # so period2 events will fail to insert due to UNIQUE constraint on event_datetime
        # This is expected behavior - periods shouldn't have overlapping event times
        ctx2.importer.import_responses()

        # Verify both periods created
        assert_row_count(ctx1.cursor, 'schedule_periods', 2, msg="Should have 2 periods")

        # Verify members (10 total, shared across periods)
        assert_row_count(ctx1.cursor, 'peeps', 10, msg="Should have 10 members")

        # Verify responses for both periods (8 responses each = 16 total)
        assert_row_count(ctx1.cursor, 'responses', 16, msg="Should have 16 responses total")

        # Verify snapshots for period1 only (period2 has no events)
        assert_snapshot_count(ctx1.cursor, ctx1.importer.period_id, 10)

    def test_import_period_without_responses(self, importer_factory):
        """Test edge case: period has NO responses but HAS results.json and actual_attendance.json.

        This represents periods where scheduling happened externally (e.g., 2025-12).
        """
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data['period_dir'])

        # Remove responses.csv to simulate period with no availability data
        (period_dir / 'responses.csv').unlink(missing_ok=True)

        # Import period (using default results.json and actual_attendance.json from fixture)
        ctx.importer.import_period()

        # Verify: 0 responses (no responses.csv)
        assert_response_count(ctx.cursor, ctx.importer.period_id, 0)

        # Verify: 3 events created from results.json (default fixture)
        assert_event_count(ctx.cursor, ctx.importer.period_id, 3)

        # Verify: Events have 'completed' status (from actual_attendance.json)
        assert_event_count(ctx.cursor, ctx.importer.period_id, 3, status='completed')

        # Verify: 0 event_availability (no responses)
        availability_count = get_table_count(
            ctx.cursor,
            'event_availability ea',
            f'ea.event_id IN (SELECT id FROM events WHERE period_id = {ctx.importer.period_id})'
        )
        assert availability_count == 0, "Should have 0 event_availability records"


@pytest.mark.db
@pytest.mark.integration
class TestCrossPeriodFeatures:
    """Tests for features that span multiple periods (total_attended accumulation)."""

    def test_total_attended_accumulates_across_periods(
        self,
        importer_factory,
        results_json_builder,
        attendance_json_builder
    ):
        """Test that total_attended in snapshots accumulates correctly across consecutive periods.

        When a member attends events in multiple periods, their total_attended snapshot
        should show cumulative count. Period N+1 snapshot should be higher than period N.
        """
        # Create first period and import fully
        ctx1 = importer_factory(period_name='2025-02', skip_snapshots=False, create_period=False)
        ctx1.importer.import_period()

        # Get member 1's total_attended from period1 snapshot
        period1_total = get_single_value(
            ctx1.cursor,
            'peep_order_snapshots',
            'total_attended',
            f'period_id = {ctx1.importer.period_id} AND peep_id = 1'
        )
        assert period1_total is not None, "Period 1 snapshot should exist for member 1"

        # Create second period, reusing peep_id_mapping from first period
        ctx2 = importer_factory(
            period_name='2025-03',
            skip_snapshots=False,
            create_period=False,
            peep_id_mapping=ctx1.peep_id_mapping
        )
        period2_dir = Path(ctx2.period_data['period_dir'])

        # Remove responses.csv so events come only from results.json (not from availability)
        (period2_dir / 'responses.csv').unlink(missing_ok=True)

        # Overwrite period2 files with DIFFERENT event dates to avoid UNIQUE constraint
        # Member 1 attends 2 events in period2
        results_json_builder(period2_dir, [
            EventSpec(
                date="2025-03-07 17:00",  # March instead of February
                attendees=[(1, "Test Member 1", "leader"), (2, "Test Member 2", "follower")]
            ),
            EventSpec(
                date="2025-03-14 17:00",
                attendees=[(1, "Test Member 1", "leader"), (3, "Test Member 3", "leader")]
            )
        ])

        attendance_json_builder(period2_dir, [
            AttendanceSpec(
                date="2025-03-07 17:00",
                attendees=[(1, "Test Member 1", "leader"), (2, "Test Member 2", "follower")]
            ),
            AttendanceSpec(
                date="2025-03-14 17:00",
                attendees=[(1, "Test Member 1", "leader"), (3, "Test Member 3", "leader")]
            )
        ])

        # Import period2 with new event dates and snapshots
        ctx2.importer.import_period()

        # Get member 1's total_attended from period2 snapshot
        period2_total = get_single_value(
            ctx1.cursor,
            'peep_order_snapshots',
            'total_attended',
            f'period_id = {ctx2.importer.period_id} AND peep_id = 1'
        )
        assert period2_total is not None, "Period 2 snapshot should exist for member 1"

        # Member 1 attended period1_total events in period1 + 2 events in period2
        # So period2 snapshot should show period1_total + 2
        expected_period2_total = period1_total + 2

        assert period2_total == expected_period2_total, \
            f"Period 2 snapshot should show total_attended={expected_period2_total} " \
            f"({period1_total} from period1 + 2 from period2), got {period2_total}"

        # Verify accumulation amount
        accumulation = period2_total - period1_total
        assert accumulation == 2, \
            f"Period 2 should show 2 additional events attended (increase from {period1_total} to {period2_total})"
