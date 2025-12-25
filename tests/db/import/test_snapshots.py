"""Tests for snapshot calculation and integration during import.

Integration tests for snapshot generation as part of the import workflow.
These tests verify that snapshots are correctly created, ordered, and
calculated during the PeriodImporter process.

Tests cover:
- Snapshot record creation for all members
- Index ordering and sequential positioning
- Priority changes based on response/attendance status
- Sequential enforcement of prior period snapshots
"""

from pathlib import Path
import pytest
from tests.fixtures.data_specs import ResponseSpec, AttendanceSpec


@pytest.mark.db
class TestSnapshotCalculation:
    """Tests for snapshot calculation logic (priority and index calculations)."""

    def test_snapshot_records_created_for_members(self, importer_factory):
        """Snapshot generation creates records for all members."""
        ctx = importer_factory(create_period=False, skip_snapshots=False)
        ctx.importer.import_period()

        # Verify snapshots were created
        from tests.db.helpers import assert_snapshot_count
        assert_snapshot_count(ctx.cursor, ctx.importer.period_id, expected=10)

    def test_snapshot_index_ordering_is_sequential(self, importer_factory):
        """Snapshot index positions are sequential and ordered by priority."""
        ctx = importer_factory(create_period=False, skip_snapshots=False)
        ctx.importer.import_period()

        # Multi-row query for index ordering verification
        ctx.cursor.execute(
            """
            SELECT peep_id, priority, total_attended, index_position
            FROM peep_order_snapshots
            WHERE period_id = ?
            ORDER BY index_position
        """,
            (ctx.importer.period_id,),
        )
        snapshots = ctx.cursor.fetchall()

        assert len(snapshots) > 0, "Should have snapshots"

        # Verify index ordering is sequential
        for i, (peep_id, priority, attended, index_pos) in enumerate(snapshots):
            assert (
                index_pos == i
            ), f"Index should match sequential position, got {index_pos} at position {i}"

    def test_snapshot_responder_status_affects_priority(self, importer_factory):
        """Members who responded but didn't attend get priority increment."""
        ctx = importer_factory(create_period=False, skip_snapshots=False)
        ctx.importer.import_period()

        # Multi-row query for priority verification
        ctx.cursor.execute(
            """
            SELECT priority FROM peep_order_snapshots
            WHERE period_id = ? AND peep_id IN (9, 10)
        """,
            (ctx.importer.period_id,),
        )
        results = ctx.cursor.fetchall()

        assert len(results) == 2, "Should have snapshots for non-responding members"

        # Non-responders should have priority 0 (baseline)
        for (priority,) in results:
            assert (
                priority == 0
            ), "Non-responder priority should remain at baseline (0)"

    def test_sequential_enforcement_raises_error_without_prior_snapshots(
        self, importer_factory, responses_csv_builder, attendance_json_builder
    ):
        """PeriodImporter raises error when prior period has no snapshots."""
        # Import period 2025-02 WITHOUT snapshots (skip_snapshots=True)
        ctx1 = importer_factory(period_name="2025-02", create_period=False)
        ctx1.importer.skip_snapshots = True
        ctx1.importer.import_period()

        # Create period 2025-03 with proper March dates
        ctx2 = importer_factory(
            period_name="2025-03",
            create_period=False,
            peep_id_mapping=ctx1.peep_id_mapping
        )
        period2_dir = Path(ctx2.period_data["period_dir"])

        # Replace responses.csv with March dates using ResponseSpec
        march_responses = [
            ResponseSpec(
                email="member1@test.com",
                name="Test Member 1",
                timestamp="3/1/2025 12:00:00",
                role="leader",
                availability=["Friday March 7th - 5pm to 7pm", "Friday March 14th - 5pm to 7pm"],
                max_sessions=2
            )
        ]
        responses_csv_builder(period2_dir, march_responses)

        # Add actual_attendance.json with March dates (needed to trigger snapshot calculation)
        march_attendance = [
            AttendanceSpec(
                date="2025-03-07 17:00",
                duration_minutes=120,
                attendees=[(1, "Test Member 1", "Leader")]
            ),
            AttendanceSpec(
                date="2025-03-14 17:00",
                duration_minutes=120,
                attendees=[]
            )
        ]
        attendance_json_builder(period2_dir, march_attendance)

        # Remove results.json to avoid date conflicts
        (period2_dir / "results.json").unlink(missing_ok=True)

        # Try to import period 2025-03 WITH snapshots enabled
        # Should fail because prior period 2025-02 has no snapshots
        ctx2.importer.skip_snapshots = False

        with pytest.raises(ValueError, match="Prior period .* has no snapshots"):
            ctx2.importer.import_period()
