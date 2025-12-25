"""Tests for snapshot generation and comparison (SnapshotGenerator unit tests).

Unit tests for SnapshotGenerator class behavior. These tests do not require
database fixtures or prepared importer - they directly instantiate the generator
with test data to verify core logic.

Tests cover:
- Snapshot generation from attendance data
- Snapshot comparison and difference detection
- Priority and index calculations
- Event attendance handling
"""

from datetime import datetime
import pytest
from db.snapshot_generator import EventAttendance, MemberSnapshot, SnapshotGenerator


@pytest.mark.db
class TestSnapshotGeneration:
    """Tests for snapshot generation core functionality."""

    def test_apply_attendance_with_unknown_peep_warning(self):
        """SnapshotGenerator handles unknown peep in attendance gracefully."""
        generator = SnapshotGenerator(verbose=False)

        # Create snapshot with peep 1 only
        peep_lookup = {
            1: MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=1,
                index_position=0,
                total_attended=0,
                active=True,
            )
        }

        # Create attendance for unknown peep 999
        attendance_records = [
            EventAttendance(
                event_id=1,
                peep_id=999,
                role="leader",
                attendance_type="actual",
                event_datetime=datetime(2025, 2, 7, 17, 0),
            )
        ]

        # Should handle gracefully without crashing
        generator._apply_attendance_records(peep_lookup, attendance_records, "actual")

        # Verify peep 1 was unchanged
        assert (
            peep_lookup[1].num_events_this_period == 0
        ), "Unknown attendance should not affect other peeps"

    def test_compare_snapshots_identical(self):
        """SnapshotGenerator detects identical snapshots."""
        generator = SnapshotGenerator()

        snapshot1 = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=2,
                index_position=0,
                total_attended=5,
                active=True,
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=2,
                index_position=0,
                total_attended=5,
                active=True,
            )
        ]

        matches, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert matches is True, "Identical snapshots should match"
        assert len(differences) == 0, "Identical snapshots should have no differences"

    def test_compare_snapshots_with_differences(self):
        """SnapshotGenerator detects differences in snapshots."""
        generator = SnapshotGenerator()

        snapshot1 = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=2,
                index_position=0,
                total_attended=5,
                active=True,
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=5,
                index_position=0,
                total_attended=8,
                active=True,
            )
        ]

        matches, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert matches is False, "Different snapshots should not match"
        assert len(differences) > 0, "Different snapshots should have differences"
        assert any(
            "priority" in diff.lower() or "total" in diff.lower()
            for diff in differences
        ), "Should detect differences in priority or total_attended"

    def test_generate_snapshot_no_response_no_priority_change(self):
        """SnapshotGenerator doesn't increment priority for non-responders."""
        generator = SnapshotGenerator()

        starting_snapshot = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=5,
                index_position=0,
                total_attended=10,
                active=True,
            )
        ]

        # No attendance, member didn't respond
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=[],
            expected_attendance=[],
            responded_peep_ids=set(),
        )

        member1 = result[0]
        assert member1.priority == 5, "Non-responder priority should stay the same"
        assert member1.total_attended == 10, "Non-responder total_attended unchanged"

    def test_generate_snapshot_with_actual_attendance_only(self):
        """SnapshotGenerator updates snapshot with actual attendance."""
        generator = SnapshotGenerator()

        # Create starting snapshot
        starting_snapshot = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=2,
                index_position=0,
                total_attended=5,
                active=True,
            ),
            MemberSnapshot(
                peep_id=2,
                email="member2@test.com",
                full_name="Member 2",
                display_name="M2",
                primary_role="follower",
                priority=3,
                index_position=1,
                total_attended=3,
                active=True,
            ),
            MemberSnapshot(
                peep_id=3,
                email="member3@test.com",
                full_name="Member 3",
                display_name="M3",
                primary_role="leader",
                priority=1,
                index_position=2,
                total_attended=4,
                active=True,
            ),
        ]

        # Member 1 attended 2 events
        actual_attendance = [
            EventAttendance(
                event_id=1,
                peep_id=1,
                role="leader",
                attendance_type="actual",
                event_datetime=datetime(2025, 2, 7),
            ),
            EventAttendance(
                event_id=2,
                peep_id=1,
                role="leader",
                attendance_type="actual",
                event_datetime=datetime(2025, 2, 14),
            ),
        ]

        # Members 2 and 3 responded but didn't attend
        responded_peep_ids = {1, 2, 3}

        # Generate snapshot
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=actual_attendance,
            expected_attendance=[],
            responded_peep_ids=responded_peep_ids,
        )

        assert len(result) == 3, "Should have 3 members in snapshot"

        # Find each member in result
        member1 = next(m for m in result if m.peep_id == 1)
        member2 = next(m for m in result if m.peep_id == 2)
        member3 = next(m for m in result if m.peep_id == 3)

        # Member 1 attended - priority reset to 0
        assert member1.priority == 0, "Attendee should have priority 0"
        assert member1.total_attended == 7, "total_attended should be 5 + 2"

        # Members 2 and 3 responded but didn't attend - priority incremented
        assert member2.priority == 4, "Non-attendee who responded should increment priority"
        assert member3.priority == 2, "Non-attendee who responded should increment priority"

    def test_generate_snapshot_with_expected_attendance(self):
        """SnapshotGenerator handles expected (scheduled) attendance."""
        generator = SnapshotGenerator()

        # Create starting snapshot
        starting_snapshot = [
            MemberSnapshot(
                peep_id=1,
                email="member1@test.com",
                full_name="Member 1",
                display_name="M1",
                primary_role="leader",
                priority=2,
                index_position=0,
                total_attended=5,
                active=True,
            ),
            MemberSnapshot(
                peep_id=2,
                email="member2@test.com",
                full_name="Member 2",
                display_name="M2",
                primary_role="follower",
                priority=1,
                index_position=1,
                total_attended=3,
                active=True,
            ),
        ]

        # Member 1 scheduled for future event
        expected_attendance = [
            EventAttendance(
                event_id=1,
                peep_id=1,
                role="leader",
                attendance_type="expected",
                event_datetime=datetime(2025, 2, 7),
            )
        ]

        responded_peep_ids = {1, 2}

        # Generate snapshot
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=[],
            expected_attendance=expected_attendance,
            responded_peep_ids=responded_peep_ids,
        )

        member1 = next(m for m in result if m.peep_id == 1)
        member2 = next(m for m in result if m.peep_id == 2)

        # Member 1 scheduled - priority reset to 0
        assert member1.priority == 0, "Expected attendee should have priority 0"
        assert member1.total_attended == 6, "total_attended should increment for expected"

        # Member 2 responded but not scheduled - priority increments
        assert member2.priority == 2, "Non-scheduled responder should increment priority"

    @pytest.mark.parametrize(
        "scenario,snapshot1_data,snapshot2_data,expected_keyword",
        [
            pytest.param(
                "active_status",
                [{"peep_id": 1, "active": True}],
                [{"peep_id": 1, "active": False}],
                "active",
                id="active_status",
            ),
            pytest.param(
                "index_position",
                [{"peep_id": 1, "index_position": 0}],
                [{"peep_id": 1, "index_position": 5}],
                "index",
                id="index_position",
            ),
            pytest.param(
                "length_mismatch",
                [{"peep_id": 1}, {"peep_id": 2}],
                [{"peep_id": 1}],
                "length mismatch",
                id="length_mismatch",
            ),
            pytest.param(
                "peep_id_mismatch",
                [{"peep_id": 1}, {"peep_id": 2}],
                [{"peep_id": 1}, {"peep_id": 3}],
                "peep id mismatch",
                id="peep_id_mismatch",
            ),
        ],
    )
    def test_snapshot_comparison_detects_differences(
        self, scenario, snapshot1_data, snapshot2_data, expected_keyword
    ):
        """SnapshotGenerator detects various types of differences in snapshots."""
        generator = SnapshotGenerator(verbose=False)

        # Helper to create a snapshot with default values and optional overrides
        def create_snapshot(data_list):
            snapshots = []
            for idx, overrides in enumerate(data_list):
                defaults = {
                    "peep_id": idx + 1,
                    "email": f"member{idx + 1}@test.com",
                    "full_name": f"Member {idx + 1}",
                    "display_name": f"M{idx + 1}",
                    "primary_role": "leader" if idx == 0 else "follower",
                    "priority": 1,
                    "index_position": idx,
                    "total_attended": 0,
                    "active": True,
                }
                defaults.update(overrides)
                snapshots.append(MemberSnapshot(**defaults))
            return snapshots

        snapshot1 = create_snapshot(snapshot1_data)
        snapshot2 = create_snapshot(snapshot2_data)

        # Compare snapshots
        is_same, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert not is_same, f"Snapshots with {scenario} should not match"
        assert any(
            expected_keyword in diff.lower() for diff in differences
        ), f"Should mention {scenario} difference"
