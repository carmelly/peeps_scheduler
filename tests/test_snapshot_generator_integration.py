"""Integration tests for snapshot_generator module.

Tests the core snapshot generation logic with realistic data and edge cases:
- Snapshot comparison with various priority/index values
- Period change detection and fairness algorithm
- Validation of priority calculations and reordering
- Edge cases: out-of-range values, missing members, unchanged data
"""

import pytest
from datetime import datetime
from peeps_scheduler.db.snapshot_generator import (
    SnapshotGenerator,
    MemberSnapshot,
    EventAttendance,
)


@pytest.fixture
def snapshot_generator():
    """Create a SnapshotGenerator instance for testing."""
    return SnapshotGenerator(verbose=False)


@pytest.fixture
def realistic_starting_snapshot():
    """Create a realistic starting snapshot with various priority/index values."""
    return [
        MemberSnapshot(
            peep_id=1,
            email="alice@test.com",
            full_name="Alice Leader",
            display_name="Alice",
            primary_role="leader",
            priority=5,
            index_position=0,
            total_attended=3,
            active=True,
        ),
        MemberSnapshot(
            peep_id=2,
            email="bob@test.com",
            full_name="Bob Follower",
            display_name="Bob",
            primary_role="follower",
            priority=3,
            index_position=1,
            total_attended=2,
            active=True,
        ),
        MemberSnapshot(
            peep_id=3,
            email="carol@test.com",
            full_name="Carol Leader",
            display_name="Carol",
            primary_role="leader",
            priority=5,
            index_position=2,
            total_attended=1,
            active=True,
        ),
        MemberSnapshot(
            peep_id=4,
            email="dave@test.com",
            full_name="Dave Follower",
            display_name="Dave",
            primary_role="follower",
            priority=0,
            index_position=3,
            total_attended=0,
            active=True,
        ),
    ]


@pytest.mark.integration
def test_snapshot_comparison_identical_snapshots(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that identical snapshots are correctly identified as matching."""
    snapshot2 = [
        MemberSnapshot(
            peep_id=s.peep_id,
            email=s.email,
            full_name=s.full_name,
            display_name=s.display_name,
            primary_role=s.primary_role,
            priority=s.priority,
            index_position=s.index_position,
            total_attended=s.total_attended,
            active=s.active,
        )
        for s in realistic_starting_snapshot
    ]

    is_match, differences = snapshot_generator.compare_snapshots(
        realistic_starting_snapshot, snapshot2
    )

    assert is_match is True
    assert len(differences) == 0


@pytest.mark.integration
def test_snapshot_comparison_priority_differences(
    snapshot_generator, realistic_starting_snapshot
):
    """Test detection of priority differences between snapshots."""
    snapshot2 = [
        MemberSnapshot(
            peep_id=s.peep_id,
            email=s.email,
            full_name=s.full_name,
            display_name=s.display_name,
            primary_role=s.primary_role,
            priority=s.priority + 1 if s.peep_id == 1 else s.priority,
            index_position=s.index_position,
            total_attended=s.total_attended,
            active=s.active,
        )
        for s in realistic_starting_snapshot
    ]

    is_match, differences = snapshot_generator.compare_snapshots(
        realistic_starting_snapshot, snapshot2
    )

    assert is_match is False
    assert any("Priority" in d and "1" in d for d in differences)


@pytest.mark.integration
def test_snapshot_comparison_with_tolerance(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that tolerance parameter allows for small differences."""
    snapshot2 = [
        MemberSnapshot(
            peep_id=s.peep_id,
            email=s.email,
            full_name=s.full_name,
            display_name=s.display_name,
            primary_role=s.primary_role,
            priority=s.priority + 2,
            index_position=s.index_position,
            total_attended=s.total_attended,
            active=s.active,
        )
        for s in realistic_starting_snapshot
    ]

    # Without tolerance, should fail
    is_match, differences = snapshot_generator.compare_snapshots(
        realistic_starting_snapshot, snapshot2, tolerance=0
    )
    assert is_match is False

    # With tolerance=2, should pass
    is_match, differences = snapshot_generator.compare_snapshots(
        realistic_starting_snapshot, snapshot2, tolerance=2
    )
    assert is_match is True
    assert len(differences) == 0


@pytest.mark.integration
def test_snapshot_comparison_length_mismatch(
    snapshot_generator, realistic_starting_snapshot
):
    """Test detection of snapshot length mismatches."""
    snapshot2 = realistic_starting_snapshot[:2]  # Only first 2 members

    is_match, differences = snapshot_generator.compare_snapshots(
        realistic_starting_snapshot, snapshot2
    )

    assert is_match is False
    assert any("length mismatch" in d.lower() for d in differences)


@pytest.mark.integration
def test_apply_actual_attendance_resets_priority_and_moves_to_back(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that actual attendance resets priority to 0 and moves members to back."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=1,
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        )
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=[],
        responded_peep_ids=None,
    )

    # After finalization, Alice (peep_id=1) should be at back with updated total
    alice = next((s for s in result if s.peep_id == 1), None)
    assert alice is not None
    assert alice.total_attended == 4  # Was 3, attended 1 event
    assert alice.priority == 0  # Reset due to attendance


@pytest.mark.integration
def test_expected_attendance_treated_same_as_actual_for_fairness(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that expected attendance resets priority like actual attendance."""
    expected_attendance = [
        EventAttendance(
            event_id=102,
            peep_id=2,
            role="follower",
            attendance_type="expected",
            event_datetime=datetime(2025, 1, 16, 19, 0),
        )
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=[],
        expected_attendance=expected_attendance,
        responded_peep_ids=None,
    )

    # Bob (peep_id=2) should have priority reset to 0
    bob = next((s for s in result if s.peep_id == 2), None)
    assert bob is not None
    assert bob.priority == 0


@pytest.mark.integration
def test_priority_increase_for_non_attendees_who_responded(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that non-attendees who responded get +1 priority."""
    responded_peep_ids = {4}  # Dave responded but not scheduled

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=responded_peep_ids,
    )

    dave = next((s for s in result if s.peep_id == 4), None)
    assert dave is not None
    assert dave.priority == 1  # Was 0, increased by 1 for responding


@pytest.mark.integration
def test_finalize_period_reorders_by_priority_descending(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that finalization reorders members by priority (highest first)."""
    # After some events, priorities are mixed: [5, 3, 5, 0]
    # Should be reordered: Alice(5), Carol(5), Bob(3), Dave(0)
    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=None,
    )

    # Check that result is sorted by priority descending
    priorities = [s.priority for s in result]
    assert priorities == sorted(priorities, reverse=True)

    # Check that indices are reassigned sequentially
    indices = [s.index_position for s in result]
    assert indices == list(range(len(result)))


@pytest.mark.integration
def test_snapshot_with_multiple_events_same_member(
    snapshot_generator, realistic_starting_snapshot
):
    """Test snapshot generation when member attends multiple events."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=1,
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        ),
        EventAttendance(
            event_id=102,
            peep_id=1,
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 22, 19, 0),
        ),
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=[],
        responded_peep_ids=None,
    )

    alice = next((s for s in result if s.peep_id == 1), None)
    assert alice is not None
    assert alice.total_attended == 5  # Was 3, attended 2 events


@pytest.mark.integration
def test_snapshot_with_nonexistent_member_in_attendance(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that attendance for non-existent members is gracefully handled."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=999,  # Non-existent peep
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        )
    ]

    # Should not raise, just log warning and continue
    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=[],
        responded_peep_ids=None,
    )

    assert len(result) == 4  # Original count unchanged
    assert all(s.peep_id != 999 for s in result)


@pytest.mark.integration
def test_snapshot_preserves_member_identity_fields(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that member identity fields are preserved through snapshot generation."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=1,
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        )
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=[],
        responded_peep_ids=None,
    )

    alice_original = realistic_starting_snapshot[0]
    alice_result = next((s for s in result if s.peep_id == 1), None)

    assert alice_result.peep_id == alice_original.peep_id
    assert alice_result.email == alice_original.email
    assert alice_result.full_name == alice_original.full_name
    assert alice_result.display_name == alice_original.display_name
    assert alice_result.primary_role == alice_original.primary_role


@pytest.mark.integration
def test_snapshot_with_mixed_actual_and_expected_attendance(
    snapshot_generator, realistic_starting_snapshot
):
    """Test snapshot generation with both actual and expected attendance records."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=1,
            role="leader",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        )
    ]
    expected_attendance = [
        EventAttendance(
            event_id=102,
            peep_id=2,
            role="follower",
            attendance_type="expected",
            event_datetime=datetime(2025, 1, 22, 19, 0),
        )
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=expected_attendance,
        responded_peep_ids=None,
    )

    alice = next((s for s in result if s.peep_id == 1), None)
    bob = next((s for s in result if s.peep_id == 2), None)

    # Both should have priority reset to 0
    assert alice.priority == 0
    assert bob.priority == 0
    # Both should have increased total_attended (expected counts same as actual)
    assert alice.total_attended == 4
    assert bob.total_attended == 3  # Was 2, expected attendance counts


@pytest.mark.integration
def test_snapshot_empty_attendance_records(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that empty attendance records still reorder by priority (finalize_period called)."""
    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=None,
    )

    # After finalize_period, members are sorted by priority descending
    # Starting priorities: [5, 3, 5, 0]
    # After sort: Alice(5), Carol(5), Bob(3), Dave(0)
    assert result[0].peep_id in [1, 3]  # Alice or Carol (both priority 5)
    assert result[-1].peep_id == 4  # Dave (priority 0)


@pytest.mark.integration
def test_snapshot_with_inactive_member(snapshot_generator):
    """Test snapshot generation with inactive members."""
    snapshots = [
        MemberSnapshot(
            peep_id=1,
            email="active@test.com",
            full_name="Active Member",
            display_name="Active",
            primary_role="leader",
            priority=5,
            index_position=0,
            total_attended=2,
            active=True,
        ),
        MemberSnapshot(
            peep_id=2,
            email="inactive@test.com",
            full_name="Inactive Member",
            display_name="Inactive",
            primary_role="follower",
            priority=3,
            index_position=1,
            total_attended=0,
            active=False,
        ),
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=snapshots,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=None,
    )

    inactive = next((s for s in result if s.peep_id == 2), None)
    assert inactive is not None
    assert inactive.active is False


@pytest.mark.integration
def test_snapshot_all_members_attended_in_period(
    snapshot_generator, realistic_starting_snapshot
):
    """Test snapshot when all members attend events in the period."""
    actual_attendance = [
        EventAttendance(
            event_id=101,
            peep_id=i,
            role="leader" if i % 2 == 1 else "follower",
            attendance_type="actual",
            event_datetime=datetime(2025, 1, 15, 19, 0),
        )
        for i in range(1, 5)
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=actual_attendance,
        expected_attendance=[],
        responded_peep_ids=None,
    )

    # All should have priority 0 after attendance
    assert all(s.priority == 0 for s in result)


@pytest.mark.integration
def test_snapshot_out_of_range_priority_values(snapshot_generator):
    """Test snapshot generation with extreme priority values."""
    snapshots = [
        MemberSnapshot(
            peep_id=1,
            email="low@test.com",
            full_name="Low Priority",
            display_name="Low",
            primary_role="leader",
            priority=-5,
            index_position=0,
            total_attended=0,
            active=True,
        ),
        MemberSnapshot(
            peep_id=2,
            email="high@test.com",
            full_name="High Priority",
            display_name="High",
            primary_role="follower",
            priority=100,
            index_position=1,
            total_attended=10,
            active=True,
        ),
    ]

    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=snapshots,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=None,
    )

    # High priority should come first
    assert result[0].peep_id == 2
    assert result[1].peep_id == 1


@pytest.mark.integration
def test_snapshot_preserves_active_status(
    snapshot_generator, realistic_starting_snapshot
):
    """Test that active status is preserved through snapshot generation."""
    result = snapshot_generator.generate_snapshot_from_attendance(
        starting_snapshot=realistic_starting_snapshot,
        actual_attendance=[],
        expected_attendance=[],
        responded_peep_ids=None,
    )

    for original, generated in zip(realistic_starting_snapshot, result):
        # Find the member in result (may be reordered)
        member = next((s for s in result if s.peep_id == original.peep_id), None)
        assert member is not None
        assert member.active == original.active
