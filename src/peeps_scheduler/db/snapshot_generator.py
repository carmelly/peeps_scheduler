#!/usr/bin/env python3
"""
Snapshot Generator: Reusable member state calculation logic

This module provides the core logic for calculating member state snapshots
based on event attendance (both actual and expected). It implements the
fair scheduling algorithm used throughout the peeps scheduler system.

IMPORTANT: Two Types of Snapshots
========================================

1. SCHEDULING SNAPSHOTS (Temporary)
   - Used during active period scheduling decisions
   - Treats both ACTUAL and EXPECTED attendance the same way
   - Both reset priority to 0 and move members to back of queue
   - Ensures fair scheduling rotation during period planning
   - NOT saved to database - used only for scheduling logic

2. PERMANENT SNAPSHOTS (Database)
   - Created when period is complete with final results
   - Uses ONLY actual attendance data
   - Saved to database as historical record
   - Used as starting point for next period's scheduling

This dual approach ensures:
- Fair scheduling decisions during active periods
- Accurate historical records based on actual attendance
- Consistent priority calculations across all periods

Usage:
    - Historical validation: Regenerate past snapshots with correct logic
    - Active scheduling: Generate current member order for scheduling decisions
    - Period completion: Create final snapshots for database storage
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MemberSnapshot:
    """Represents a member's state at a point in time."""

    peep_id: int
    email: str
    full_name: str
    display_name: str
    primary_role: str

    # State fields that change over time
    priority: int
    index_position: int
    total_attended: int
    active: bool

    # Internal tracking for period calculations
    num_events_this_period: int = field(default=0, init=False)
    responded_this_period: bool = field(default=False, init=False)
    original_priority: int = field(default=0, init=False)  # For tracking

    def __post_init__(self):
        self.original_priority = self.priority


@dataclass
class EventAttendance:
    """Represents attendance for a specific event."""

    event_id: int
    peep_id: int
    role: str  # 'leader' or 'follower'
    attendance_type: str  # 'actual', 'expected', 'cancelled', 'no_show'
    event_datetime: datetime


class SnapshotGenerator:
    """
    Generates member state snapshots using the same logic as apply_results.

    This ensures consistency between historical data validation and future
    production snapshot generation.
    """

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for snapshot operations."""
        logger = logging.getLogger("snapshot_generator")
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        # Don't add handlers - use root logger's handler to avoid duplicates
        return logger

    def generate_snapshot_from_attendance(
        self,
        starting_snapshot: list[MemberSnapshot],
        actual_attendance: list[EventAttendance],
        expected_attendance: list[EventAttendance],
        responded_peep_ids: set[int] | None = None,
    ) -> list[MemberSnapshot]:
        """
        Generate a new member state snapshot by applying attendance to a starting snapshot.

        SCHEDULING vs PERMANENT SNAPSHOTS:
        ==================================

        For SCHEDULING snapshots (during active period):
        - Pass both actual_attendance AND expected_attendance
        - Both types reset priority and reorder members
        - Use result for fair scheduling decisions
        - Do NOT save to database

        For PERMANENT snapshots (period completion):
        - Pass only actual_attendance (expected_attendance = [])
        - Only actual attendance affects final state
        - Save result to database as historical record

        FAIRNESS ALGORITHM:
        ==================
        1. Apply actual attendance (resets priority=0, moves to back)
        2. Apply expected attendance (same treatment for scheduling fairness)
        3. Increase priority for non-attendees who responded (+1 for fairness)
        4. Sort by priority (highest first) and reassign indices

        Args:
            starting_snapshot: Current member state to start from
            actual_attendance: Events that actually occurred with real attendance
            expected_attendance: Events scheduled but not yet completed
            responded_peep_ids: Set of peep IDs who responded this period

        Returns:
            New snapshot with updated member states
        """
        try:
            self.logger.info(f"Generating snapshot from {len(starting_snapshot)} members")
            self.logger.info(
                f"Applying {len(actual_attendance)} actual + {len(expected_attendance)} expected attendance records"
            )

            # Create working copy of snapshots
            snapshots = [
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
                for s in starting_snapshot
            ]

            # Create lookup for faster processing
            peep_lookup = {s.peep_id: s for s in snapshots}

            # Track who responded this period
            if responded_peep_ids:
                for peep_id in responded_peep_ids:
                    if peep_id in peep_lookup:
                        peep_lookup[peep_id].responded_this_period = True

            # Apply actual attendance (events that have completed)
            self._apply_attendance_records(peep_lookup, actual_attendance, "actual")

            # Apply expected attendance (events scheduled but not completed)
            self._apply_attendance_records(peep_lookup, expected_attendance, "expected")

            # Finalize the period (same logic as EventSequence.finalize())
            self._finalize_period(snapshots)

            self.logger.info(f"Generated snapshot with {len(snapshots)} members")
            return snapshots

        except Exception as e:
            self.logger.error(f"Error generating snapshot: {e}")
            raise

    def _apply_attendance_records(
        self,
        peep_lookup: dict[int, MemberSnapshot],
        attendance_records: list[EventAttendance],
        attendance_type: str,
    ) -> None:
        """
        Apply attendance records to update member states.

        CRITICAL: Both 'actual' and 'expected' attendance are treated identically
        for scheduling fairness. This ensures that people scheduled for events
        (even if not yet completed) are moved to the back of the queue, allowing
        fair rotation opportunities for others.

        The distinction between actual vs expected only matters for:
        - Database persistence (only actual attendance saved permanently)
        - Historical record keeping (what actually happened vs what was planned)
        """
        attended_peeps = []

        for attendance in attendance_records:
            peep = peep_lookup.get(attendance.peep_id)
            if not peep:
                self.logger.warning(f"Peep {attendance.peep_id} not found in snapshot")
                continue

            if attendance.attendance_type in ["actual", "expected"]:
                # This person attended (or is expected to attend)
                peep.num_events_this_period += 1

                # Both actual and expected attendance reset priority and move to back for scheduling
                peep.priority = 0  # Reset priority after attendance
                attended_peeps.append(peep)

        # Move attendees to end of list (back of the line) for both actual and expected
        # This ensures fair scheduling order for in-progress periods
        if attended_peeps:
            # Create new list with attendees moved to the end
            all_snapshots = list(peep_lookup.values())
            non_attendees = [p for p in all_snapshots if p not in attended_peeps]
            reordered_snapshots = non_attendees + attended_peeps

            # Update the lookup to maintain consistency
            for i, peep in enumerate(reordered_snapshots):
                peep.index_position = i

    def _finalize_period(self, snapshots: list[MemberSnapshot]) -> None:
        """
        Finalize period by updating totals, priorities, and reordering.

        This replicates the exact logic from EventSequence.finalize()
        """
        for peep in snapshots:
            if peep.num_events_this_period == 0:
                # Increase priority if peep responded but was not scheduled
                if peep.responded_this_period:
                    peep.priority += 1
            else:
                # Peep was scheduled to at least one event
                peep.total_attended += peep.num_events_this_period

            # Reset period tracking
            peep.num_events_this_period = 0
            peep.responded_this_period = False

        # Sort by priority descending (same as original)
        snapshots.sort(key=lambda p: p.priority, reverse=True)

        # Reassign index based on sorted order
        for i, peep in enumerate(snapshots):
            peep.index_position = i

    def compare_snapshots(
        self, snapshot1: list[MemberSnapshot], snapshot2: list[MemberSnapshot], tolerance: int = 0
    ) -> tuple[bool, list[str]]:
        """
        Compare two snapshots for equality.

        Args:
            snapshot1: First snapshot to compare
            snapshot2: Second snapshot to compare
            tolerance: Allowed difference in priority/total_attended values

        Returns:
            (is_match, list_of_differences)
        """
        differences = []

        if len(snapshot1) != len(snapshot2):
            differences.append(f"Snapshot length mismatch: {len(snapshot1)} vs {len(snapshot2)}")
            return False, differences

        # Sort by peep_id for consistent comparison
        s1_sorted = sorted(snapshot1, key=lambda p: p.peep_id)
        s2_sorted = sorted(snapshot2, key=lambda p: p.peep_id)

        for p1, p2 in zip(s1_sorted, s2_sorted, strict=True):
            if p1.peep_id != p2.peep_id:
                differences.append(f"Peep ID mismatch: {p1.peep_id} vs {p2.peep_id}")
                continue

            if abs(p1.priority - p2.priority) > tolerance:
                differences.append(f"Peep {p1.peep_id}: Priority {p1.priority} vs {p2.priority}")

            if abs(p1.index_position - p2.index_position) > tolerance:
                differences.append(
                    f"Peep {p1.peep_id}: Index {p1.index_position} vs {p2.index_position}"
                )

            if abs(p1.total_attended - p2.total_attended) > tolerance:
                differences.append(
                    f"Peep {p1.peep_id}: Total attended {p1.total_attended} vs {p2.total_attended}"
                )

            if p1.active != p2.active:
                differences.append(f"Peep {p1.peep_id}: Active {p1.active} vs {p2.active}")

        return len(differences) == 0, differences

    def generate_snapshot_for_period(
        self, cursor, period_id: int, snapshot_type: str = "permanent"
    ) -> list[MemberSnapshot]:
        """
        Generate a complete snapshot for a period by automatically gathering all required data.

        This is the main entry point - it handles everything:
        1. Loads the starting snapshot from the previous period
        2. Gathers actual attendance data (completed events)
        3. Gathers expected attendance data (scheduled but incomplete events)
        4. Identifies who responded this period
        5. Applies the fairness algorithm
        6. Returns the calculated snapshot

        Args:
            cursor: Database cursor
            period_id: ID of the period to generate snapshot for
            snapshot_type: 'scheduling' (includes expected) or 'permanent' (actual only)

        Returns:
            Complete snapshot for the period
        """
        # Get starting snapshot from previous period
        starting_snapshot = self._load_starting_snapshot(cursor, period_id)

        # Load actual attendance (completed events)
        actual_attendance = self._load_actual_attendance(cursor, period_id)

        # Load expected attendance if this is for scheduling
        expected_attendance = []
        if snapshot_type == "scheduling":
            expected_attendance = self._load_expected_attendance(cursor, period_id)

        # Get who responded this period
        responded_peep_ids = self._load_period_responses(cursor, period_id)

        # Generate the snapshot using the core algorithm
        return self.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=actual_attendance,
            expected_attendance=expected_attendance,
            responded_peep_ids=responded_peep_ids,
        )

    def _load_starting_snapshot(self, cursor, period_id: int) -> list[MemberSnapshot]:
        """Load the starting snapshot from the previous period."""
        # Get the previous period
        cursor.execute(
            """
            SELECT id FROM schedule_periods
            WHERE period_name < (SELECT period_name FROM schedule_periods WHERE id = ?)
            ORDER BY period_name DESC
            LIMIT 1
        """,
            (period_id,),
        )

        result = cursor.fetchone()
        if result:
            prev_period_id = result[0]
            return self.snapshot_from_database(cursor, prev_period_id)
        else:
            # No previous period - this must be the baseline
            return self.snapshot_from_database(cursor, period_id)

    def _load_actual_attendance(self, cursor, period_id: int) -> list[EventAttendance]:
        """Load actual attendance records for completed events in this period."""
        cursor.execute(
            """
            SELECT ea.event_id, ea.peep_id, ea.actual_role, e.event_datetime
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
            AND ea.attendance_status = 'attended'
            ORDER BY e.event_datetime, ea.event_id, ea.peep_id
        """,
            (period_id,),
        )

        attendance_records = []
        for event_id, peep_id, actual_role, event_datetime in cursor.fetchall():
            attendance_records.append(
                EventAttendance(
                    event_id=event_id,
                    peep_id=peep_id,
                    role=actual_role or "leader",  # Fallback if null
                    attendance_type="actual",
                    event_datetime=datetime.fromisoformat(event_datetime),
                )
            )

        return attendance_records

    def _load_expected_attendance(self, cursor, period_id: int) -> list[EventAttendance]:
        """Load expected attendance from scheduled assignments for incomplete events."""
        cursor.execute(
            """
            SELECT ea.event_id, ea.peep_id, ea.assigned_role, e.event_datetime
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            LEFT JOIN event_attendance att ON ea.event_id = att.event_id AND ea.peep_id = att.peep_id
            WHERE e.period_id = ?
            AND ea.assignment_type = 'attendee'  -- Only actual attendees, not alternates
            AND att.id IS NULL  -- No attendance record yet (event hasn't happened)
            ORDER BY e.event_datetime, ea.event_id, ea.peep_id
        """,
            (period_id,),
        )

        attendance_records = []
        for event_id, peep_id, assigned_role, event_datetime in cursor.fetchall():
            attendance_records.append(
                EventAttendance(
                    event_id=event_id,
                    peep_id=peep_id,
                    role=assigned_role,
                    attendance_type="expected",
                    event_datetime=datetime.fromisoformat(event_datetime),
                )
            )

        return attendance_records

    def _load_period_responses(self, cursor, period_id: int) -> set[int]:
        """Load set of peep IDs who responded during this period."""
        cursor.execute(
            """
            SELECT DISTINCT r.peep_id
            FROM responses r
            WHERE r.period_id = ?
        """,
            (period_id,),
        )

        return {row[0] for row in cursor.fetchall()}

    def snapshot_from_database(self, cursor, period_id: int) -> list[MemberSnapshot]:
        """
        Create a snapshot from database peep_order_snapshots for a specific period.

        This is used to get the starting snapshot for calculations.
        """
        cursor.execute(
            """
            SELECT pos.peep_id, p.email, p.full_name, p.display_name, p.primary_role,
                   pos.priority, pos.index_position, pos.total_attended, pos.active
            FROM peep_order_snapshots pos
            JOIN peeps p ON pos.peep_id = p.id
            WHERE pos.period_id = ?
            ORDER BY pos.index_position
        """,
            (period_id,),
        )

        snapshots = []
        for row in cursor.fetchall():
            (
                peep_id,
                email,
                full_name,
                display_name,
                primary_role,
                priority,
                index_position,
                total_attended,
                active,
            ) = row

            snapshot = MemberSnapshot(
                peep_id=peep_id,
                email=email,
                full_name=full_name,
                display_name=display_name,
                primary_role=primary_role,
                priority=priority,
                index_position=index_position,
                total_attended=total_attended,
                active=active,
            )
            snapshots.append(snapshot)

        return snapshots
