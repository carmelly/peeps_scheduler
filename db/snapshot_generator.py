#!/usr/bin/env python3
"""
Snapshot Generator: Reusable member state calculation logic

This module provides the core logic for calculating member state snapshots
based on event attendance (both actual and expected). It extracts and reuses
the same logic from the existing apply_results workflow to ensure consistency
between historical data generation and future production use.

Usage:
    - Validation: Generate snapshots to validate transformation accuracy
    - Production: Generate current member order for scheduling
    - Archival: Create snapshots at period boundaries
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field


@dataclass
class MemberSnapshot:
    """Represents a member's state at a point in time."""
    peep_id: int
    csv_id: str
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
        logger = logging.getLogger('snapshot_generator')
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def generate_snapshot_from_attendance(
        self,
        starting_snapshot: List[MemberSnapshot],
        actual_attendance: List[EventAttendance],
        expected_attendance: List[EventAttendance],
        responded_peep_ids: Optional[Set[int]] = None
    ) -> List[MemberSnapshot]:
        """
        Generate a new member state snapshot by applying attendance to a starting snapshot.

        This replicates the exact logic from apply_results:
        1. Apply actual attendance (updates total_attended, resets priority, moves to back)
        2. Apply expected attendance (for scheduling incomplete periods)
        3. Increase priority for non-attendees who responded
        4. Sort by priority and reassign indices

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
            self.logger.info(f"Applying {len(actual_attendance)} actual + {len(expected_attendance)} expected attendance records")

            # Create working copy of snapshots
            snapshots = [
                MemberSnapshot(
                    peep_id=s.peep_id,
                    csv_id=s.csv_id,
                    email=s.email,
                    full_name=s.full_name,
                    display_name=s.display_name,
                    primary_role=s.primary_role,
                    priority=s.priority,
                    index_position=s.index_position,
                    total_attended=s.total_attended,
                    active=s.active
                ) for s in starting_snapshot
            ]

            # Create lookup for faster processing
            peep_lookup = {s.peep_id: s for s in snapshots}

            # Track who responded this period
            if responded_peep_ids:
                for peep_id in responded_peep_ids:
                    if peep_id in peep_lookup:
                        peep_lookup[peep_id].responded_this_period = True

            # Apply actual attendance (events that have completed)
            self._apply_attendance_records(peep_lookup, actual_attendance, 'actual')

            # Apply expected attendance (events scheduled but not completed)
            self._apply_attendance_records(peep_lookup, expected_attendance, 'expected')

            # Finalize the period (same logic as EventSequence.finalize())
            self._finalize_period(snapshots)

            self.logger.info(f"Generated snapshot with {len(snapshots)} members")
            return snapshots

        except Exception as e:
            self.logger.error(f"Error generating snapshot: {e}")
            raise

    def _apply_attendance_records(
        self,
        peep_lookup: Dict[int, MemberSnapshot],
        attendance_records: List[EventAttendance],
        attendance_type: str
    ) -> None:
        """Apply attendance records to update member states."""
        attended_peeps = []

        for attendance in attendance_records:
            peep = peep_lookup.get(attendance.peep_id)
            if not peep:
                self.logger.warning(f"Peep {attendance.peep_id} not found in snapshot")
                continue

            if attendance.attendance_type in ['actual', 'expected']:
                # This person attended (or is expected to attend)
                peep.num_events_this_period += 1

                if attendance_type == 'actual':
                    # Only actual attendance affects priority reset and list reordering
                    peep.priority = 0  # Reset priority after successful attendance
                    attended_peeps.append(peep)

        # For actual attendance, move attendees to end of list (back of the line)
        # This replicates the logic from update_event_attendees
        if attendance_type == 'actual' and attended_peeps:
            # Create new list with attendees moved to the end
            all_snapshots = list(peep_lookup.values())
            non_attendees = [p for p in all_snapshots if p not in attended_peeps]
            reordered_snapshots = non_attendees + attended_peeps

            # Update the lookup to maintain consistency
            for i, peep in enumerate(reordered_snapshots):
                peep.index_position = i

    def _finalize_period(self, snapshots: List[MemberSnapshot]) -> None:
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
        self,
        snapshot1: List[MemberSnapshot],
        snapshot2: List[MemberSnapshot],
        tolerance: int = 0
    ) -> Tuple[bool, List[str]]:
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

        for p1, p2 in zip(s1_sorted, s2_sorted):
            if p1.peep_id != p2.peep_id:
                differences.append(f"Peep ID mismatch: {p1.peep_id} vs {p2.peep_id}")
                continue

            if abs(p1.priority - p2.priority) > tolerance:
                differences.append(f"Peep {p1.peep_id}: Priority {p1.priority} vs {p2.priority}")

            if abs(p1.index_position - p2.index_position) > tolerance:
                differences.append(f"Peep {p1.peep_id}: Index {p1.index_position} vs {p2.index_position}")

            if abs(p1.total_attended - p2.total_attended) > tolerance:
                differences.append(f"Peep {p1.peep_id}: Total attended {p1.total_attended} vs {p2.total_attended}")

            if p1.active != p2.active:
                differences.append(f"Peep {p1.peep_id}: Active {p1.active} vs {p2.active}")

        return len(differences) == 0, differences

    def snapshot_from_raw_members(self, cursor, period_name: str) -> List[MemberSnapshot]:
        """
        Create a snapshot from raw_members data for a specific period.

        This is used to get the "target" snapshot we're trying to validate against.
        """
        cursor.execute("""
            SELECT rm.csv_id, rm.Name, rm."Display Name", rm.Role, rm."Email Address",
                   rm."Index", rm.Priority, rm."Total Attended", rm.Active
            FROM raw_members rm
            WHERE rm.period_name = ?
            ORDER BY CAST(rm.csv_id AS INTEGER)
        """, (period_name,))

        snapshots = []
        for row in cursor.fetchall():
            csv_id, name, display_name, role, email, index_pos, priority, total_attended, active = row

            # Convert active text to boolean
            active_bool = active.lower() in ['true', 'yes', '1'] if active else True

            snapshot = MemberSnapshot(
                peep_id=0,  # Will be filled in by caller
                csv_id=csv_id,
                email=email,
                full_name=name,
                display_name=display_name,
                primary_role=role.lower(),
                priority=priority or 0,
                index_position=index_pos or 0,
                total_attended=total_attended or 0,
                active=active_bool
            )
            snapshots.append(snapshot)

        return snapshots

    def snapshot_from_database(self, cursor, period_id: int) -> List[MemberSnapshot]:
        """
        Create a snapshot from database peep_order_snapshots for a specific period.

        This is used to get the starting snapshot for calculations.
        """
        cursor.execute("""
            SELECT pos.peep_id, p.email, p.full_name, p.display_name, p.primary_role,
                   pos.priority, pos.index_position, pos.total_attended, pos.active,
                   rm.csv_id
            FROM peep_order_snapshots pos
            JOIN peeps p ON pos.peep_id = p.id
            LEFT JOIN raw_members rm ON p.email = rm."Email Address"
            WHERE pos.period_id = ?
            ORDER BY pos.index_position
        """, (period_id,))

        snapshots = []
        for row in cursor.fetchall():
            peep_id, email, full_name, display_name, primary_role, priority, index_position, total_attended, active, csv_id = row

            snapshot = MemberSnapshot(
                peep_id=peep_id,
                csv_id=csv_id or str(peep_id),  # Fallback if no CSV ID
                email=email,
                full_name=full_name,
                display_name=display_name,
                primary_role=primary_role,
                priority=priority,
                index_position=index_position,
                total_attended=total_attended,
                active=active
            )
            snapshots.append(snapshot)

        return snapshots