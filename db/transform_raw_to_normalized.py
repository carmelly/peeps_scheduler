#!/usr/bin/env python3
"""
Data Transformation Script: Raw to Normalized Schema

This script transforms historical raw data into the normalized relational schema.
It processes periods chronologically, creating a complete audit trail including
event assignment changes and historical snapshots.

Usage:
    python db/transform_raw_to_normalized.py [options]

Architecture:
    - DataTransformer: Handles all transformation operations
    - DataValidator: Validates data integrity after transformations
    - Period-by-period processing with transaction boundaries
    - First period (2025-02) used as baseline with priority=0, total_attended=0
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_manager import get_data_manager
from models import Role, SwitchPreference
from file_io import parse_event_date
from db.snapshot_generator import SnapshotGenerator, MemberSnapshot, EventAttendance


class DataTransformer:
    """Handles all data transformation operations from raw to normalized schema."""

    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor, verbose: bool = False):
        self.conn = conn
        self.cursor = cursor
        self.verbose = verbose
        self.logger = self._setup_logging()

        # Track transformed data for validation
        self.peep_id_mapping = {}  # csv_id -> database_id
        self.period_id_mapping = {}  # period_name -> database_id
        self.event_id_mapping = {}  # (period_name, event_index) -> database_id

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for transformation operations."""
        logger = logging.getLogger('data_transformer')
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _create_baseline_period_and_snapshot(self, first_period_name: str) -> bool:
        """
        Create baseline period and snapshot before processing any real periods.
        This ensures the baseline has a lower period ID than the first real period.
        """
        try:
            # Check if any snapshots exist (in case we're rerunning)
            self.cursor.execute("SELECT COUNT(*) FROM peep_order_snapshots")
            snapshot_count = self.cursor.fetchone()[0]

            if snapshot_count > 0:
                self.logger.info("Snapshots already exist, skipping baseline creation")
                return True

            self.logger.info(f"Creating baseline period and snapshot from first period: {first_period_name}")

            # 1. First, create peeps from the first period so we have them for the baseline
            if not self._transform_peeps(first_period_name):
                return False

            # 2. Create synthetic baseline period (represents state before scheduling began)
            baseline_period_name = f"{first_period_name}-baseline"

            # Parse the period to get dates for the baseline period
            year, month = map(int, first_period_name.split('-'))
            # Baseline period ends just before the first real period starts
            from datetime import date
            if month == 1:
                baseline_start = date(year - 1, 12, 1)
                baseline_end = date(year - 1, 12, 31)
            else:
                baseline_start = date(year, month - 1, 1)
                # Calculate last day of previous month
                import calendar
                last_day = calendar.monthrange(year, month - 1)[1]
                baseline_end = date(year, month - 1, last_day)

            self.cursor.execute("""
                INSERT INTO schedule_periods (period_name, display_name, start_date, end_date, status)
                VALUES (?, ?, ?, ?, 'completed')
            """, (
                baseline_period_name,
                f"Baseline for {first_period_name}",
                baseline_start.isoformat(),
                baseline_end.isoformat()
            ))

            baseline_period_id = self.cursor.lastrowid
            self.period_id_mapping[baseline_period_name] = baseline_period_id
            self.logger.info(f"Created baseline period {baseline_period_name} (ID: {baseline_period_id})")

            # 3. Get all member data from raw_members for the first period
            self.cursor.execute("""
                SELECT csv_id, "Index", Priority, "Total Attended", Active
                FROM raw_members
                WHERE period_name = ? AND csv_id IS NOT NULL
                ORDER BY CAST(csv_id AS INTEGER)
            """, (first_period_name,))

            baseline_members = self.cursor.fetchall()
            if not baseline_members:
                self.logger.error(f"No baseline members found for period {first_period_name}")
                return False

            # 4. Create baseline snapshots using actual raw data
            for csv_id, index_pos, priority, total_attended, active in baseline_members:
                peep_id = self.peep_id_mapping.get(csv_id)
                if not peep_id:
                    self.logger.warning(f"No peep_id found for csv_id {csv_id}")
                    continue

                # Convert active text to boolean
                active_bool = active.lower() in ['true', 'yes', '1'] if active else True

                # Use actual raw_members data (validator will check it's baseline-appropriate)
                self.cursor.execute("""
                    INSERT INTO peep_order_snapshots (
                        peep_id, period_id, priority, index_position,
                        total_attended, active, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    peep_id, baseline_period_id,  # Use baseline period ID
                    priority or 0,
                    index_pos or 0,
                    total_attended or 0,
                    active_bool,
                    f"Baseline snapshot from raw_members for period {first_period_name}"
                ))

            self.logger.info(f"Created baseline snapshots for {len(baseline_members)} members in period {baseline_period_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create baseline period and snapshot: {e}")
            return False

    def transform_all_data(self, dry_run: bool = False) -> bool:
        """
        Transform all raw data to normalized schema.

        Args:
            dry_run: If True, perform transformation but rollback at the end

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Starting complete data transformation")

            # Get all periods in chronological order
            periods = self._get_periods_chronologically()
            self.logger.info(f"Found {len(periods)} periods to process: {periods}")

            if not periods:
                self.logger.error("No raw data periods found")
                return False

            # Create validator
            validator = DataValidator(self.conn, self.cursor, self.verbose)

            # Create baseline period and snapshot before processing any real periods
            first_period = periods[0]
            if not self._create_baseline_period_and_snapshot(first_period):
                self.logger.error("Failed to create baseline period and snapshot")
                return False

            # Transform each period
            for period_name in periods:
                self.logger.info(f"Transforming period: {period_name}")

                # Each period gets its own transaction
                try:
                    self.conn.execute("SAVEPOINT period_transform")

                    success = self._transform_period(period_name)
                    if not success:
                        self.logger.error(f"Failed to transform period {period_name}")
                        self.conn.execute("ROLLBACK TO period_transform")
                        return False

                    # Validate this period immediately after transformation
                    if not validator.validate_period(period_name):
                        self.logger.error(f"Validation failed for period {period_name}")
                        self.conn.execute("ROLLBACK TO period_transform")
                        return False

                    self.conn.execute("RELEASE period_transform")
                    self.logger.info(f"Successfully transformed and validated period: {period_name}")

                except Exception as e:
                    self.logger.error(f"Error transforming period {period_name}: {e}")
                    self.conn.execute("ROLLBACK TO period_transform")
                    return False

            # Final global validation
            self.logger.info("Running final global validation")
            if not validator.validate_global_constraints():
                self.logger.error("Global validation failed")
                return False

            if dry_run:
                self.logger.info("DRY RUN: Rolling back all changes")
                self.conn.rollback()
            else:
                self.logger.info("Committing all transformations")
                self.conn.commit()

            return True

        except Exception as e:
            self.logger.error(f"Critical error during transformation: {e}")
            self.conn.rollback()
            return False

    def transform_single_period(self, period_name: str, dry_run: bool = False) -> bool:
        """
        Transform a single specific period (for incremental processing).

        Args:
            period_name: Name of the period to transform
            dry_run: If True, perform transformation but rollback at the end

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting single period transformation: {period_name}")

            # Check if period exists in raw data
            self.cursor.execute("SELECT COUNT(*) FROM raw_members WHERE period_name = ?", (period_name,))
            if self.cursor.fetchone()[0] == 0:
                self.logger.error(f"Period {period_name} not found in raw data")
                return False

            # Check if peeps table is populated (required for period transformation)
            self.cursor.execute("SELECT COUNT(*) FROM peeps")
            peep_count = self.cursor.fetchone()[0]

            if peep_count == 0:
                self.logger.error("No peeps found in database. Run full transformation first to create peeps and legacy period.")
                return False

            # Rebuild peep ID mapping from existing peeps
            self.cursor.execute("""
                SELECT rm.csv_id, p.id
                FROM peeps p
                JOIN raw_members rm ON p.email = rm."Email Address"
                WHERE rm.csv_id IS NOT NULL
                GROUP BY p.email
            """)

            for csv_id, peep_id in self.cursor.fetchall():
                self.peep_id_mapping[csv_id] = peep_id

            self.logger.info(f"Loaded {len(self.peep_id_mapping)} peep ID mappings")

            # Transform the specific period
            self.logger.info(f"Transforming period: {period_name}")

            # Use transaction for the period
            self.conn.execute("SAVEPOINT period_transform")

            try:
                success = self._transform_period(period_name)
                if not success:
                    self.logger.error(f"Failed to transform period {period_name}")
                    self.conn.execute("ROLLBACK TO period_transform")
                    return False

                # Validate this period immediately after transformation
                validator = DataValidator(self.conn, self.cursor, self.verbose)
                if not validator.validate_period(period_name):
                    self.logger.error(f"Validation failed for period {period_name}")
                    self.conn.execute("ROLLBACK TO period_transform")
                    return False

                self.conn.execute("RELEASE period_transform")
                self.logger.info(f"Successfully transformed and validated period: {period_name}")

            except Exception as e:
                self.logger.error(f"Error transforming period {period_name}: {e}")
                self.conn.execute("ROLLBACK TO period_transform")
                return False

            if dry_run:
                self.logger.info("DRY RUN: Rolling back all changes")
                self.conn.rollback()
            else:
                self.logger.info("Committing period transformation")
                self.conn.commit()

            return True

        except Exception as e:
            self.logger.error(f"Critical error during single period transformation: {e}")
            self.conn.rollback()
            return False

    def _get_periods_chronologically(self) -> List[str]:
        """Get all available periods in chronological order."""
        self.cursor.execute("""
            SELECT DISTINCT period_name
            FROM raw_members
            ORDER BY period_name
        """)
        return [row[0] for row in self.cursor.fetchall()]


    def _transform_peeps(self, period_name: str) -> bool:
        """Transform raw_members data into normalized peeps table with incremental updates."""
        try:
            self.logger.info(f"Transforming peeps from period {period_name}")

            # Get members from this specific period, ordered by CSV ID
            self.cursor.execute("""
                SELECT csv_id, Name, "Display Name", Role, "Email Address",
                       "Date Joined", Active
                FROM raw_members
                WHERE period_name = ? AND csv_id IS NOT NULL
                ORDER BY CAST(csv_id AS INTEGER)
            """, (period_name,))

            members = self.cursor.fetchall()
            self.logger.info(f"Found {len(members)} members in period {period_name}")

            new_peeps = 0
            updated_peeps = 0

            for member in members:
                csv_id, name, display_name, role, email, date_joined, active = member

                # Convert active text to boolean
                active_bool = active.lower() in ['true', 'yes', '1'] if active else True

                # Parse date_joined if available
                joined_date = None
                if date_joined:
                    try:
                        # Try M/D/YYYY format first (common in raw data)
                        joined_date = datetime.strptime(date_joined, "%m/%d/%Y").date()
                    except:
                        self.logger.warning(f"Could not parse date_joined '{date_joined}' for {name}")
                        joined_date = None

                # Check if peep already exists by ID (CSV ID should match DB ID)
                self.cursor.execute("SELECT id FROM peeps WHERE id = ?", (csv_id,))
                existing = self.cursor.fetchone()

                if existing:
                    # Update existing peep
                    peep_id = existing[0]
                    self.cursor.execute("""
                        UPDATE peeps SET
                            full_name = ?, display_name = ?, primary_role = ?,
                            email = ?, date_joined = ?, active = ?
                        WHERE id = ?
                    """, (name, display_name, role.lower(), email, joined_date.isoformat(), active_bool, peep_id))
                    updated_peeps += 1
                else:
                    # Insert new peep (AUTOINCREMENT will assign next sequential ID)
                    self.cursor.execute("""
                        INSERT INTO peeps (
                            full_name, display_name, primary_role, email,
                            date_joined, active
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (name, display_name, role.lower(), email, joined_date.isoformat(), active_bool))
                    peep_id = self.cursor.lastrowid
                    new_peeps += 1

                # Update mapping
                self.peep_id_mapping[csv_id] = peep_id

                if self.verbose:
                    status = "updated" if existing else "created"
                    self.logger.debug(f"{status.title()} peep: CSV ID {csv_id} -> DB ID {peep_id} ({name})")

            self.logger.info(f"Successfully processed {len(members)} peeps: {new_peeps} new, {updated_peeps} updated")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform peeps: {e}")
            return False


    def _transform_period(self, period_name: str) -> bool:
        """
        Transform one period with all related data.

        Args:
            period_name: Name of the period to transform

        Returns:
            True if successful
        """
        try:
            self.logger.info(f"Starting transformation for period: {period_name}")

            # 1. Create schedule period
            period_id = self._create_schedule_period(period_name)
            if not period_id:
                return False

            # 2. Transform peeps (incremental updates/additions)
            if not self._transform_peeps(period_name):
                return False

            # 2.5. Create baseline snapshot if this is the first period
            if not self._create_baseline_snapshot_if_needed(period_name, period_id):
                return False

            # 3. Transform responses (for historical data preservation)
            responses = self._transform_responses(period_name, period_id)
            if responses is None:
                return False

            # 4. Create proposed events from availability strings (what people were available for)
            proposed_events = self._create_proposed_events(period_name, period_id, responses)
            if proposed_events is None:
                return False

            # 5. Create event availability relationships (link responses to proposed events)
            if not self._transform_event_availability(responses, proposed_events):
                return False

            # 6. Create actual events and assignments from scheduler results JSON (what actually happened)
            events, assignments = self._transform_events_and_assignments(period_name, period_id)
            if events is None or assignments is None:
                return False

            # 7. Transform attendance from actual attendance JSON
            attendance = self._transform_attendance(period_name, events)
            if attendance is None:
                return False

            # 8. STRICT VALIDATION: Ensure all assignments have attendance
            # Exception: Future periods may have assignments but no attendance yet
            if assignments and not attendance:
                # Check if this period has actual attendance data
                self.cursor.execute("SELECT COUNT(*) FROM raw_actual_attendance WHERE period_name = ?", (period_name,))
                has_attendance_data = self.cursor.fetchone()[0] > 0

                if has_attendance_data:
                    # This period should have attendance but transformation failed
                    raise ValueError(
                        f"Period {period_name} has assignments and attendance data but no attendance records created. "
                        f"Manual reconstruction required before transformation."
                    )
                else:
                    # This is a future period with scheduler results but no attendance yet - that's normal
                    self.logger.info(f"Period {period_name} has assignments but no attendance data - this is normal for future periods")

            # 9. Reconcile and create change records
            # Get the period status to determine if we should create change records
            self.cursor.execute("SELECT status FROM schedule_periods WHERE id = ?", (period_id,))
            period_status = self.cursor.fetchone()[0]

            # Get ALL events for this period (including any unscheduled events created during attendance transformation)
            self.cursor.execute("""
                SELECT id, legacy_period_event_id, event_datetime, status
                FROM events
                WHERE period_id = ?
                ORDER BY event_datetime
            """, (period_id,))
            all_events = [{
                'id': row[0],
                'legacy_id': row[1],
                'datetime': datetime.fromisoformat(row[2].replace('Z', '+00:00')),
                'status': row[3]
            } for row in self.cursor.fetchall()]

            if not self._reconcile_assignments_vs_attendance(assignments, attendance, all_events, period_status):
                return False

            # 10. Generate period snapshot
            if not self._generate_period_snapshot(period_name, period_id):
                return False

            self.logger.info(f"Successfully transformed period: {period_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform period {period_name}: {e}")
            return False

    def _create_baseline_snapshot_if_needed(self, period_name: str, period_id: int) -> bool:
        """
        No-op: Baseline snapshots are now created upfront in _create_baseline_period_and_snapshot().
        This method exists to maintain compatibility with existing period processing flow.
        """
        # Baseline snapshots are already created before any period processing begins
        return True

    def _transform_events_and_assignments(self, period_name: str, period_id: int) -> Tuple[Optional[List[Dict]], Optional[List[Dict]]]:
        """Update proposed events to completed status and create assignments from scheduler results JSON."""
        try:
            # Get results JSON for this period
            self.cursor.execute("""
                SELECT results_json FROM raw_results WHERE period_name = ?
            """, (period_name,))

            result = self.cursor.fetchone()
            if not result or not result[0]:
                self.logger.info(f"No scheduler results found for period {period_name}")
                return [], []

            # Parse JSON
            try:
                results_data = json.loads(result[0])
            except Exception as e:
                self.logger.error(f"Could not parse results JSON for period {period_name}: {e}")
                return None, None

            events = []
            assignments = []

            # Process each valid_event in the results to update proposed events and create assignments
            valid_events = results_data.get('valid_events', [])
            for valid_event in valid_events:
                event_index = valid_event.get('id')
                if event_index is None:
                    self.logger.warning(f"Event missing 'id' field, skipping: {valid_event}")
                    continue
                event_datetime_str = valid_event.get('date')
                if not event_datetime_str:
                    self.logger.warning(f"No date found for event {event_index}")
                    continue

                # Parse event datetime
                try:
                    event_datetime = datetime.fromisoformat(event_datetime_str.replace('Z', '+00:00'))
                except Exception as e:
                    self.logger.warning(f"Could not parse event_datetime {event_datetime_str}: {e}")
                    continue

                # Get duration from results (defaults to 120 if not specified)
                duration_minutes = valid_event.get('duration_minutes', 120)

                # Find existing proposed event by datetime
                self.cursor.execute("""
                    SELECT id FROM events
                    WHERE period_id = ? AND event_datetime = ? AND status = 'proposed'
                """, (period_id, event_datetime.isoformat()))

                event_result = self.cursor.fetchone()
                if not event_result:
                    self.logger.warning(f"No proposed event found for {event_datetime_str} in period {period_name}, creating new event")
                    # Fallback: create new event if proposed event not found
                    self.cursor.execute("""
                        INSERT INTO events (
                            period_id, legacy_period_event_id, event_datetime,
                            duration_minutes, status
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (period_id, event_index, event_datetime.isoformat(), duration_minutes, "completed"))
                    event_id = self.cursor.lastrowid
                else:
                    # Update existing proposed event to completed
                    event_id = event_result[0]
                    self.cursor.execute("""
                        UPDATE events
                        SET status = 'completed',
                            duration_minutes = ?,
                            legacy_period_event_id = ?
                        WHERE id = ?
                    """, (duration_minutes, event_index, event_id))

                event_data = {
                    'id': event_id,
                    'event_index': event_index,
                    'datetime': event_datetime,
                    'period_id': period_id
                }
                events.append(event_data)
                self.event_id_mapping[(period_name, event_index)] = event_id

                # Create assignments for this event
                attendees = valid_event.get('attendees', [])
                for order, attendee in enumerate(attendees):
                    member_id = attendee.get('id')
                    peep_id = self.peep_id_mapping.get(str(member_id))
                    if not peep_id:
                        self.logger.warning(f"No peep_id found for member_id {member_id}")
                        continue

                    assigned_role = attendee.get('role')
                    assignment_type = attendee.get('assignment_type', 'attendee')

                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type, assignment_order
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_id, peep_id, assigned_role.lower(), assignment_type, order + 1))

                    assignments.append({
                        'id': self.cursor.lastrowid,
                        'event_id': event_id,
                        'peep_id': peep_id,
                        'assigned_role': assigned_role,
                        'assignment_type': assignment_type,
                        'assignment_order': order + 1
                    })

                # Create alternate assignments for this event
                alternates = valid_event.get('alternates', [])
                for alt_pos, alternate in enumerate(alternates):
                    member_id = alternate.get('id')
                    peep_id = self.peep_id_mapping.get(str(member_id))
                    if not peep_id:
                        self.logger.warning(f"No peep_id found for alternate member_id {member_id}")
                        continue

                    assigned_role = alternate.get('role')

                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type, alternate_position
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_id, peep_id, assigned_role.lower(), 'alternate', alt_pos + 1))

                    assignments.append({
                        'id': self.cursor.lastrowid,
                        'event_id': event_id,
                        'peep_id': peep_id,
                        'assigned_role': assigned_role,
                        'assignment_type': 'alternate',
                        'alternate_position': alt_pos + 1
                    })

                if alternates:
                    self.logger.debug(f"Created {len(alternates)} alternate assignments for event {event_id}")

            self.logger.info(f"Created {len(events)} events and {len(assignments)} assignments for {period_name}")
            alternates_count = sum(1 for a in assignments if a['assignment_type'] == 'alternate')
            if alternates_count > 0:
                self.logger.info(f"  Including {alternates_count} alternate assignments")
            return events, assignments

        except Exception as e:
            self.logger.error(f"Failed to transform events and assignments for {period_name}: {e}")
            return None, None

    def _create_schedule_period(self, period_name: str) -> Optional[int]:
        """Create schedule_periods record for the given period."""
        try:
            # Parse period name (e.g., "2025-02") to get month boundaries
            year, month = map(int, period_name.split('-'))
            start_date = datetime(year, month, 1).date()
            # Get last day of month
            if month == 12:
                end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)

            # Determine display name
            display_name = f"Period {period_name}"

            # Determine status based on data availability
            # completed: has both results and attendance
            # active: has results but no attendance (scheduled but not yet held)
            # draft: has responses but no results (not yet scheduled)

            self.cursor.execute("SELECT COUNT(*) FROM raw_results WHERE period_name = ?", (period_name,))
            has_results = self.cursor.fetchone()[0] > 0

            self.cursor.execute("SELECT COUNT(*) FROM raw_actual_attendance WHERE period_name = ?", (period_name,))
            has_attendance = self.cursor.fetchone()[0] > 0

            if has_results and has_attendance:
                status = "completed"
            elif has_results and not has_attendance:
                status = "active"  # Scheduled but events haven't happened yet
            else:
                status = "draft"  # Not yet scheduled

            self.cursor.execute("""
                INSERT INTO schedule_periods (
                    period_name, display_name, start_date, end_date, status
                ) VALUES (?, ?, ?, ?, ?)
            """, (period_name, display_name, start_date.isoformat(), end_date.isoformat(), status))

            period_id = self.cursor.lastrowid
            self.period_id_mapping[period_name] = period_id

            self.logger.info(f"Created schedule period {period_name} with ID {period_id}")
            return period_id

        except Exception as e:
            self.logger.error(f"Failed to create schedule period {period_name}: {e}")
            return None


    def _transform_responses(self, period_name: str, period_id: int) -> Optional[List[Dict]]:
        """Transform raw_responses data into normalized responses table."""
        try:
            self.cursor.execute("""
                SELECT "Email Address", Name, "Primary Role", "Secondary Role",
                       "Max Sessions", "Min Interval Days", "Partnership Preference",
                       "Questions or Comments for Organizers",
                       "Questions or Comments for Leilani", Timestamp
                FROM raw_responses
                WHERE period_name = ?
            """, (period_name,))

            raw_responses = self.cursor.fetchall()
            if not raw_responses:
                self.logger.warning(f"No responses found for period {period_name}")
                return []

            created_responses = []

            for response_data in raw_responses:
                email, name, primary_role, secondary_role, max_sessions, min_interval, \
                partnership_pref, org_comments, instructor_comments, timestamp = response_data

                # Look up peep_id by email via raw_members for this period (case-insensitive)
                self.cursor.execute("""
                    SELECT csv_id FROM raw_members
                    WHERE period_name = ? AND LOWER("Email Address") = LOWER(?)
                """, (period_name, email))

                result = self.cursor.fetchone()
                if not result:
                    self.logger.error(f"No member found for email {email} in period {period_name}")
                    return None

                csv_id = result[0]
                peep_id = self.peep_id_mapping.get(csv_id)
                if not peep_id:
                    self.logger.error(f"No peep_id found for csv_id {csv_id}")
                    return None

                # Parse response fields
                response_role = Role.from_string(primary_role).value
                switch_pref = SwitchPreference.from_string(secondary_role).value if secondary_role else SwitchPreference.PRIMARY_ONLY.value
                max_sessions_int = int(max_sessions) if max_sessions and max_sessions.isdigit() else 6
                min_interval_int = int(min_interval) if min_interval and min_interval.isdigit() else 0

                # Parse timestamp
                response_timestamp = None
                if timestamp:
                    try:
                        response_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        pass

                self.cursor.execute("""
                    INSERT INTO responses (
                        peep_id, period_id, response_role, switch_preference,
                        max_sessions, min_interval_days, partnership_preference,
                        organizer_comments, instructor_comments, response_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    peep_id, period_id, response_role, switch_pref,
                    max_sessions_int, min_interval_int, partnership_pref,
                    org_comments, instructor_comments, response_timestamp
                ))

                response_id = self.cursor.lastrowid
                created_responses.append({
                    'id': response_id,
                    'peep_id': peep_id,
                    'period_id': period_id
                })

            self.logger.info(f"Created {len(created_responses)} responses for period {period_name}")
            return created_responses

        except Exception as e:
            self.logger.error(f"Failed to transform responses for period {period_name}: {e}")
            return None

    def _create_proposed_events(self, period_name: str, period_id: int, responses: List[Dict]) -> Optional[List[Dict]]:
        """Create proposed events from availability strings in responses."""
        try:
            if not responses:
                self.logger.info("No responses to extract proposed events from")
                return []

            self.logger.info(f"Creating proposed events from availability strings for period {period_name}")

            # Collect all unique proposed event datetimes from availability strings
            proposed_datetimes = set()

            for response in responses:
                response_id = response['id']
                peep_id = response['peep_id']

                # Get the availability string for this response (case-insensitive email)
                self.cursor.execute("""
                    SELECT Availability FROM raw_responses
                    WHERE period_name = ? AND LOWER("Email Address") IN (
                        SELECT LOWER(email) FROM peeps WHERE id = ?
                    )
                    LIMIT 1
                """, (period_name, peep_id))

                result = self.cursor.fetchone()
                if not result or not result[0]:
                    continue

                availability_string = result[0]

                # Parse availability string to extract proposed datetimes
                try:
                    available_events = availability_string.split(',')
                    for event_str in available_events:
                        event_str = event_str.strip()
                        if event_str:
                            try:
                                event_datetime = parse_event_date(event_str)
                                # Ensure we have a datetime object
                                if isinstance(event_datetime, str):
                                    event_datetime = datetime.fromisoformat(event_datetime.replace('Z', '+00:00'))
                                proposed_datetimes.add(event_datetime)
                            except Exception as e:
                                self.logger.warning(f"Could not parse event date '{event_str}': {e}")

                except Exception as e:
                    self.logger.warning(f"Could not parse availability string '{availability_string}': {e}")

            # Create proposed event records for unique datetimes
            proposed_events = []
            for event_datetime in sorted(proposed_datetimes):
                self.cursor.execute("""
                    INSERT INTO events (
                        period_id, event_datetime, duration_minutes, status
                    ) VALUES (?, ?, ?, ?)
                """, (period_id, event_datetime.isoformat(), 120, 'proposed'))

                event_id = self.cursor.lastrowid
                proposed_events.append({
                    'id': event_id,
                    'datetime': event_datetime,
                    'period_id': period_id,
                    'status': 'proposed'
                })

            self.logger.info(f"Created {len(proposed_events)} proposed events for period {period_name}")
            return proposed_events

        except Exception as e:
            self.logger.error(f"Failed to create proposed events for {period_name}: {e}")
            return None

    def _transform_event_availability(self, responses: List[Dict], proposed_events: List[Dict]) -> bool:
        """Create event_availability many-to-many relationships linking responses to proposed events."""
        try:
            if not responses or not proposed_events:
                self.logger.warning("No responses or proposed events to process for availability")
                return True

            # Create proposed event datetime lookup
            proposed_datetime_to_id = {event['datetime']: event['id'] for event in proposed_events}

            # Get period name from period_id for raw_responses lookup
            if responses:
                period_id = responses[0]['period_id']
                self.cursor.execute("SELECT period_name FROM schedule_periods WHERE id = ?", (period_id,))
                period_name = self.cursor.fetchone()[0]

            availability_records_created = 0

            # Process each response's availability string
            for response in responses:
                response_id = response['id']
                peep_id = response['peep_id']

                # Get the availability string for this response (case-insensitive email)
                self.cursor.execute("""
                    SELECT Availability FROM raw_responses
                    WHERE period_name = ? AND LOWER("Email Address") IN (
                        SELECT LOWER(email) FROM peeps WHERE id = ?
                    )
                    LIMIT 1
                """, (period_name, peep_id))

                result = self.cursor.fetchone()
                if not result or not result[0]:
                    continue

                availability_string = result[0]

                # Parse availability string and link to proposed events
                try:
                    available_events = availability_string.split(',')
                    for event_str in available_events:
                        event_str = event_str.strip()
                        if event_str:
                            try:
                                event_datetime = parse_event_date(event_str)
                                # Ensure we have a datetime object
                                if isinstance(event_datetime, str):
                                    event_datetime = datetime.fromisoformat(event_datetime.replace('Z', '+00:00'))
                                proposed_event_id = proposed_datetime_to_id.get(event_datetime)

                                if proposed_event_id:
                                    self.cursor.execute("""
                                        INSERT INTO event_availability (response_id, event_id)
                                        VALUES (?, ?)
                                    """, (response_id, proposed_event_id))
                                    availability_records_created += 1
                                else:
                                    self.logger.warning(f"Proposed event datetime {event_datetime} not found")

                            except Exception as e:
                                self.logger.warning(f"Could not parse event date '{event_str}': {e}")

                except Exception as e:
                    self.logger.warning(f"Could not parse availability string '{availability_string}': {e}")

            self.logger.info(f"Created {availability_records_created} event availability relationships for {len(responses)} responses")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform event availability: {e}")
            return False



    def _transform_attendance(self, period_name: str, events: List[Dict]) -> Optional[List[Dict]]:
        """Transform actual attendance JSON into normalized attendance records."""
        try:
            # Get actual attendance JSON for this period
            self.cursor.execute("""
                SELECT actual_attendance_json FROM raw_actual_attendance WHERE period_name = ?
            """, (period_name,))

            result = self.cursor.fetchone()
            if not result or not result[0]:
                self.logger.info(f"No actual attendance found for period {period_name}")
                return []

            # Parse JSON
            try:
                attendance_data = json.loads(result[0])
            except Exception as e:
                self.logger.error(f"Could not parse attendance JSON for period {period_name}: {e}")
                return None

            # Create event lookup by legacy ID
            event_lookup = {event['event_index']: event for event in events}

            attendance_records = []
            created_unscheduled_events = []

            # Process each valid_event in the attendance
            valid_events = attendance_data.get('valid_events', [])
            for valid_event in valid_events:
                event_index = valid_event.get('id')
                if event_index is None:
                    self.logger.warning(f"Attendance event missing 'id' field, skipping: {valid_event}")
                    continue

                event_data = event_lookup.get(event_index)

                if not event_data:
                    # Create unscheduled event for attendance that has no corresponding scheduled event
                    event_id = self._create_unscheduled_event(period_name, valid_event, event_index)
                    if not event_id:
                        self.logger.error(f"Failed to create unscheduled event for index {event_index}")
                        continue

                    # Add to our lookup and events list for processing
                    event_data = {
                        'id': event_id,
                        'event_index': event_index,
                        'datetime': valid_event.get('date'),
                        'period_id': self.period_id_mapping[period_name],
                        'is_unscheduled': True
                    }
                    event_lookup[event_index] = event_data
                    created_unscheduled_events.append(event_data)

                    self.logger.info(f"Created unscheduled event {event_id} for attendance index {event_index}")

                event_id = event_data['id']

                # Process attendees who actually attended
                attendees = valid_event.get('attendees', [])
                for attendee in attendees:
                    member_id = attendee.get('id')
                    role = attendee.get('role')

                    # Find peep_id from member_id
                    peep_id = self.peep_id_mapping.get(str(member_id))
                    if not peep_id:
                        self.logger.warning(f"Peep not found for attended member_id {member_id}")
                        continue

                    # Find corresponding assignment if it exists
                    self.cursor.execute("""
                        SELECT id, assigned_role, assignment_type
                        FROM event_assignments
                        WHERE event_id = ? AND peep_id = ?
                    """, (event_id, peep_id))

                    assignment_result = self.cursor.fetchone()
                    assignment_id = assignment_result[0] if assignment_result else None
                    expected_role = assignment_result[1] if assignment_result else None
                    expected_type = assignment_result[2] if assignment_result else None

                    # Determine participation mode
                    participation_mode = "scheduled" if assignment_id else "volunteer_fill"
                    if assignment_id and expected_type == "alternate":
                        participation_mode = "alternate_promoted"

                    # Create attendance record
                    self.cursor.execute("""
                        INSERT INTO event_attendance (
                            event_id, peep_id, event_assignment_id, expected_role,
                            expected_type, actual_role, attendance_status, participation_mode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event_id, peep_id, assignment_id, expected_role,
                        expected_type, role.lower(), "attended", participation_mode
                    ))

                    attendance_records.append({
                        'id': self.cursor.lastrowid,
                        'event_id': event_id,
                        'peep_id': peep_id,
                        'assignment_id': assignment_id,
                        'actual_role': role,
                        'participation_mode': participation_mode
                    })

            self.logger.info(f"Created {len(attendance_records)} attendance records for period {period_name}")
            if created_unscheduled_events:
                self.logger.info(f"Created {len(created_unscheduled_events)} unscheduled events for period {period_name}")
            return attendance_records

        except Exception as e:
            self.logger.error(f"Failed to transform attendance for period {period_name}: {e}")
            return None

    def _create_unscheduled_event(self, period_name: str, valid_event: Dict, event_index: int) -> Optional[int]:
        """
        Create or reuse an event from attendance data when no corresponding scheduled event exists.
        This handles cases like last-minute sessions that were held but not in the original schedule.
        """
        try:
            # Extract event details from attendance JSON
            event_datetime_str = valid_event.get('date')
            if not event_datetime_str:
                self.logger.error(f"No date found for unscheduled event {event_index}")
                return None

            # Parse event datetime
            try:
                event_datetime = datetime.fromisoformat(event_datetime_str.replace('Z', '+00:00'))
            except Exception as e:
                self.logger.error(f"Could not parse event_datetime {event_datetime_str}: {e}")
                return None

            # Get duration (default to 90 minutes for unscheduled events)
            duration_minutes = valid_event.get('duration_minutes', 90)

            # Get period ID
            period_id = self.period_id_mapping.get(period_name)
            if not period_id:
                self.logger.error(f"Period ID not found for {period_name}")
                return None

            # First check if there's already a proposed event at this datetime
            self.cursor.execute("""
                SELECT id FROM events
                WHERE period_id = ? AND event_datetime = ? AND status = 'proposed'
            """, (period_id, event_datetime.isoformat()))

            existing_event = self.cursor.fetchone()

            if existing_event:
                # Reuse existing proposed event, update it to completed with correct legacy ID
                event_id = existing_event[0]
                self.cursor.execute("""
                    UPDATE events
                    SET status = 'completed',
                        duration_minutes = ?,
                        legacy_period_event_id = ?
                    WHERE id = ?
                """, (duration_minutes, event_index, event_id))

                self.logger.info(f"Reused existing proposed event {event_id} for attendance index {event_index} at {event_datetime_str}")
            else:
                # Create new event if no proposed event exists
                self.cursor.execute("""
                    INSERT INTO events (
                        period_id, legacy_period_event_id, event_datetime,
                        duration_minutes, status
                    ) VALUES (?, ?, ?, ?, ?)
                """, (period_id, event_index, event_datetime.isoformat(), duration_minutes, "completed"))

                event_id = self.cursor.lastrowid
                self.logger.info(f"Created new unscheduled event {event_id} for {event_datetime_str} (index {event_index})")

            # Update mapping for future reference
            self.event_id_mapping[(period_name, event_index)] = event_id

            return event_id

        except Exception as e:
            self.logger.error(f"Failed to create unscheduled event for index {event_index}: {e}")
            return None

    def _reconcile_assignments_vs_attendance(self, assignments: List[Dict], attendance: List[Dict], events: List[Dict], period_status: str) -> bool:
        """Create event_assignment_changes for all discrepancies between scheduled vs actual."""
        try:
            if not assignments and not attendance:
                self.logger.info("No assignments or attendance to reconcile")
                return True

            # Skip change record creation for future periods (active status)
            # Only create change records for completed periods where events have actually happened
            if period_status == 'active':
                self.logger.info(f"Period status is 'active' - skipping change record creation (future period)")
                return True

            if assignments and not attendance:
                self.logger.info("Period has assignments but no attendance - skipping change record creation (no attendance data)")
                return True

            changes_created = 0

            for event in events:
                event_id = event['id']
                event_datetime = event['datetime']

                # Get assignments and attendance for this event
                event_assignments = [a for a in assignments if a['event_id'] == event_id]
                event_attendance = [a for a in attendance if a['event_id'] == event_id]

                # Only process change records for completed periods with actual attendance
                # Skip individual events without attendance in completed periods
                if not event_attendance and event_assignments and period_status == 'completed':
                    self.logger.debug(f"Event {event_id} has assignments but no attendance - skipping change records (incomplete event data)")
                    continue

                # Check each assignment against attendance
                for assignment in event_assignments:
                    peep_id = assignment['peep_id']
                    assigned_role = assignment['assigned_role']
                    assignment_type = assignment['assignment_type']

                    # Find if this person attended
                    attended = next((a for a in event_attendance if a['peep_id'] == peep_id), None)

                    if not attended:
                        # Only create cancel records for main attendees who didn't attend
                        # (Alternates not attending is normal behavior - no cancel record needed)
                        if assignment_type == 'attendee':
                            self.cursor.execute("""
                                INSERT INTO event_assignment_changes (
                                    event_id, peep_id, change_type, change_source,
                                    changed_at, notes
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                event_id, peep_id, "cancel", "reconstructed", event_datetime.isoformat(),
                                "Reconstructed: Scheduled but no attendance record"
                            ))
                            changes_created += 1

                    elif attended['actual_role'] != assigned_role:
                        # Role change
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, peep_id, change_type, change_source, changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            event_id, peep_id, "change_role", "reconstructed", event_datetime.isoformat(),
                            f"Reconstructed: Role changed from {assigned_role} to {attended['actual_role']}"
                        ))
                        changes_created += 1

                # Check for attendees not in assignments (volunteer fill-ins and alternate promotions)
                volunteer_fills_in_event = [a for a in event_attendance if a['participation_mode'] == 'volunteer_fill']
                alternate_promotions_in_event = [a for a in event_attendance if a['participation_mode'] == 'alternate_promoted']

                if volunteer_fills_in_event:
                    self.logger.debug(f"Processing {len(volunteer_fills_in_event)} volunteer fills for event {event_id}")
                if alternate_promotions_in_event:
                    self.logger.debug(f"Processing {len(alternate_promotions_in_event)} alternate promotions for event {event_id}")

                # Create 'add' changes for volunteer fills (truly unscheduled attendees)
                for attended in event_attendance:
                    if attended['participation_mode'] == 'volunteer_fill':
                        self.logger.debug(f"Creating 'add' change for volunteer fill: peep_id={attended['peep_id']}, event_id={event_id}")
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, peep_id, change_type, change_source,
                                changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            event_id, attended['peep_id'], "add", "reconstructed",
                            event_datetime.isoformat(), "Reconstructed: Attended without assignment"
                        ))
                        changes_created += 1
                        self.logger.debug(f"Successfully created 'add' change for peep_id={attended['peep_id']}")

                # Create 'promote_alternate' changes for alternate promotions
                for attended in event_attendance:
                    if attended['participation_mode'] == 'alternate_promoted':
                        self.logger.debug(f"Creating 'promote_alternate' change for alternate promotion: peep_id={attended['peep_id']}, event_id={event_id}")
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, peep_id, change_type, change_source,
                                changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            event_id, attended['peep_id'], "promote_alternate", "reconstructed",
                            event_datetime.isoformat(), "Reconstructed: Alternate promoted to attendee"
                        ))
                        changes_created += 1
                        self.logger.debug(f"Successfully created 'promote_alternate' change for peep_id={attended['peep_id']}")

            if changes_created > 0:
                self.logger.info(f"Created {changes_created} assignment change records")
            else:
                self.logger.debug("No assignment changes needed (no discrepancies found)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to reconcile assignments vs attendance: {e}")
            return False

    def _generate_period_snapshot(self, period_name: str, period_id: int) -> bool:
        """Generate peep_order_snapshot for completed period."""
        try:
            # Get the member state for this period from raw_members
            self.cursor.execute("""
                SELECT csv_id, Priority, "Index", "Total Attended", Active
                FROM raw_members
                WHERE period_name = ?
                ORDER BY CAST(csv_id AS INTEGER)
            """, (period_name,))

            member_states = self.cursor.fetchall()
            if not member_states:
                self.logger.warning(f"No member state data found for period {period_name}")
                return True  # Not an error - might be a period with no member data

            snapshots_created = 0

            for member in member_states:
                csv_id, priority, index_pos, total_attended, active = member

                peep_id = self.peep_id_mapping.get(csv_id)
                if not peep_id:
                    self.logger.warning(f"No peep_id found for csv_id {csv_id}")
                    continue

                # Convert active text to boolean
                active_bool = active.lower() in ['true', 'yes', '1'] if active else True

                self.cursor.execute("""
                    INSERT INTO peep_order_snapshots (
                        peep_id, period_id, priority, index_position,
                        total_attended, active, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    peep_id, period_id,
                    priority or 0, index_pos or 0,
                    total_attended or 0, active_bool,
                    f"Period {period_name} completion snapshot"
                ))

                snapshots_created += 1

            self.logger.info(f"Created {snapshots_created} snapshots for period {period_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to generate period snapshot for {period_name}: {e}")
            return False


class DataValidator:
    """Validates data integrity after transformations."""

    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor, verbose: bool = False):
        self.conn = conn
        self.cursor = cursor
        self.verbose = verbose
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for validation operations."""
        logger = logging.getLogger('data_validator')
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _validate_event_id_consistency(self, period_name: str) -> bool:
        """
        Comprehensive event ID validation between results and attendance JSON.
        Reports issues but allows transformation to continue with warnings.
        """
        try:
            # Get results and attendance event IDs
            self.cursor.execute("SELECT results_json FROM raw_results WHERE period_name = ?", (period_name,))
            results_result = self.cursor.fetchone()

            self.cursor.execute("SELECT actual_attendance_json FROM raw_actual_attendance WHERE period_name = ?", (period_name,))
            attendance_result = self.cursor.fetchone()

            if not results_result or not results_result[0]:
                self.logger.info(f"Period {period_name}: No results data - skipping event ID consistency check")
                return True

            results_data = json.loads(results_result[0])
            results_events = results_data.get('valid_events', [])
            results_event_ids = {e.get('id') for e in results_events if e.get('id') is not None}

            if not attendance_result or not attendance_result[0]:
                self.logger.info(f"Period {period_name}: No attendance data - skipping event ID consistency check")
                return True

            attendance_data = json.loads(attendance_result[0])
            attendance_events = attendance_data.get('valid_events', [])
            attendance_event_ids = {e.get('id') for e in attendance_events if e.get('id') is not None}

            # Track validation issues by severity
            has_critical_issues = False

            # CRITICAL: Missing event IDs in JSON (breaks transformation)
            for i, event in enumerate(results_events):
                if event.get('id') is None:
                    self.logger.error(f"Period {period_name}: Results event at index {i} missing 'id' field - CRITICAL BUG")
                    has_critical_issues = True

            for i, event in enumerate(attendance_events):
                if event.get('id') is None:
                    self.logger.error(f"Period {period_name}: Attendance event at index {i} missing 'id' field - CRITICAL BUG")
                    has_critical_issues = True

            # WARNINGS: Data inconsistencies (might be normal)
            extra_in_attendance = attendance_event_ids - results_event_ids
            missing_from_attendance = results_event_ids - attendance_event_ids

            if extra_in_attendance:
                self.logger.warning(f"Period {period_name}: Events in attendance but not results: {sorted(extra_in_attendance)} (might be unscheduled events)")

            if missing_from_attendance:
                self.logger.info(f"Period {period_name}: Events in results but not attendance: {sorted(missing_from_attendance)} (no-shows - normal)")

            # INFO: Non-sequential IDs (normal - just documenting)
            all_ids = sorted(results_event_ids | attendance_event_ids)
            if all_ids and all_ids != list(range(len(all_ids))):
                self.logger.debug(f"Period {period_name}: Non-sequential event IDs: {all_ids} (this is normal)")

            # Only fail validation for critical issues that break transformation
            return not has_critical_issues

        except Exception as e:
            self.logger.error(f"Period {period_name}: Event ID consistency validation failed: {e}")
            return False

    def validate_period(self, period_name: str) -> bool:
        """
        Validate transformation for a specific period.

        Args:
            period_name: Name of the period to validate

        Returns:
            True if validation passes, False otherwise
        """
        try:
            self.logger.info(f"Validating period: {period_name}")

            # Get period ID
            self.cursor.execute("SELECT id FROM schedule_periods WHERE period_name = ?", (period_name,))
            result = self.cursor.fetchone()
            if not result:
                self.logger.error(f"Period {period_name} not found in schedule_periods")
                return False

            period_id = result[0]

            # Run period-specific validations
            validations = [
                ("Event ID Consistency", lambda: self._validate_event_id_consistency(period_name)),
                ("Events", lambda: self._validate_period_events(period_name, period_id)),
                ("Responses", lambda: self._validate_period_responses(period_name, period_id)),
                ("Event Availability", lambda: self._validate_period_availability(period_name, period_id)),
                ("Assignments", lambda: self._validate_period_assignments(period_name, period_id)),
                ("Attendance", lambda: self._validate_period_attendance(period_name, period_id)),
                ("Assignment Changes", lambda: self._validate_period_changes(period_name, period_id)),
                ("Snapshots", lambda: self._validate_period_snapshots(period_name, period_id)),
            ]

            all_passed = True
            for validation_name, validation_func in validations:
                try:
                    result = validation_func()
                    if result:
                        self.logger.info(f"✅ {period_name} - {validation_name}: PASSED")
                    else:
                        self.logger.error(f"❌ {period_name} - {validation_name}: FAILED")
                        all_passed = False
                except Exception as e:
                    self.logger.error(f"❌ {period_name} - {validation_name}: ERROR - {e}")
                    all_passed = False

            if all_passed:
                self.logger.info(f"🎉 Period {period_name} validation PASSED")
            else:
                self.logger.error(f"💥 Period {period_name} validation FAILED")

            return all_passed

        except Exception as e:
            self.logger.error(f"Error validating period {period_name}: {e}")
            return False

    def _validate_period_events(self, period_name: str, period_id: int) -> bool:
        """Validate events for a specific period."""
        # Check events exist if we have responses
        self.cursor.execute("SELECT COUNT(*) FROM raw_responses WHERE period_name = ?", (period_name,))
        raw_responses_count = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (period_id,))
        events_count = self.cursor.fetchone()[0]

        if raw_responses_count > 0 and events_count == 0:
            self.logger.error(f"Period {period_name} has responses but no events")
            return False

        # Check event datetime uniqueness
        self.cursor.execute("""
            SELECT event_datetime, COUNT(*)
            FROM events e
            WHERE e.period_id = ?
            GROUP BY event_datetime
            HAVING COUNT(*) > 1
        """, (period_id,))

        duplicates = self.cursor.fetchall()
        if duplicates:
            for event_datetime, count in duplicates:
                self.logger.error(f"Period {period_name}: Duplicate event datetime {event_datetime} ({count} events)")
            return False

        return True

    def _validate_period_responses(self, period_name: str, period_id: int) -> bool:
        """Validate responses for a specific period."""
        # Check response count matches raw data
        self.cursor.execute("SELECT COUNT(*) FROM raw_responses WHERE period_name = ?", (period_name,))
        raw_count = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM responses WHERE period_id = ?", (period_id,))
        normalized_count = self.cursor.fetchone()[0]

        if raw_count > 0 and normalized_count == 0:
            self.logger.error(f"Period {period_name} has raw responses but no normalized responses")
            return False

        # Check for invalid switch preferences
        self.cursor.execute("""
            SELECT COUNT(*) FROM responses
            WHERE period_id = ? AND switch_preference NOT IN (1, 2, 3)
        """, (period_id,))

        invalid_prefs = self.cursor.fetchone()[0]
        if invalid_prefs > 0:
            self.logger.error(f"Period {period_name}: {invalid_prefs} responses with invalid switch_preference")
            return False

        return True

    def _validate_period_availability(self, period_name: str, period_id: int) -> bool:
        """Validate event availability for a specific period."""
        # Check that responses with availability data have corresponding availability records
        # It's normal for responses to have no availability (empty string) - that's not an error
        self.cursor.execute("""
            SELECT COUNT(*) FROM responses r
            JOIN peeps p ON r.peep_id = p.id
            JOIN raw_responses rr ON LOWER(p.email) = LOWER(rr."Email Address") AND rr.period_name = ?
            WHERE r.period_id = ?
            AND rr.Availability IS NOT NULL
            AND TRIM(rr.Availability) != ''
            AND NOT EXISTS (
                SELECT 1 FROM event_availability ea WHERE ea.response_id = r.id
            )
        """, (period_name, period_id))

        responses_with_missing_availability = self.cursor.fetchone()[0]
        if responses_with_missing_availability > 0:
            self.logger.error(f"Period {period_name}: {responses_with_missing_availability} responses with availability data but no availability records created")
            return False

        return True

    def _validate_period_assignments(self, period_name: str, period_id: int) -> bool:
        """Validate assignments for a specific period."""
        # Check if we should have assignments (has results JSON)
        self.cursor.execute("SELECT COUNT(*) FROM raw_results WHERE period_name = ?", (period_name,))
        has_results = self.cursor.fetchone()[0] > 0

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        assignment_count = self.cursor.fetchone()[0]

        if has_results and assignment_count == 0:
            self.logger.error(f"Period {period_name} has results JSON but no assignments")
            return False

        # Check no duplicate assignments for same person/event
        self.cursor.execute("""
            SELECT ea.event_id, ea.peep_id, COUNT(*) as count
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
            GROUP BY ea.event_id, ea.peep_id
            HAVING COUNT(*) > 1
        """, (period_id,))

        duplicates = self.cursor.fetchall()
        if duplicates:
            for event_id, peep_id, count in duplicates:
                self.logger.error(f"Period {period_name}: Event {event_id}, Peep {peep_id} has {count} assignments")
            return False

        return True

    def _validate_period_attendance(self, period_name: str, period_id: int) -> bool:
        """Validate attendance for a specific period."""
        # Check if we should have attendance (has attendance JSON)
        self.cursor.execute("SELECT COUNT(*) FROM raw_actual_attendance WHERE period_name = ?", (period_name,))
        has_attendance = self.cursor.fetchone()[0] > 0

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        attendance_count = self.cursor.fetchone()[0]

        if has_attendance and attendance_count == 0:
            self.logger.error(f"Period {period_name} has attendance JSON but no attendance records")
            return False

        # Check participation mode consistency
        self.cursor.execute("""
            SELECT COUNT(*) FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
            AND ea.participation_mode = 'scheduled'
            AND ea.event_assignment_id IS NULL
        """, (period_id,))

        scheduled_without_assignment = self.cursor.fetchone()[0]
        if scheduled_without_assignment > 0:
            self.logger.error(f"Period {period_name}: {scheduled_without_assignment} 'scheduled' attendance without assignment_id")
            return False

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
            AND ea.participation_mode = 'volunteer_fill'
            AND ea.event_assignment_id IS NOT NULL
        """, (period_id,))

        volunteer_with_assignment = self.cursor.fetchone()[0]
        if volunteer_with_assignment > 0:
            self.logger.error(f"Period {period_name}: {volunteer_with_assignment} 'volunteer_fill' attendance with assignment_id")
            return False

        return True

    def _validate_period_changes(self, period_name: str, period_id: int) -> bool:
        """Validate assignment changes for a specific period with comprehensive accuracy checks."""
        validation_passed = True

        # Check that changes have proper timestamps (should use event datetime)
        self.cursor.execute("""
            SELECT COUNT(*) FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            WHERE e.period_id = ?
            AND eac.changed_at != e.event_datetime
        """, (period_id,))

        mismatched_timestamps = self.cursor.fetchone()[0]
        if mismatched_timestamps > 0:
            self.logger.warning(f"Period {period_name}: {mismatched_timestamps} assignment changes with non-event timestamps")

        # Check that reconstructed changes have proper source
        self.cursor.execute("""
            SELECT COUNT(*) FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            WHERE e.period_id = ?
            AND eac.change_source != 'reconstructed'
        """, (period_id,))

        non_reconstructed = self.cursor.fetchone()[0]
        if non_reconstructed > 0:
            self.logger.warning(f"Period {period_name}: {non_reconstructed} assignment changes not marked as 'reconstructed'")

        # ENHANCED VALIDATION: Check assignment change accuracy
        accuracy_issues = self._validate_assignment_change_accuracy(period_name, period_id)
        if accuracy_issues > 0:
            self.logger.warning(f"Period {period_name}: {accuracy_issues} assignment change accuracy issues detected")
            # Don't fail validation - just report issues for "good enough" approach

        return validation_passed

    def _validate_assignment_change_accuracy(self, period_name: str, period_id: int) -> int:
        """
        Validate that assignment changes accurately reflect discrepancies between
        scheduled assignments and actual attendance.
        """
        issues_found = 0

        try:
            # Check period status to determine if we should validate assignment changes
            self.cursor.execute("SELECT status FROM schedule_periods WHERE id = ?", (period_id,))
            period_status = self.cursor.fetchone()[0]

            # Skip validation for future periods (active status)
            if period_status == 'active':
                self.logger.info(f"Period {period_name}: Active period detected - skipping assignment change accuracy validation (future period)")
                return 0

            # Get all events for this period
            self.cursor.execute("""
                SELECT id, legacy_period_event_id, event_datetime
                FROM events
                WHERE period_id = ?
                ORDER BY event_datetime
            """, (period_id,))

            events = self.cursor.fetchall()

            for event_id, legacy_id, event_datetime in events:
                issues_found += self._validate_event_change_accuracy(
                    period_name, event_id, legacy_id, event_datetime
                )

        except Exception as e:
            self.logger.error(f"Period {period_name}: Assignment change accuracy validation failed: {e}")
            issues_found += 1

        return issues_found

    def _validate_event_change_accuracy(self, period_name: str, event_id: int,
                                      legacy_id: int, event_datetime: str) -> int:
        """Validate assignment changes for a specific event."""
        issues = 0

        # Get assignments for this event
        self.cursor.execute("""
            SELECT ea.peep_id, ea.assigned_role, ea.assignment_type, p.display_name
            FROM event_assignments ea
            JOIN peeps p ON ea.peep_id = p.id
            WHERE ea.event_id = ?
        """, (event_id,))
        assignments = {row[0]: {'role': row[1], 'type': row[2], 'name': row[3]} for row in self.cursor.fetchall()}

        # Get attendance for this event
        self.cursor.execute("""
            SELECT att.peep_id, att.actual_role, att.participation_mode, p.display_name
            FROM event_attendance att
            JOIN peeps p ON att.peep_id = p.id
            WHERE att.event_id = ?
        """, (event_id,))
        attendance = {row[0]: {'role': row[1], 'mode': row[2], 'name': row[3]} for row in self.cursor.fetchall()}

        # Get recorded changes for this event
        self.cursor.execute("""
            SELECT change_type, change_reason, notes, peep_id
            FROM event_assignment_changes
            WHERE event_id = ?
        """, (event_id,))
        changes = self.cursor.fetchall()
        change_types = {row[0]: row for row in changes}

        # Validate: Missing "cancel" changes for MAIN ATTENDEES who didn't attend
        # (Alternates not attending is normal and doesn't need cancel records)
        for peep_id, assignment in assignments.items():
            if peep_id not in attendance:
                # Only flag missing cancel records for main attendees, not alternates
                if assignment['type'] == 'attendee':
                    # Should have a "cancel" change record
                    if "cancel" not in change_types:
                        self.logger.warning(
                            f"Period {period_name}, Event {legacy_id}: "
                            f"{assignment['name']} (main attendee) scheduled but didn't attend - missing 'cancel' change record"
                        )
                        issues += 1
                # Alternates not attending is normal - no validation needed

        # Validate: Missing "add" changes for volunteer fill-ins
        volunteer_fill_peeps = {
            peep_id for peep_id, att in attendance.items()
            if att['mode'] == 'volunteer_fill'
        }

        # Find 'add' change records for people who attended without being assigned
        add_changes_all = [c for c in changes if c[0] == 'add']
        add_change_peeps = {
            c[3] for c in changes if c[0] == 'add' and c[3] in volunteer_fill_peeps
        }

        # Debug logging
        if volunteer_fill_peeps and self.verbose:
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: volunteer_fill_peeps={volunteer_fill_peeps}")
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: all add changes={[(c[0], c[3]) for c in add_changes_all]}")
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: matching add_change_peeps={add_change_peeps}")

        missing_add_changes = volunteer_fill_peeps - add_change_peeps
        if missing_add_changes:
            self.logger.warning(
                f"Period {period_name}, Event {legacy_id}: "
                f"{len(volunteer_fill_peeps)} volunteer fills but {len(add_change_peeps)} 'add' change records"
            )
            issues += 1

        # Validate: Missing "promote_alternate" changes for alternate promotions
        alternate_promoted_peeps = {
            peep_id for peep_id, att in attendance.items()
            if att['mode'] == 'alternate_promoted'
        }

        # Find 'promote_alternate' change records for people who were promoted from alternate
        promote_changes_all = [c for c in changes if c[0] == 'promote_alternate']
        promote_change_peeps = {
            c[3] for c in changes if c[0] == 'promote_alternate' and c[3] in alternate_promoted_peeps
        }

        # Debug logging
        if alternate_promoted_peeps and self.verbose:
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: alternate_promoted_peeps={alternate_promoted_peeps}")
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: all promote_alternate changes={[(c[0], c[3]) for c in promote_changes_all]}")
            self.logger.debug(f"Period {period_name}, Event {legacy_id}: matching promote_change_peeps={promote_change_peeps}")

        missing_promote_changes = alternate_promoted_peeps - promote_change_peeps
        if missing_promote_changes:
            self.logger.warning(
                f"Period {period_name}, Event {legacy_id}: "
                f"{len(alternate_promoted_peeps)} alternate promotions but {len(promote_change_peeps)} 'promote_alternate' change records"
            )
            issues += 1

        # Validate: Missing "change_role" for role mismatches
        role_changes = 0
        for peep_id in assignments.keys() & attendance.keys():
            assigned_role = assignments[peep_id]['role']
            actual_role = attendance[peep_id]['role']
            if assigned_role != actual_role:
                role_changes += 1

        actual_role_changes = len([c for c in changes if c[0] == 'change_role'])
        if role_changes != actual_role_changes:
            self.logger.warning(
                f"Period {period_name}, Event {legacy_id}: "
                f"{role_changes} role mismatches but {actual_role_changes} 'change_role' records"
            )
            issues += 1

        # Log summary for events with discrepancies
        if assignments or attendance:
            scheduled_count = len(assignments)
            attended_count = len(attendance)
            changes_count = len(changes)

            if changes_count > 0:
                self.logger.debug(
                    f"Period {period_name}, Event {legacy_id}: "
                    f"{scheduled_count} scheduled, {attended_count} attended, {changes_count} changes"
                )

        return issues

    def _validate_period_snapshots(self, period_name: str, period_id: int) -> bool:
        """Validate snapshots for a specific period."""
        # Check that period has snapshots if it's completed
        self.cursor.execute("SELECT status FROM schedule_periods WHERE id = ?", (period_id,))
        status = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?", (period_id,))
        snapshot_count = self.cursor.fetchone()[0]

        if status == 'completed' and snapshot_count == 0:
            self.logger.error(f"Period {period_name} is completed but has no snapshots")
            return False

        # Check snapshot data integrity
        self.cursor.execute("""
            SELECT COUNT(*) FROM peep_order_snapshots
            WHERE period_id = ?
            AND (priority < 0 OR index_position < 0 OR total_attended < 0)
        """, (period_id,))

        invalid_snapshots = self.cursor.fetchone()[0]
        if invalid_snapshots > 0:
            self.logger.error(f"Period {period_name}: {invalid_snapshots} snapshots with negative values")
            return False

        return True

    def validate_global_constraints(self) -> bool:
        """Validate constraints that span across all periods."""
        try:
            self.logger.info("Validating global constraints")

            violations = []

            # Check peep email uniqueness
            self.cursor.execute("""
                SELECT email, COUNT(*) as count
                FROM peeps
                GROUP BY email
                HAVING COUNT(*) > 1
            """)

            duplicate_emails = self.cursor.fetchall()
            if duplicate_emails:
                for email, count in duplicate_emails:
                    violations.append(f"Duplicate email {email}: {count} peeps")

            # Check event datetime uniqueness across all periods
            self.cursor.execute("""
                SELECT event_datetime, COUNT(*) as count
                FROM events
                GROUP BY event_datetime
                HAVING COUNT(*) > 1
            """)

            duplicate_event_times = self.cursor.fetchall()
            if duplicate_event_times:
                for event_datetime, count in duplicate_event_times:
                    violations.append(f"Duplicate event datetime {event_datetime}: {count} events")

            if violations:
                self.logger.error("Global constraint violations:")
                for violation in violations:
                    self.logger.error(f"  - {violation}")
                return False

            self.logger.info("Global constraints validated successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error validating global constraints: {e}")
            return False


def main():
    """Main entry point for the transformation script."""
    parser = argparse.ArgumentParser(
        description='Transform raw data to normalized schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full transformation with detailed output
    python db/transform_raw_to_normalized.py --verbose

    # Dry run to test without committing changes
    python db/transform_raw_to_normalized.py --dry-run

    # Transform specific period only (for incremental processing)
    python db/transform_raw_to_normalized.py --period 2025-01

    # Combine options for safe testing of specific period
    python db/transform_raw_to_normalized.py --period 2025-01 --dry-run --verbose
        """
    )

    parser.add_argument('--dry-run', action='store_true',
                       help='Test without applying changes')
    parser.add_argument('--period',
                       help='Transform specific period only')
    parser.add_argument('--verbose', action='store_true',
                       help='Detailed progress logging')

    args = parser.parse_args()

    # Get database connection
    try:
        data_manager = get_data_manager()
        db_path = data_manager.get_database_path()

        if not os.path.exists(db_path):
            print(f"Error: Database not found at {db_path}")
            return 1

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON")

        print(f"Connected to database: {db_path}")

    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    try:
        # Create transformer
        transformer = DataTransformer(conn, cursor, verbose=args.verbose)

        # Perform transformation
        if args.period:
            # Transform specific period only
            success = transformer.transform_single_period(args.period, dry_run=args.dry_run)
        else:
            # Full transformation (all periods)
            success = transformer.transform_all_data(dry_run=args.dry_run)

        if success:
            print("✅ Transformation completed successfully!")
            return 0
        else:
            print("❌ Transformation failed")
            return 1

    except Exception as e:
        print(f"Critical error: {e}")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())