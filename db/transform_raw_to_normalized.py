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
    - Legacy period creation from first available raw data
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_manager import get_data_manager
from models import SwitchPreference
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
        self.event_id_mapping = {}  # (period_name, legacy_id) -> database_id

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

            # Create legacy period first
            self.logger.info("Creating legacy period from baseline data")
            legacy_success = self._transform_legacy_period(periods[0])
            if not legacy_success:
                self.logger.error("Failed to create legacy period")
                return False

            # Validate legacy period immediately
            validator = DataValidator(self.conn, self.cursor, self.verbose)
            if not validator.validate_legacy_period():
                self.logger.error("Legacy period validation failed")
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

    def _transform_legacy_period(self, first_period: str) -> bool:
        """
        Create legacy period and synthetic events from first period's baseline data.

        Args:
            first_period: The chronologically first period to use as baseline

        Returns:
            True if successful
        """
        try:
            self.logger.info(f"Creating legacy period from {first_period} baseline data")

            # First, transform peeps from the first period
            if not self._transform_peeps():
                return False

            # Get baseline member data
            self.cursor.execute("""
                SELECT csv_id, Name, "Display Name", Role, "Email Address",
                       "Index", Priority, "Total Attended", Active, "Date Joined"
                FROM raw_members
                WHERE period_name = ?
                ORDER BY CAST(csv_id AS INTEGER)
            """, (first_period,))

            baseline_members = self.cursor.fetchall()
            if not baseline_members:
                self.logger.error(f"No baseline members found for period {first_period}")
                return False

            # Determine max total_attended for synthetic events
            max_attended = max(member[7] or 0 for member in baseline_members)
            self.logger.info(f"Creating {max_attended} synthetic events for legacy period")

            # Create legacy schedule period
            legacy_date = datetime(2024, 1, 1)  # Arbitrary early date
            self.cursor.execute("""
                INSERT INTO schedule_periods (
                    period_name, display_name, start_date, end_date, status
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                "legacy",
                "Legacy Baseline",
                legacy_date.date(),
                legacy_date.date(),
                "completed"
            ))

            legacy_period_id = self.cursor.lastrowid
            self.period_id_mapping["legacy"] = legacy_period_id

            # Create synthetic events (2-hour blocks on same day)
            synthetic_events = []
            for i in range(max_attended):
                event_time = legacy_date + timedelta(hours=i * 2)

                self.cursor.execute("""
                    INSERT INTO events (
                        period_id, legacy_period_event_id, event_datetime,
                        duration_minutes, status
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    legacy_period_id, i, event_time, 120, "completed"
                ))

                event_id = self.cursor.lastrowid
                synthetic_events.append({
                    'id': event_id,
                    'legacy_id': i,
                    'datetime': event_time
                })
                self.event_id_mapping[("legacy", i)] = event_id

            # Create synthetic assignments and attendance
            for member in baseline_members:
                csv_id, name, display_name, role, email, index_pos, priority, total_attended, active, date_joined = member

                if not total_attended or total_attended == 0:
                    continue

                peep_id = self.peep_id_mapping.get(csv_id)
                if not peep_id:
                    self.logger.warning(f"No peep_id found for csv_id {csv_id}")
                    continue

                # Create assignments and attendance for their total_attended count
                for event_idx in range(min(total_attended, max_attended)):
                    event = synthetic_events[event_idx]

                    # Create assignment
                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type, assignment_order
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        event['id'], peep_id, role.lower(), "attendee", event_idx + 1
                    ))

                    assignment_id = self.cursor.lastrowid

                    # Create attendance
                    self.cursor.execute("""
                        INSERT INTO event_attendance (
                            event_id, peep_id, event_assignment_id, expected_role,
                            expected_type, actual_role, attendance_status, participation_mode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event['id'], peep_id, assignment_id, role.lower(),
                        "attendee", role.lower(), "attended", "scheduled"
                    ))

            # Create legacy snapshot
            self._generate_legacy_snapshot(legacy_period_id, baseline_members)

            self.logger.info("Successfully created legacy period with synthetic events")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create legacy period: {e}")
            return False

    def _transform_peeps(self) -> bool:
        """Transform raw_members data into normalized peeps table with CSV ID preservation."""
        try:
            self.logger.info("Transforming peeps with CSV ID preservation")

            # Get unique members across all periods, ordered by CSV ID
            self.cursor.execute("""
                SELECT csv_id, Name, "Display Name", Role, "Email Address",
                       "Date Joined", Active,
                       MIN(imported_at) as first_seen
                FROM raw_members
                WHERE csv_id IS NOT NULL
                GROUP BY "Email Address"
                ORDER BY CAST(csv_id AS INTEGER)
            """)

            unique_members = self.cursor.fetchall()
            self.logger.info(f"Found {len(unique_members)} unique members to transform")

            for member in unique_members:
                csv_id, name, display_name, role, email, date_joined, active, first_seen = member

                # Convert active text to boolean
                active_bool = active.lower() in ['true', 'yes', '1'] if active else True

                # Parse date_joined if available
                joined_date = None
                if date_joined:
                    try:
                        joined_date = datetime.strptime(date_joined, "%Y-%m-%d").date()
                    except:
                        joined_date = None

                # Insert peep (AUTOINCREMENT will preserve CSV ID order)
                self.cursor.execute("""
                    INSERT INTO peeps (
                        full_name, display_name, primary_role, email,
                        date_joined, active
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    name, display_name, role.lower(), email, joined_date, active_bool
                ))

                peep_id = self.cursor.lastrowid
                self.peep_id_mapping[csv_id] = peep_id

                if self.verbose:
                    self.logger.debug(f"Transformed peep: CSV ID {csv_id} -> DB ID {peep_id} ({name})")

            self.logger.info(f"Successfully transformed {len(unique_members)} peeps")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform peeps: {e}")
            return False

    def _generate_legacy_snapshot(self, legacy_period_id: int, baseline_members: List[Tuple]) -> bool:
        """Generate snapshot for legacy period from baseline member data."""
        try:
            for member in baseline_members:
                csv_id, name, display_name, role, email, index_pos, priority, total_attended, active, date_joined = member

                peep_id = self.peep_id_mapping.get(csv_id)
                if not peep_id:
                    continue

                # Convert active text to boolean
                active_bool = active.lower() in ['true', 'yes', '1'] if active else True

                self.cursor.execute("""
                    INSERT INTO peep_order_snapshots (
                        peep_id, period_id, priority, index_position,
                        total_attended, active, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    peep_id, legacy_period_id,
                    priority or 0, index_pos or 0,
                    total_attended or 0, active_bool,
                    "Legacy baseline snapshot from raw member data"
                ))

            self.logger.info(f"Generated legacy snapshots for {len(baseline_members)} members")
            return True

        except Exception as e:
            self.logger.error(f"Failed to generate legacy snapshot: {e}")
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

            # 2. Transform events
            events = self._transform_events(period_name, period_id)
            if events is None:
                return False

            # 3. Transform responses
            responses = self._transform_responses(period_name, period_id)
            if responses is None:
                return False

            # 4. Transform event availability
            if not self._transform_event_availability(responses, events):
                return False

            # 5. Transform assignments from scheduler results JSON
            assignments = self._transform_assignments(period_name, events)
            if assignments is None:
                return False

            # 6. Transform attendance from actual attendance JSON
            attendance = self._transform_attendance(period_name, events)
            if attendance is None:
                return False

            # 7. STRICT VALIDATION: Ensure all assignments have attendance
            if assignments and not attendance:
                raise ValueError(
                    f"Period {period_name} has assignments but no attendance data. "
                    f"Manual reconstruction required before transformation."
                )

            # 8. Reconcile and create change records
            if not self._reconcile_assignments_vs_attendance(assignments, attendance, events):
                return False

            # 9. Generate period snapshot
            if not self._generate_period_snapshot(period_name, period_id):
                return False

            self.logger.info(f"Successfully transformed period: {period_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform period {period_name}: {e}")
            return False

    def _create_schedule_period(self, period_name: str) -> Optional[int]:
        """Create schedule_periods record for the given period."""
        try:
            # Determine period dates from events if possible
            start_date = datetime(2024, 1, 1).date()  # Default fallback
            end_date = start_date

            # Check if we have event data to determine actual dates
            self.cursor.execute("""
                SELECT MIN(response_timestamp), MAX(response_timestamp)
                FROM raw_responses
                WHERE period_name = ? AND response_timestamp IS NOT NULL
            """, (period_name,))

            result = self.cursor.fetchone()
            if result and result[0]:
                try:
                    start_date = datetime.fromisoformat(result[0]).date()
                    end_date = datetime.fromisoformat(result[1]).date() if result[1] else start_date
                except:
                    pass

            # Determine display name
            display_name = f"Period {period_name}"

            # Determine status (completed if we have results/attendance)
            status = "completed"
            self.cursor.execute("""
                SELECT COUNT(*) FROM raw_results WHERE period_name = ?
                UNION ALL
                SELECT COUNT(*) FROM raw_actual_attendance WHERE period_name = ?
            """, (period_name, period_name))

            results = self.cursor.fetchall()
            if not any(count[0] > 0 for count in results):
                status = "draft"

            self.cursor.execute("""
                INSERT INTO schedule_periods (
                    period_name, display_name, start_date, end_date, status
                ) VALUES (?, ?, ?, ?, ?)
            """, (period_name, display_name, start_date, end_date, status))

            period_id = self.cursor.lastrowid
            self.period_id_mapping[period_name] = period_id

            self.logger.info(f"Created schedule period {period_name} with ID {period_id}")
            return period_id

        except Exception as e:
            self.logger.error(f"Failed to create schedule period {period_name}: {e}")
            return None

    def _transform_events(self, period_name: str, period_id: int) -> Optional[List[Dict]]:
        """Transform events from response availability data."""
        try:
            # Extract unique event datetimes from availability strings
            self.cursor.execute("""
                SELECT DISTINCT Availability
                FROM raw_responses
                WHERE period_name = ? AND Availability IS NOT NULL AND Availability != ''
            """, (period_name,))

            availability_strings = [row[0] for row in self.cursor.fetchall()]
            if not availability_strings:
                self.logger.warning(f"No availability data found for period {period_name}")
                return []

            # Parse all availability strings to extract unique events
            unique_events = set()
            for avail_str in availability_strings:
                try:
                    events = avail_str.split(',')
                    for event in events:
                        event = event.strip()
                        if event:
                            try:
                                event_datetime = parse_event_date(event)
                                unique_events.add(event_datetime)
                            except Exception as e:
                                self.logger.warning(f"Could not parse event date '{event}': {e}")
                except Exception as e:
                    self.logger.warning(f"Could not parse availability string '{avail_str}': {e}")

            if not unique_events:
                self.logger.warning(f"No valid events found for period {period_name}")
                return []

            # Sort events chronologically and create database records
            sorted_events = sorted(unique_events)
            created_events = []

            for legacy_id, event_datetime in enumerate(sorted_events):
                self.cursor.execute("""
                    INSERT INTO events (
                        period_id, legacy_period_event_id, event_datetime,
                        duration_minutes, status
                    ) VALUES (?, ?, ?, ?, ?)
                """, (period_id, legacy_id, event_datetime, 90, "completed"))

                event_id = self.cursor.lastrowid
                event_data = {
                    'id': event_id,
                    'period_id': period_id,
                    'legacy_id': legacy_id,
                    'datetime': event_datetime
                }
                created_events.append(event_data)
                self.event_id_mapping[(period_name, legacy_id)] = event_id

            self.logger.info(f"Created {len(created_events)} events for period {period_name}")
            return created_events

        except Exception as e:
            self.logger.error(f"Failed to transform events for period {period_name}: {e}")
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

                # Find peep_id by email
                peep_id = None
                for csv_id, db_id in self.peep_id_mapping.items():
                    self.cursor.execute("""
                        SELECT id FROM peeps WHERE id = ? AND email = ?
                    """, (db_id, email))
                    if self.cursor.fetchone():
                        peep_id = db_id
                        break

                if not peep_id:
                    self.logger.warning(f"No peep found for email {email} in period {period_name}")
                    continue

                # Parse response fields
                response_role = (primary_role or "follower").lower()
                switch_pref = SwitchPreference.from_string(secondary_role or "no").value
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

    def _transform_event_availability(self, responses: List[Dict], events: List[Dict]) -> bool:
        """Create event_availability many-to-many relationships from response availability strings."""
        try:
            if not responses or not events:
                self.logger.warning("No responses or events to process for availability")
                return True

            # Create event datetime lookup
            event_datetime_to_id = {event['datetime']: event['id'] for event in events}

            # Process each response's availability string
            for response in responses:
                response_id = response['id']
                peep_id = response['peep_id']

                # Get the availability string for this response
                self.cursor.execute("""
                    SELECT Availability FROM raw_responses
                    WHERE period_id = ? AND "Email Address" IN (
                        SELECT email FROM peeps WHERE id = ?
                    )
                    LIMIT 1
                """, (response['period_id'], peep_id))

                result = self.cursor.fetchone()
                if not result or not result[0]:
                    continue

                availability_string = result[0]

                # Parse availability string
                try:
                    available_events = availability_string.split(',')
                    for event_str in available_events:
                        event_str = event_str.strip()
                        if event_str:
                            try:
                                event_datetime = parse_event_date(event_str)
                                event_id = event_datetime_to_id.get(event_datetime)

                                if event_id:
                                    self.cursor.execute("""
                                        INSERT INTO event_availability (response_id, event_id)
                                        VALUES (?, ?)
                                    """, (response_id, event_id))
                                else:
                                    self.logger.warning(f"Event datetime {event_datetime} not found in events")

                            except Exception as e:
                                self.logger.warning(f"Could not parse event date '{event_str}': {e}")

                except Exception as e:
                    self.logger.warning(f"Could not parse availability string '{availability_string}': {e}")

            self.logger.info(f"Created event availability relationships for {len(responses)} responses")
            return True

        except Exception as e:
            self.logger.error(f"Failed to transform event availability: {e}")
            return False

    def _transform_assignments(self, period_name: str, events: List[Dict]) -> Optional[List[Dict]]:
        """Transform scheduler results JSON into normalized assignments."""
        try:
            # Get results JSON for this period
            self.cursor.execute("""
                SELECT results_json FROM raw_results WHERE period_name = ?
            """, (period_name,))

            result = self.cursor.fetchone()
            if not result or not result[0]:
                self.logger.info(f"No scheduler results found for period {period_name}")
                return []

            # Parse JSON
            try:
                results_data = json.loads(result[0])
            except Exception as e:
                self.logger.error(f"Could not parse results JSON for period {period_name}: {e}")
                return None

            # Create event lookup by legacy ID
            event_lookup = {event['legacy_id']: event for event in events}

            assignments = []

            # Process each valid_event in the results
            valid_events = results_data.get('valid_events', [])
            for valid_event in valid_events:
                legacy_event_id = valid_event.get('id')
                event_data = event_lookup.get(legacy_event_id)

                if not event_data:
                    self.logger.warning(f"Event with legacy ID {legacy_event_id} not found")
                    continue

                event_id = event_data['id']

                # Process attendees
                attendees = valid_event.get('attendees', [])
                for idx, attendee in enumerate(attendees):
                    member_id = attendee.get('member_id')
                    role = attendee.get('role', 'follower').lower()

                    # Find peep_id from member_id
                    peep_id = self.peep_id_mapping.get(str(member_id))
                    if not peep_id:
                        self.logger.warning(f"Peep not found for member_id {member_id}")
                        continue

                    # Create assignment
                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type,
                            assignment_order
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_id, peep_id, role, "attendee", idx + 1))

                    assignments.append({
                        'id': self.cursor.lastrowid,
                        'event_id': event_id,
                        'peep_id': peep_id,
                        'role': role,
                        'type': 'attendee',
                        'order': idx + 1
                    })

                # Process alternates if they exist
                alternates = valid_event.get('alternates', [])
                for idx, alternate in enumerate(alternates):
                    member_id = alternate.get('member_id')
                    role = alternate.get('role', 'follower').lower()

                    peep_id = self.peep_id_mapping.get(str(member_id))
                    if not peep_id:
                        self.logger.warning(f"Peep not found for alternate member_id {member_id}")
                        continue

                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type,
                            alternate_position
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_id, peep_id, role, "alternate", idx + 1))

                    assignments.append({
                        'id': self.cursor.lastrowid,
                        'event_id': event_id,
                        'peep_id': peep_id,
                        'role': role,
                        'type': 'alternate',
                        'position': idx + 1
                    })

            self.logger.info(f"Created {len(assignments)} assignments for period {period_name}")
            return assignments

        except Exception as e:
            self.logger.error(f"Failed to transform assignments for period {period_name}: {e}")
            return None

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
            event_lookup = {event['legacy_id']: event for event in events}

            attendance_records = []

            # Process each valid_event in the attendance
            valid_events = attendance_data.get('valid_events', [])
            for valid_event in valid_events:
                legacy_event_id = valid_event.get('id')
                event_data = event_lookup.get(legacy_event_id)

                if not event_data:
                    self.logger.warning(f"Event with legacy ID {legacy_event_id} not found for attendance")
                    continue

                event_id = event_data['id']

                # Process attendees who actually attended
                attendees = valid_event.get('attendees', [])
                for attendee in attendees:
                    member_id = attendee.get('member_id')
                    role = attendee.get('role', 'follower').lower()

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
                        expected_type, role, "attended", participation_mode
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
            return attendance_records

        except Exception as e:
            self.logger.error(f"Failed to transform attendance for period {period_name}: {e}")
            return None

    def _reconcile_assignments_vs_attendance(self, assignments: List[Dict], attendance: List[Dict], events: List[Dict]) -> bool:
        """Create event_assignment_changes for all discrepancies between scheduled vs actual."""
        try:
            if not assignments and not attendance:
                self.logger.info("No assignments or attendance to reconcile")
                return True

            changes_created = 0

            for event in events:
                event_id = event['id']
                event_datetime = event['datetime']

                # Get assignments and attendance for this event
                event_assignments = [a for a in assignments if a['event_id'] == event_id]
                event_attendance = [a for a in attendance if a['event_id'] == event_id]

                # Check each assignment against attendance
                for assignment in event_assignments:
                    peep_id = assignment['peep_id']
                    assigned_role = assignment['role']

                    # Find if this person attended
                    attended = next((a for a in event_attendance if a['peep_id'] == peep_id), None)

                    if not attended:
                        # Scheduled but didn't attend
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, change_type, change_source, change_reason,
                                changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            event_id, "cancel", "reconstructed", "no_show_reconstructed",
                            event_datetime, "Reconstructed: Scheduled but no attendance record"
                        ))
                        changes_created += 1

                    elif attended['actual_role'] != assigned_role:
                        # Role change
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, change_type, change_source, changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            event_id, "change_role", "reconstructed", event_datetime,
                            f"Reconstructed: Role changed from {assigned_role} to {attended['actual_role']}"
                        ))
                        changes_created += 1

                # Check for attendees not in assignments (volunteer fill-ins)
                for attended in event_attendance:
                    if attended['participation_mode'] == 'volunteer_fill':
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, change_type, change_source, change_reason,
                                changed_at, notes
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            event_id, "add", "reconstructed", "volunteer_fill",
                            event_datetime, "Reconstructed: Attended without assignment"
                        ))
                        changes_created += 1

            self.logger.info(f"Created {changes_created} assignment change records")
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

    def validate_legacy_period(self) -> bool:
        """Validate the legacy period transformation specifically."""
        try:
            self.logger.info("Validating legacy period transformation")

            # Check legacy period exists
            self.cursor.execute("SELECT id FROM schedule_periods WHERE period_name = 'legacy'")
            legacy_result = self.cursor.fetchone()
            if not legacy_result:
                self.logger.error("Legacy period not found")
                return False

            legacy_period_id = legacy_result[0]

            # Check synthetic events exist
            self.cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (legacy_period_id,))
            event_count = self.cursor.fetchone()[0]
            if event_count == 0:
                self.logger.error("Legacy period has no synthetic events")
                return False

            # Check all synthetic events are 2-hour blocks on same day
            self.cursor.execute("""
                SELECT event_datetime, duration_minutes
                FROM events
                WHERE period_id = ?
                ORDER BY event_datetime
            """, (legacy_period_id,))

            events = self.cursor.fetchall()
            if events:
                base_date = events[0][0].split()[0]  # Get date part
                for i, (event_datetime, duration) in enumerate(events):
                    if duration != 120:
                        self.logger.error(f"Legacy event {i} has duration {duration}, expected 120 minutes")
                        return False

                    event_date = event_datetime.split()[0]
                    if event_date != base_date:
                        self.logger.error(f"Legacy event {i} on different date than expected")
                        return False

            # Check snapshots exist for legacy period
            self.cursor.execute("SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?", (legacy_period_id,))
            snapshot_count = self.cursor.fetchone()[0]
            if snapshot_count == 0:
                self.logger.error("Legacy period has no snapshots")
                return False

            # Check assignments and attendance are consistent (no changes expected for synthetic data)
            self.cursor.execute("""
                SELECT COUNT(*) FROM event_assignment_changes eac
                JOIN events e ON eac.event_id = e.id
                WHERE e.period_id = ?
            """, (legacy_period_id,))

            change_count = self.cursor.fetchone()[0]
            if change_count > 0:
                self.logger.warning(f"Legacy period has {change_count} assignment changes (unexpected for synthetic data)")

            self.logger.info(f"Legacy period validation passed: {event_count} events, {snapshot_count} snapshots")
            return True

        except Exception as e:
            self.logger.error(f"Error validating legacy period: {e}")
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
        # Check that responses have corresponding availability records
        self.cursor.execute("""
            SELECT COUNT(*) FROM responses r
            WHERE r.period_id = ?
            AND NOT EXISTS (
                SELECT 1 FROM event_availability ea WHERE ea.response_id = r.id
            )
        """, (period_id,))

        responses_without_availability = self.cursor.fetchone()[0]
        if responses_without_availability > 0:
            self.logger.warning(f"Period {period_name}: {responses_without_availability} responses without availability records")

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
        """Validate assignment changes for a specific period."""
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

        return True

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

    def validate_snapshot_progression(self) -> bool:
        """
        Validate that snapshots can be recreated from previous snapshots + attendance.

        This validates: snapshot[period N] + events_since_snapshot = raw_members[period N+1]

        Since we don't know exactly when the next period was scheduled, we try generating
        a snapshot after each event to see if any match the target raw_members state.
        """
        try:
            self.logger.info("Validating snapshot progression across periods")

            # Get all periods with snapshots (excluding legacy)
            self.cursor.execute("""
                SELECT sp.id, sp.period_name, sp.start_date
                FROM schedule_periods sp
                WHERE sp.period_name != 'legacy'
                AND EXISTS (SELECT 1 FROM peep_order_snapshots pos WHERE pos.period_id = sp.id)
                ORDER BY sp.period_name
            """)

            periods_with_snapshots = self.cursor.fetchall()
            if len(periods_with_snapshots) < 2:
                self.logger.warning("Need at least 2 periods with snapshots for progression validation")
                return True

            generator = SnapshotGenerator(verbose=self.verbose)
            validation_passed = True

            # Validate progression between consecutive periods
            for i in range(len(periods_with_snapshots) - 1):
                current_period = periods_with_snapshots[i]
                next_period = periods_with_snapshots[i + 1]

                current_period_id, current_period_name, _ = current_period
                next_period_id, next_period_name, _ = next_period

                self.logger.info(f"Validating progression: {current_period_name} → {next_period_name}")

                # Get starting snapshot from current period
                starting_snapshot = generator.snapshot_from_database(self.cursor, current_period_id)
                if not starting_snapshot:
                    self.logger.error(f"No starting snapshot found for period {current_period_name}")
                    validation_passed = False
                    continue

                # Get target snapshot from raw_members of next period
                target_snapshot = generator.snapshot_from_raw_members(self.cursor, next_period_name)
                if not target_snapshot:
                    self.logger.error(f"No target raw_members found for period {next_period_name}")
                    validation_passed = False
                    continue

                # Fill in peep_ids for target snapshot
                peep_id_mapping = {}
                for member in target_snapshot:
                    self.cursor.execute("""
                        SELECT id FROM peeps WHERE email = ?
                    """, (member.email,))
                    result = self.cursor.fetchone()
                    if result:
                        member.peep_id = result[0]
                        peep_id_mapping[member.email] = result[0]

                # Try to find the event cutoff that recreates the target snapshot
                match_found = self._try_event_cutoffs(
                    generator, current_period_name, next_period_name,
                    starting_snapshot, target_snapshot, peep_id_mapping
                )

                if not match_found:
                    self.logger.error(f"Could not recreate {next_period_name} snapshot from {current_period_name}")
                    validation_passed = False
                else:
                    self.logger.info(f"✅ Successfully validated progression: {current_period_name} → {next_period_name}")

            if validation_passed:
                self.logger.info("🎉 All snapshot progressions validated successfully")
            else:
                self.logger.error("💥 Some snapshot progressions failed validation")

            return validation_passed

        except Exception as e:
            self.logger.error(f"Error validating snapshot progression: {e}")
            return False

    def _try_event_cutoffs(
        self,
        generator: SnapshotGenerator,
        current_period_name: str,
        next_period_name: str,
        starting_snapshot: List[MemberSnapshot],
        target_snapshot: List[MemberSnapshot],
        peep_id_mapping: Dict[str, int]
    ) -> bool:
        """
        Try different event cutoff points to see if we can recreate the target snapshot.

        Returns True if a matching cutoff point is found.
        """
        try:
            # Get all events from current period, ordered chronologically
            self.cursor.execute("""
                SELECT e.id, e.event_datetime, e.legacy_period_event_id
                FROM events e
                JOIN schedule_periods sp ON e.period_id = sp.id
                WHERE sp.period_name = ?
                ORDER BY e.event_datetime
            """, (current_period_name,))

            period_events = self.cursor.fetchall()
            if not period_events:
                self.logger.warning(f"No events found for period {current_period_name}")
                return False

            self.logger.info(f"Trying {len(period_events)} different event cutoff points")

            # Try each possible cutoff point (0 events, 1 event, 2 events, etc.)
            for cutoff in range(len(period_events) + 1):
                try:
                    # Split events into actual (completed) and expected (scheduled but not completed)
                    completed_events = period_events[:cutoff]
                    scheduled_events = period_events[cutoff:]

                    # Get actual attendance for completed events
                    actual_attendance = self._get_attendance_for_events(
                        [e[0] for e in completed_events], 'actual', peep_id_mapping
                    )

                    # Get expected attendance (assignments) for scheduled events
                    expected_attendance = self._get_attendance_for_events(
                        [e[0] for e in scheduled_events], 'expected', peep_id_mapping
                    )

                    # Get who responded for this period
                    responded_peep_ids = self._get_responded_peep_ids(next_period_name, peep_id_mapping)

                    # Generate snapshot using this cutoff
                    generated_snapshot = generator.generate_snapshot_from_attendance(
                        starting_snapshot, actual_attendance, expected_attendance, responded_peep_ids
                    )

                    # Compare with target
                    is_match, differences = generator.compare_snapshots(
                        generated_snapshot, target_snapshot, tolerance=0
                    )

                    if is_match:
                        self.logger.info(f"🎯 Found matching cutoff at {cutoff}/{len(period_events)} events completed")
                        return True
                    else:
                        if self.verbose:
                            self.logger.debug(f"Cutoff {cutoff}: {len(differences)} differences")
                            for diff in differences[:5]:  # Show first 5 differences
                                self.logger.debug(f"  - {diff}")

                except Exception as e:
                    self.logger.warning(f"Error testing cutoff {cutoff}: {e}")
                    continue

            self.logger.error(f"No matching cutoff found for {current_period_name} → {next_period_name}")
            return False

        except Exception as e:
            self.logger.error(f"Error trying event cutoffs: {e}")
            return False

    def _get_attendance_for_events(
        self,
        event_ids: List[int],
        attendance_type: str,
        peep_id_mapping: Dict[str, int]
    ) -> List[EventAttendance]:
        """Get attendance records for specified events."""
        if not event_ids:
            return []

        attendance_records = []

        for event_id in event_ids:
            if attendance_type == 'actual':
                # Get actual attendance
                self.cursor.execute("""
                    SELECT ea.peep_id, ea.actual_role, e.event_datetime
                    FROM event_attendance ea
                    JOIN events e ON ea.event_id = e.id
                    WHERE ea.event_id = ? AND ea.attendance_status = 'attended'
                """, (event_id,))

                for peep_id, role, event_datetime in self.cursor.fetchall():
                    attendance_records.append(EventAttendance(
                        event_id=event_id,
                        peep_id=peep_id,
                        role=role,
                        attendance_type='actual',
                        event_datetime=datetime.fromisoformat(event_datetime) if isinstance(event_datetime, str) else event_datetime
                    ))

            elif attendance_type == 'expected':
                # Get assignments (expected attendance)
                self.cursor.execute("""
                    SELECT eas.peep_id, eas.assigned_role, e.event_datetime
                    FROM event_assignments eas
                    JOIN events e ON eas.event_id = e.id
                    WHERE eas.event_id = ? AND eas.assignment_type = 'attendee'
                """, (event_id,))

                for peep_id, role, event_datetime in self.cursor.fetchall():
                    attendance_records.append(EventAttendance(
                        event_id=event_id,
                        peep_id=peep_id,
                        role=role,
                        attendance_type='expected',
                        event_datetime=datetime.fromisoformat(event_datetime) if isinstance(event_datetime, str) else event_datetime
                    ))

        return attendance_records

    def _get_responded_peep_ids(self, period_name: str, peep_id_mapping: Dict[str, int]) -> Set[int]:
        """Get set of peep IDs who responded for the given period."""
        self.cursor.execute("""
            SELECT DISTINCT r.peep_id
            FROM responses r
            JOIN schedule_periods sp ON r.period_id = sp.id
            WHERE sp.period_name = ?
        """, (period_name,))

        return {row[0] for row in self.cursor.fetchall()}


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
                       help='Transform specific period only (skips legacy period creation)')
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