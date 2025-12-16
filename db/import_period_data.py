#!/usr/bin/env python3
"""
Consolidated CSV to Database Importer

Imports historical CSV/JSON files directly to normalized database schema,
skipping raw table intermediary. Fixes peep_id_mapping bug by ensuring
ALL members are tracked in a complete mapping.

Architecture:
    Phase 1: Member Identity Collection
        - Scan all periods chronologically
        - Build complete member roster from first appearances
        - Determine date_joined from first CSV appearance
        - Insert into peeps table with preserved CSV IDs
        - Build comprehensive peep_id_mapping for ALL members

    Phase 2: Period Processing (Sequential)
        - For each period in chronological order:
            - Import responses and availability
            - Create events and assignments
            - Import actual attendance
            - Derive assignment changes (scheduled vs actual)
            - Calculate snapshots from attendance data

Usage:
    python db/import_period_data.py --all                    # Import all periods
    python db/import_period_data.py --period 2025-02         # Import single period
    python db/import_period_data.py --period 2025-02 --dry-run  # Test without commit
    python db/import_period_data.py --validate-only          # Validate without import
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import existing utilities
from file_io import (
    load_csv,
    load_json,
    normalize_email,
    parse_event_date,
    load_responses
)
from models import Role, SwitchPreference
import constants
from data_manager import get_data_manager

# Import snapshot generator
from db.snapshot_generator import SnapshotGenerator, MemberSnapshot, EventAttendance

# Database path
DB_PATH = constants.DEFAULT_DB_PATH
data_manager = get_data_manager()
PROCESSED_DATA_PATH = data_manager.get_processed_data_path()


# ============================================================================
# Phase 1: Member Identity Collection
# ============================================================================

class MemberCollector:
    """
    Scans all periods to build complete member roster with date_joined tracking.

    Fixes the peep_id_mapping bug by ensuring ALL members from ALL periods
    are added to the mapping, preventing silent skips during snapshot generation.
    """

    def __init__(self, processed_data_path: Path, verbose: bool = False):
        self.processed_data_path = processed_data_path
        self.verbose = verbose
        self.logger = self._setup_logging()

        # Member tracking: csv_id -> member data
        self.members: Dict[str, Dict] = {}

        # First appearance tracking: csv_id -> (period_name, date)
        self.first_appearances: Dict[str, Tuple[str, date]] = {}

        # Mapping after DB insertion: csv_id -> database peep_id
        self.peep_id_mapping: Dict[str, int] = {}

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for member collection."""
        from logging_config import get_logger
        level = 'DEBUG' if self.verbose else 'INFO'
        return get_logger('member_collector', 'import', level=level, console_output=True)

    def get_available_periods(self) -> List[str]:
        """Get all available period directories in chronological order."""
        periods = []

        for item in os.listdir(self.processed_data_path):
            period_path = os.path.join(self.processed_data_path, item)
            if os.path.isdir(period_path) and not item.startswith('.'):
                # Filter to YYYY-MM format periods
                if len(item.split('-')) == 2:
                    periods.append(item)

        return sorted(periods)

    def scan_all_periods(self) -> int:
        """
        Scan all periods and collect member identity information.

        For each member:
        - Track identity: csv_id, name, email, role
        - Determine date_joined from first appearance
        - Handle edge cases: inactive members, missing emails, data variations

        Returns:
            Number of unique members found
        """
        periods = self.get_available_periods()
        self.logger.info(f"Phase 1: Scanning {len(periods)} periods for member identities")

        for period_name in periods:
            members_csv_path = os.path.join(
                self.processed_data_path, period_name, 'members.csv'
            )

            if not os.path.exists(members_csv_path):
                raise FileNotFoundError(
                    f"Required file not found: {members_csv_path}\n"
                    f"Each period directory must contain members.csv"
                )

            self._scan_period_members(period_name, members_csv_path)

        self.logger.info(f"Found {len(self.members)} unique members across {len(periods)} periods")

        # Strict validation: Require email for all members
        no_email = [csv_id for csv_id, data in self.members.items() if not data['email']]
        if no_email:
            member_details = [
                f"ID {csv_id}: {self.members[csv_id]['full_name']}"
                for csv_id in no_email
            ]
            raise ValueError(
                f"{len(no_email)} members are missing required email addresses:\n" +
                "\n".join(member_details) + "\n\n" +
                "Please add email addresses to members.csv, or use placeholder format:\n" +
                "  unknown{id}@invalid  (e.g., unknown42@invalid for member ID 42)"
            )

        return len(self.members)

    def _scan_period_members(self, period_name: str, csv_path: str):
        """Scan a single period's members.csv and track identities."""
        try:
            rows = load_csv(csv_path, required_columns=['id', 'Name', 'Role'])
        except Exception as e:
            self.logger.error(f"Failed to load {csv_path}: {e}")
            return

        for row in rows:
            csv_id = row.get('id', '').strip()
            if not csv_id:
                raise ValueError(
                    f"Invalid member data in {period_name}/members.csv: "
                    f"Row missing required 'id' field: {row}"
                )

            # Extract identity fields
            full_name = row.get('Name', '').strip()
            display_name = row.get('Display Name', '').strip() or full_name
            email = row.get('Email Address', '').strip()
            role = row.get('Role', '').strip().lower()
            active = row.get('Active', 'TRUE').strip().upper() in ['TRUE', 'YES', '1']

            # Parse Date Joined from CSV (for reference, but we'll use first appearance)
            date_joined_str = row.get('Date Joined', '').strip()
            date_joined_from_csv = None
            if date_joined_str:
                try:
                    # Try M/D/YYYY format
                    date_joined_from_csv = datetime.strptime(date_joined_str, "%m/%d/%Y").date()
                except ValueError:
                    try:
                        # Try YYYY-MM-DD format
                        date_joined_from_csv = datetime.strptime(date_joined_str, "%Y-%m-%d").date()
                    except ValueError:
                        self.logger.warning(
                            f"Could not parse Date Joined '{date_joined_str}' for {full_name}"
                        )

            # Calculate period date (approximate - use start of month)
            try:
                year, month = map(int, period_name.split('-'))
                period_date = date(year, month, 1)
            except ValueError:
                raise ValueError(
                    f"Invalid period directory name: '{period_name}'\n"
                    f"Period directories must follow YYYY-MM format (e.g., '2025-02')"
                )

            # Track first appearance
            if csv_id not in self.first_appearances:
                self.first_appearances[csv_id] = (period_name, period_date)
                self.logger.debug(
                    f"First appearance: {csv_id} ({full_name}) in {period_name}"
                )

            # Update member data (latest version wins for name/email/role)
            if csv_id not in self.members:
                self.members[csv_id] = {
                    'csv_id': csv_id,
                    'full_name': full_name,
                    'display_name': display_name,
                    'email': email,
                    'primary_role': role,
                    'active': active,
                    'date_joined_csv': date_joined_from_csv
                }
            else:
                # Update with latest data (in case name/email changed)
                self.members[csv_id].update({
                    'full_name': full_name,
                    'display_name': display_name,
                    'email': email,
                    'primary_role': role,
                    'active': active
                })

    def insert_members_to_db(self, cursor: sqlite3.Cursor) -> int:
        """
        Insert all collected members into peeps table.

        Uses first appearance to determine date_joined.
        Preserves CSV ID by using explicit ID in INSERT.

        Returns:
            Number of members inserted
        """
        self.logger.info(f"Inserting {len(self.members)} members into peeps table")

        inserted = 0

        for csv_id, member_data in sorted(self.members.items(), key=lambda x: int(x[0])):
            # Get first appearance date
            period_name, period_date = self.first_appearances[csv_id]
            date_joined = period_date  # Use period date as date_joined

            # Use CSV date_joined if available and earlier than first appearance
            if member_data['date_joined_csv']:
                if member_data['date_joined_csv'] < date_joined:
                    date_joined = member_data['date_joined_csv']

            email = member_data['email']

            # Email is guaranteed to exist (validated in scan_all_periods)
            # Normalize email before inserting (removes periods from Gmail, lowercase)
            email = normalize_email(email)

            try:
                # Insert with explicit ID to preserve CSV ID
                cursor.execute("""
                    INSERT INTO peeps (
                        id, full_name, display_name, primary_role, email,
                        date_joined, active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(csv_id),
                    member_data['full_name'],
                    member_data['display_name'],
                    member_data['primary_role'],
                    email,
                    date_joined.isoformat(),
                    member_data['active']
                ))

                peep_id = int(csv_id)  # ID is same as CSV ID
                self.peep_id_mapping[csv_id] = peep_id
                inserted += 1

                self.logger.debug(
                    f"Inserted peep {peep_id}: {member_data['full_name']} "
                    f"(first seen: {period_name}, joined: {date_joined})"
                )

            except sqlite3.IntegrityError as e:  # pragma: no cover
                self.logger.error(
                    f"Failed to insert member {csv_id} ({member_data['full_name']}): {e}"
                )
                continue

        self.logger.info(f"Successfully inserted {inserted} members into peeps table")

        self.logger.info(
            f"✓ Mapping complete: {len(self.peep_id_mapping)} members tracked"
        )

        return inserted


# ============================================================================
# Phase 2: Period Processing
# ============================================================================

class PeriodImporter:
    """
    Imports period data: responses, events, assignments, attendance, snapshots.

    Processes one period at a time in chronological order.
    """

    def __init__(
        self,
        period_name: str,
        processed_data_path: Path,
        peep_id_mapping: Dict[str, int],
        cursor: sqlite3.Cursor,
        verbose: bool = False,
        skip_snapshots: bool = False
    ):
        self.period_name = period_name
        self.processed_data_path = processed_data_path
        self.peep_id_mapping = peep_id_mapping
        self.cursor = cursor
        self.verbose = verbose
        self.skip_snapshots = skip_snapshots
        self.logger = self._setup_logging()

        # Period data path
        self.period_path = os.path.join(processed_data_path, period_name)

        # Tracking for this period
        self.period_id = None
        self.event_id_mapping: Dict[str, int] = {}  # event_date_str -> db event_id

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for period import."""
        from logging_config import get_logger
        level = 'DEBUG' if self.verbose else 'INFO'
        return get_logger(f'period_importer.{self.period_name}', 'import', level=level, console_output=True)

    def import_period(self):
        """
        Import complete period data.

        Order:
        1. Create schedule_period
        2. Import responses
        3. Create events from availability
        4. Create event_availability relationships
        5. Import assignments (Step 5)
        6. Import attendance (Step 5)
        7. Derive assignment changes (Step 5)
        8. Calculate snapshots (Step 6)
        """
        self.logger.info(f"Importing period {self.period_name}")

        # Step 1: Create schedule_period
        self.create_schedule_period()

        # Step 2: Import responses (returns response_mapping for availability)
        response_mapping = self.import_responses()

        # Step 3: Create events from availability strings
        num_events = self.create_events(response_mapping)
        self.logger.info(f"Created {num_events} events")

        # Step 4: Create event_availability
        num_availability = self.create_event_availability(response_mapping)
        self.logger.info(f"Created {num_availability} availability records")

        # Step 5: Import assignments and attendance
        num_assignments = self.import_assignments()
        self.logger.info(f"Created {num_assignments} assignments")

        num_attendance = self.import_attendance()
        self.logger.info(f"Created {num_attendance} attendance records")

        # Only derive changes if there's attendance data (skip for future periods)
        if num_attendance > 0:
            num_changes = self.derive_assignment_changes()
            self.logger.info(f"Derived {num_changes} assignment changes")
        else:
            num_changes = 0
            self.logger.info(f"Skipped change derivation (no attendance data for future/incomplete period)")

        # Step 6: Calculate and save snapshots
        if not self.skip_snapshots:
            num_snapshots = self.calculate_snapshots()
            self.logger.info(f"Created {num_snapshots} period snapshots")
        else:
            self.logger.info("Snapshot calculation skipped (--skip-snapshots flag)")

        self.logger.info(f"Period {self.period_name} import complete")

    def create_schedule_period(self):
        """Create schedule_periods record from period_name."""
        # Parse period name (YYYY-MM format)
        try:
            year, month = map(int, self.period_name.split('-'))
            period_date = date(year, month, 1)
        except ValueError as e:
            raise ValueError(f"Invalid period name format '{self.period_name}': {e}")

        # Calculate period end (last day of month)
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        period_end = next_month - timedelta(days=1)

        # Insert schedule_period
        self.cursor.execute("""
            INSERT INTO schedule_periods (
                period_name, start_date, end_date, status
            ) VALUES (?, ?, ?, ?)
        """, (
            self.period_name,
            period_date.isoformat(),
            period_end.isoformat(),
            'completed'  # Historical periods are completed
        ))

        self.period_id = self.cursor.lastrowid
        self.logger.debug(
            f"Created schedule_period {self.period_id}: {self.period_name} "
            f"({period_date} to {period_end})"
        )

    def import_responses(self) -> Dict[int, Tuple[int, str]]:
        """
        Import responses from responses.csv.

        Returns:
            Dict mapping peep_id -> (response_id, availability_string) for event_availability creation
        """
        responses_csv_path = os.path.join(self.period_path, 'responses.csv')

        if not os.path.exists(responses_csv_path):
            self.logger.warning(f"No responses.csv found for {self.period_name}")
            return {}

        rows = load_responses(responses_csv_path)
        inserted = 0
        response_mapping = {}  # peep_id -> (response_id, availability_string)

        for row in rows:
            email = row.get('Email Address', '').strip()
            name = row.get('Name', '').strip()

            # Skip legacy "Event:" rows (backward compatibility - events are now auto-derived or explicitly defined)
            if name.startswith("Event:"):
                continue

            # Validate required fields
            if not email or not name:
                raise ValueError(
                    f"Invalid response in period {self.period_name}: "
                    f"Missing required field(s) - Name: '{name}', Email: '{email}'. "
                    f"All response rows must have both Name and Email Address fields populated."
                )

            # Look up peep by email
            normalized_email = normalize_email(email)
            self.cursor.execute(
                "SELECT id FROM peeps WHERE email = ?",
                (normalized_email,)
            )
            result = self.cursor.fetchone()

            if not result:
                # Email doesn't match after normalization - this is a data quality error
                raise ValueError(
                    f"Email mismatch in period {self.period_name}: "
                    f"Response from '{name}' with email '{email}' (normalized: '{normalized_email}') "
                    f"does not match any member in peeps table. "
                    f"Verify data quality - email may differ between members.csv and responses.csv."
                )

            peep_id = result[0]

            # Parse response fields
            timestamp_str = row.get('Timestamp', '').strip()
            try:
                # Try parsing "M/D/YYYY H:MM:SS" format
                timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            except ValueError:
                try:
                    # Try ISO format
                    timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError:
                    self.logger.warning(
                        f"Could not parse timestamp '{timestamp_str}' for {name}"
                    )
                    timestamp = None

            response_role = row.get('Primary Role', '').strip().lower()

            # Map Secondary Role to switch_preference enum
            secondary_role = row.get('Secondary Role', '').strip()
            if not secondary_role:
                # Default for missing value (early periods didn't collect this)
                switch_preference = SwitchPreference.PRIMARY_ONLY.value
            else:
                # Error on unrecognized values instead of silently defaulting
                switch_pref_enum = SwitchPreference.from_string(secondary_role)
                switch_preference = switch_pref_enum.value

            max_sessions_str = row.get('Max Sessions', '0').strip()
            try:
                max_sessions = int(max_sessions_str)
            except ValueError:
                max_sessions = 0

            availability_str = row.get('Availability', '').strip()

            min_interval_str = row.get('Min Interval Days', '0').strip()
            try:
                min_interval_days = int(min_interval_str)
            except ValueError:
                min_interval_days = 0

            # Comments fields (optional)
            organizer_comments = row.get('Questions or Comments for Organizers', '').strip() or None
            instructor_comments = row.get('Questions or Comments for Leilani', '').strip() or None

            # Insert response
            self.cursor.execute("""
                INSERT INTO responses (
                    period_id, peep_id, response_role, switch_preference,
                    max_sessions, min_interval_days, organizer_comments,
                    instructor_comments, response_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.period_id,
                peep_id,
                response_role,
                switch_preference,
                max_sessions,
                min_interval_days,
                organizer_comments,
                instructor_comments,
                timestamp.isoformat() if timestamp else None
            ))

            response_id = self.cursor.lastrowid
            response_mapping[peep_id] = (response_id, availability_str)

            inserted += 1
            self.logger.debug(
                f"Imported response {response_id}: {name} ({email}), max_sessions={max_sessions}"
            )

        self.logger.info(f"Imported {inserted} responses")
        return response_mapping

    def create_events(self, response_mapping: Dict[int, Tuple[int, str]]) -> int:
        """
        Create events from availability strings in responses.

        Events are created with status='proposed' (per migration 009).
        Uses parse_event_date() to extract event info from date strings.

        Args:
            response_mapping: Dict of peep_id -> (response_id, availability_string)

        Returns:
            Number of events created
        """
        # Extract unique availability strings from response_mapping
        availability_strings = [avail_str for _, avail_str in response_mapping.values() if avail_str]

        # Extract unique event date strings
        unique_event_dates = set()

        # Parse period year from period_name
        year = int(self.period_name.split('-')[0])

        for availability_str in availability_strings:
            # Split by comma to get individual date strings
            date_strings = [s.strip() for s in availability_str.split(',') if s.strip()]

            for date_str in date_strings:
                try:
                    event_id, duration, display_name = parse_event_date(date_str, year=year)
                    unique_event_dates.add((event_id, duration, display_name))
                except Exception as e:
                    raise ValueError(
                        f"Invalid event date format in availability data: '{date_str}'\n"
                        f"Error: {e}\n"
                        f"Expected format: 'DayOfWeek Month Day - StartTime to EndTime'\n"
                        f"Example: 'Friday February 7th - 5pm to 7pm'"
                    )

        # Create events
        inserted = 0

        for event_date_str, duration_minutes, display_name in sorted(unique_event_dates):
            # Parse event_date_str to get datetime
            try:
                event_datetime = datetime.strptime(event_date_str, "%Y-%m-%d %H:%M")
            except ValueError as e:
                self.logger.error(f"Invalid event_id format '{event_date_str}': {e}")
                continue

            # Determine duration if not specified
            if duration_minutes is None:
                # Default to 120 minutes for old format
                duration_minutes = 120
                self.logger.debug(
                    f"No duration in event string, defaulting to {duration_minutes} minutes"
                )

            # Insert event with status='proposed'
            self.cursor.execute("""
                INSERT INTO events (
                    period_id, event_datetime, duration_minutes, status
                ) VALUES (?, ?, ?, ?)
            """, (
                self.period_id,
                event_datetime.isoformat(),
                duration_minutes,
                'proposed'
            ))

            event_db_id = self.cursor.lastrowid
            self.event_id_mapping[event_date_str] = event_db_id

            inserted += 1
            self.logger.debug(
                f"Created event {event_db_id}: {event_date_str} ({duration_minutes} min)"
            )

        return inserted

    def create_event_availability(self, response_mapping: Dict[int, Tuple[int, str]]) -> int:
        """
        Create event_availability many-to-many relationships.

        Links responses to events based on availability strings.

        Args:
            response_mapping: Dict of peep_id -> (response_id, availability_string)

        Returns:
            Number of availability records created
        """
        # Parse period year
        year = int(self.period_name.split('-')[0])

        inserted = 0

        for peep_id, (response_id, availability_str) in response_mapping.items():
            if not availability_str:
                continue

            # Parse availability string
            date_strings = [s.strip() for s in availability_str.split(',') if s.strip()]

            for date_str in date_strings:
                try:
                    event_id_str, _, _ = parse_event_date(date_str, year=year)
                except Exception as e:
                    raise ValueError(
                        f"Invalid event date in response {response_id}: '{date_str}'\n"
                        f"Error: {e}\n"
                        f"Expected format: 'DayOfWeek Month Day - StartTime to EndTime'\n"
                        f"Example: 'Friday February 7th - 5pm to 7pm'"
                    )

                # Look up event_db_id from mapping
                event_db_id = self.event_id_mapping.get(event_id_str)

                if not event_db_id:
                    raise ValueError(
                        f"Data integrity error in response {response_id}:\n"
                        f"Response references event '{event_id_str}' which does not exist in the database.\n"
                        f"This indicates a mismatch between responses.csv and the derived events."
                    )

                # Insert event_availability using response_id (not peep_id)
                try:
                    self.cursor.execute("""
                        INSERT INTO event_availability (response_id, event_id)
                        VALUES (?, ?)
                    """, (response_id, event_db_id))

                    inserted += 1
                except sqlite3.IntegrityError:
                    # Duplicate availability record - skip
                    self.logger.debug(
                        f"Duplicate availability: response {response_id}, event {event_db_id}"
                    )
                    continue

        return inserted

    def import_assignments(self) -> int:
        """
        Import event assignments from results.json.

        Returns:
            Number of assignments created
        """
        results_json_path = os.path.join(self.period_path, 'results.json')

        if not os.path.exists(results_json_path):
            self.logger.warning(f"No results.json found for {self.period_name}")
            return 0

        try:
            results_data = load_json(results_json_path)
        except Exception as e:
            self.logger.error(f"Failed to load results.json: {e}")
            raise

        valid_events = results_data.get('valid_events', [])
        inserted = 0

        # Create events from results.json if they don't already exist (e.g., when no responses)
        for event_data in valid_events:
            event_date_str = event_data.get('date')  # e.g., "2025-02-15 13:00"

            # Check if event already exists in mapping
            if event_date_str not in self.event_id_mapping:
                # Create event from results.json
                duration = event_data.get('duration_minutes', 120)
                legacy_id = event_data.get('id')

                try:
                    self.cursor.execute("""
                        INSERT INTO events (period_id, legacy_period_event_id, event_datetime, duration_minutes, status)
                        VALUES (?, ?, ?, ?, ?)
                    """, (self.period_id, legacy_id, event_date_str, duration, 'completed'))

                    event_db_id = self.cursor.lastrowid
                    self.event_id_mapping[event_date_str] = event_db_id
                    self.logger.info(f"Created event from results.json: {event_date_str}")
                except sqlite3.IntegrityError as e:
                    # Event might already exist from another source
                    self.logger.warning(f"Event {event_date_str} already exists: {e}")
                    # Try to fetch it
                    self.cursor.execute("""
                        SELECT id FROM events WHERE event_datetime = ? AND period_id = ?
                    """, (event_date_str, self.period_id))
                    result = self.cursor.fetchone()
                    if result:
                        event_db_id = result[0]
                        self.event_id_mapping[event_date_str] = event_db_id
                    else:
                        raise

        for event_data in valid_events:
            event_date_str = event_data.get('date')  # e.g., "2025-02-15 13:00"

            # Look up event_id from our mapping
            event_db_id = self.event_id_mapping.get(event_date_str)

            if not event_db_id:
                raise ValueError(
                    f"Data integrity error in results.json:\n"
                    f"Assignment references event '{event_date_str}' which does not exist in the database.\n"
                    f"Available events: {list(self.event_id_mapping.keys())}"
                )

            # Import attendees
            attendees = event_data.get('attendees', [])
            for idx, attendee in enumerate(attendees):
                csv_id = str(attendee.get('id'))
                peep_id = self.peep_id_mapping.get(csv_id)

                if not peep_id:
                    raise ValueError(
                        f"Data integrity error in results.json:\n"
                        f"Assignment references unknown member CSV ID '{csv_id}' ({attendee.get('name')}).\n"
                        f"This member was not found in any members.csv file."
                    )

                role = attendee.get('role', '').lower()

                try:
                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type, assignment_order
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_db_id, peep_id, role, 'attendee', idx))
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    self.logger.warning(f"Duplicate assignment for event {event_db_id}, peep {peep_id}: {e}")
                    continue

            # Import alternates
            alternates = event_data.get('alternates', [])
            for idx, alternate in enumerate(alternates):
                csv_id = str(alternate.get('id'))
                peep_id = self.peep_id_mapping.get(csv_id)

                if not peep_id:
                    raise ValueError(
                        f"Data integrity error in results.json:\n"
                        f"Alternate assignment references unknown member CSV ID '{csv_id}' ({alternate.get('name')}).\n"
                        f"This member was not found in any members.csv file."
                    )

                role = alternate.get('role', '').lower()

                try:
                    self.cursor.execute("""
                        INSERT INTO event_assignments (
                            event_id, peep_id, assigned_role, assignment_type, alternate_position
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_db_id, peep_id, role, 'alternate', idx))
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    self.logger.warning(f"Duplicate alternate for event {event_db_id}, peep {peep_id}: {e}")
                    continue

        return inserted

    def import_attendance(self) -> int:
        """
        Import event attendance from actual_attendance.json.

        Returns:
            Number of attendance records created
        """
        attendance_json_path = os.path.join(self.period_path, 'actual_attendance.json')

        if not os.path.exists(attendance_json_path):
            self.logger.warning(f"No actual_attendance.json found for {self.period_name}")
            return 0

        try:
            attendance_data = load_json(attendance_json_path)
        except Exception as e:
            self.logger.error(f"Failed to load actual_attendance.json: {e}")
            raise

        valid_events = attendance_data.get('valid_events', [])
        inserted = 0

        for event_data in valid_events:
            event_date_str = event_data.get('date')

            # Look up event_id
            event_db_id = self.event_id_mapping.get(event_date_str)

            if not event_db_id:
                raise ValueError(
                    f"Data integrity error in actual_attendance.json:\n"
                    f"Attendance references event '{event_date_str}' which does not exist in the database.\n"
                    f"Available events: {list(self.event_id_mapping.keys())}"
                )

            # Import actual attendees
            attendees = event_data.get('attendees', [])

            for attendee in attendees:
                csv_id = str(attendee.get('id'))
                peep_id = self.peep_id_mapping.get(csv_id)

                if not peep_id:
                    raise ValueError(
                        f"Data integrity error in actual_attendance.json:\n"
                        f"Attendance references unknown member CSV ID '{csv_id}' ({attendee.get('name')}).\n"
                        f"This member was not found in any members.csv file."
                    )

                actual_role = attendee.get('role', '').lower()

                # Look up their assignment for this event (if they had one)
                self.cursor.execute("""
                    SELECT id, assigned_role, assignment_type
                    FROM event_assignments
                    WHERE event_id = ? AND peep_id = ?
                """, (event_db_id, peep_id))

                assignment = self.cursor.fetchone()

                if assignment:
                    assignment_id = assignment[0]
                    expected_role = assignment[1]
                    expected_type = assignment[2]

                    # Determine participation mode
                    if expected_type == 'attendee':
                        participation_mode = 'scheduled'
                    elif expected_type == 'alternate':
                        participation_mode = 'alternate_promoted'
                    else:
                        participation_mode = 'scheduled'  # Default
                else:
                    # No assignment - volunteer fill
                    assignment_id = None
                    expected_role = None
                    expected_type = None
                    participation_mode = 'volunteer_fill'

                # Insert attendance record
                try:
                    self.cursor.execute("""
                        INSERT INTO event_attendance (
                            event_id, peep_id, event_assignment_id,
                            expected_role, expected_type,
                            actual_role, attendance_status, participation_mode,
                            last_minute_cancel
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event_db_id, peep_id, assignment_id,
                        expected_role, expected_type,
                        actual_role, 'attended', participation_mode,
                        False  # They attended, so not a cancellation
                    ))
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    self.logger.warning(f"Duplicate attendance for event {event_db_id}, peep {peep_id}: {e}")
                    continue

        return inserted

    def derive_assignment_changes(self) -> int:
        """
        Derive event_assignment_changes by comparing scheduled vs actual attendance.

        Change types:
        - cancel: Assigned but didn't attend
        - promote_alternate: Alternate who attended
        - add: Volunteer fill (no assignment but attended)

        Returns:
            Number of change records created
        """
        inserted = 0

        # Get all assignments for this period's events
        self.cursor.execute("""
            SELECT ea.id, ea.event_id, ea.peep_id, ea.assignment_type, p.full_name
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN peeps p ON ea.peep_id = p.id
            WHERE e.period_id = ?
        """, (self.period_id,))

        assignments = self.cursor.fetchall()

        for assignment_id, event_id, peep_id, assignment_type, full_name in assignments:
            # Check if they attended
            self.cursor.execute("""
                SELECT attendance_status, participation_mode
                FROM event_attendance
                WHERE event_id = ? AND peep_id = ?
            """, (event_id, peep_id))

            attendance = self.cursor.fetchone()

            if not attendance:
                # Assigned but didn't attend
                # Only treat as cancellation if they were an ATTENDEE (not alternate)
                # Alternates not attending is normal/expected - they're backups
                if assignment_type == 'attendee':
                    change_type = 'cancel'
                    change_reason = 'did_not_attend'

                    try:
                        self.cursor.execute("""
                            INSERT INTO event_assignment_changes (
                                event_id, peep_id, change_type, change_source, change_reason
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (event_id, peep_id, change_type, 'system', change_reason))
                        inserted += 1
                    except sqlite3.IntegrityError:
                        continue
                # else: alternate didn't attend - this is normal, no change record needed

            elif assignment_type == 'alternate' and attendance[1] == 'alternate_promoted':
                # Alternate was promoted
                change_type = 'promote_alternate'
                change_reason = 'alternate_attended'

                try:
                    self.cursor.execute("""
                        INSERT INTO event_assignment_changes (
                            event_id, peep_id, change_type, change_source, change_reason
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (event_id, peep_id, change_type, 'system', change_reason))
                    inserted += 1
                except sqlite3.IntegrityError:
                    continue

        # Find volunteer fills (attended without assignment)
        self.cursor.execute("""
            SELECT ea.event_id, ea.peep_id
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ? AND ea.participation_mode = 'volunteer_fill'
        """, (self.period_id,))

        volunteer_fills = self.cursor.fetchall()

        for event_id, peep_id in volunteer_fills:
            change_type = 'add'
            change_reason = 'volunteer_fill'

            try:
                self.cursor.execute("""
                    INSERT INTO event_assignment_changes (
                        event_id, peep_id, change_type, change_source, change_reason
                    ) VALUES (?, ?, ?, ?, ?)
                """, (event_id, peep_id, change_type, 'system', change_reason))
                inserted += 1
            except sqlite3.IntegrityError:
                continue

        return inserted

    def calculate_snapshots(self) -> int:
        """
        Calculate period-end snapshots using snapshot_generator logic.

        Returns:
            Number of snapshots created
        """
        # Get prior period to load starting snapshot
        self.cursor.execute("""
            SELECT id, period_name
            FROM schedule_periods
            WHERE period_name < ?
            ORDER BY period_name DESC
            LIMIT 1
        """, (self.period_name,))

        prior_period = self.cursor.fetchone()

        # Load starting snapshot
        if prior_period:
            prior_period_id = prior_period[0]
            prior_period_name = prior_period[1]
            starting_snapshot = self._load_prior_snapshot(prior_period_id)

            # Error if prior period has no snapshots - periods must be imported sequentially
            if not starting_snapshot:
                raise ValueError(
                    f"Cannot calculate snapshots for {self.period_name}: "
                    f"Prior period {prior_period_name} has no snapshots. "
                    f"Periods must be imported in chronological order to build accurate historical snapshots."
                )
        else:
            # First period - create baseline snapshot (no prior history)
            starting_snapshot = self._create_baseline_snapshot()

        # Get actual attendance for this period
        actual_attendance = self._load_attendance_records()

        # Get who responded this period
        responded_peep_ids = self._get_responded_peep_ids()

        # Generate new snapshot using SnapshotGenerator
        generator = SnapshotGenerator(verbose=self.verbose)
        new_snapshots = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=actual_attendance,
            expected_attendance=[],  # PERMANENT snapshot - only actual attendance
            responded_peep_ids=responded_peep_ids
        )

        # Save snapshots to database
        inserted = 0
        for snapshot in new_snapshots:
            try:
                self.cursor.execute("""
                    INSERT INTO peep_order_snapshots (
                        peep_id, period_id, priority, index_position, total_attended, active
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    snapshot.peep_id,
                    self.period_id,
                    snapshot.priority,
                    snapshot.index_position,
                    snapshot.total_attended,
                    snapshot.active
                ))
                inserted += 1
            except sqlite3.IntegrityError as e:
                self.logger.warning(f"Duplicate snapshot for peep {snapshot.peep_id}, period {self.period_id}: {e}")
                continue

        return inserted

    def _load_prior_snapshot(self, prior_period_id: int) -> List[MemberSnapshot]:
        """Load snapshots from prior period."""
        self.cursor.execute("""
            SELECT
                pos.peep_id,
                p.email,
                p.full_name,
                p.display_name,
                p.primary_role,
                pos.priority,
                pos.index_position,
                pos.total_attended,
                pos.active
            FROM peep_order_snapshots pos
            JOIN peeps p ON pos.peep_id = p.id
            WHERE pos.period_id = ?
            ORDER BY pos.index_position
        """, (prior_period_id,))

        rows = self.cursor.fetchall()

        snapshots = []
        for row in rows:
            snapshot = MemberSnapshot(
                peep_id=row[0],
                email=row[1],
                full_name=row[2],
                display_name=row[3],
                primary_role=row[4],
                priority=row[5],
                index_position=row[6],
                total_attended=row[7],
                active=row[8]
            )
            snapshots.append(snapshot)

        self.logger.debug(f"Loaded {len(snapshots)} snapshots from prior period")
        return snapshots

    def _create_baseline_snapshot(self) -> List[MemberSnapshot]:
        """Create baseline snapshot for first period from all peeps."""
        self.cursor.execute("""
            SELECT
                id, email, full_name, display_name, primary_role, active
            FROM peeps
            ORDER BY id
        """)

        rows = self.cursor.fetchall()

        snapshots = []
        for idx, row in enumerate(rows):
            snapshot = MemberSnapshot(
                peep_id=row[0],
                email=row[1],
                full_name=row[2],
                display_name=row[3],
                primary_role=row[4],
                priority=0,  # Everyone starts with 0 priority
                index_position=idx,  # Order by peep_id
                total_attended=0,  # No attendance yet
                active=row[5]
            )
            snapshots.append(snapshot)

        self.logger.debug(f"Created baseline snapshot with {len(snapshots)} members")
        return snapshots

    def _load_attendance_records(self) -> List[EventAttendance]:
        """Load actual attendance for this period."""
        self.cursor.execute("""
            SELECT
                ea.event_id,
                ea.peep_id,
                ea.actual_role,
                ea.attendance_status,
                e.event_datetime
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ? AND ea.attendance_status = 'attended'
            ORDER BY e.event_datetime
        """, (self.period_id,))

        rows = self.cursor.fetchall()

        attendance_records = []
        for row in rows:
            # Parse event_datetime string to datetime object
            event_datetime_str = row[4]
            try:
                event_datetime = datetime.fromisoformat(event_datetime_str)
            except (ValueError, TypeError):
                self.logger.warning(f"Could not parse event_datetime: {event_datetime_str}")
                continue

            attendance = EventAttendance(
                event_id=row[0],
                peep_id=row[1],
                role=row[2],
                attendance_type='actual',  # Only actual attendance for PERMANENT snapshots
                event_datetime=event_datetime
            )
            attendance_records.append(attendance)

        self.logger.debug(f"Loaded {len(attendance_records)} attendance records")
        return attendance_records

    def _get_responded_peep_ids(self) -> Set[int]:
        """Get set of peep IDs who submitted responses this period."""
        self.cursor.execute("""
            SELECT DISTINCT peep_id
            FROM responses
            WHERE period_id = ?
        """, (self.period_id,))

        responded_ids = {row[0] for row in self.cursor.fetchall()}
        self.logger.debug(f"{len(responded_ids)} peeps responded this period")
        return responded_ids


# ============================================================================
# Main Execution
# ============================================================================

def setup_logging(verbose: bool = False):
    """Configure logging for import operations."""
    # Import logging_config from project root
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from logging_config import get_logger

    # Return configured logger for import operations
    level = 'DEBUG' if verbose else 'INFO'
    return get_logger('import_csv', 'import', level=level, console_output=True)


def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def check_peeps_table(cursor: sqlite3.Cursor) -> int:
    """
    Check if peeps table is already populated.

    Returns:
        Number of existing peeps, or 0 if empty
    """
    cursor.execute("SELECT COUNT(*) FROM peeps")
    count = cursor.fetchone()[0]
    return count


def validate_schema(cursor: sqlite3.Cursor, logger: logging.Logger = None) -> bool:
    """
    Validate database schema before import.

    Checks for existence of required tables and critical columns.

    Args:
        cursor: Database cursor
        logger: Logger instance (optional, will create one if not provided)

    Returns:
        True if schema is valid, False otherwise
    """
    if logger is None:
        from logging_config import get_logger
        logger = get_logger('import_csv', 'import')

    required_tables = [
        'peeps', 'schedule_periods', 'responses', 'events',
        'event_availability', 'event_assignments', 'event_attendance',
        'event_assignment_changes', 'peep_order_snapshots'
    ]

    logger.info("Validating database schema...")

    # Check tables exist
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
    """)
    existing_tables = {row[0] for row in cursor.fetchall()}

    missing_tables = set(required_tables) - existing_tables

    if missing_tables:
        logger.error(f"Missing required tables: {', '.join(sorted(missing_tables))}")
        logger.error("Run migrations first: python db/migrate.py")
        return False

    logger.info(f"✓ All {len(required_tables)} required tables exist")

    # Check critical columns in peeps table
    cursor.execute("PRAGMA table_info(peeps)")
    peeps_columns = {row[1] for row in cursor.fetchall()}
    required_peeps_cols = {'id', 'full_name', 'email', 'primary_role', 'date_joined', 'active'}

    missing_cols = required_peeps_cols - peeps_columns
    if missing_cols:
        logger.error(f"Missing columns in peeps table: {', '.join(sorted(missing_cols))}")
        return False

    logger.info("✓ Schema validation passed")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Import historical CSV data directly to normalized database",
        epilog="""
Examples:
  # Import all periods sequentially (recommended first run)
  python db/import_period_data.py --all

  # Import single period (requires prior periods already imported)
  python db/import_period_data.py --period 2025-03

  # Dry run to test import without database changes
  python db/import_period_data.py --all --dry-run --verbose

  # Validate schema before import
  python db/import_period_data.py --validate-schema

  # Force re-run Phase 1 member collection
  python db/import_period_data.py --all --force-phase1

  # Test import without snapshot calculation
  python db/import_period_data.py --period 2025-02 --skip-snapshots

Common Workflows:
  1. First Time Setup:
     - Ensure migrations applied: python db/migrate.py
     - Validate schema: python db/import_period_data.py --validate-schema
     - Import all: python db/import_period_data.py --all

  2. Single Period Re-import:
     - Delete period: DELETE FROM schedule_periods WHERE period_name='2025-03';
     - Re-import: python db/import_period_data.py --period 2025-03

  3. Troubleshooting:
     - Test with dry-run: python db/import_period_data.py --period 2025-02 --dry-run --verbose
     - Skip snapshots: python db/import_period_data.py --period 2025-02 --skip-snapshots

Notes:
  - Periods must be imported in chronological order for accurate snapshots
  - Phase 1 (member collection) runs once; subsequent runs skip unless --force-phase1
  - Email mismatches between members.csv and responses.csv will log warnings
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--period',
        help='Import specific period only (e.g., 2025-02). Requires prior periods already imported.'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Import all available periods in chronological order'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test import without committing changes to database'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable detailed debug logging'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Validate CSV data without importing to database'
    )
    parser.add_argument(
        '--validate-schema',
        action='store_true',
        help='Validate database schema and exit (run before import)'
    )
    parser.add_argument(
        '--force-phase1',
        action='store_true',
        help='Force re-run of Phase 1 (member collection) even if peeps table populated'
    )
    parser.add_argument(
        '--skip-snapshots',
        action='store_true',
        help='Skip snapshot calculation (for testing import without snapshot generation)'
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    # Handle --validate-schema flag
    if args.validate_schema:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            is_valid = validate_schema(cursor, logger)
            conn.close()
            sys.exit(0 if is_valid else 1)
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            sys.exit(1)

    if not args.period and not args.all and not args.validate_only:
        parser.print_help()
        sys.exit(1)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if Phase 1 needed
        existing_peeps = check_peeps_table(cursor)

        if existing_peeps > 0 and not args.force_phase1:
            logger.info(
                f"Phase 1 skipped: {existing_peeps} peeps already exist in database"
            )
            logger.info("Use --force-phase1 to re-run member collection")

            # TODO: Rebuild peep_id_mapping from existing peeps
            # For now, we'll implement this in Phase 2
        else:
            # Phase 1: Member Collection
            logger.info("=" * 60)
            logger.info("PHASE 1: MEMBER IDENTITY COLLECTION")
            logger.info("=" * 60)

            collector = MemberCollector(PROCESSED_DATA_PATH, verbose=args.verbose)
            num_members = collector.scan_all_periods()

            if num_members == 0:
                logger.error("No members found - check processed data path")
                sys.exit(1)

            if args.validate_only:
                logger.info(f"Validation complete: {num_members} members found")
                sys.exit(0)

            # Insert members to database
            if not args.dry_run:
                num_inserted = collector.insert_members_to_db(cursor)
                conn.commit()
                logger.info(f"Phase 1 complete: {num_inserted} members inserted")
            else:
                logger.info(f"DRY RUN: Would insert {num_members} members")
                conn.rollback()

        # Phase 2: Period Processing
        if args.period or args.all:
            logger.info("=" * 60)
            logger.info("PHASE 2: PERIOD DATA IMPORT")
            logger.info("=" * 60)

            # Build or rebuild peep_id_mapping from database
            if existing_peeps > 0 and not args.force_phase1:
                # Rebuild mapping from existing peeps
                cursor.execute("SELECT id FROM peeps ORDER BY id")
                peep_id_mapping = {str(row[0]): row[0] for row in cursor.fetchall()}
                logger.info(f"Rebuilt peep_id_mapping from {len(peep_id_mapping)} existing peeps")
            else:
                # Use mapping from Phase 1
                peep_id_mapping = collector.peep_id_mapping

            # Determine which periods to import
            periods_to_import = []

            if args.period:
                periods_to_import = [args.period]
            elif args.all:
                # Get all available periods in chronological order
                available_periods = []
                for item in os.listdir(PROCESSED_DATA_PATH):
                    period_path = os.path.join(PROCESSED_DATA_PATH, item)
                    if os.path.isdir(period_path) and not item.startswith('.'):
                        # Filter to YYYY-MM format
                        if len(item.split('-')) == 2:
                            available_periods.append(item)

                periods_to_import = sorted(available_periods)

            logger.info(f"Importing {len(periods_to_import)} period(s): {', '.join(periods_to_import)}")

            # Import statistics
            import_stats = {
                'periods_imported': 0,
                'periods_failed': 0,
                'total_events': 0,
                'total_responses': 0,
                'total_assignments': 0,
                'total_attendance': 0,
                'total_changes': 0,
                'total_snapshots': 0
            }

            # Import each period
            for idx, period_name in enumerate(periods_to_import, 1):
                logger.info(f"{'=' * 60}")
                logger.info(f"Processing period {idx}/{len(periods_to_import)}: {period_name}")
                logger.info(f"{'=' * 60}")

                try:
                    importer = PeriodImporter(
                        period_name=period_name,
                        processed_data_path=PROCESSED_DATA_PATH,
                        peep_id_mapping=peep_id_mapping,
                        cursor=cursor,
                        verbose=args.verbose,
                        skip_snapshots=args.skip_snapshots
                    )

                    importer.import_period()

                    # Gather statistics
                    cursor.execute("""
                        SELECT COUNT(*) FROM events WHERE period_id = ?
                    """, (importer.period_id,))
                    period_events = cursor.fetchone()[0]
                    import_stats['total_events'] += period_events

                    cursor.execute("""
                        SELECT COUNT(*) FROM responses WHERE period_id = ?
                    """, (importer.period_id,))
                    period_responses = cursor.fetchone()[0]
                    import_stats['total_responses'] += period_responses

                    cursor.execute("""
                        SELECT COUNT(*) FROM event_assignments ea
                        JOIN events e ON ea.event_id = e.id
                        WHERE e.period_id = ?
                    """, (importer.period_id,))
                    period_assignments = cursor.fetchone()[0]
                    import_stats['total_assignments'] += period_assignments

                    cursor.execute("""
                        SELECT COUNT(*) FROM event_attendance ea
                        JOIN events e ON ea.event_id = e.id
                        WHERE e.period_id = ?
                    """, (importer.period_id,))
                    period_attendance = cursor.fetchone()[0]
                    import_stats['total_attendance'] += period_attendance

                    cursor.execute("""
                        SELECT COUNT(*) FROM event_assignment_changes eac
                        JOIN events e ON eac.event_id = e.id
                        WHERE e.period_id = ?
                    """, (importer.period_id,))
                    period_changes = cursor.fetchone()[0]
                    import_stats['total_changes'] += period_changes

                    if not args.skip_snapshots:
                        cursor.execute("""
                            SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?
                        """, (importer.period_id,))
                        period_snapshots = cursor.fetchone()[0]
                        import_stats['total_snapshots'] += period_snapshots
                    else:
                        period_snapshots = 0

                    if not args.dry_run:
                        conn.commit()
                        logger.info(f"✓ Period {period_name} committed to database")
                    else:
                        logger.info(f"DRY RUN: Period {period_name} would be committed")

                    logger.info(f"Period {period_name} summary: {period_events} events, "
                              f"{period_responses} responses, {period_assignments} assignments, "
                              f"{period_attendance} attendance, {period_changes} changes, "
                              f"{period_snapshots} snapshots")

                    import_stats['periods_imported'] += 1

                except Exception as e:
                    logger.error(f"Failed to import period {period_name}: {e}", exc_info=True)
                    import_stats['periods_failed'] += 1
                    conn.rollback()
                    raise

            # Print summary statistics
            logger.info(f"{'=' * 60}")
            logger.info("PHASE 2 IMPORT SUMMARY")
            logger.info(f"{'=' * 60}")
            logger.info(f"Periods imported: {import_stats['periods_imported']}")
            logger.info(f"Periods failed: {import_stats['periods_failed']}")
            logger.info(f"Total events: {import_stats['total_events']}")
            logger.info(f"Total responses: {import_stats['total_responses']}")
            logger.info(f"Total assignments: {import_stats['total_assignments']}")
            logger.info(f"Total attendance: {import_stats['total_attendance']}")
            logger.info(f"Total assignment changes: {import_stats['total_changes']}")
            if not args.skip_snapshots:
                logger.info(f"Total snapshots: {import_stats['total_snapshots']}")
            logger.info(f"{'=' * 60}")
        else:
            logger.info("\nPhase 2: Skipped (no --period or --all specified)")

        if args.dry_run:
            logger.info("DRY RUN: Rolling back all changes")
            conn.rollback()
        else:
            logger.info("All changes committed successfully")
            conn.commit()

    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
