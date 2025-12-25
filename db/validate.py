#!/usr/bin/env python3
"""
Data Validation CLI Tools

Validate imported data against source CSV/JSON files and perform integrity checks.

Usage:
    python db/validate.py --validate-members [--db PATH]
    python db/validate.py --validate-period <name> [--db PATH]
    python db/validate.py --show-period <name> [--db PATH]
    python db/validate.py --list-periods [--db PATH]

Validation Commands:
    --validate-members          Validate all members against CSV files
    --validate-period <name>    Comprehensive period validation

Inspection Commands:
    --show-period <name>        Display period summary
    --list-periods              List all periods

Options:
    --db PATH                   Database path (default: peeps_data/peeps_scheduler.db)
"""

import argparse
import sqlite3
import sys
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import constants
from file_io import normalize_email, extract_events

# ============================================================================
# Validation Constants
# ============================================================================

MAX_REASONABLE_PRIORITY = 20
"""Maximum reasonable priority value for snapshot validation.
Priorities beyond this indicate potential data integrity issues.
Used in validate_period_snapshots() to detect out-of-range values."""

MAX_INDEX_MULTIPLIER = 2
"""Maximum index position multiplier for snapshot validation.
Index position should not exceed (total_active_peeps * MAX_INDEX_MULTIPLIER).
Used in validate_period_snapshots() to detect out-of-range positions."""


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Create database connection with row factory."""
    if not Path(db_path).exists():
        print(f"Error: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# CSV/JSON Reading Helpers
# =============================================================================

def read_members_csv(period_path: Path) -> List[Dict]:
    """Read members.csv and return list of member dictionaries."""
    members_file = period_path / 'members.csv'
    if not members_file.exists():
        return []

    with open(members_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def read_responses_csv(period_path: Path) -> List[Dict]:
    """Read responses.csv and return list of response dictionaries."""
    responses_file = period_path / 'responses.csv'
    if not responses_file.exists():
        return []

    with open(responses_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def read_results_json(period_path: Path) -> Optional[Dict]:
    """Read results.json and return assignment data."""
    results_file = period_path / 'results.json'
    if not results_file.exists():
        return None

    with open(results_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_attendance_json(period_path: Path) -> Optional[Dict]:
    """Read actual_attendance.json and return attendance data."""
    attendance_file = period_path / 'actual_attendance.json'
    if not attendance_file.exists():
        return None

    with open(attendance_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_cancellations_json(period_path: Path) -> Optional[Dict]:
    """Read cancellations.json and return cancellation data."""
    cancellations_file = period_path / 'cancellations.json'
    if not cancellations_file.exists():
        return None

    with open(cancellations_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_partnerships_json(period_path: Path) -> Optional[Dict]:
    """Read partnerships.json and return partnership data."""
    partnerships_file = period_path / 'partnerships.json'
    if not partnerships_file.exists():
        return None

    with open(partnerships_file, 'r', encoding='utf-8') as f:
        return json.load(f)


# =============================================================================
# Validation Commands
# =============================================================================

def validate_members(db_path: str, data_dir: str = None) -> int:
    """
    Validate all members in database against source CSV files.

    Args:
        db_path: Path to database file
        data_dir: Path to processed data directory (default: peeps_data/processed)

    Returns:
        0 if validation passes, 1 if issues found
    """
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # Get processed data directory
    if data_dir is None:
        project_root = Path(__file__).parent.parent
        processed_dir = project_root / 'peeps_data' / 'processed'
    else:
        processed_dir = Path(data_dir)

    if not processed_dir.exists():
        print(f"Error: Processed data directory not found: {processed_dir}", file=sys.stderr)
        return 1

    # Find all period directories
    period_dirs = sorted([d for d in processed_dir.iterdir() if d.is_dir()])
    if not period_dirs:
        print("Error: No period directories found", file=sys.stderr)
        return 1

    # Use latest period for member validation
    latest_period = period_dirs[-1]
    members_csv = read_members_csv(latest_period)

    if not members_csv:
        print(f"Error: No members.csv found in {latest_period}", file=sys.stderr)
        return 1

    print(f"\n=== Validating Members (against {latest_period.name}/members.csv) ===\n")

    issues = []
    members_checked = 0

    for csv_member in members_csv:
        csv_email = normalize_email(csv_member.get('Email Address', '').strip())
        csv_name = csv_member.get('Name', '').strip()

        if not csv_email:
            continue

        # Get DB member
        cursor.execute("""
            SELECT id, full_name, display_name, email, primary_role, active
            FROM peeps
            WHERE email = ?
        """, (csv_email,))
        db_member = cursor.fetchone()

        if not db_member:
            issues.append(f"Member not in DB: {csv_name} ({csv_email})")
            continue

        # Compare fields
        csv_display = csv_member.get('Display Name', '').strip()
        csv_role = csv_member.get('Role', '').strip().lower()
        csv_active = csv_member.get('Active', '').strip().upper() == 'TRUE'

        if db_member['full_name'] != csv_name:
            issues.append(f"{csv_name}: Name mismatch - DB='{db_member['full_name']}'")

        if db_member['display_name'] != csv_display:
            issues.append(f"{csv_name}: Display name mismatch - DB='{db_member['display_name']}' vs CSV='{csv_display}'")

        if db_member['primary_role'] != csv_role:
            issues.append(f"{csv_name}: Role mismatch - DB='{db_member['primary_role']}' vs CSV='{csv_role}'")

        if bool(db_member['active']) != csv_active:
            issues.append(f"{csv_name}: Active status mismatch - DB={db_member['active']} vs CSV={csv_active}")

        members_checked += 1

    # Check for members in DB not in CSV
    cursor.execute("SELECT full_name, email FROM peeps")
    all_db_members = cursor.fetchall()
    csv_emails = {normalize_email(m.get('Email Address', '').strip()) for m in members_csv}

    for db_member in all_db_members:
        if db_member['email'] not in csv_emails:
            issues.append(f"Member in DB but not in CSV: {db_member['full_name']} ({db_member['email']})")

    # Print results
    print(f"Members checked: {members_checked}")
    print(f"Total DB members: {len(all_db_members)}")
    print(f"Total CSV members: {len(members_csv)}\n")

    if issues:
        print(f"FAILED - {len(issues)} issue(s) found:\n")
        for issue in issues:
            print(f"  - {issue}")
        conn.close()
        return 1
    else:
        print("PASSED - All members match CSV data")
        conn.close()
        return 0


def validate_period(db_path: str, period_name: str, data_dir: str = None) -> int:
    """
    Comprehensive validation of a period's data against source CSV/JSON files.

    Args:
        db_path: Path to database file
        period_name: Name of period to validate
        data_dir: Path to processed data directory (default: peeps_data/processed)

    Validates:
    - Responses against responses.csv
    - Assignments against results.json
    - Attendance against actual_attendance.json
    - Snapshots against members.csv

    Returns:
        0 if validation passes, 1 if issues found
    """
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # Get period
    cursor.execute("SELECT id FROM schedule_periods WHERE period_name = ?", (period_name,))
    period_row = cursor.fetchone()
    if not period_row:
        print(f"Error: Period not found in DB: {period_name}", file=sys.stderr)
        return 1

    period_id = period_row['id']

    # Get period source data path
    if data_dir is None:
        project_root = Path(__file__).parent.parent
        period_path = project_root / 'peeps_data' / 'processed' / period_name
    else:
        period_path = Path(data_dir) / period_name

    if not period_path.exists():
        print(f"Error: Period directory not found: {period_path}", file=sys.stderr)
        return 1

    print(f"\n=== Validating Period: {period_name} ===\n")

    all_issues = []

    # Validate responses
    responses_csv = read_responses_csv(period_path)
    if responses_csv:
        print(f"Validating responses ({len(responses_csv)} in CSV)...")
        response_issues = validate_period_responses(cursor, period_id, responses_csv)
        all_issues.extend(response_issues)
        print(f"  {'PASSED' if not response_issues else f'FAILED - {len(response_issues)} issues'}\n")

    # Validate events (all events from responses.csv availability)
    if responses_csv:
        # Get event count from database
        cursor.execute("SELECT COUNT(*) as count FROM events WHERE period_id = ?", (period_id,))
        num_events = cursor.fetchone()['count']
        print(f"Validating events ({num_events} in DB)...")
        event_issues = validate_events(cursor, period_id, period_name, responses_csv, period_path)
        all_issues.extend(event_issues)
        print(f"  {'PASSED' if not event_issues else f'FAILED - {len(event_issues)} issues'}\n")

    # Validate assignments
    results_json = read_results_json(period_path)
    if results_json:
        num_events = len(results_json.get('valid_events', [])) + len(results_json.get('downgraded_events', []))
        print(f"Validating assignments ({num_events} events in JSON)...")
        assignment_issues = validate_period_assignments(cursor, period_id, results_json)
        all_issues.extend(assignment_issues)
        print(f"  {'PASSED' if not assignment_issues else f'FAILED - {len(assignment_issues)} issues'}\n")

    # Validate attendance
    attendance_json = read_attendance_json(period_path)
    if attendance_json:
        num_events = len(attendance_json) if isinstance(attendance_json, list) else len(attendance_json.get('valid_events', []))
        print(f"Validating attendance ({num_events} events in JSON)...")
        attendance_issues = validate_period_attendance(cursor, period_id, attendance_json)
        all_issues.extend(attendance_issues)
        print(f"  {'PASSED' if not attendance_issues else f'FAILED - {len(attendance_issues)} issues'}\n")

    # Validate snapshots
    members_csv = read_members_csv(period_path)
    if members_csv:
        print(f"Validating snapshots ({len(members_csv)} members in CSV)...")
        snapshot_issues = validate_period_snapshots(cursor, period_id, members_csv)
        all_issues.extend(snapshot_issues)
        print(f"  {'PASSED' if not snapshot_issues else f'FAILED - {len(snapshot_issues)} issues'}\n")

    # Validate Phase 1 features: Cancelled events
    cancellations_json = read_cancellations_json(period_path)
    if cancellations_json:
        cancelled_events = cancellations_json.get('cancelled_events', [])
        if cancelled_events:
            print(f"Validating cancelled events ({len(cancelled_events)} in JSON)...")
            cancellation_issues = validate_period_cancellations(cursor, period_id, cancellations_json, period_name)
            all_issues.extend(cancellation_issues)
            print(f"  {'PASSED' if not cancellation_issues else f'FAILED - {len(cancellation_issues)} issues'}\n")

    # Validate Phase 1 features: Cancelled availability
    if cancellations_json:
        cancelled_availability = cancellations_json.get('cancelled_availability', [])
        if cancelled_availability:
            print(f"Validating cancelled availability ({len(cancelled_availability)} entries in JSON)...")
            cancelled_avail_issues = validate_period_cancelled_availability(cursor, period_id, cancellations_json, period_name)
            all_issues.extend(cancelled_avail_issues)
            print(f"  {'PASSED' if not cancelled_avail_issues else f'FAILED - {len(cancelled_avail_issues)} issues'}\n")

    # Validate Phase 1 features: Partnerships
    partnerships_json = read_partnerships_json(period_path)
    if partnerships_json:
        print(f"Validating partnerships ({len(partnerships_json)} entries in JSON)...")
        partnership_issues = validate_period_partnerships(cursor, period_id, partnerships_json)
        all_issues.extend(partnership_issues)
        print(f"  {'PASSED' if not partnership_issues else f'FAILED - {len(partnership_issues)} issues'}\n")

    # Print summary
    if all_issues:
        print(f"=== VALIDATION FAILED ===")
        print(f"Total issues: {len(all_issues)}\n")
        for issue in all_issues:
            print(f"  - {issue}")
        conn.close()
        return 1
    else:
        print(f"=== VALIDATION PASSED ===")
        print(f"All period data matches source files")
        conn.close()
        return 0


# =============================================================================
# Period Validation Helpers
# =============================================================================

def validate_period_responses(cursor, period_id: int, responses_csv: List[Dict]) -> List[str]:
    """Validate responses table against responses.csv."""
    issues = []

    for csv_response in responses_csv:
        csv_email = normalize_email(csv_response.get('Email Address', '').strip())
        if not csv_email:
            continue

        # Get DB response
        cursor.execute("""
            SELECT r.response_role, r.max_sessions, r.min_interval_days
            FROM responses r
            JOIN peeps p ON r.peep_id = p.id
            WHERE r.period_id = ? AND p.email = ?
        """, (period_id, csv_email))
        db_response = cursor.fetchone()

        if not db_response:
            issues.append(f"Response not in DB: {csv_response.get('Name')} ({csv_email})")
            continue

        # Compare fields
        csv_role = csv_response.get('Primary Role', '').strip().lower()
        csv_max_sessions = int(csv_response.get('Max Sessions', 0) or 0)
        csv_min_interval = int(csv_response.get('Min Interval Days', 0) or 0)

        if db_response['response_role'] != csv_role:
            issues.append(f"Response role mismatch for {csv_email}: DB='{db_response['response_role']}' vs CSV='{csv_role}'")

        if db_response['max_sessions'] != csv_max_sessions:
            issues.append(f"Max sessions mismatch for {csv_email}: DB={db_response['max_sessions']} vs CSV={csv_max_sessions}")

        if db_response['min_interval_days'] != csv_min_interval:
            issues.append(f"Min interval mismatch for {csv_email}: DB={db_response['min_interval_days']} vs CSV={csv_min_interval}")

    return issues


def validate_period_assignments(cursor, period_id: int, results_json: Dict) -> List[str]:
    """Validate assignments table against results.json."""
    issues = []

    # results.json structure: {"valid_events": [...], "downgraded_events": [...]}
    all_events = results_json.get('valid_events', []) + results_json.get('downgraded_events', [])

    for event_data in all_events:
        event_date = event_data.get('date', '')
        # Convert datetime from JSON format (space separator) to ISO 8601 (T separator)
        event_date_iso = event_date.replace(' ', 'T')

        # Get event_id from DB
        cursor.execute("""
            SELECT id FROM events
            WHERE period_id = ? AND event_datetime LIKE ?
        """, (period_id, f"{event_date_iso}%"))
        db_event = cursor.fetchone()

        if not db_event:
            issues.append(f"Event not in DB: {event_date}")
            continue

        event_id = db_event['id']

        # Check attendees
        for attendee in event_data.get('attendees', []):
            peep_id = attendee.get('id')
            csv_role = attendee.get('role', '').lower()

            cursor.execute("""
                SELECT assigned_role, assignment_type
                FROM event_assignments
                WHERE event_id = ? AND peep_id = ?
            """, (event_id, peep_id))
            db_assignment = cursor.fetchone()

            if not db_assignment:
                issues.append(f"Assignment not in DB: event {event_date}, peep {peep_id}")
                continue

            if db_assignment['assigned_role'] != csv_role:
                issues.append(f"Assignment role mismatch: event {event_date}, peep {peep_id} - DB='{db_assignment['assigned_role']}' vs JSON='{csv_role}'")

            if db_assignment['assignment_type'] != 'attendee':
                issues.append(f"Assignment type mismatch: event {event_date}, peep {peep_id} - expected 'attendee', got '{db_assignment['assignment_type']}'")

        # Check alternates
        for alternate in event_data.get('alternates', []):
            peep_id = alternate.get('id')
            csv_role = alternate.get('role', '').lower()

            cursor.execute("""
                SELECT assigned_role, assignment_type
                FROM event_assignments
                WHERE event_id = ? AND peep_id = ?
            """, (event_id, peep_id))
            db_assignment = cursor.fetchone()

            if not db_assignment:
                issues.append(f"Alternate assignment not in DB: event {event_date}, peep {peep_id}")
                continue

            if db_assignment['assignment_type'] != 'alternate':
                issues.append(f"Assignment type mismatch: event {event_date}, peep {peep_id} - expected 'alternate', got '{db_assignment['assignment_type']}'")

    return issues


def validate_period_attendance(cursor, period_id: int, attendance_json: Dict) -> List[str]:
    """Validate attendance table against actual_attendance.json."""
    issues = []

    # attendance_json might be array or dict - handle both
    events = attendance_json if isinstance(attendance_json, list) else attendance_json.get('valid_events', [])

    for event_data in events:
        event_date = event_data.get('date', '')
        # Convert datetime from JSON format (space separator) to ISO 8601 (T separator)
        event_date_iso = event_date.replace(' ', 'T')

        # Get event_id from DB
        cursor.execute("""
            SELECT id FROM events
            WHERE period_id = ? AND event_datetime LIKE ?
        """, (period_id, f"{event_date_iso}%"))
        db_event = cursor.fetchone()

        if not db_event:
            issues.append(f"Event not in DB: {event_date}")
            continue

        event_id = db_event['id']

        # Check each attendee
        for attendee in event_data.get('attendees', []):
            peep_id = attendee.get('id')
            json_role = attendee.get('role', '').lower()

            cursor.execute("""
                SELECT actual_role, attendance_status, participation_mode
                FROM event_attendance
                WHERE event_id = ? AND peep_id = ?
            """, (event_id, peep_id))
            db_attendance = cursor.fetchone()

            if not db_attendance:
                issues.append(f"Attendance not in DB: event {event_date}, peep {peep_id}")
                continue

            if db_attendance['actual_role'] != json_role:
                issues.append(f"Attendance role mismatch: event {event_date}, peep {peep_id} - DB='{db_attendance['actual_role']}' vs JSON='{json_role}'")

            if db_attendance['attendance_status'] != 'attended':
                issues.append(f"Attendance status mismatch: event {event_date}, peep {peep_id} - expected 'attended', got '{db_attendance['attendance_status']}'")

    return issues


def validate_period_snapshots(cursor, period_id: int, members_csv: List[Dict]) -> List[str]:
    """
    Validate snapshots integrity.

    Checks:
    - All active members have snapshots (only if period has attendance)
    - total_attended matches actual attendance records
    - priority/index are in reasonable ranges (not comparing to CSV as CSV might be wrong)

    Note: Snapshots are only created for periods with attendance data. Periods with
    no attendance (e.g., future periods) should not have snapshots.
    """
    issues = []

    # Check if period has any attendance records
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM event_attendance ea
        JOIN events e ON ea.event_id = e.id
        WHERE e.period_id = ?
    """, (period_id,))
    attendance_count = cursor.fetchone()['count']

    # Skip snapshot validation if period has no attendance
    if attendance_count == 0:
        return issues

    # Get all active members from DB
    cursor.execute("SELECT id, full_name, email FROM peeps WHERE active = 1")
    active_members = cursor.fetchall()

    # Get total active count once (used for all members)
    cursor.execute("SELECT COUNT(*) as count FROM peeps WHERE active = 1")
    total_active = cursor.fetchone()['count']

    for member in active_members:
        peep_id = member['id']

        # Check snapshot exists
        cursor.execute("""
            SELECT priority, index_position, total_attended
            FROM peep_order_snapshots
            WHERE peep_id = ? AND period_id = ?
        """, (peep_id, period_id))
        db_snapshot = cursor.fetchone()

        if not db_snapshot:
            issues.append(f"Snapshot missing for active member: {member['full_name']} ({member['email']})")
            continue

        # Validate total_attended matches actual attendance count
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE ea.peep_id = ?
            AND ea.attendance_status = 'attended'
            AND e.period_id <= ?
        """, (peep_id, period_id))
        actual_attended = cursor.fetchone()['count']

        if db_snapshot['total_attended'] != actual_attended:
            issues.append(f"Snapshot total_attended incorrect for {member['full_name']}: DB snapshot={db_snapshot['total_attended']}, actual attendance={actual_attended}")

        # Check priority/index are in reasonable ranges (integrity check, not comparing to CSV)
        if db_snapshot['priority'] < 0 or db_snapshot['priority'] > MAX_REASONABLE_PRIORITY:
            issues.append(f"Snapshot priority out of range for {member['full_name']}: {db_snapshot['priority']}")

        if db_snapshot['index_position'] < 0 or db_snapshot['index_position'] >= total_active * MAX_INDEX_MULTIPLIER:
            issues.append(f"Snapshot index out of range for {member['full_name']}: {db_snapshot['index_position']}")

    return issues


def validate_period_cancellations(cursor, period_id: int, cancellations_json: Dict, period_name: str) -> List[str]:
    """
    Validate cancelled events against database.

    Checks that cancelled events from cancellations.json have status='cancelled' in database.

    Args:
        cursor: Database cursor with row factory enabled
        period_id: Database period ID
        cancellations_json: Dictionary from cancellations.json containing 'cancelled_events' list
        period_name: Period name (e.g., '2025-02') - used to extract year for event parsing

    Returns:
        List of issue strings. Empty list means validation passed.

    Example:
        >>> cancellations = {'cancelled_events': ['Friday February 7th - 5pm to 7pm'], ...}
        >>> issues = validate_period_cancellations(cursor, 42, cancellations, '2025-02')
        >>> if issues:
        ...     print("Validation failed:", issues)
    """
    issues = []

    cancelled_events = cancellations_json.get('cancelled_events', [])
    if not cancelled_events:
        return issues

    # Extract year from period_name
    try:
        year = int(period_name.split('-')[0])
    except (ValueError, IndexError):
        year = None

    # Parse event strings to event_id format (YYYY-MM-DD HH:MM)
    from file_io import parse_event_date

    for event_str in cancelled_events:
        try:
            event_id, _, _ = parse_event_date(event_str, year=year)
        except Exception as e:
            issues.append(f"Cannot parse cancelled event string '{event_str}': {e}")
            continue

        # Convert to ISO 8601 format for database matching
        event_datetime_pattern = event_id.replace(' ', 'T')

        # Check if event exists and has status='cancelled'
        cursor.execute("""
            SELECT status FROM events
            WHERE period_id = ? AND event_datetime LIKE ?
        """, (period_id, f"{event_datetime_pattern}%"))

        event_row = cursor.fetchone()
        if event_row is None:
            issues.append(f"Cancelled event not found in DB: {event_id}")
        elif event_row['status'] != 'cancelled':
            issues.append(f"Event {event_id} should have status='cancelled', got '{event_row['status']}'")

    return issues


def validate_period_cancelled_availability(cursor, period_id: int, cancellations_json: Dict, period_name: str) -> List[str]:
    """
    Validate that cancelled availability records were removed from event_availability table.

    Checks that members listed in cancelled_availability don't have event_availability records
    for the specified events.

    Args:
        cursor: Database cursor with row factory enabled
        period_id: Database period ID
        cancellations_json: Dictionary from cancellations.json containing 'cancelled_availability' list
        period_name: Period name (e.g., '2025-02') - used to extract year for event parsing

    Returns:
        List of issue strings. Empty list means validation passed.

    Example:
        >>> cancellations = {
        ...     'cancelled_availability': [
        ...         {'email': 'alice@example.com', 'events': ['Friday February 7th - 5pm to 7pm']}
        ...     ]
        ... }
        >>> issues = validate_period_cancelled_availability(cursor, 42, cancellations, '2025-02')
        >>> if not issues:
        ...     print("Alice's availability was successfully removed")
    """
    issues = []

    cancelled_availability = cancellations_json.get('cancelled_availability', [])
    if not cancelled_availability:
        return issues

    # Extract year from period_name
    try:
        year = int(period_name.split('-')[0])
    except (ValueError, IndexError):
        year = None

    from file_io import parse_event_date, normalize_email

    for entry in cancelled_availability:
        email = normalize_email(entry.get('email', ''))
        if not email:
            continue

        events = entry.get('events', [])

        # Find peep by email
        cursor.execute("SELECT id FROM peeps WHERE email = ?", (email,))
        peep_row = cursor.fetchone()
        if peep_row is None:
            continue

        peep_id = peep_row['id']

        for event_str in events:
            try:
                event_id, _, _ = parse_event_date(event_str, year=year)
            except Exception:
                continue

            event_datetime_pattern = event_id.replace(' ', 'T')

            # Check that event_availability doesn't exist for this peep/event combo
            cursor.execute("""
                SELECT COUNT(*) as count FROM event_availability ea
                JOIN responses r ON ea.response_id = r.id
                JOIN events e ON ea.event_id = e.id
                WHERE r.peep_id = ? AND e.period_id = ? AND e.event_datetime LIKE ?
            """, (peep_id, period_id, f"{event_datetime_pattern}%"))

            count = cursor.fetchone()['count']
            if count > 0:
                issues.append(f"Cancelled availability not removed: {email} still has availability for {event_id}")

    return issues


def validate_period_partnerships(cursor, period_id: int, partnerships_json: Dict) -> List[str]:
    """
    Validate partnership requests against database.

    Checks that partnerships from partnerships.json are correctly stored in partnership_requests table.
    Supports both wrapped {"partnerships": {...}} and unwrapped {...} JSON formats.

    Args:
        cursor: Database cursor with row factory enabled
        period_id: Database period ID
        partnerships_json: Dictionary from partnerships.json with partnership data.
            Formats: {'1': [2, 3]} or {'partnerships': {'1': [2, 3]}}

    Returns:
        List of issue strings. Empty list means validation passed.

    Example:
        >>> partnerships = {'1': [2], '2': [1, 3]}  # Member 1 partners with 2, etc.
        >>> issues = validate_period_partnerships(cursor, 42, partnerships)
        >>> if not issues:
        ...     print("All partnerships are correctly stored")
    """
    issues = []

    # Handle both wrapped {"partnerships": {...}} and unwrapped {...} formats
    raw_partnerships = partnerships_json.get("partnerships") if "partnerships" in partnerships_json else partnerships_json

    if not raw_partnerships or not isinstance(raw_partnerships, dict):
        return issues

    for requester_key, partner_ids in raw_partnerships.items():
        try:
            requester_id = int(requester_key)
        except (TypeError, ValueError):
            issues.append(f"Invalid requester ID in partnerships.json: {requester_key}")
            continue

        if not isinstance(partner_ids, list):
            issues.append(f"Partner list for requester {requester_id} should be a list")
            continue

        for partner_id in partner_ids:
            try:
                partner_id = int(partner_id)
            except (TypeError, ValueError):
                issues.append(f"Invalid partner ID for requester {requester_id}: {partner_id}")
                continue

            # Check if partnership exists in database
            cursor.execute("""
                SELECT COUNT(*) as count FROM partnership_requests
                WHERE period_id = ? AND requester_peep_id = ? AND partner_peep_id = ?
            """, (period_id, requester_id, partner_id))

            count = cursor.fetchone()['count']
            if count == 0:
                issues.append(f"Partnership not found in DB: {requester_id} -> {partner_id}")

    return issues


def validate_events(cursor, period_id: int, period_name: str, responses_csv: List[Dict], period_path: Path = None) -> List[str]:
    """
    Validate events table against responses.csv availability data.

    This checks ALL events (proposed, scheduled, cancelled, completed) against
    the events derived from responses.csv Availability column.

    Duration validation uses status-appropriate source files:
    - proposed events: check against responses.csv
    - scheduled events: check against results.json
    - completed events: check against actual_attendance.json

    Args:
        cursor: Database cursor
        period_id: Period ID
        period_name: Period name (for year extraction)
        responses_csv: List of response dictionaries from responses.csv
        period_path: Path to period data directory (for results.json/actual_attendance.json)

    Returns:
        List of issue strings (empty if validation passes)
    """
    issues = []

    # Extract year from period_name (format: "2025-02")
    year = int(period_name.split('-')[0])

    # Extract expected events from responses.csv
    expected_event_map = extract_events(responses_csv, year=year)
    expected_event_ids = set(expected_event_map.keys())

    # Load duration maps from results.json and actual_attendance.json (if available)
    scheduled_durations = {}
    completed_durations = {}

    if period_path:
        # Load results.json for scheduled event durations
        results_json = read_results_json(period_path)
        if results_json:
            all_events = results_json.get('valid_events', []) + results_json.get('downgraded_events', [])
            for event_data in all_events:
                event_date = event_data.get('date', '')
                # Convert to event_id format (YYYY-MM-DD HH:MM)
                event_id = event_date.replace(' ', ' ').strip()[:16]  # "2025-02-07 17:00"
                duration = event_data.get('duration_minutes')
                if duration is not None:
                    scheduled_durations[event_id] = duration

        # Load actual_attendance.json for completed event durations
        attendance_json = read_attendance_json(period_path)
        if attendance_json:
            events = attendance_json if isinstance(attendance_json, list) else attendance_json.get('valid_events', [])
            for event_data in events:
                event_date = event_data.get('date', '')
                # Convert to event_id format (YYYY-MM-DD HH:MM)
                event_id = event_date.replace(' ', ' ').strip()[:16]  # "2025-02-07 17:00"
                duration = event_data.get('duration_minutes')
                if duration is not None:
                    completed_durations[event_id] = duration

    # Get all events from database for this period
    cursor.execute("""
        SELECT event_datetime, duration_minutes, status
        FROM events
        WHERE period_id = ?
        ORDER BY event_datetime
    """, (period_id,))
    db_events = cursor.fetchall()

    # Convert DB events to comparable format (event_id strings)
    db_event_ids = set()
    db_event_details = {}
    for db_event in db_events:
        # Convert datetime to event_id format (YYYY-MM-DD HH:MM)
        dt = datetime.fromisoformat(db_event['event_datetime'])
        event_id = dt.strftime("%Y-%m-%d %H:%M")
        db_event_ids.add(event_id)
        db_event_details[event_id] = {
            'duration': db_event['duration_minutes'],
            'status': db_event['status']
        }

    # Check for events in CSV but not in DB
    missing_in_db = expected_event_ids - db_event_ids
    for event_id in sorted(missing_in_db):
        issues.append(f"Event in responses.csv but not in DB: {event_id}")

    # Check for events in DB but not in CSV
    extra_in_db = db_event_ids - expected_event_ids
    for event_id in sorted(extra_in_db):
        issues.append(f"Event in DB but not in responses.csv: {event_id}")

    # Check event details for matching events
    for event_id in expected_event_ids & db_event_ids:
        expected_event = expected_event_map[event_id]
        db_details = db_event_details[event_id]

        # Check duration against status-appropriate source
        event_status = db_details['status']

        if event_status == 'completed' and event_id in completed_durations:
            # Completed events: check against actual_attendance.json
            expected_duration = completed_durations[event_id]
            source = "actual_attendance.json"
        elif event_status == 'scheduled' and event_id in scheduled_durations:
            # Scheduled events: check against results.json
            expected_duration = scheduled_durations[event_id]
            source = "results.json"
        else:
            # Proposed/cancelled events or fallback: check against responses.csv
            expected_duration = expected_event.duration_minutes
            source = "responses.csv"

        if db_details['duration'] != expected_duration:
            issues.append(f"Event duration mismatch for {event_id}: DB={db_details['duration']} vs {source}={expected_duration}")

    return issues


# =============================================================================
# Inspection Commands (simplified - just show info, no validation)
# =============================================================================

def show_period(db_path: str, period_name: str) -> int:
    """Display period summary with counts."""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, period_name, start_date, end_date, status
        FROM schedule_periods
        WHERE period_name = ?
    """, (period_name,))

    period = cursor.fetchone()
    if not period:
        print(f"Error: Period not found: {period_name}", file=sys.stderr)
        return 1

    period_id = period['id']

    # Get counts
    cursor.execute("SELECT COUNT(*) as count FROM events WHERE period_id = ?", (period_id,))
    event_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) as count FROM responses WHERE period_id = ?", (period_id,))
    response_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) as count FROM event_assignments WHERE event_id IN (SELECT id FROM events WHERE period_id = ?)", (period_id,))
    assignment_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) as count FROM event_attendance WHERE event_id IN (SELECT id FROM events WHERE period_id = ?)", (period_id,))
    attendance_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) as count FROM peep_order_snapshots WHERE period_id = ?", (period_id,))
    snapshot_count = cursor.fetchone()['count']

    # Get Phase 1 feature counts
    cursor.execute("SELECT COUNT(*) as count FROM events WHERE period_id = ? AND status = 'cancelled'", (period_id,))
    cancelled_events_count = cursor.fetchone()['count']

    cursor.execute("SELECT COUNT(*) as count FROM partnership_requests WHERE period_id = ?", (period_id,))
    partnership_count = cursor.fetchone()['count']

    # Display
    print(f"\n=== Period Summary: {period_name} ===")
    print(f"Start Date: {period['start_date']}")
    print(f"End Date: {period['end_date']}")
    print(f"Status: {period['status']}")
    print(f"\nCounts:")
    print(f"  Events: {event_count}")
    print(f"  Responses: {response_count}")
    print(f"  Assignments: {assignment_count}")
    print(f"  Attendance: {attendance_count}")
    print(f"  Snapshots: {snapshot_count}")
    print(f"  Cancelled Events: {cancelled_events_count}")
    print(f"  Partnerships: {partnership_count}")

    conn.close()
    return 0


def list_periods(db_path: str) -> int:
    """List all periods."""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT period_name, start_date, end_date, status
        FROM schedule_periods
        ORDER BY start_date
    """)

    periods = cursor.fetchall()

    print("\n=== Periods List ===")
    print(f"{'Period':<15} {'Start':<12} {'End':<12} {'Status':<12}")
    print("-" * 55)

    for period in periods:
        print(f"{period['period_name']:<15} {period['start_date']:<12} {period['end_date']:<12} {period['status']:<12}")

    print(f"\nTotal: {len(periods)} period(s)")

    conn.close()
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data validation and inspection CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Database path
    parser.add_argument('--db', type=str, default=constants.DEFAULT_DB_PATH,
                        help='Database path (default: %(default)s)')

    # Data directory
    parser.add_argument('--data-dir', type=str, default=None,
                        help='Processed data directory path (default: peeps_data/processed)')

    # Commands
    commands = parser.add_mutually_exclusive_group()
    commands.add_argument('--validate-members', action='store_true',
                          help='Validate all members against CSV files')
    commands.add_argument('--validate-period', type=str, metavar='NAME',
                          help='Comprehensive period validation')
    commands.add_argument('--show-period', type=str, metavar='NAME',
                          help='Display period summary')
    commands.add_argument('--list-periods', action='store_true',
                          help='List all periods')

    # Options
    parser.add_argument('--active-only', action='store_true',
                        help='Filter to active members only')

    args = parser.parse_args()

    # Check if at least one command was provided
    if not any([args.validate_members, args.validate_period,
                args.show_period, args.list_periods]):
        parser.error("one of the validation/inspection commands is required")
        return 2

    # Execute command
    try:
        if args.validate_members:
            return validate_members(args.db, args.data_dir)
        elif args.validate_period:
            return validate_period(args.db, args.validate_period, args.data_dir)
        elif args.show_period:
            return show_period(args.db, args.show_period)
        elif args.list_periods:
            return list_periods(args.db)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
