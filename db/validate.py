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
from pathlib import Path
from typing import Optional, Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import constants
from file_io import normalize_email


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
    csv_emails = {m.get('Email Address', '').strip() for m in members_csv}

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

    # Validate assignments
    results_json = read_results_json(period_path)
    if results_json:
        print(f"Validating assignments ({len(results_json)} events in JSON)...")
        assignment_issues = validate_period_assignments(cursor, period_id, results_json)
        all_issues.extend(assignment_issues)
        print(f"  {'PASSED' if not assignment_issues else f'FAILED - {len(assignment_issues)} issues'}\n")

    # Validate attendance
    attendance_json = read_attendance_json(period_path)
    if attendance_json:
        print(f"Validating attendance ({len(attendance_json)} events in JSON)...")
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

        # Get event_id from DB
        cursor.execute("""
            SELECT id FROM events
            WHERE period_id = ? AND event_datetime LIKE ?
        """, (period_id, f"{event_date}%"))
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
    events = attendance_json if isinstance(attendance_json, list) else attendance_json.get('events', [])

    for event_data in events:
        event_date = event_data.get('date', '')

        # Get event_id from DB
        cursor.execute("""
            SELECT id FROM events
            WHERE period_id = ? AND event_datetime LIKE ?
        """, (period_id, f"{event_date}%"))
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
    - All active members have snapshots
    - total_attended matches actual attendance records
    - priority/index are in reasonable ranges (not comparing to CSV as CSV might be wrong)
    """
    issues = []

    # Get all active members from DB
    cursor.execute("SELECT id, full_name, email FROM peeps WHERE active = 1")
    active_members = cursor.fetchall()

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
        if db_snapshot['priority'] < 0 or db_snapshot['priority'] > 20:
            issues.append(f"Snapshot priority out of range for {member['full_name']}: {db_snapshot['priority']}")

        cursor.execute("SELECT COUNT(*) as count FROM peeps WHERE active = 1")
        total_active = cursor.fetchone()['count']

        if db_snapshot['index_position'] < 0 or db_snapshot['index_position'] >= total_active * 2:
            issues.append(f"Snapshot index out of range for {member['full_name']}: {db_snapshot['index_position']}")

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
