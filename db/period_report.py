#!/usr/bin/env python3
"""
Period Report Generator

Generates human-readable reports for each period showing events, assignments,
attendance, and changes to help with manual verification of transformation results.

Usage:
    python db/period_report.py [options]
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_manager import get_data_manager


class PeriodReporter:
    """Generates comprehensive reports for period verification."""

    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor):
        self.conn = conn
        self.cursor = cursor

    def generate_all_periods_report(self, output_file: Optional[str] = None) -> None:
        """Generate reports for all periods."""
        # Get all periods in chronological order
        self.cursor.execute("""
            SELECT period_name FROM schedule_periods
            WHERE period_name NOT LIKE '%-baseline'
            ORDER BY period_name
        """)
        periods = [row[0] for row in self.cursor.fetchall()]

        output = []
        output.append("# PEEPS SCHEDULER - PERIOD VERIFICATION REPORT")
        output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("=" * 80)
        output.append("")

        for period_name in periods:
            output.extend(self._generate_period_report(period_name))
            output.append("")

        report_text = "\n".join(output)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            print(f"Report saved to: {output_file}")
        else:
            print(report_text)

    def generate_period_report(self, period_name: str, output_file: Optional[str] = None) -> None:
        """Generate report for a specific period."""
        output = []
        output.append(f"# PERIOD REPORT: {period_name}")
        output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("=" * 60)
        output.append("")

        output.extend(self._generate_period_report(period_name))

        report_text = "\n".join(output)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            print(f"Report saved to: {output_file}")
        else:
            print(report_text)

    def _generate_period_report(self, period_name: str) -> List[str]:
        """Generate detailed report for a single period."""
        output = []

        # Period Header
        output.append(f"## PERIOD: {period_name}")
        output.append("-" * 40)

        # Period Summary
        summary = self._get_period_summary(period_name)
        if summary:
            output.append("### SUMMARY")
            output.append(f"Status: {summary['status']}")
            output.append(f"Events: {summary['event_count']}")
            output.append(f"Total Assignments: {summary['assignment_count']}")
            output.append(f"Total Attendance: {summary['attendance_count']}")
            output.append(f"Assignment Changes: {summary['change_count']}")
            output.append("")

        # Event Summary Table
        output.extend(self._generate_event_summary_table(period_name))
        output.append("")

        # Event Details Tables
        output.extend(self._generate_event_details_tables(period_name))
        output.append("")

        # Issues Summary
        output.extend(self._generate_issues_summary(period_name))
        output.append("")

        # Proposed Events (not scheduled)
        proposed_events = self._get_proposed_events_not_scheduled(period_name)
        if proposed_events:
            output.append("### PROPOSED EVENTS (NOT SCHEDULED)")
            for event in proposed_events:
                output.append(f"- {event['datetime']} ({event['duration']} min)")
            output.append("")

        # Change Summary
        output.extend(self._generate_change_summary(period_name))
        output.append("")

        # Raw Data Summary
        raw_summary = self._get_raw_data_summary(period_name)
        if raw_summary:
            output.append("### RAW DATA SUMMARY")
            output.append(f"Responses: {raw_summary['response_count']}")
            output.append(f"Has Results JSON: {'Yes' if raw_summary['has_results'] else 'No'}")
            output.append(f"Has Attendance JSON: {'Yes' if raw_summary['has_attendance'] else 'No'}")
            output.append("")

        return output

    def _get_period_summary(self, period_name: str) -> Optional[Dict]:
        """Get high-level summary for the period."""
        # Get period info
        self.cursor.execute("SELECT id, status FROM schedule_periods WHERE period_name = ?", (period_name,))
        result = self.cursor.fetchone()
        if not result:
            return None

        period_id, status = result

        # Count events, assignments, attendance, changes
        self.cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (period_id,))
        event_count = self.cursor.fetchone()[0]

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        assignment_count = self.cursor.fetchone()[0]

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_attendance att
            JOIN events e ON att.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        attendance_count = self.cursor.fetchone()[0]

        self.cursor.execute("""
            SELECT COUNT(*) FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            WHERE e.period_id = ?
        """, (period_id,))
        change_count = self.cursor.fetchone()[0]

        return {
            'status': status,
            'event_count': event_count,
            'assignment_count': assignment_count,
            'attendance_count': attendance_count,
            'change_count': change_count
        }

    def _get_period_events(self, period_name: str) -> List[Dict]:
        """Get all events for the period."""
        self.cursor.execute("""
            SELECT e.id, e.legacy_period_event_id, e.event_datetime, e.status, e.duration_minutes
            FROM events e
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE sp.period_name = ?
            ORDER BY e.event_datetime
        """, (period_name,))

        events = []
        for row in self.cursor.fetchall():
            event_id, legacy_id, datetime_str, status, duration = row

            # Format datetime nicely
            try:
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                formatted_datetime = dt.strftime('%a %b %d, %Y at %I:%M %p')
            except:
                formatted_datetime = datetime_str

            events.append({
                'id': event_id,
                'legacy_id': legacy_id or "N/A",
                'datetime': formatted_datetime,
                'status': status.title(),
                'duration': duration
            })

        return events

    def _get_event_assignments(self, event_id: int) -> List[Dict]:
        """Get assignments for an event."""
        self.cursor.execute("""
            SELECT p.display_name, ea.assigned_role, ea.assignment_type, ea.assignment_order
            FROM event_assignments ea
            JOIN peeps p ON ea.peep_id = p.id
            WHERE ea.event_id = ?
            ORDER BY ea.assignment_order
        """, (event_id,))

        assignments = []
        for row in self.cursor.fetchall():
            name, role, assignment_type, order = row
            assignments.append({
                'name': name,
                'role': role.title(),
                'type': assignment_type,
                'order': order
            })

        return assignments

    def _get_event_attendance(self, event_id: int) -> List[Dict]:
        """Get attendance for an event."""
        self.cursor.execute("""
            SELECT p.display_name, att.actual_role, att.participation_mode,
                   att.event_assignment_id IS NOT NULL as has_assignment
            FROM event_attendance att
            JOIN peeps p ON att.peep_id = p.id
            WHERE att.event_id = ?
            ORDER BY p.display_name
        """, (event_id,))

        attendance = []
        for row in self.cursor.fetchall():
            name, role, mode, has_assignment = row
            attendance.append({
                'name': name,
                'role': role.title(),
                'mode': mode,
                'has_assignment': bool(has_assignment)
            })

        return attendance

    def _get_event_changes(self, event_id: int) -> List[Dict]:
        """Get assignment changes for an event."""
        self.cursor.execute("""
            SELECT change_type, change_reason, notes, changed_at
            FROM event_assignment_changes
            WHERE event_id = ?
            ORDER BY changed_at
        """, (event_id,))

        changes = []
        for row in self.cursor.fetchall():
            change_type, reason, notes, changed_at = row
            changes.append({
                'type': change_type.replace('_', ' ').title(),
                'reason': reason,
                'notes': notes,
                'changed_at': changed_at
            })

        return changes

    def _analyze_period_discrepancies(self, period_name: str) -> Dict[str, List[str]]:
        """Analyze discrepancies for the period."""
        discrepancies = {
            'Assignment/Attendance Mismatches': [],
            'Missing Change Records': [],
            'Data Quality Issues': []
        }

        # Get period ID
        self.cursor.execute("SELECT id FROM schedule_periods WHERE period_name = ?", (period_name,))
        result = self.cursor.fetchone()
        if not result:
            return discrepancies

        period_id = result[0]

        # Find people assigned but didn't attend
        self.cursor.execute("""
            SELECT e.legacy_period_event_id, p.display_name, ea.assigned_role
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN peeps p ON ea.peep_id = p.id
            WHERE e.period_id = ?
            AND NOT EXISTS (
                SELECT 1 FROM event_attendance att
                WHERE att.event_id = ea.event_id AND att.peep_id = ea.peep_id
            )
        """, (period_id,))

        for row in self.cursor.fetchall():
            event_id, name, role = row
            discrepancies['Assignment/Attendance Mismatches'].append(
                f"Event {event_id}: {name} assigned as {role} but no attendance record"
            )

        # Find people who attended without assignment (volunteer fills)
        self.cursor.execute("""
            SELECT e.legacy_period_event_id, p.display_name, att.actual_role
            FROM event_attendance att
            JOIN events e ON att.event_id = e.id
            JOIN peeps p ON att.peep_id = p.id
            WHERE e.period_id = ?
            AND att.participation_mode = 'volunteer_fill'
        """, (period_id,))

        for row in self.cursor.fetchall():
            event_id, name, role = row
            discrepancies['Assignment/Attendance Mismatches'].append(
                f"Event {event_id}: {name} attended as {role} without assignment (volunteer fill)"
            )

        # Find role mismatches
        self.cursor.execute("""
            SELECT e.legacy_period_event_id, p.display_name, ea.assigned_role, att.actual_role
            FROM event_assignments ea
            JOIN event_attendance att ON ea.event_id = att.event_id AND ea.peep_id = att.peep_id
            JOIN events e ON ea.event_id = e.id
            JOIN peeps p ON ea.peep_id = p.id
            WHERE e.period_id = ?
            AND ea.assigned_role != att.actual_role
        """, (period_id,))

        for row in self.cursor.fetchall():
            event_id, name, assigned_role, actual_role = row
            discrepancies['Assignment/Attendance Mismatches'].append(
                f"Event {event_id}: {name} assigned as {assigned_role} but attended as {actual_role}"
            )

        return discrepancies

    def _generate_change_summary(self, period_name: str) -> List[str]:
        """Generate a summary of schedule changes during the period."""
        output = []
        output.append("### SCHEDULE CHANGES SUMMARY")

        # Get change counts by type
        self.cursor.execute("""
            SELECT eac.change_type, COUNT(*) as count
            FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE sp.period_name = ?
            GROUP BY eac.change_type
            ORDER BY count DESC
        """, (period_name,))

        changes = self.cursor.fetchall()

        if not changes:
            output.append("No schedule changes recorded.")
            return output

        # Format change summary
        change_descriptions = {
            'add': 'People added (volunteer fills)',
            'cancel': 'People cancelled/removed',
            'change_role': 'Role changes (leader ↔ follower)',
            'promote_alternate': 'Alternates promoted to main attendee',
            'add_alternate': 'People added as alternates'
        }

        total_changes = sum(count for _, count in changes)
        output.append(f"**Total Changes: {total_changes}**")
        output.append("")

        for change_type, count in changes:
            description = change_descriptions.get(change_type, change_type.replace('_', ' ').title())
            output.append(f"- **{count}** {description}")

        # Get period status context
        self.cursor.execute("SELECT status FROM schedule_periods WHERE period_name = ?", (period_name,))
        status_result = self.cursor.fetchone()
        if status_result:
            status = status_result[0]
            if status == 'completed':
                output.append("")
                output.append("*Period completed - all changes reflect actual historical events*")
            elif status == 'active':
                output.append("")
                output.append("*Period active - changes reflect schedule adjustments made*")

        return output

    def _get_raw_data_summary(self, period_name: str) -> Optional[Dict]:
        """Get summary of raw data availability."""
        # Count responses
        self.cursor.execute("SELECT COUNT(*) FROM raw_responses WHERE period_name = ?", (period_name,))
        response_count = self.cursor.fetchone()[0]

        # Check for results JSON
        self.cursor.execute("SELECT results_json FROM raw_results WHERE period_name = ?", (period_name,))
        results_result = self.cursor.fetchone()
        has_results = bool(results_result and results_result[0])

        # Check for attendance JSON
        self.cursor.execute("SELECT actual_attendance_json FROM raw_actual_attendance WHERE period_name = ?", (period_name,))
        attendance_result = self.cursor.fetchone()
        has_attendance = bool(attendance_result and attendance_result[0])

        return {
            'response_count': response_count,
            'has_results': has_results,
            'has_attendance': has_attendance
        }

    def _get_proposed_events_not_scheduled(self, period_name: str) -> List[Dict]:
        """Get proposed events that were not scheduled."""
        self.cursor.execute("""
            SELECT e.event_datetime, e.duration_minutes
            FROM events e
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE sp.period_name = ?
            AND e.status = 'proposed'
            AND NOT EXISTS (
                SELECT 1 FROM event_assignments ea WHERE ea.event_id = e.id
            )
            ORDER BY e.event_datetime
        """, (period_name,))

        events = []
        for row in self.cursor.fetchall():
            datetime_str, duration = row
            try:
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                formatted_datetime = dt.strftime('%a %b %d, %Y at %I:%M %p')
            except:
                formatted_datetime = datetime_str

            events.append({
                'datetime': formatted_datetime,
                'duration': duration
            })

        return events

    def _get_scheduled_events(self, period_name: str) -> List[Dict]:
        """Get scheduled events (events with assignments)."""
        self.cursor.execute("""
            SELECT DISTINCT e.id, e.legacy_period_event_id, e.event_datetime, e.status, e.duration_minutes
            FROM events e
            JOIN schedule_periods sp ON e.period_id = sp.id
            JOIN event_assignments ea ON ea.event_id = e.id
            WHERE sp.period_name = ?
            ORDER BY e.event_datetime
        """, (period_name,))

        events = []
        for row in self.cursor.fetchall():
            event_id, legacy_id, datetime_str, status, duration = row
            try:
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                formatted_datetime = dt.strftime('%a %b %d, %Y at %I:%M %p')
            except:
                formatted_datetime = datetime_str

            events.append({
                'id': event_id,
                'legacy_id': legacy_id,
                'datetime': formatted_datetime,
                'status': status.title(),
                'duration': duration
            })

        return events

    def _get_period_peep_details(self, period_name: str) -> List[Dict]:
        """Get peep-centric view with assignments, changes, and attendance."""
        # Get all peeps involved in this period (assignments or attendance)
        self.cursor.execute("""
            SELECT DISTINCT p.id, p.display_name
            FROM peeps p
            WHERE p.id IN (
                SELECT DISTINCT ea.peep_id
                FROM event_assignments ea
                JOIN events e ON ea.event_id = e.id
                JOIN schedule_periods sp ON e.period_id = sp.id
                WHERE sp.period_name = ?
                UNION
                SELECT DISTINCT att.peep_id
                FROM event_attendance att
                JOIN events e ON att.event_id = e.id
                JOIN schedule_periods sp ON e.period_id = sp.id
                WHERE sp.period_name = ?
            )
            ORDER BY p.display_name
        """, (period_name, period_name))

        peep_details = []
        for peep_id, display_name in self.cursor.fetchall():
            # Get assignments for this peep
            assignments = self._get_peep_assignments(peep_id, period_name)

            # Get changes for this peep
            changes = self._get_peep_changes(peep_id, period_name)

            # Get attendance for this peep
            attendance = self._get_peep_attendance(peep_id, period_name)

            peep_details.append({
                'id': peep_id,
                'name': display_name,
                'assignments': assignments,
                'changes': changes,
                'attendance': attendance
            })

        return peep_details

    def _get_peep_assignments(self, peep_id: int, period_name: str) -> List[Dict]:
        """Get assignments for a specific peep in a period."""
        self.cursor.execute("""
            SELECT e.id, e.legacy_period_event_id, ea.assigned_role, ea.assignment_type
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE ea.peep_id = ? AND sp.period_name = ?
            ORDER BY e.event_datetime
        """, (peep_id, period_name))

        assignments = []
        for row in self.cursor.fetchall():
            event_id, event_legacy_id, role, assignment_type = row
            assignments.append({
                'event_id': event_id,
                'event_legacy_id': event_legacy_id,
                'role': role.title(),
                'type': assignment_type
            })

        return assignments

    def _get_peep_changes(self, peep_id: int, period_name: str) -> List[Dict]:
        """Get assignment changes for a specific peep in a period."""
        self.cursor.execute("""
            SELECT e.id, e.legacy_period_event_id, eac.change_type, eac.change_reason
            FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE eac.peep_id = ? AND sp.period_name = ?
            ORDER BY eac.changed_at
        """, (peep_id, period_name))

        changes = []
        for row in self.cursor.fetchall():
            event_id, event_legacy_id, change_type, reason = row
            changes.append({
                'event_id': event_id,
                'event_legacy_id': event_legacy_id,
                'type': change_type.replace('_', ' ').title(),
                'reason': reason
            })

        return changes

    def _get_peep_attendance(self, peep_id: int, period_name: str) -> List[Dict]:
        """Get attendance for a specific peep in a period."""
        self.cursor.execute("""
            SELECT e.id, e.legacy_period_event_id, att.actual_role, att.participation_mode
            FROM event_attendance att
            JOIN events e ON att.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            WHERE att.peep_id = ? AND sp.period_name = ?
            ORDER BY e.event_datetime
        """, (peep_id, period_name))

        attendance = []
        for row in self.cursor.fetchall():
            event_id, event_legacy_id, role, mode = row
            attendance.append({
                'event_id': event_id,
                'event_legacy_id': event_legacy_id,
                'role': role.title(),
                'mode': mode
            })

        return attendance

    def _generate_event_summary_table(self, period_name: str) -> List[str]:
        """Generate a summary table of all events."""
        output = []
        output.append("### EVENT SUMMARY")

        # Get all events for this period
        self.cursor.execute("""
            SELECT e.id, e.legacy_period_event_id, e.event_datetime, e.status, e.duration_minutes,
                   COUNT(DISTINCT ea.peep_id) as assigned_count,
                   COUNT(DISTINCT att.peep_id) as attended_count,
                   COUNT(DISTINCT eac.id) as changes_count
            FROM events e
            JOIN schedule_periods sp ON e.period_id = sp.id
            LEFT JOIN event_assignments ea ON ea.event_id = e.id
            LEFT JOIN event_attendance att ON att.event_id = e.id
            LEFT JOIN event_assignment_changes eac ON eac.event_id = e.id
            WHERE sp.period_name = ?
            GROUP BY e.id, e.legacy_period_event_id, e.event_datetime, e.status, e.duration_minutes
            ORDER BY e.event_datetime
        """, (period_name,))

        events = self.cursor.fetchall()
        if not events:
            output.append("No events found.")
            return output

        # Table header
        output.append("```")
        output.append("Event ID | Date & Time           | Duration | Status    | Assigned | Attended | Changes | Issues")
        output.append("---------|----------------------|----------|-----------|----------|----------|---------|--------")

        for event_id, legacy_id, datetime_str, status, duration, assigned, attended, changes in events:
            try:
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                formatted_date = dt.strftime('%m/%d %I:%M%p')
            except:
                formatted_date = datetime_str[:10]

            # Detect issues (only flag if there are discrepancies without change records)
            # But first check if this is a completed period
            issues = []

            # Get period status for this event
            self.cursor.execute("""
                SELECT sp.status FROM schedule_periods sp
                JOIN events e ON e.period_id = sp.id
                WHERE e.id = ? LIMIT 1
            """, (event_id,))
            period_status_result = self.cursor.fetchone()
            is_completed = period_status_result and period_status_result[0] == 'completed'

            if is_completed and assigned != attended and changes == 0:
                if assigned > attended:
                    issues.append("MISSING_NO_CHANGE")
                elif attended > assigned:
                    issues.append("EXTRA_NO_CHANGE")
            # Note: Having changes when assigned != attended is GOOD, not an issue

            issues_str = ", ".join(issues) if issues else "✓"
            status_short = status[:9]  # Truncate status

            duration_str = f"{duration}min" if duration else "?"
            output.append(f"{event_id:8} | {formatted_date:20} | {duration_str:8} | {status_short:9} | {assigned:8} | {attended:8} | {changes:7} | {issues_str}")

        output.append("```")
        return output

    def _generate_event_details_tables(self, period_name: str) -> List[str]:
        """Generate detailed tables for each event with assignments and attendance."""
        output = []

        # Get events with people involved
        self.cursor.execute("""
            SELECT DISTINCT e.id, e.legacy_period_event_id, e.event_datetime, e.status
            FROM events e
            JOIN schedule_periods sp ON e.period_id = sp.id
            LEFT JOIN event_assignments ea ON ea.event_id = e.id
            LEFT JOIN event_attendance att ON att.event_id = e.id
            WHERE sp.period_name = ?
            AND (ea.peep_id IS NOT NULL OR att.peep_id IS NOT NULL)
            ORDER BY e.event_datetime
        """, (period_name,))

        events = self.cursor.fetchall()

        for event_id, legacy_id, datetime_str, status in events:
            try:
                dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                formatted_datetime = dt.strftime('%a %b %d, %Y at %I:%M %p')
            except:
                formatted_datetime = datetime_str

            output.append(f"### EVENT {event_id} - {formatted_datetime}")
            output.append(f"Status: {status.title()}")
            output.append("")

            # Get all people involved in this event
            self.cursor.execute("""
                SELECT DISTINCT p.id, p.display_name
                FROM peeps p
                WHERE p.id IN (
                    SELECT ea.peep_id FROM event_assignments ea WHERE ea.event_id = ?
                    UNION
                    SELECT att.peep_id FROM event_attendance att WHERE att.event_id = ?
                )
                ORDER BY p.display_name
            """, (event_id, event_id))

            people = self.cursor.fetchall()

            if people:
                output.append("#### Assignments & Attendance")
                output.append("```")
                output.append("Person               | Type      | Role      | Attended? | Actual Role | Changes               | Issues")
                output.append("---------------------|-----------|-----------|-----------|-------------|----------------------|-------")

                # Get period status for this event once
                self.cursor.execute("""
                    SELECT sp.status FROM schedule_periods sp
                    JOIN events e ON e.period_id = sp.id
                    WHERE e.id = ? LIMIT 1
                """, (event_id,))
                period_status_result = self.cursor.fetchone()
                is_completed = period_status_result and period_status_result[0] == 'completed'

                for peep_id, name in people:
                    # Get assignment
                    self.cursor.execute("""
                        SELECT assigned_role, assignment_type, assignment_order, alternate_position
                        FROM event_assignments
                        WHERE event_id = ? AND peep_id = ?
                    """, (event_id, peep_id))
                    assignment = self.cursor.fetchone()

                    if assignment:
                        assigned_role, assignment_type, order, alt_pos = assignment
                        type_display = assignment_type.title()
                        if assignment_type == 'alternate' and alt_pos:
                            type_display = f"Alt #{alt_pos}"
                        role_display = assigned_role.title()
                    else:
                        type_display = "-"
                        role_display = "-"

                    # Get attendance
                    self.cursor.execute("""
                        SELECT actual_role, participation_mode
                        FROM event_attendance
                        WHERE event_id = ? AND peep_id = ?
                    """, (event_id, peep_id))
                    attendance = self.cursor.fetchone()

                    if attendance:
                        actual_role, participation_mode = attendance
                        attended_display = "Yes"
                        actual_role_display = actual_role.title()

                        # Add participation mode info for non-scheduled attendance
                        if participation_mode == 'volunteer_fill':
                            attended_display = "Yes (Vol)"
                        elif participation_mode == 'alternate_promoted':
                            attended_display = "Yes (Prom)"
                    else:
                        attended_display = "No"
                        actual_role_display = "-"

                    # Get changes
                    self.cursor.execute("""
                        SELECT GROUP_CONCAT(change_type, ', ') as changes
                        FROM event_assignment_changes
                        WHERE event_id = ? AND peep_id = ?
                    """, (event_id, peep_id))
                    changes_result = self.cursor.fetchone()
                    changes = changes_result[0] if changes_result and changes_result[0] else "-"

                    # Detect issues (only for completed periods)
                    issues = []
                    has_changes = changes != "-"

                    if is_completed:
                        # Only flag missing attendance as an issue for main attendees, not alternates
                        # (Alternates not attending is normal behavior and doesn't need change records)
                        if assignment and not attendance and not has_changes:
                            if assignment_type == 'attendee':  # Only flag main attendees
                                issues.append("NO_CHANGE")
                            # Alternates not attending is normal - no issue
                        elif attendance and not assignment and not has_changes:
                            issues.append("UNDOC_VOL")
                        elif assignment and attendance and assigned_role != actual_role and not has_changes:
                            issues.append("ROLE_CHG")

                    # Format displays with truncation
                    name_display = name[:20]
                    type_display = type_display[:9]
                    role_display = role_display[:9]
                    attended_display = attended_display[:9]
                    actual_role_display = actual_role_display[:11]
                    changes_display = changes[:21] if changes != "-" else "-"
                    issues_display = ",".join(issues) if issues else "✓"

                    output.append(f"{name_display:20} | {type_display:9} | {role_display:9} | {attended_display:9} | {actual_role_display:11} | {changes_display:21} | {issues_display}")

                output.append("```")

                # Add legend for abbreviations
                output.append("")
                output.append("**Legend:** Vol=Volunteer Fill, Prom=Alternate Promoted, Alt #N=Alternate Position N")
            else:
                output.append("No assignments or attendance.")

            output.append("")

        return output

    def _generate_issues_summary(self, period_name: str) -> List[str]:
        """Generate a summary of all issues found."""
        output = []
        output.append("### ISSUES SUMMARY")

        # Check period status - only flag issues for completed periods
        self.cursor.execute("SELECT status FROM schedule_periods WHERE period_name = ?", (period_name,))
        period_status = self.cursor.fetchone()
        if not period_status:
            output.append(f"Period {period_name} not found.")
            return output

        period_status = period_status[0]
        if period_status != 'completed':
            output.append(f"✅ Period status is '{period_status}' - no attendance/change validation performed")
            output.append("(Issues are only checked for completed periods where events have actually happened)")
            return output

        issues_found = []

        # Find MAIN ATTENDEES assigned but didn't attend WITHOUT a change record documenting it
        # (Alternates not attending is normal and doesn't need cancel records)
        self.cursor.execute("""
            SELECT e.id, p.display_name, ea.assigned_role
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            JOIN peeps p ON ea.peep_id = p.id
            WHERE sp.period_name = ?
            AND ea.assignment_type = 'attendee'  -- Only check main attendees
            AND NOT EXISTS (
                SELECT 1 FROM event_attendance att
                WHERE att.event_id = ea.event_id AND att.peep_id = ea.peep_id
            )
            AND NOT EXISTS (
                SELECT 1 FROM event_assignment_changes eac
                WHERE eac.event_id = ea.event_id AND eac.peep_id = ea.peep_id AND eac.change_type = 'cancel'
            )
        """, (period_name,))

        undocumented_no_shows = self.cursor.fetchall()
        for event_id, name, role in undocumented_no_shows:
            issues_found.append(f"🚫 Event {event_id}: {name} assigned as {role} but didn't attend (NO CHANGE RECORD)")

        # Find people who attended without assignment WITHOUT a change record documenting it
        self.cursor.execute("""
            SELECT e.id, p.display_name, att.actual_role
            FROM event_attendance att
            JOIN events e ON att.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            JOIN peeps p ON att.peep_id = p.id
            WHERE sp.period_name = ?
            AND att.participation_mode = 'volunteer_fill'
            AND NOT EXISTS (
                SELECT 1 FROM event_assignment_changes eac
                WHERE eac.event_id = att.event_id AND eac.peep_id = att.peep_id AND eac.change_type = 'add'
            )
        """, (period_name,))

        undocumented_volunteers = self.cursor.fetchall()
        for event_id, name, role in undocumented_volunteers:
            issues_found.append(f"🆕 Event {event_id}: {name} attended as {role} without assignment (NO CHANGE RECORD)")

        # Find role mismatches WITHOUT a change record documenting it
        self.cursor.execute("""
            SELECT e.id, p.display_name, ea.assigned_role, att.actual_role
            FROM event_assignments ea
            JOIN event_attendance att ON ea.event_id = att.event_id AND ea.peep_id = att.peep_id
            JOIN events e ON ea.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            JOIN peeps p ON ea.peep_id = p.id
            WHERE sp.period_name = ?
            AND ea.assigned_role != att.actual_role
            AND NOT EXISTS (
                SELECT 1 FROM event_assignment_changes eac
                WHERE eac.event_id = ea.event_id AND eac.peep_id = ea.peep_id AND eac.change_type = 'change_role'
            )
        """, (period_name,))

        undocumented_role_changes = self.cursor.fetchall()
        for event_id, name, assigned_role, actual_role in undocumented_role_changes:
            issues_found.append(f"🔄 Event {event_id}: {name} assigned as {assigned_role} but attended as {actual_role} (NO CHANGE RECORD)")

        # Find alternate promotions WITHOUT a change record documenting it
        self.cursor.execute("""
            SELECT e.id, p.display_name, att.actual_role
            FROM event_attendance att
            JOIN events e ON att.event_id = e.id
            JOIN schedule_periods sp ON e.period_id = sp.id
            JOIN peeps p ON att.peep_id = p.id
            WHERE sp.period_name = ?
            AND att.participation_mode = 'alternate_promoted'
            AND NOT EXISTS (
                SELECT 1 FROM event_assignment_changes eac
                WHERE eac.event_id = att.event_id AND eac.peep_id = att.peep_id AND eac.change_type = 'promote_alternate'
            )
        """, (period_name,))

        undocumented_alt_promotions = self.cursor.fetchall()
        for event_id, name, role in undocumented_alt_promotions:
            issues_found.append(f"🔼 Event {event_id}: {name} attended as {role} (alternate promoted) but missing promote_alternate change record")

        if issues_found:
            for issue in issues_found:
                output.append(issue)
        else:
            output.append("✅ No issues found - all assignments match attendance perfectly!")

        return output


def main():
    """Main entry point for the period report generator."""
    parser = argparse.ArgumentParser(
        description='Generate period verification reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate report for all periods
    python db/period_report.py

    # Generate report for specific period
    python db/period_report.py --period 2025-06

    # Save report to file
    python db/period_report.py --output period_report.txt

    # Generate report for specific period and save to file
    python db/period_report.py --period 2025-06 --output june_report.txt
        """
    )

    parser.add_argument('--period',
                       help='Generate report for specific period only')
    parser.add_argument('--output', '-o',
                       help='Save report to file instead of printing to console')

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

        print(f"Connected to database: {db_path}")

    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    try:
        # Create reporter
        reporter = PeriodReporter(conn, cursor)

        # Generate report
        if args.period:
            reporter.generate_period_report(args.period, args.output)
        else:
            reporter.generate_all_periods_report(args.output)

        return 0

    except Exception as e:
        print(f"Error generating report: {e}")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())