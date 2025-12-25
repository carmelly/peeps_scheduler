"""File builder fixtures for creating test data files.

These fixtures convert data specifications (EventSpec, ResponseSpec, MemberSpec)
into production file formats (CSV/JSON) for testing.

Shared by both CLI tests and database import tests to ensure consistent test data.
"""

import csv
import json
from pathlib import Path
from typing import Any
import pytest
from .data_specs import AttendanceSpec, EventSpec, MemberSpec, ResponseSpec


@pytest.fixture
def results_json_builder():
    """Factory: Build results.json from EventSpec list.

    Args:
        period_dir: Path to period directory where results.json will be written
        events: List of EventSpec objects

    Returns:
        Path to created results.json file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        events = [
            EventSpec(date="2025-02-07 17:00", attendees=[(1, "Alice", "leader")]),
            EventSpec(date="2025-02-14 17:00", attendees=[(2, "Bob", "follower")])
        ]
        results_path = results_json_builder(period_dir, events)
    """
    def _build(period_dir: Path, events: list[EventSpec]) -> Path:
        # Build valid_events structure
        valid_events = []
        for idx, event_spec in enumerate(events):
            # Build attendees list
            attendees = [
                {'id': peep_id, 'name': name, 'role': role}
                for peep_id, name, role in event_spec.attendees
            ]

            # Build alternates list
            alternates = [
                {'id': peep_id, 'name': name, 'role': role}
                for peep_id, name, role in event_spec.alternates
            ]

            # Auto-generate leader/follower strings if not provided
            if event_spec.leaders_string is None:
                leaders = [a for a in attendees + alternates if a['role'] == 'leader']
                leader_names = ', '.join(a['name'] for a in leaders)
                leaders_string = f"Leaders({len(leaders)}): {leader_names}" if leaders else "Leaders(0):"
            else:
                leaders_string = event_spec.leaders_string

            if event_spec.followers_string is None:
                followers = [a for a in attendees + alternates if a['role'] == 'follower']
                follower_names = ', '.join(a['name'] for a in followers)
                followers_string = f"Followers({len(followers)}): {follower_names}" if followers else "Followers(0):"
            else:
                followers_string = event_spec.followers_string

            valid_events.append({
                'id': idx,
                'date': event_spec.date,
                'duration_minutes': event_spec.duration_minutes,
                'attendees': attendees,
                'alternates': alternates,
                'leaders_string': leaders_string,
                'followers_string': followers_string
            })

        # Build complete results structure
        results_data = {
            'valid_events': valid_events,
            'peeps': [],
            'num_unique_attendees': 0,
            'priority_fulfilled': 0,
            'system_weight': 0
        }

        # Write results.json
        results_path = period_dir / 'results.json'
        with open(results_path, 'w') as f:
            json.dump(results_data, f, indent=2)

        return results_path

    return _build


@pytest.fixture
def responses_csv_builder():
    """Factory: Build responses.csv from ResponseSpec list.

    Args:
        period_dir: Path to period directory where responses.csv will be written
        responses: List of ResponseSpec objects

    Returns:
        Path to created responses.csv file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        responses = [
            ResponseSpec(
                email="alice@test.com",
                name="Alice",
                role="leader",
                availability=["Friday February 7th - 5pm to 7pm"]
            )
        ]
        csv_path = responses_csv_builder(period_dir, responses)
    """
    def _build(period_dir: Path, responses: list[ResponseSpec]) -> Path:
        # Build response rows (match production format)
        rows = []
        for resp_spec in responses:
            availability_str = ', '.join(resp_spec.availability)
            rows.append({
                'Timestamp': resp_spec.timestamp,
                'Email Address': resp_spec.email,
                'Name': resp_spec.name,
                'Primary Role': resp_spec.role,
                'Secondary Role': resp_spec.secondary_role,
                'Max Sessions': resp_spec.max_sessions,
                'Availability': availability_str,
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Min Interval Days': resp_spec.min_interval_days,
                'Partnership Preference': resp_spec.partnership_preference,
                'Questions or Comments': resp_spec.questions_comments
            })

        # Write responses.csv
        csv_path = period_dir / 'responses.csv'
        with open(csv_path, 'w', newline='') as f:
            fieldnames = [
                'Timestamp', 'Email Address', 'Name', 'Primary Role', 'Secondary Role',
                'Max Sessions', 'Availability', 'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return csv_path

    return _build


@pytest.fixture
def members_csv_builder():
    """Factory: Build members.csv from MemberSpec list.

    Args:
        period_dir: Path to period directory where members.csv will be written
        members: List of MemberSpec objects

    Returns:
        Path to created members.csv file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        members = [
            MemberSpec(csv_id=1, name="Alice", role="leader"),
            MemberSpec(csv_id=2, name="Bob", role="follower")
        ]
        csv_path = members_csv_builder(period_dir, members)
    """
    def _build(period_dir: Path, members: list[MemberSpec]) -> Path:
        # Build member rows (match production format)
        rows = []
        for member_spec in members:
            rows.append({
                'id': member_spec.csv_id,
                'Name': member_spec.name,
                'Display Name': member_spec.display_name,
                'Email Address': member_spec.email,
                'Role': member_spec.role,
                'Index': member_spec.index,
                'Priority': member_spec.priority,
                'Total Attended': member_spec.total_attended,
                'Active': 'TRUE' if member_spec.active else 'FALSE',
                'Date Joined': member_spec.date_joined
            })

        # Write members.csv
        csv_path = period_dir / 'members.csv'
        with open(csv_path, 'w', newline='') as f:
            fieldnames = [
                'id', 'Name', 'Display Name', 'Email Address', 'Role',
                'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return csv_path

    return _build


@pytest.fixture
def attendance_json_builder():
    """Factory: Build actual_attendance.json from AttendanceSpec list.

    Args:
        period_dir: Path to period directory where actual_attendance.json will be written
        attendance_events: List of AttendanceSpec objects

    Returns:
        Path to created actual_attendance.json file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        attendance = [
            AttendanceSpec(
                date="2025-02-07 17:00",
                attendees=[(1, "Alice", "leader"), (2, "Bob", "follower")]
            )
        ]
        json_path = attendance_json_builder(period_dir, attendance)
    """
    def _build(period_dir: Path, attendance_events: list[AttendanceSpec]) -> Path:
        # Build valid_events structure for attendance
        valid_events = []
        for idx, attendance_spec in enumerate(attendance_events):
            # Build attendees list
            attendees = [
                {'id': peep_id, 'name': name, 'role': role}
                for peep_id, name, role in attendance_spec.attendees
            ]

            # Auto-generate leader/follower strings if not provided
            if attendance_spec.leaders_string is None:
                leaders = [a for a in attendees if a['role'] == 'leader']
                leader_names = ', '.join(a['name'] for a in leaders)
                leaders_string = f"Leaders({len(leaders)}): {leader_names}" if leaders else "Leaders(0):"
            else:
                leaders_string = attendance_spec.leaders_string

            if attendance_spec.followers_string is None:
                followers = [a for a in attendees if a['role'] == 'follower']
                follower_names = ', '.join(a['name'] for a in followers)
                followers_string = f"Followers({len(followers)}): {follower_names}" if followers else "Followers(0):"
            else:
                followers_string = attendance_spec.followers_string

            valid_events.append({
                'id': idx,
                'date': attendance_spec.date,
                'duration_minutes': attendance_spec.duration_minutes,
                'attendees': attendees,
                'alternates': [],  # Attendance has no alternates
                'leaders_string': leaders_string,
                'followers_string': followers_string
            })

        # Build attendance structure
        attendance_data = {
            'valid_events': valid_events
        }

        # Write actual_attendance.json
        attendance_path = period_dir / 'actual_attendance.json'
        with open(attendance_path, 'w') as f:
            json.dump(attendance_data, f, indent=2)

        return attendance_path

    return _build


@pytest.fixture
def cancellations_json_builder():
    """Factory: Build cancellations.json from dict structure.

    Args:
        period_dir: Path to period directory where cancellations.json will be written
        data: Dict with 'cancelled_events', 'cancelled_availability', 'notes' keys

    Returns:
        Path to created cancellations.json file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        cancellations_data = {
            'cancelled_events': [0, 2],
            'cancelled_availability': [{'peep_id': 1, 'event_id': 1}],
            'notes': 'Test cancellations'
        }
        json_path = cancellations_json_builder(period_dir, cancellations_data)
    """
    def _build(period_dir: Path, data: dict[str, Any]) -> Path:
        # Set defaults
        cancellations_data = {
            'cancelled_events': data.get('cancelled_events', []),
            'cancelled_availability': data.get('cancelled_availability', []),
            'notes': data.get('notes', '')
        }

        # Write cancellations.json
        cancellations_path = period_dir / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f, indent=2)

        return cancellations_path

    return _build


@pytest.fixture
def partnerships_json_builder():
    """Factory: Build partnerships.json from dict structure.

    Args:
        period_dir: Path to period directory where partnerships.json will be written
        partnerships: Dict mapping peep_id (as string) to list of partner IDs

    Returns:
        Path to created partnerships.json file

    Example:
        period_dir = Path(prepared_importer['period_data']['period_dir'])
        partnerships = {
            "1": [2, 3],
            "2": [1],
            "3": [1]
        }
        json_path = partnerships_json_builder(period_dir, partnerships)
    """
    def _build(period_dir: Path, partnerships: dict[str, list[int]]) -> Path:
        # Write partnerships.json
        partnerships_path = period_dir / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships, f, indent=2)

        return partnerships_path

    return _build
