"""Database testing fixtures and helpers.

This conftest makes database helpers available to all db tests.
"""

import datetime
import json
import pytest
from tests.fixtures.data_specs import (
	AttendanceSpec,
	EventSpec,
	MemberSpec,
	ResponseSpec,
)

# Import helpers to make them available to test modules
# Tests can use: from tests.db.helpers import assert_row_count
# Or import specific helpers as needed


@pytest.fixture
def test_period_data(
    tmp_path,
    members_csv_builder,
    responses_csv_builder,
    results_json_builder,
    attendance_json_builder,
    cancellations_json_builder,
    partnerships_json_builder,
):
    """Generate complete test period data using file builders.

    This shadows the old test_period_data from main conftest.py for all tests
    in tests/db/. Uses builders internally to ensure single source of truth for
    file formats.

    Returns:
        A factory function that creates test data. Call it with optional
        parameters to customize:
            test_period_data(period_name="2025-02", num_members=10, num_events=3)
    """

    def _create(period_name="2025-02", num_members=10, num_events=3):
        """Create test CSV/JSON files for a period in a temporary directory."""
        # Create directory structure
        period_dir = tmp_path / period_name
        period_dir.mkdir(parents=True, exist_ok=True)

        # Generate members using builder
        members = [
            MemberSpec(
                csv_id=i,
                name=f"Test Member {i}",
                email=f"member{i}@test.com",  # Must match response emails
                display_name=f"Member{i}",
                role="leader" if i % 2 == 1 else "follower",
                priority=i,
            )
            for i in range(1, num_members + 1)
        ]
        members_csv_builder(period_dir, members)

        # Generate responses using builder (first 8 members)
        event_dates = [
            "Friday February 7th - 5pm to 7pm",
            "Friday February 14th - 5pm to 7pm",
            "Friday February 21st - 5pm to 7pm",
        ][:num_events]

        responses = [
            ResponseSpec(
                email=f"member{i}@test.com",
                name=f"Test Member {i}",
                role="leader" if i % 2 == 1 else "follower",
                availability=event_dates,
                max_sessions=2,
                min_interval_days=0,
            )
            for i in range(1, min(num_members, 8) + 1)
        ]
        responses_csv_builder(period_dir, responses)

        # Generate results.json using builder
        base_date = datetime.datetime(2025, 2, 7, 17, 0)
        events = [
            EventSpec(
                date=(
                    base_date + datetime.timedelta(days=7 * idx)
                ).strftime("%Y-%m-%d %H:%M"),
                duration_minutes=120,
                attendees=[(1, "Member1", "leader"), (2, "Member2", "follower")],
                alternates=[(3, "Member3", "leader")],
            )
            for idx in range(num_events)
        ]
        results_json_builder(period_dir, events)

        # Generate attendance using builder
        attendance_events = [
            AttendanceSpec(
                date=(
                    base_date + datetime.timedelta(days=7 * idx)
                ).strftime("%Y-%m-%d %H:%M"),
                duration_minutes=120,
                attendees=[(1, "Member1", "leader"), (2, "Member2", "follower")],
            )
            for idx in range(num_events)
        ]
        attendance_json_builder(period_dir, attendance_events)

        # Generate empty cancellations and partnerships using builders
        cancellations_json_builder(period_dir, {})
        partnerships_json_builder(period_dir, {})

        # Write notes.json directly (not worth a builder)
        (period_dir / "notes.json").write_text(json.dumps([], indent=2))

        # Write output.json directly (not worth a builder)
        # Mirror results structure but represents scheduler's input state
        output_data = {
            "events": [
                {
                    "id": idx,
                    "date": (
                        base_date + datetime.timedelta(days=7 * idx)
                    ).strftime("%Y-%m-%d %H:%M"),
                    "duration_minutes": 120,
                    "attendees": [
                        {"id": 1, "name": "Member1", "role": "leader"},
                        {"id": 2, "name": "Member2", "role": "follower"},
                    ],
                    "alternates": [{"id": 3, "name": "Member3", "role": "leader"}],
                    "leaders_string": "Leaders(2): Member1, Member3",
                    "followers_string": "Followers(1): Member2",
                }
                for idx in range(num_events)
            ],
            "peeps": [],
            "members": [
                {
                    "id": i,
                    "Name": f"Test Member {i}",
                    "Display Name": f"Member{i}",
                    "Email Address": f"member{i}@test.com",
                    "Role": "leader" if i % 2 == 1 else "follower",
                    "Index": 0,
                    "Priority": i,
                    "Total Attended": 0,
                    "Active": "TRUE",
                    "Date Joined": "1/1/2025",
                }
                for i in range(1, num_members + 1)
            ],
        }
        (period_dir / "output.json").write_text(json.dumps(output_data, indent=2))

        yield {
            "temp_dir": str(tmp_path),
            "period_dir": str(period_dir),
            "period_name": period_name,
            "num_members": num_members,
            "num_events": num_events,
        }

    return _create
