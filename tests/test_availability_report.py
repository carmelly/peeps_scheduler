import json

import pytest

from peeps_scheduler.availability_report import parse_availability
from peeps_scheduler.file_io import load_cancellations


def test_parse_availability_applies_cancellations(tmp_path):
    members_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,4,0,TRUE,2025-01-01
2,Dana Follower,Dana,dana@test.com,Follower,1,4,0,TRUE,2025-01-01
"""
    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
2025-02-01 10:00:00,dana@test.com,Dana,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    cancellations_content = {
        "cancelled_events": ["Sunday March 2 - 5pm"],
        "cancelled_availability": [
            {
                "email": "alex@test.com",
                "events": ["Saturday March 1 - 5pm"]
            }
        ]
    }

    members_path = tmp_path / "members.csv"
    responses_path = tmp_path / "responses.csv"
    cancellations_path = tmp_path / "cancellations.json"

    members_path.write_text(members_content)
    responses_path.write_text(responses_content)
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(str(cancellations_path), year=2025)
    availability, unavailable, non_responders, _, _ = parse_availability(
        str(responses_path),
        str(members_path),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025
    )

    assert "Saturday March 1 - 5pm" in availability
    assert availability["Saturday March 1 - 5pm"]["leader"] == []
    assert availability["Saturday March 1 - 5pm"]["follower"] == ["Dana"]
    assert "Sunday March 2 - 5pm" not in availability
    assert unavailable == ["Alex"]
    assert non_responders == []


def test_parse_availability_raises_for_unknown_cancellation_email(tmp_path):
    members_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,4,0,TRUE,2025-01-01
"""
    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    cancellations_content = {
        "cancelled_events": [],
        "cancelled_availability": [
            {
                "email": "unknown@test.com",
                "events": ["Saturday March 1 - 5pm"]
            }
        ]
    }

    members_path = tmp_path / "members.csv"
    responses_path = tmp_path / "responses.csv"
    cancellations_path = tmp_path / "cancellations.json"

    members_path.write_text(members_content)
    responses_path.write_text(responses_content)
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(str(cancellations_path), year=2025)
    with pytest.raises(ValueError, match="unknown email"):
        parse_availability(
            str(responses_path),
            str(members_path),
            cancelled_event_ids=cancelled_event_ids,
            cancelled_availability=cancelled_availability,
            year=2025
        )
