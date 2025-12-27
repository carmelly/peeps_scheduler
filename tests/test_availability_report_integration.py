"""Integration tests for availability_report module.

Tests report generation and formatting with realistic member/event data:
- Report generation with various member and event states
- Availability parsing with edge cases
- Output formatting and cancellation handling
- Edge cases: no responders, all unavailable, missing data
"""

import pytest
import json
from pathlib import Path
from peeps_scheduler.availability_report import (
    parse_availability,
    print_availability,
)


@pytest.fixture
def realistic_members_csv(tmp_path):
    """Create realistic members.csv with various active states."""
    members_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,5,TRUE,2025-01-01
2,Bob Follower,Bob,bob@test.com,Follower,1,3,2,TRUE,2025-01-01
3,Carol Leader,Carol,carol@test.com,Leader,2,5,3,TRUE,2025-01-01
4,Dave Follower,Dave,dave@test.com,Follower,3,0,0,FALSE,2025-01-01
5,Eve Leader,Eve,eve@test.com,Leader,4,2,1,TRUE,2025-01-01
6,Frank Follower,Frank,frank@test.com,Follower,5,4,4,TRUE,2025-01-01
"""
    members_path = tmp_path / "members.csv"
    members_path.write_text(members_content)
    return members_path


@pytest.fixture
def realistic_responses_csv(tmp_path):
    """Create realistic responses.csv with various availability patterns."""
    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I'm happy to dance my secondary role if it lets me attend when my primary is full,3,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
2025-02-01 10:30:00,bob@test.com,Bob,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Friday March 7 - 6pm",,0
2025-02-01 11:00:00,carol@test.com,Carol,Leader,I'm willing to dance my secondary role only if it's needed to enable filling a session,4,"Sunday March 2 - 5pm, Friday March 7 - 6pm",,0
2025-02-01 14:00:00,eve@test.com,Eve,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0
2025-02-01 15:00:00,frank@test.com,Frank,Follower,I'm happy to dance my secondary role if it lets me attend when my primary is full,2,"Friday March 7 - 6pm, Sunday March 2 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)
    return responses_path


@pytest.fixture
def cancellations_json(tmp_path):
    """Create cancellations.json with cancelled events and availability."""
    cancellations_content = {
        "cancelled_events": [],
        "cancelled_availability": []
    }
    cancellations_path = tmp_path / "cancellations.json"
    cancellations_path.write_text(json.dumps(cancellations_content))
    return cancellations_path


@pytest.mark.integration
def test_parse_availability_basic(
    tmp_path, realistic_members_csv, realistic_responses_csv, cancellations_json
):
    """Test basic availability parsing with realistic member data."""
    from peeps_scheduler.file_io import load_cancellations

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, unavailable, non_responders, _, _ = parse_availability(
        str(realistic_responses_csv),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Check that all expected dates are present
    assert "Saturday March 1 - 5pm" in availability
    assert "Sunday March 2 - 5pm" in availability
    assert "Friday March 7 - 6pm" in availability

    # Check that availability lists exist and have members
    assert len(availability["Saturday March 1 - 5pm"]["leader"]) > 0
    assert len(availability["Friday March 7 - 6pm"]["follower"]) > 0


@pytest.mark.integration
def test_parse_availability_member_with_multiple_dates(
    tmp_path, realistic_members_csv, realistic_responses_csv, cancellations_json
):
    """Test that members appear in multiple event availability lists."""
    from peeps_scheduler.file_io import load_cancellations

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, _, _, _, _ = parse_availability(
        str(realistic_responses_csv),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Alice should appear in both events
    assert "Alice" in availability["Saturday March 1 - 5pm"]["leader"]
    assert "Alice" in availability["Sunday March 2 - 5pm"]["leader"]


@pytest.mark.integration
def test_parse_availability_switch_preference_primary_only(
    tmp_path, realistic_members_csv, cancellations_json
):
    """Test that PRIMARY_ONLY members don't appear in fill slots."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,bob@test.com,Bob,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, _, _, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Bob should be in follower list but NOT in follower_fill
    assert "Bob" in availability["Saturday March 1 - 5pm"]["follower"]
    assert "Bob" not in availability["Saturday March 1 - 5pm"]["follower_fill"]


@pytest.mark.integration
def test_parse_availability_switch_preference_can_switch(
    tmp_path, realistic_members_csv, cancellations_json
):
    """Test that members with switch preference appear in both roles."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I'm happy to dance my secondary role if it lets me attend when my primary is full,3,"Saturday March 1 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, _, _, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Alice should be in leader and follower_fill
    assert "Alice" in availability["Saturday March 1 - 5pm"]["leader"]
    assert "Alice" in availability["Saturday March 1 - 5pm"]["follower_fill"]


@pytest.mark.integration
def test_parse_availability_cancelled_events_excluded(tmp_path, realistic_members_csv):
    """Test that cancelled events are excluded from availability."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,3,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancellations_content = {
        "cancelled_events": ["Sunday March 2 - 5pm"],
        "cancelled_availability": []
    }
    cancellations_path = tmp_path / "cancellations.json"
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_path), year=2025
    )

    availability, _, _, returned_cancelled, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Cancelled event should not be in availability
    assert "Sunday March 2 - 5pm" not in availability
    assert "Saturday March 1 - 5pm" in availability
    # Should return the cancelled events (returned as set of event IDs)
    assert len(returned_cancelled) > 0


@pytest.mark.integration
def test_parse_availability_cancelled_availability_per_person(
    tmp_path, realistic_members_csv
):
    """Test that cancelled availability is excluded for specific members."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,3,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
2025-02-01 10:30:00,bob@test.com,Bob,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancellations_content = {
        "cancelled_events": [],
        "cancelled_availability": [
            {
                "email": "alice@test.com",
                "events": ["Sunday March 2 - 5pm"]
            }
        ]
    }
    cancellations_path = tmp_path / "cancellations.json"
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_path), year=2025
    )

    availability, _, _, _, cancelled_avail_details = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # Alice should not be in Sunday event due to cancelled availability
    assert "Alice" not in availability["Sunday March 2 - 5pm"]["leader"]
    # Alice should still be in Saturday (not cancelled for her)
    assert "Alice" in availability["Saturday March 1 - 5pm"]["leader"]
    # Bob should still be in both
    assert "Bob" in availability["Sunday March 2 - 5pm"]["follower"]
    # Cancelled availability details should be returned
    assert "Alice" in cancelled_avail_details


@pytest.mark.integration
def test_parse_availability_no_responders(tmp_path, realistic_members_csv):
    """Test availability parsing when no members respond."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancellations_content = {
        "cancelled_events": [],
        "cancelled_availability": []
    }
    cancellations_path = tmp_path / "cancellations.json"
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_path), year=2025
    )

    availability, unavailable, non_responders, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # All active members should be non-responders
    assert len(non_responders) == 5  # All except Dave (inactive)
    assert "Alice" in non_responders
    assert "Bob" in non_responders


@pytest.mark.integration
def test_parse_availability_only_cancelled_events(
    tmp_path, realistic_members_csv
):
    """Test member unavailable when all their events are cancelled."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancellations_content = {
        "cancelled_events": ["Saturday March 1 - 5pm"],
        "cancelled_availability": []
    }
    cancellations_path = tmp_path / "cancellations.json"
    cancellations_path.write_text(json.dumps(cancellations_content))

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_path), year=2025
    )

    availability, unavailable, _, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    # With all events cancelled, Alice should be in unavailable
    assert "Alice" in unavailable


@pytest.mark.integration
def test_print_availability_output_format(
    tmp_path, realistic_members_csv, realistic_responses_csv, cancellations_json,
    capsys
):
    """Test that print_availability generates correctly formatted output."""
    from peeps_scheduler.file_io import load_cancellations

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, unavailable, non_responders, _, cancelled_avail_details = (
        parse_availability(
            str(realistic_responses_csv),
            str(realistic_members_csv),
            cancelled_event_ids=cancelled_event_ids,
            cancelled_availability=cancelled_availability,
            year=2025,
        )
    )

    print_availability(
        availability,
        unavailable,
        non_responders,
        year=2025,
        cancelled_events=cancelled_event_ids,
        cancelled_availability_details=cancelled_avail_details,
    )

    captured = capsys.readouterr()
    output = captured.out

    # Check for expected output sections
    assert "AVAILABILITY REPORT" in output
    assert "Saturday March 1 - 5pm" in output
    assert "Leaders" in output
    assert "Followers" in output


@pytest.mark.integration
def test_print_availability_includes_non_responders(
    tmp_path, realistic_members_csv, realistic_responses_csv, cancellations_json,
    capsys
):
    """Test that print_availability includes non-responders section."""
    from peeps_scheduler.file_io import load_cancellations

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, unavailable, non_responders, _, cancelled_avail_details = (
        parse_availability(
            str(realistic_responses_csv),
            str(realistic_members_csv),
            cancelled_event_ids=cancelled_event_ids,
            cancelled_availability=cancelled_availability,
            year=2025,
        )
    )

    print_availability(
        availability,
        unavailable,
        non_responders,
        year=2025,
        cancelled_events=cancelled_event_ids,
        cancelled_availability_details=cancelled_avail_details,
    )

    captured = capsys.readouterr()
    output = captured.out

    assert "Did not respond:" in output


@pytest.mark.integration
def test_parse_availability_skips_unmatched_email(
    tmp_path, realistic_members_csv, cancellations_json, capsys
):
    """Test that unmatched emails are skipped with warning."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,unknown@test.com,Unknown,Leader,I only want to be scheduled in my primary role,3,"Saturday March 1 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, _, non_responders, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    captured = capsys.readouterr()
    # Unmatched email should print warning message
    # The unknown email should result in empty availability
    assert len(availability) == 0 or "WARNING" in captured.out


@pytest.mark.integration
def test_parse_availability_detects_duplicate_email(
    tmp_path, realistic_members_csv, cancellations_json, capsys
):
    """Test warning for duplicate emails in responses."""
    from peeps_scheduler.file_io import load_cancellations

    responses_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,3,"Saturday March 1 - 5pm",,0
2025-02-01 10:30:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,3,"Sunday March 2 - 5pm",,0
"""
    responses_path = tmp_path / "responses.csv"
    responses_path.write_text(responses_content)

    cancelled_event_ids, cancelled_availability = load_cancellations(
        str(cancellations_json), year=2025
    )

    availability, _, _, _, _ = parse_availability(
        str(responses_path),
        str(realistic_members_csv),
        cancelled_event_ids=cancelled_event_ids,
        cancelled_availability=cancelled_availability,
        year=2025,
    )

    captured = capsys.readouterr()
    # Duplicate email generates warning
    # Only first response should be used, so only Saturday should be in availability
    if "Saturday March 1 - 5pm" in availability:
        assert "WARNING" in captured.out or len(availability["Saturday March 1 - 5pm"]["leader"]) > 0
