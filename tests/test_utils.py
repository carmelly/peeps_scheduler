"""
Test utils.py functions, particularly apply_event_results which handles result application.

Following testing philosophy:
- Test what could actually break in result processing
- Use fixtures for complex file-based scenarios
- Focus on individual behaviors with separate test methods
- Fail fast on missing required files
"""

import pytest
import tempfile
import os
import json
import utils
from models import Peep, Event, Role


# --- Fixtures ---

@pytest.fixture
def members_csv_content():
    """Standard members CSV content with current format."""
    return """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,John Doe,John D.,john@example.com,Leader,0,5,2,True,2025-01-01
2,Jane Smith,Jane S.,jane@example.com,Follower,1,3,1,True,2025-01-01
3,Bob Wilson,Bob W.,bob@example.com,Leader,2,4,0,True,2025-01-01
4,Alice Brown,Alice B.,alice@example.com,Follower,3,2,1,True,2025-01-01"""


@pytest.fixture
def responses_csv_content():
    """Standard responses CSV content - John and Bob responded."""
    return """Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability
John Doe,john@example.com,Leader,Follower,2,"March 1, March 8"
Bob Wilson,bob@example.com,Leader,None,1,March 1"""


@pytest.fixture
def actual_attendance_data():
    """Actual attendance JSON - John attended both events, Jane attended one event."""
    return {
        'valid_events': [
            {
                'id': 0,
                'date': '2025-03-01 19:00',
                'duration_minutes': 90,
                'attendees': [
                    {'id': 1, 'role': 'leader'},    # John attended
                    {'id': 2, 'role': 'follower'}   # Jane attended
                ]
            },
            {
                'id': 1,
                'date': '2025-03-08 19:00',
                'duration_minutes': 90,
                'attendees': [
                    {'id': 1, 'role': 'leader'}     # John attended
                ]
            }
        ]
    }


@pytest.fixture
def temp_files(members_csv_content, responses_csv_content, actual_attendance_data):
    """Create temporary files for testing."""
    temp_dir = tempfile.mkdtemp()

    # Create members.csv
    members_path = os.path.join(temp_dir, 'members.csv')
    with open(members_path, 'w') as f:
        f.write(members_csv_content)

    # Create responses.csv
    responses_path = os.path.join(temp_dir, 'responses.csv')
    with open(responses_path, 'w') as f:
        f.write(responses_csv_content)

    # Create actual_attendance.json
    attendance_path = os.path.join(temp_dir, 'actual_attendance.json')
    with open(attendance_path, 'w') as f:
        json.dump(actual_attendance_data, f)

    yield {
        'temp_dir': temp_dir,
        'members': members_path,
        'responses': responses_path,
        'attendance': attendance_path
    }

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.unit
class TestApplyEventResultsErrorHandling:
    """Test error handling for missing files."""

    def test_missing_members_file_raises_error(self, temp_files):
        """Test that missing members.csv raises an error."""
        os.remove(temp_files['members'])

        with pytest.raises(FileNotFoundError):
            utils.apply_event_results(
                temp_files['attendance'],
                temp_files['members'],
                temp_files['responses']
            )

    def test_missing_attendance_file_raises_error(self, temp_files):
        """Test that missing actual_attendance.json raises an error."""
        os.remove(temp_files['attendance'])

        with pytest.raises(FileNotFoundError):
            utils.apply_event_results(
                temp_files['attendance'],
                temp_files['members'],
                temp_files['responses']
            )

    def test_missing_responses_file_handles_gracefully(self, temp_files):
        """Test that missing responses.csv is handled gracefully (responses_csv is optional)."""
        os.remove(temp_files['responses'])

        # Should not raise an error, just handle gracefully
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        # Should return peeps even without responses file
        assert len(result_peeps) > 0

        # All peeps should have responded=False since no responses file was processed
        for peep in result_peeps:
            assert peep.responded == False

    def test_none_responses_file_handles_gracefully(self, temp_files):
        """Test that None can be passed for responses_csv (responses_csv is optional)."""
        # Should not raise an error when responses_csv is None
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            None
        )

        # Should return peeps even with None responses file
        assert len(result_peeps) > 0

        # All peeps should have responded=False since no responses file was processed
        for peep in result_peeps:
            assert peep.responded == False

    def test_missing_responses_file_logs_debug_message(self, temp_files, caplog):
        """Test that debug message is logged when responses.csv is missing."""
        import logging
        os.remove(temp_files['responses'])

        with caplog.at_level(logging.DEBUG):
            utils.apply_event_results(
                temp_files['attendance'],
                temp_files['members'],
                temp_files['responses']
            )

        assert "No responses file provided or file does not exist" in caplog.text
        assert "skipping response processing" in caplog.text

    def test_none_responses_file_logs_debug_message(self, temp_files, caplog):
        """Test that debug message is logged when responses_csv is None."""
        import logging

        with caplog.at_level(logging.DEBUG):
            utils.apply_event_results(
                temp_files['attendance'],
                temp_files['members'],
                None
            )

        assert "No responses file provided or file does not exist" in caplog.text
        assert "skipping response processing" in caplog.text


@pytest.mark.unit
class TestRespondedFlagSetting:
    """Test that peep.responded is set correctly based on responses file."""

    @pytest.mark.parametrize("peep_id,expected_responded", [
        (1, True),   # John responded
        (2, False),  # Jane didn't respond
        (3, True),   # Bob responded
        (4, False),  # Alice didn't respond
    ])
    def test_responded_flag(self, peep_id, expected_responded, temp_files):
        """Test that responded flag is set correctly based on response file."""
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        peep = next(p for p in result_peeps if p.id == peep_id)
        assert peep.responded == expected_responded

    def test_email_matching_case_insensitive(self, temp_files):
        """Test that email matching works regardless of case."""
        # Create responses with different case emails
        responses_content = """Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability
John Doe,JOHN@EXAMPLE.COM,Leader,Follower,2,"March 1, March 8"
Bob Wilson,BOB@EXAMPLE.COM,Leader,None,1,March 1"""

        with open(temp_files['responses'], 'w') as f:
            f.write(responses_content)

        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        john = next(p for p in result_peeps if p.id == 1)
        bob = next(p for p in result_peeps if p.id == 3)

        assert john.responded == True
        assert bob.responded == True


@pytest.mark.unit
class TestAttendanceIncrementing:
    """Test that total_attended is incremented correctly."""

    def test_total_attended_incremented_for_attendees(self, temp_files):
        """Test that total_attended is incremented for event attendees."""
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        john = next(p for p in result_peeps if p.id == 1)
        jane = next(p for p in result_peeps if p.id == 2)

        # John attended 2 events (originally had 2, should now have 4)
        assert john.total_attended == 4

        # Jane attended 1 event (originally had 1, should now have 2)
        assert jane.total_attended == 2

    def test_total_attended_unchanged_for_non_attendees(self, temp_files):
        """Test that total_attended is unchanged for non-attendees."""
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        bob = next(p for p in result_peeps if p.id == 3)
        alice = next(p for p in result_peeps if p.id == 4)

        # Bob didn't attend any events (originally had 0, should still have 0)
        assert bob.total_attended == 0

        # Alice didn't attend any events (originally had 1, should still have 1)
        assert alice.total_attended == 1


@pytest.mark.unit
class TestPriorityLogic:
    """Test priority changes based on attendance and response status."""

    @pytest.mark.parametrize("peep_id,expected_priority,scenario", [
        (1, 0, "attended_resets_priority"),
        (2, 0, "attended_resets_priority"),
        (3, 5, "responded_but_didnt_attend_increases"),
        (4, 2, "didnt_respond_didnt_attend_unchanged"),
    ])
    def test_priority_changes(self, peep_id, expected_priority, scenario, temp_files):
        """Test priority changes based on attendance and response status."""
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        peep = next(p for p in result_peeps if p.id == peep_id)
        assert peep.priority == expected_priority


@pytest.mark.unit
class TestPeepIndexOrdering:
    """Test that peep index ordering is updated correctly after priority changes."""

    def test_index_ordering_updated_after_priority_changes(self, temp_files):
        """Test that peeps are reordered by priority after applying results."""
        result_peeps = utils.apply_event_results(
            temp_files['attendance'],
            temp_files['members'],
            temp_files['responses']
        )

        # After applying results:
        # Bob: priority 5, index should be 0 (highest priority)
        # Alice: priority 2, index should be 1
        # Jane: priority 0, index should be 2 (attended 1 event)
        # John: priority 0, index should be 3 (attended 2 events, pushed to back twice)

        bob = next(p for p in result_peeps if p.id == 3)
        alice = next(p for p in result_peeps if p.id == 4)
        john = next(p for p in result_peeps if p.id == 1)
        jane = next(p for p in result_peeps if p.id == 2)

        assert bob.index == 0    # Highest priority
        assert alice.index == 1
        assert jane.index == 2   # Attended 1 event
        assert john.index == 3   # Attended 2 events, most recent attendee