"""
Integration tests for the Novice Peeps scheduling system.

Following testing philosophy:
- Test end-to-end workflows that users actually encounter
- Focus on data pipeline integrity and real-world scenarios
- Use realistic (but small) datasets to verify system behavior
- Test cross-component integration, not individual unit behavior
"""

import pytest
import tempfile
import os
import json
import datetime
import shutil
from pathlib import Path

from models import Event, Peep, Role, EventSequence, SwitchPreference
from scheduler import Scheduler
import file_io
import utils


@pytest.mark.integration
@pytest.mark.slow
class TestEndToEndWorkflows:
    """Test complete user workflows from start to finish."""

    def test_full_pipeline_gracefully_handles_impossible_events(self):
        """Test complete end-to-end pipeline when no events can be scheduled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data with insufficient peeps (60-min events need 2 per role, we have 1 per role)
            test_data = {
                "events": [
                    {"id": 0, "date": "2025-03-15 19:00", "duration_minutes": 60}
                ],
                "peeps": [
                    {
                        "id": 1, "full_name": "Alice", "display_name": "Alice", "email": "alice@test.com",
                        "role": "leader", "index": 0, "priority": 1, "total_attended": 0,
                        "availability": [0], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    },
                    {
                        "id": 2, "full_name": "Bob", "display_name": "Bob", "email": "bob@test.com",
                        "role": "follower", "index": 1, "priority": 1, "total_attended": 0,
                        "availability": [0], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    }
                ],
                "responses": []
            }

            # Set up period directory structure (as scheduler.run() expects)
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Write input data to expected location
            output_json = period_path / "output.json"
            with open(output_json, 'w') as f:
                json.dump(test_data, f)

            # Run complete scheduler workflow
            scheduler = Scheduler(data_folder=str(period_path), max_events=1, interactive=False)
            result = scheduler.run()

            # Verify scheduler handled impossible scenario gracefully
            results_json = period_path / "results.json"

            # With impossible constraints, scheduler.run() should:
            # 1. Return None (early return when no sequences found)
            # 2. NOT create results.json file (save_event_sequence never called)
            assert result is None, f"Expected scheduler.run() to return None with impossible constraints, got {result}"
            assert not results_json.exists(), "Expected no results.json file created when no sequences can be scheduled"

    def test_scheduler_handles_impossible_constraints(self):
        """Test complete end-to-end pipeline with extremely impossible attendance constraints."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create scenario with 120-min event (needs 6 per role) but only 1 of each role
            test_data = {
                "events": [
                    {"id": 1, "date": "2025-03-15 19:00", "duration_minutes": 120}  # Needs 6 leaders + 6 followers
                ],
                "peeps": [
                    {
                        "id": 1, "full_name": "OnlyLeader", "display_name": "OnlyLeader", "email": "leader@test.com",
                        "role": "leader", "index": 0, "priority": 1, "total_attended": 0,
                        "availability": [1], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    },
                    {
                        "id": 2, "full_name": "OnlyFollower", "display_name": "OnlyFollower", "email": "follower@test.com",
                        "role": "follower", "index": 1, "priority": 1, "total_attended": 0,
                        "availability": [1], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    }
                ],
                "responses": []
            }

            # Set up period directory structure
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Write input data
            output_json = period_path / "output.json"
            with open(output_json, 'w') as f:
                json.dump(test_data, f)

            # Run complete scheduler workflow
            scheduler = Scheduler(data_folder=str(period_path), max_events=1, interactive=False)
            result = scheduler.run()

            # Verify scheduler handled extremely impossible scenario gracefully
            results_json = period_path / "results.json"

            # With extremely impossible constraints (1 peep per role for 120-min event), scheduler.run() should:
            # 1. Return None (early return when no sequences found)
            # 2. NOT create results.json file (save_event_sequence never called)
            assert result is None, f"Expected scheduler.run() to return None with impossible constraints, got {result}"
            assert not results_json.exists(), "Expected no results.json file created when constraints are impossible to meet"

    def test_scheduler_run_golden_master(self):
        """Test complete CSV-to-JSON-to-scheduler pipeline with golden master data.

        This test uses 2025-09-sanitized data as the golden master, which reflects
        the current state of the scheduling algorithm with sanitized test data.

        This test validates the complete end-to-end workflow:
        1. Load CSV files (members.csv and responses.csv)
        2. Convert CSV to JSON (output.json)
        3. Run scheduler algorithm
        4. Generate results (results.json)
        5. Verify all generated files match golden master exactly
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Load expected results from 2025-09-sanitized data
            golden_master_dir = Path(__file__).parent / "golden_master_2025_09_sanitized"

            with open(golden_master_dir / "results.json", 'r') as f:
                expected_results = json.load(f)

            # Set up period directory structure as scheduler expects
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Step 1: Copy CSV files from golden master for true end-to-end testing
            # This tests the complete CSV-to-JSON-to-scheduler pipeline
            responses_csv = period_path / "responses.csv"
            members_csv = period_path / "members.csv"
            shutil.copy(golden_master_dir / "responses.csv", responses_csv)
            shutil.copy(golden_master_dir / "members.csv", members_csv)

            # Step 2: Run scheduler with load_from_csv=True for full integration test
            # This tests CSV parsing, JSON conversion, and scheduling in one workflow
            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)
            result = scheduler.run(load_from_csv=True)

            # Verify scheduler succeeded (should not return None)
            assert result is not None, "Scheduler should succeed with valid historical data"

            # Verify generated files match golden master exactly
            output_json = period_path / "output.json"
            result_json = period_path / "results.json"

            assert output_json.exists(), "output.json should be created during CSV conversion"
            assert result_json.exists(), "results.json should be created for successful scheduling"

            # Load expected and actual files for comparison
            with open(golden_master_dir / "output.json", 'r') as f:
                expected_output = json.load(f)
            with open(output_json, 'r') as f:
                actual_output = json.load(f)

            with open(golden_master_dir / "results.json", 'r') as f:
                expected_results = json.load(f)
            with open(result_json, 'r') as f:
                actual_results = json.load(f)

            # File-based integration test: generated files should match golden master exactly
            assert actual_output == expected_output, "Generated output.json should match golden master"
            assert actual_results == expected_results, "Generated results.json should match golden master"

            print("Golden master integration test passed: CSV -> JSON -> Scheduler pipeline produces identical results")


@pytest.mark.integration
@pytest.mark.slow
class TestCancellationsWorkflow:
    """Test cancellations.json integration with the scheduler.

    Cancelled events should be:
    - Preserved in output.json (to maintain peep availability data)
    - Filtered out from results.json (not scheduled)
    """

    def test_scheduler_raises_error_for_unknown_cancelled_event(self):
        """Test that scheduler raises error when cancellations.json specifies non-existent event.

        Configuration error: user mistakenly specified an event that doesn't exist in responses.

        Scenario:
        - Create 2 events: "Saturday March 1 - 5pm" and "Sunday March 2 - 5pm"
        - Create cancellations.json cancelling non-existent: "Friday March 7 - 5pm"
        - Run scheduler
        - Assert: Raises ValueError about cancelled event not found
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: minimal (4 leaders + 4 followers for two 60-min events)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,2025-01-01
5,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,2025-01-01
2,Bob Leader,Bob,bob@test.com,Leader,2,3,0,TRUE,2025-01-01
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,3,0,TRUE,2025-01-01
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,2,0,TRUE,2025-01-01
7,Grace Follower,Grace,grace@test.com,Follower,5,2,0,TRUE,2025-01-01
4,David Leader,David,david@test.com,Leader,6,1,0,TRUE,2025-01-01
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,1,0,TRUE,2025-01-01"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: 2 valid events
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # Create cancellations.json with a NON-EXISTENT event
            cancelled_events_content = {
                "cancelled_events": [
                    "Friday March 7 - 5pm to 6pm"  # Doesn't exist in responses
                ],
                "cancelled_availability": [],
                "notes": "User mistakenly cancelled non-existent event"
            }
            cancelled_path = period_path / "cancellations.json"
            with open(cancelled_path, 'w') as f:
                json.dump(cancelled_events_content, f)

            # Run scheduler should raise error
            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)

            with pytest.raises(ValueError, match="cancelled event.*not found|unknown.*cancelled"):
                scheduler.run(load_from_csv=True)

    def test_scheduler_skips_cancelled_events(self):
        """Test that cancelled events are filtered from results but preserved in output.json.

        Scenario:
        - Create 2 events (60-min each, require 2 leaders + 2 followers)
        - Cancel 1 event via cancellations.json
        - Run scheduler
        - Assert: output.json contains both events (preserved)
        - Assert: results.json contains only 1 event (cancelled filtered)
        - Assert: No peeps scheduled for cancelled event in results.json
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Create minimal test data: 2 events, enough peeps for both
            # Event 1: Saturday March 1 - 5pm (60 min)
            # Event 2: Sunday March 2 - 5pm (60 min)

            # Members.csv: 4 leaders + 4 followers (enough for both events), sorted by priority descending
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,10,0,TRUE,2025-01-01
5,Eve Follower,Eve,eve@test.com,Follower,1,9,0,TRUE,2025-01-01
2,Bob Leader,Bob,bob@test.com,Leader,2,8,0,TRUE,2025-01-01
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,7,0,TRUE,2025-01-01
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,6,0,TRUE,2025-01-01
7,Grace Follower,Grace,grace@test.com,Follower,5,5,0,TRUE,2025-01-01
4,David Leader,David,david@test.com,Leader,6,4,0,TRUE,2025-01-01
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,3,0,TRUE,2025-01-01"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: Event rows format with all required columns
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # Create cancellations.json with one event cancelled
            cancelled_events_content = {
                "cancelled_events": [
                    "Sunday March 2 - 5pm"
                ],
                "cancelled_availability": [],
                "notes": "Instructor unavailable - notified members on 2025-02-15"
            }
            cancelled_path = period_path / "cancellations.json"
            with open(cancelled_path, 'w') as f:
                json.dump(cancelled_events_content, f)

            # Run scheduler
            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)
            result = scheduler.run(load_from_csv=True)

            # Verify scheduler succeeded
            assert result is not None, "Scheduler should succeed with valid data and valid cancelled events"

            # Verify output.json exists and contains BOTH events
            output_json = period_path / "output.json"
            assert output_json.exists(), "output.json should be created"

            with open(output_json, 'r') as f:
                output_data = json.load(f)

            output_events = output_data.get("events", [])
            assert len(output_events) == 2, f"output.json should preserve both events, got {len(output_events)}"

            # Events are stored by date format in output.json
            event_dates = [e.get("date") for e in output_events]
            assert len(event_dates) == 2, f"Should have 2 events, got {len(event_dates)}"

            # Verify results.json exists and contains ONLY 1 event (cancelled filtered)
            results_json = period_path / "results.json"
            assert results_json.exists(), "results.json should be created"

            with open(results_json, 'r') as f:
                results_data = json.load(f)

            results_events = results_data.get("valid_events", [])
            assert len(results_events) == 1, f"results.json should have 1 event (cancelled filtered), got {len(results_events)}"
            assert len(results_events[0]["attendees"]) == 6, "Non-cancelled event should have 6 attendees"

    def test_scheduler_works_without_cancellations_json(self):
        """Test scheduling when cancellations.json doesn't exist.

        Scenario:
        - Create 2 events
        - NO cancellations.json file
        - Run scheduler
        - Assert: Scheduler succeeds (backward compatible)
        - Assert: Both events are scheduled normally
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: 4 leaders + 4 followers, sorted by priority (highest to lowest)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,2025-01-01
5,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,2025-01-01
2,Bob Leader,Bob,bob@test.com,Leader,2,3,0,TRUE,2025-01-01
6,Fiona Follower,Fiona,fiona@test.com,Follower,3,3,0,TRUE,2025-01-01
3,Charlie Leader,Charlie,charlie@test.com,Leader,4,2,0,TRUE,2025-01-01
7,Grace Follower,Grace,grace@test.com,Follower,5,2,0,TRUE,2025-01-01
4,David Leader,David,david@test.com,Leader,6,1,0,TRUE,2025-01-01
8,Hannah Follower,Hannah,hannah@test.com,Follower,7,1,0,TRUE,2025-01-01"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: Event rows format with all required columns
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
,,Event: Sunday March 2 - 5pm,,,,,60,,,,,
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,charlie@test.com,Charlie,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,david@test.com,David,Leader,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,fiona@test.com,Fiona,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,grace@test.com,Grace,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,
2025-02-01 10:00:00,hannah@test.com,Hannah,Follower,I only want to be scheduled in my primary role,2,"Saturday March 1 - 5pm, Sunday March 2 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            # DO NOT create cancellations.json

            # Run scheduler
            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)
            result = scheduler.run(load_from_csv=True)

            # Verify scheduler succeeded
            assert result is not None, "Scheduler should succeed without cancellations.json"

            # Verify results.json exists and contains both events
            results_json = period_path / "results.json"
            assert results_json.exists(), "results.json should be created"

            with open(results_json, 'r') as f:
                results_data = json.load(f)

            results_events = results_data.get("valid_events", [])
            assert len(results_events) == 2, f"Without cancellations.json, both events should be scheduled. Got {len(results_events)}"

            # Just verify we have 2 events scheduled (no filtering without cancellations.json)
            assert len(results_events) == 2, "Both events should be scheduled without cancellations.json"

    def test_scheduler_skips_cancelled_availability(self):
        """Test that cancelled availability prevents scheduling for that event."""
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            # Members.csv: 5 leaders + 4 followers (enough to pass ABS_MIN_ROLE after cancellation)
            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alex Leader,Alex,alex@test.com,Leader,0,10,0,TRUE,2025-01-01
2,Bob Leader,Bob,bob@test.com,Leader,1,9,0,TRUE,2025-01-01
3,Casey Leader,Casey,casey@test.com,Leader,2,8,0,TRUE,2025-01-01
4,Drew Leader,Drew,drew@test.com,Leader,3,7,0,TRUE,2025-01-01
5,Eli Leader,Eli,eli@test.com,Leader,4,6,0,TRUE,2025-01-01
6,Dana Follower,Dana,dana@test.com,Follower,5,5,0,TRUE,2025-01-01
7,Eve Follower,Eve,eve@test.com,Follower,6,4,0,TRUE,2025-01-01
8,Fran Follower,Fran,fran@test.com,Follower,7,3,0,TRUE,2025-01-01
9,Gia Follower,Gia,gia@test.com,Follower,8,2,0,TRUE,2025-01-01"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            # Responses.csv: one event, all members available
            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
2025-02-01 10:00:00,alex@test.com,Alex,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,bob@test.com,Bob,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,casey@test.com,Casey,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,drew@test.com,Drew,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,eli@test.com,Eli,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,dana@test.com,Dana,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,fran@test.com,Fran,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,gia@test.com,Gia,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            cancellations_content = {
                "cancelled_events": [],
                "cancelled_availability": [
                    {
                        "email": "alex@test.com",
                        "events": ["Saturday March 1 - 5pm"]
                    }
                ],
                "notes": "Alex is no longer available"
            }
            cancelled_path = period_path / "cancellations.json"
            with open(cancelled_path, 'w') as f:
                json.dump(cancellations_content, f)

            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)
            result = scheduler.run(load_from_csv=True)

            assert result is not None, "Scheduler should succeed with cancelled availability"

            results_json = period_path / "results.json"
            with open(results_json, 'r') as f:
                results_data = json.load(f)

            attendees = results_data["valid_events"][0]["attendees"]
            alternates = results_data["valid_events"][0]["alternates"]
            assigned_ids = {a["id"] for a in attendees + alternates}
            assert 1 not in assigned_ids, "Cancelled leader should not be scheduled"

    def test_scheduler_raises_error_for_cancelled_availability_unknown_email(self):
        """Test that cancellations.json fails for unknown email in cancelled availability."""
        with tempfile.TemporaryDirectory() as temp_dir:
            period_path = Path(temp_dir) / "test_period"
            period_path.mkdir()

            members_csv_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Leader,Alice,alice@test.com,Leader,0,4,0,TRUE,2025-01-01
2,Eve Follower,Eve,eve@test.com,Follower,1,4,0,TRUE,2025-01-01"""

            members_path = period_path / "members.csv"
            members_path.write_text(members_csv_content)

            responses_csv_content = """Timestamp,Email Address,Name,Primary Role,Secondary Role,Max Sessions,Availability,Event Duration,Min Interval Days,Preferred gap between sessions?,Partnership Preference,Questions or Comments for Organizers,Questions or Comments for Leilani
,,Event: Saturday March 1 - 5pm,,,,,60,,,,,
2025-02-01 10:00:00,alice@test.com,Alice,Leader,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,
2025-02-01 10:00:00,eve@test.com,Eve,Follower,I only want to be scheduled in my primary role,1,"Saturday March 1 - 5pm",,0,,,"""

            responses_path = period_path / "responses.csv"
            responses_path.write_text(responses_csv_content)

            cancellations_content = {
                "cancelled_events": [],
                "cancelled_availability": [
                    {
                        "email": "unknown@test.com",
                        "events": ["Saturday March 1 - 5pm"]
                    }
                ]
            }
            cancelled_path = period_path / "cancellations.json"
            with open(cancelled_path, 'w') as f:
                json.dump(cancellations_content, f)

            scheduler = Scheduler(data_folder=str(period_path), max_events=10, interactive=False)
            with pytest.raises(ValueError, match="unknown email|cancelled availability"):
                scheduler.run(load_from_csv=True)
