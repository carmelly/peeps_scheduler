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
                        "role": "Leader", "index": 0, "priority": 1, "total_attended": 0,
                        "availability": [0], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    },
                    {
                        "id": 2, "full_name": "Bob", "display_name": "Bob", "email": "bob@test.com",
                        "role": "Follower", "index": 1, "priority": 1, "total_attended": 0,
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
                        "role": "Leader", "index": 0, "priority": 1, "total_attended": 0,
                        "availability": [1], "event_limit": 1, "min_interval_days": 0,
                        "switch_pref": 1, "responded": True
                    },
                    {
                        "id": 2, "full_name": "OnlyFollower", "display_name": "OnlyFollower", "email": "follower@test.com",
                        "role": "Follower", "index": 1, "priority": 1, "total_attended": 0,
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
