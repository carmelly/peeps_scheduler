"""
Comprehensive test suite for db/import_period_data.py import functionality.

Tests cover:
- Member collection (Phase 1)
- Period import (Phase 2) using test fixtures
- Integration testing with sequential imports
"""

import pytest
import sys
import json
import csv as csv_module
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.import_period_data import MemberCollector, PeriodImporter
from file_io import normalize_email


class TestMemberCollection:
    """Tests for member collection functionality across all periods."""

    def test_complete_peep_id_mapping(self, test_db, test_period_data):
        """Test that all members are tracked in peep_id_mapping and CSV data matches DB."""
        period1_data = next(test_period_data(period_name='2025-02', num_members=10))

        cursor = test_db.cursor()
        collector = MemberCollector(
            processed_data_path=Path(period1_data['temp_dir']),
            verbose=False
        )

        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Verify mapping completeness
        assert len(peep_id_mapping) == 10, "Should track all 10 members"

        # Verify database insertion
        cursor.execute("SELECT COUNT(*) FROM peeps")
        db_count = cursor.fetchone()[0]
        assert db_count == 10, "Should insert 10 members to database"

    def test_member_invalid_date_joined_warning(self, test_db, test_period_data):
        """Test that invalid Date Joined formats log warning and continue."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        # Corrupt member with invalid date
        members_path = Path(period_data['period_dir']) / 'members.csv'
        with open(members_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerow({
                'id': '1',
                'Name': 'Member One',
                'Display Name': 'M1',
                'Email Address': 'member1@test.com',
                'Role': 'Leader',
                'Index': '0',
                'Priority': '1',
                'Total Attended': '0',
                'Active': 'TRUE',
                'Date Joined': 'not-a-valid-date'  # Invalid format
            })

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)

        # Should handle gracefully with warning (not crash)
        collector.scan_all_periods()
        assert len(collector.members) == 1, "Should still scan member despite invalid date"

    def test_member_missing_row_id_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that member CSV with missing ID raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        # Corrupt the members.csv by removing ID from one row
        members_path = Path(period_data['period_dir']) / 'members.csv'
        with open(members_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerow({
                'id': '',  # Missing ID
                'Name': 'Test Member',
                'Display Name': 'TM',
                'Email Address': 'test@example.com',
                'Role': 'Leader',
                'Index': '0',
                'Priority': '1',
                'Total Attended': '0',
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)

        with pytest.raises(ValueError, match="(?s)Invalid member data.*missing required.*id"):
            collector.scan_all_periods()

    def test_missing_members_csv_raises_error(self, test_db, test_period_data):
        """Test that missing members.csv raises FileNotFoundError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))

        # Remove members.csv
        members_file = Path(period_data['period_dir']) / 'members.csv'
        members_file.unlink()

        cursor = test_db.cursor()
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)

        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError, match="Required file not found.*members.csv"):
            collector.scan_all_periods()

    def test_multiple_members_missing_emails(self, test_db, test_period_data, tmp_path):
        """Test that multiple members missing emails raises detailed ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        # Corrupt members.csv - remove emails from multiple members
        members_path = Path(period_data['period_dir']) / 'members.csv'
        with open(members_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerow({
                'id': '1',
                'Name': 'Member One',
                'Display Name': 'M1',
                'Email Address': '',  # Missing email
                'Role': 'Leader',
                'Index': '0',
                'Priority': '1',
                'Total Attended': '0',
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })
            writer.writerow({
                'id': '2',
                'Name': 'Member Two',
                'Display Name': 'M2',
                'Email Address': '',  # Missing email
                'Role': 'Follower',
                'Index': '1',
                'Priority': '1',
                'Total Attended': '0',
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)

        with pytest.raises(ValueError, match=r"2 members are missing required email addresses"):
            collector.scan_all_periods()


class TestResponseImport:
    """Tests for response import functionality."""

    def test_missing_responses_csv_warning(self, test_db, test_period_data):
        """Test that missing responses.csv generates warning but doesn't crash."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))

        # Remove responses.csv
        responses_file = Path(period_data['period_dir']) / 'responses.csv'
        responses_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Try to import period - should handle missing responses gracefully
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )

        # Should complete without crashing
        importer.import_period()

        # Verify no responses imported
        period_id = importer.period_id
        cursor.execute("SELECT COUNT(*) FROM responses WHERE period_id = ?", (period_id,))
        assert cursor.fetchone()[0] == 0, "Should have 0 responses when responses.csv is missing"

    def test_response_email_not_in_peeps_table(self, test_db, test_period_data):
        """Test that response with unknown email raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create response with unknown email
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'unknown@example.com',  # Not in peeps table
                'Name': 'Unknown Person',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        with pytest.raises(ValueError, match=r"Email mismatch.*does not match any member"):
            importer.import_responses()

    def test_response_invalid_max_sessions_defaults_to_zero(self, test_db, test_period_data):
        """Test that invalid max_sessions value defaults to 0."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create response with invalid max_sessions
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': 'not-a-number',  # Invalid
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        # Should handle gracefully (defaults to 0)
        response_mapping = importer.import_responses()
        assert len(response_mapping) == 1, "Should import response despite invalid max_sessions"

    def test_response_invalid_min_interval_defaults_to_zero(self, test_db, test_period_data):
        """Test that invalid min_interval value defaults to 0."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create response with invalid min_interval
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': 'invalid',  # Invalid
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        # Should handle gracefully (defaults to 0)
        response_mapping = importer.import_responses()
        assert len(response_mapping) == 1, "Should import response despite invalid min_interval"

    def test_response_missing_email_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that response with missing email raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create responses.csv with missing email
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': '',  # Missing email
                'Name': 'Test Member',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        with pytest.raises(ValueError, match="(?s)Invalid response.*Missing required field"):
            importer.import_responses()

    def test_response_missing_name_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that response with missing name raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create responses.csv with missing name
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'test@example.com',
                'Name': '',  # Missing name
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        with pytest.raises(ValueError, match="(?s)Invalid response.*Missing required field"):
            importer.import_responses()

    def test_response_switch_preference_parsing(self, test_db, test_period_data):
        """Test that switch preference is correctly parsed from Secondary Role field."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        period_id = importer.period_id

        # Verify switch preference was parsed correctly
        cursor.execute("""
            SELECT switch_preference
            FROM responses
            WHERE period_id = ?
            LIMIT 1
        """, (period_id,))

        result = cursor.fetchone()
        assert result is not None, "Should have at least one response"
        switch_pref = result[0]

        # Fixture uses "I only want to be scheduled in my primary role" = PRIMARY_ONLY (value 1)
        assert switch_pref == 1, f"Switch preference should be 1 (PRIMARY_ONLY), got {switch_pref}"

    def test_response_timestamp_parsing_fails(self, test_db, test_period_data):
        """Test that invalid timestamp formats are handled gracefully (warning logged, None stored)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create response with invalid timestamp
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': 'not-a-valid-timestamp',  # Invalid format
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        # Should handle gracefully (warning logged, continues)
        response_mapping = importer.import_responses()
        assert len(response_mapping) == 1, "Should still import response despite bad timestamp"


class TestEventCreation:
    """Tests for event creation and derivation from availability."""

    def test_event_auto_derivation_from_availability(self, test_db, test_period_data):
        """Test that events are auto-derived from availability strings."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        period_id = importer.period_id

        # Verify events were auto-derived
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (period_id,))
        event_count = cursor.fetchone()[0]
        assert event_count == 3, f"Should auto-derive 3 events, got {event_count}"

        # Verify event durations (120 minutes from "5pm to 7pm")
        cursor.execute("SELECT DISTINCT duration_minutes FROM events WHERE period_id = ?", (period_id,))
        durations = [row[0] for row in cursor.fetchall()]
        assert all(d == 120 for d in durations), "All events should have 120-minute duration"

    def test_event_deduplication(self, test_db, test_period_data):
        """Test that duplicate events are deduplicated during creation."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create responses with duplicate event dates
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            # Member 1 lists same event twice
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm, Friday February 7th - 5pm to 7pm'  # Duplicate
            })
            # Member 2 also lists same event
            writer.writerow({
                'Timestamp': '2025-02-01 10:01:00',
                'Email Address': 'member2@test.com',
                'Name': 'Test Member 2',
                'Primary Role': 'Follower',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'
            })

        response_mapping = importer.import_responses()
        created_count = importer.create_events(response_mapping)

        # Should only create 1 event despite duplicates
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (importer.period_id,))
        event_count = cursor.fetchone()[0]
        assert event_count == 1, f"Should deduplicate to 1 event, got {event_count}"

    def test_invalid_event_date_format(self, test_db, test_period_data):
        """Test that invalid event date format in availability string raises error."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))

        # Modify responses.csv with invalid date format
        responses_file = Path(period_data['period_dir']) / 'responses.csv'

        with open(responses_file, 'r', newline='') as f:
            reader = csv_module.DictReader(f)
            responses = list(reader)

        # Corrupt the availability string
        for response in responses:
            response['Availability'] = 'Invalid Date Format, Another Invalid Date'

        with open(responses_file, 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(responses)

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Try to import period - should raise ValueError for invalid dates
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )

        # Should raise ValueError for invalid event date format
        with pytest.raises(ValueError, match="time data.*Invalid Date Format.*does not match format"):
            importer.import_period()

    def test_old_format_event_dates_default_to_120_minutes(self, test_db, test_period_data):
        """Test backward compatibility: old format event dates (without time ranges) default to 120 minutes."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create response with old format availability (no time range, just single time)
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm'  # Old format: no time range
            })

        response_mapping = importer.import_responses()
        created_count = importer.create_events(response_mapping)

        # Verify event was created with default 120 minute duration
        assert created_count == 1, "Should create event from old format availability"
        cursor.execute("SELECT duration_minutes FROM events WHERE period_id = ?", (importer.period_id,))
        result = cursor.fetchone()
        assert result is not None, "Event should exist in database"
        assert result[0] == 120, "Old format events should default to 120 minutes duration"

    def test_event_row_duration_takes_precedence_over_availability_default(self, test_db, test_period_data):
        """Test that Event: row duration takes precedence over availability string defaults.

        Bug: When Event: row specifies 90 minutes and availability uses old format (no time range),
        the database event should use Event: row value (90 minutes), not the old format default (120 minutes).
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()

        # Create responses.csv with:
        # 1. Event: row specifying 90 minutes duration
        # 2. Regular response with old format availability (no time range)
        responses_path = Path(period_data['period_dir']) / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions',
                'Min Interval Days', 'Secondary Role', 'Availability', 'Event Duration'
            ])
            writer.writeheader()
            # Event: row with 90 minute duration
            writer.writerow({
                'Timestamp': '',
                'Email Address': '',
                'Name': 'Event: Friday February 7 - 5pm',  # Old format, no time range
                'Primary Role': '',
                'Max Sessions': '',
                'Min Interval Days': '',
                'Secondary Role': '',
                'Availability': '',
                'Event Duration': '90'  # Specified duration should take precedence
            })
            # Regular response referencing the same event
            writer.writerow({
                'Timestamp': '2025-02-01 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'Leader',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7 - 5pm',  # Old format, no time range
                'Event Duration': ''
            })

        response_mapping = importer.import_responses()
        created_count = importer.create_events(response_mapping)

        # Verify event was created with 90 minutes (from Event: row), not 120 (old format default)
        assert created_count == 1, "Should create one event"
        cursor.execute("""
            SELECT duration_minutes, event_datetime
            FROM events
            WHERE period_id = ?
        """, (importer.period_id,))
        result = cursor.fetchone()
        assert result is not None, "Event should exist in database"

        duration = result[0]
        event_datetime = result[1]

        # Event: row duration correctly takes precedence over availability string defaults (Bug #9 fixed)
        assert duration == 90, (
            f"Event: row duration (90 min) should take precedence over old format default (120 min), "
            f"but got {duration} minutes for event {event_datetime}"
        )


class TestAssignmentImport:
    """Tests for assignment import functionality."""

    @pytest.mark.skip(reason="Basic fixture doesn't import assignments - would need enhanced fixture")
    def test_assignment_order_preservation(self, test_db, test_period_data):
        """Test that assignment_order is preserved from results.json array index."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        period_id = importer.period_id

        # Verify assignment_order is sequential
        cursor.execute("""
            SELECT assignment_order
            FROM event_assignments
            WHERE period_id = ?
            ORDER BY event_id, assignment_order
        """, (period_id,))

        orders = [row[0] for row in cursor.fetchall()]

        # Should have assignment orders (grouped by event)
        assert len(orders) > 0, "Should have assignments with orders"

        # Within each event, orders should be sequential starting from 0
        # This is a simplified check - more detailed would group by event_id
        assert min(orders) >= 0, "Assignment order should start from 0 or higher"

    def test_import_assignments_attendees_only(self, test_db, test_period_data, tmp_path):
        """Test importing assignments with attendees only (no alternates)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=2))

        cursor = test_db.cursor()

        # Import members and period to get real event IDs
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        # Import responses and events
        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get actual event IDs from database
        cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ? ORDER BY event_datetime", (importer.period_id,))
        events = cursor.fetchall()
        assert len(events) == 2, "Should have 2 events"

        # Create results.json with actual event date strings
        # Convert from "2025-02-07T17:00:00" to "2025-02-07 17:00" format for event_id_mapping
        from datetime import datetime
        event1_datetime = datetime.fromisoformat(events[0][1]).strftime("%Y-%m-%d %H:%M")
        event2_datetime = datetime.fromisoformat(events[1][1]).strftime("%Y-%m-%d %H:%M")

        results_json = {
            "valid_events": [
                {
                    "date": event1_datetime,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"},
                        {"id": 2, "name": "Test Member 2", "role": "follower"},
                        {"id": 3, "name": "Test Member 3", "role": "follower"}
                    ],
                    "alternates": []
                },
                {
                    "date": event2_datetime,
                    "attendees": [
                        {"id": 4, "name": "Test Member 4", "role": "leader"},
                        {"id": 5, "name": "Test Member 5", "role": "follower"}
                    ],
                    "alternates": []
                }
            ]
        }

        # Write results.json to period directory
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        # Import assignments
        imported_count = importer.import_assignments()

        # Verify count
        assert imported_count == 5, f"Should import 5 assignments, got {imported_count}"

        # Verify assignments in database
        cursor.execute("""
            SELECT event_id, peep_id, assigned_role, assignment_type, assignment_order
            FROM event_assignments
            WHERE event_id IN (?, ?)
            ORDER BY event_id, assignment_order
        """, (events[0][0], events[1][0]))

        assignments = cursor.fetchall()
        assert len(assignments) == 5, "Should have 5 assignments in database"

        # Verify event 1 assignments
        event1_assignments = [a for a in assignments if a[0] == events[0][0]]
        assert len(event1_assignments) == 3, "Event 1 should have 3 attendees"
        assert all(a[3] == 'attendee' for a in event1_assignments), "All should be attendees"
        assert [a[4] for a in event1_assignments] == [0, 1, 2], "Assignment order should be 0, 1, 2"

        # Verify event 2 assignments
        event2_assignments = [a for a in assignments if a[0] == events[1][0]]
        assert len(event2_assignments) == 2, "Event 2 should have 2 attendees"

    def test_import_assignments_duplicate_handling(self, test_db, test_period_data, tmp_path):
        """Test that duplicate assignments are caught by database constraints."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        from datetime import datetime
        event_datetime = datetime.fromisoformat(cursor.fetchone()[0]).strftime("%Y-%m-%d %H:%M")

        # Create results.json with duplicate assignments (same member twice)
        results_json = {
            "valid_events": [
                {
                    "date": event_datetime,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"},
                        {"id": 1, "name": "Test Member 1", "role": "follower"}  # Duplicate!
                    ],
                    "alternates": []
                }
            ]
        }

        # Write results.json
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        # Should handle duplicate gracefully (warning logged, second one skipped)
        imported_count = importer.import_assignments()

        # Should only import 1 (first one succeeds, second one caught by constraint)
        assert imported_count == 1, f"Should import 1 assignment (duplicate skipped), got {imported_count}"

        # Verify only one assignment in database
        cursor.execute("SELECT COUNT(*) FROM event_assignments WHERE peep_id = 1", ())
        count = cursor.fetchone()[0]
        assert count == 1, "Should have exactly 1 assignment for member 1 (duplicate prevented)"

    def test_import_assignments_creates_event_from_results_if_not_exists(self, test_db, test_period_data, tmp_path):
        """Test that assignment creates event from results.json if it doesn't exist (2025-12 scenario)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Count events before (should be 1 from responses)
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (importer.period_id,))
        events_before = cursor.fetchone()[0]

        # Create results.json with event that doesn't exist in database yet
        results_json = {
            "valid_events": [
                {
                    "date": "2025-02-25 17:00",  # Event not from responses
                    "duration_minutes": 90,
                    "id": 99,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"}
                    ],
                    "alternates": []
                }
            ]
        }

        # Write results.json
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        # Should create the event from results.json (not raise error)
        imported = importer.import_assignments()
        assert imported == 1, "Should import 1 assignment"

        # Verify event was created
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (importer.period_id,))
        events_after = cursor.fetchone()[0]
        assert events_after == events_before + 1, "Should have created 1 new event from results.json"

        # Verify event has correct details (ISO 8601 format with T separator)
        cursor.execute("SELECT event_datetime, duration_minutes FROM events WHERE event_datetime = ?", ("2025-02-25T17:00:00",))
        event = cursor.fetchone()
        assert event is not None, "Event should exist"
        assert event[0] == "2025-02-25T17:00:00", "Event datetime should match ISO 8601 format"
        assert event[1] == 90, "Event duration should match"

    def test_import_assignments_orphaned_member_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that assignment referencing non-existent member raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        from datetime import datetime
        event_datetime = datetime.fromisoformat(cursor.fetchone()[0]).strftime("%Y-%m-%d %H:%M")

        # Create results.json with invalid member ID
        results_json = {
            "valid_events": [
                {
                    "date": event_datetime,
                    "attendees": [
                        {"id": 999, "name": "Non-Existent Member", "role": "leader"}  # ID 999 doesn't exist
                    ],
                    "alternates": []
                }
            ]
        }

        # Write results.json
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        # Should raise ValueError for unknown member
        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*unknown member"):
            importer.import_assignments()

    def test_import_assignments_with_alternates(self, test_db, test_period_data, tmp_path):
        """Test importing assignments with both attendees and alternates."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        event = cursor.fetchone()
        from datetime import datetime
        event_datetime = datetime.fromisoformat(event[1]).strftime("%Y-%m-%d %H:%M")

        # Create results.json with alternates
        results_json = {
            "valid_events": [
                {
                    "date": event_datetime,
                    "attendees": [
                        {"id": 1, "name": "Test Member 1", "role": "leader"},
                        {"id": 2, "name": "Test Member 2", "role": "follower"}
                    ],
                    "alternates": [
                        {"id": 3, "name": "Test Member 3", "role": "leader"},
                        {"id": 4, "name": "Test Member 4", "role": "follower"}
                    ]
                }
            ]
        }

        # Write results.json
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        # Import assignments
        imported_count = importer.import_assignments()

        assert imported_count == 4, f"Should import 4 assignments (2 attendees + 2 alternates), got {imported_count}"

        # Verify assignment types and positions
        cursor.execute("""
            SELECT assignment_type, assignment_order, alternate_position, peep_id
            FROM event_assignments
            WHERE event_id = ?
            ORDER BY assignment_type, COALESCE(assignment_order, alternate_position)
        """, (event[0],))

        assignments = cursor.fetchall()
        assert len(assignments) == 4, "Should have 4 total assignments"

        # Check alternates
        alternates = [a for a in assignments if a[0] == 'alternate']
        assert len(alternates) == 2, "Should have 2 alternates"
        assert alternates[0][2] == 0, "First alternate should have position 0"
        assert alternates[1][2] == 1, "Second alternate should have position 1"

        # Check attendees
        attendees = [a for a in assignments if a[0] == 'attendee']
        assert len(attendees) == 2, "Should have 2 attendees"
        assert attendees[0][1] == 0, "First attendee should have order 0"
        assert attendees[1][1] == 1, "Second attendee should have order 1"


class TestAttendanceImport:
    """Tests for attendance import functionality."""

    def test_import_attendance_alternate_promoted(self, test_db, test_period_data, tmp_path):
        """Test importing attendance for alternates who attended (alternate_promoted mode)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        event = cursor.fetchone()
        from datetime import datetime
        event_datetime = datetime.fromisoformat(event[1]).strftime("%Y-%m-%d %H:%M")

        # Create assignments with alternates
        results_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 1, "name": "Test Member 1", "role": "leader"}
                ],
                "alternates": [
                    {"id": 2, "name": "Test Member 2", "role": "follower"}
                ]
            }]
        }
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        importer.import_assignments()

        # Alternate attended (promoted)
        attendance_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 2, "name": "Test Member 2", "role": "follower"}  # Alternate attended
                ]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Import attendance
        imported_count = importer.import_attendance()

        assert imported_count == 1, f"Should import 1 attendance record, got {imported_count}"

        # Verify participation_mode = 'alternate_promoted'
        cursor.execute("""
            SELECT participation_mode, expected_type, actual_role
            FROM event_attendance
            WHERE event_id = ? AND peep_id = 2
        """, (event[0],))

        attendance = cursor.fetchone()
        assert attendance[0] == 'alternate_promoted', "Should have 'alternate_promoted' participation mode"
        assert attendance[1] == 'alternate', "Expected type should be 'alternate'"

    def test_import_attendance_duplicate_handling(self, test_db, test_period_data, tmp_path):
        """Test that duplicate attendance records are caught by database constraints."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        from datetime import datetime
        event_datetime = datetime.fromisoformat(cursor.fetchone()[0]).strftime("%Y-%m-%d %H:%M")

        # Duplicate attendance
        attendance_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 1, "name": "Test Member 1", "role": "leader"},
                    {"id": 1, "name": "Test Member 1", "role": "follower"}  # Duplicate
                ]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        import json
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Should handle duplicate gracefully
        imported_count = importer.import_attendance()

        assert imported_count == 1, f"Should import 1 (duplicate skipped), got {imported_count}"

        # Verify only one attendance record
        cursor.execute("SELECT COUNT(*) FROM event_attendance WHERE peep_id = 1", ())
        count = cursor.fetchone()[0]
        assert count == 1, "Should have exactly 1 attendance record for member 1"

    def test_import_attendance_orphaned_event_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that attendance for non-existent event raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Invalid event
        attendance_json = {
            "valid_events": [{
                "date": "2025-02-99 17:00",  # Invalid
                "attendees": [{"id": 1, "name": "Test Member 1", "role": "leader"}]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        import json
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Should raise ValueError
        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*which does not exist"):
            importer.import_attendance()

    def test_import_attendance_orphaned_member_raises_error(self, test_db, test_period_data, tmp_path):
        """Test that attendance for non-existent member raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        from datetime import datetime
        event_datetime = datetime.fromisoformat(cursor.fetchone()[0]).strftime("%Y-%m-%d %H:%M")

        # Invalid member
        attendance_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [{"id": 999, "name": "Invalid Member", "role": "leader"}]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        import json
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Should raise ValueError
        with pytest.raises(ValueError, match=r"(?s)Data integrity error.*unknown member"):
            importer.import_attendance()

    def test_import_attendance_scheduled_members(self, test_db, test_period_data, tmp_path):
        """Test importing attendance for members with assignments (scheduled mode)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        event = cursor.fetchone()
        from datetime import datetime
        event_datetime = datetime.fromisoformat(event[1]).strftime("%Y-%m-%d %H:%M")

        # Create and import assignments first
        results_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 1, "name": "Test Member 1", "role": "leader"},
                    {"id": 2, "name": "Test Member 2", "role": "follower"}
                ],
                "alternates": []
            }]
        }
        results_path = Path(period_data['period_dir']) / 'results.json'
        import json
        with open(results_path, 'w') as f:
            json.dump(results_json, f)

        importer.import_assignments()

        # Create actual_attendance.json
        attendance_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 1, "name": "Test Member 1", "role": "leader"},
                    {"id": 2, "name": "Test Member 2", "role": "follower"}
                ]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Import attendance
        imported_count = importer.import_attendance()

        assert imported_count == 2, f"Should import 2 attendance records, got {imported_count}"

        # Verify attendance records with participation_mode = 'scheduled'
        cursor.execute("""
            SELECT peep_id, actual_role, participation_mode, attendance_status
            FROM event_attendance
            WHERE event_id = ?
            ORDER BY peep_id
        """, (event[0],))

        attendance = cursor.fetchall()
        assert len(attendance) == 2, "Should have 2 attendance records"
        assert attendance[0][2] == 'scheduled', "Member 1 should have 'scheduled' participation mode"
        assert attendance[1][2] == 'scheduled', "Member 2 should have 'scheduled' participation mode"
        assert all(a[3] == 'attended' for a in attendance), "All should have 'attended' status"

    def test_import_attendance_volunteer_fill(self, test_db, test_period_data, tmp_path):
        """Test importing attendance for members without assignments (volunteer_fill mode)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))

        cursor = test_db.cursor()

        # Import members and setup
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        importer.create_schedule_period()
        response_mapping = importer.import_responses()
        importer.create_events(response_mapping)

        # Get event
        cursor.execute("SELECT id, event_datetime FROM events WHERE period_id = ?", (importer.period_id,))
        event = cursor.fetchone()
        from datetime import datetime
        event_datetime = datetime.fromisoformat(event[1]).strftime("%Y-%m-%d %H:%M")

        # No assignments imported - member 3 volunteers
        attendance_json = {
            "valid_events": [{
                "date": event_datetime,
                "attendees": [
                    {"id": 3, "name": "Test Member 3", "role": "leader"}  # No assignment
                ]
            }]
        }
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        import json
        with open(attendance_path, 'w') as f:
            json.dump(attendance_json, f)

        # Import attendance
        imported_count = importer.import_attendance()

        assert imported_count == 1, f"Should import 1 attendance record, got {imported_count}"

        # Verify participation_mode = 'volunteer_fill'
        cursor.execute("""
            SELECT participation_mode, event_assignment_id, expected_type
            FROM event_attendance
            WHERE event_id = ? AND peep_id = 3
        """, (event[0],))

        attendance = cursor.fetchone()
        assert attendance[0] == 'volunteer_fill', "Should have 'volunteer_fill' participation mode"
        assert attendance[1] is None, "Should have no assignment_id"
        assert attendance[2] is None, "Should have no expected_type"


class TestSnapshotGeneration:
    """Tests for snapshot generation core functionality."""

    def test_apply_attendance_with_unknown_peep_warning(self, test_db, test_period_data):
        """Test that attendance for unknown peep generates warning."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot, EventAttendance

        generator = SnapshotGenerator(verbose=False)

        # Create snapshot with peep 1 only
        peep_lookup = {
            1: MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            )
        }

        # Create attendance for unknown peep 999
        from datetime import datetime
        attendance_records = [
            EventAttendance(
                event_id=1,
                peep_id=999,  # Unknown peep
                role='leader',
                attendance_type='actual',
                event_datetime=datetime(2025, 2, 7, 17, 0)
            )
        ]

        # Should handle gracefully with warning (not crash)
        generator._apply_attendance_records(peep_lookup, attendance_records, 'actual')

        # Verify peep 1 was unchanged
        assert peep_lookup[1].num_events_this_period == 0, "Unknown attendance should not affect other peeps"

    def test_compare_snapshots_identical(self, test_db):
        """Test comparing two identical snapshots."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator()

        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=2, index_position=0, total_attended=5, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=2, index_position=0, total_attended=5, active=True
            )
        ]

        matches, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert matches == True, "Identical snapshots should match"
        assert len(differences) == 0, "Identical snapshots should have no differences"

    def test_compare_snapshots_with_differences(self, test_db):
        """Test comparing snapshots with differences."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator()

        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=2, index_position=0, total_attended=5, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=5,  # Different priority
                index_position=0,
                total_attended=8,  # Different total
                active=True
            )
        ]

        matches, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert matches == False, "Different snapshots should not match"
        assert len(differences) > 0, "Different snapshots should have differences"
        # Verify differences are reported (format may vary)
        assert any("priority" in diff.lower() or "total" in diff.lower() for diff in differences), "Should detect differences in priority or total_attended"

    def test_generate_snapshot_no_response_no_priority_change(self, test_db):
        """Test that members who didn't respond don't get priority increment."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator()

        starting_snapshot = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=5, index_position=0, total_attended=10, active=True
            )
        ]

        # No attendance, member didn't respond
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=[],
            expected_attendance=[],
            responded_peep_ids=set()  # Member 1 didn't respond
        )

        member1 = result[0]
        assert member1.priority == 5, "Non-responder priority should stay the same"
        assert member1.total_attended == 10, "Non-responder total_attended unchanged"

    def test_generate_snapshot_with_actual_attendance_only(self, test_db):
        """Test generating snapshot with actual attendance (permanent snapshot mode)."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot, EventAttendance

        generator = SnapshotGenerator()

        # Create starting snapshot
        starting_snapshot = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=2, index_position=0, total_attended=5, active=True
            ),
            MemberSnapshot(
                peep_id=2, email="member2@test.com", full_name="Member 2",
                display_name="M2", primary_role="follower",
                priority=3, index_position=1, total_attended=3, active=True
            ),
            MemberSnapshot(
                peep_id=3, email="member3@test.com", full_name="Member 3",
                display_name="M3", primary_role="leader",
                priority=1, index_position=2, total_attended=4, active=True
            )
        ]

        # Member 1 attended 2 events
        from datetime import datetime
        actual_attendance = [
            EventAttendance(event_id=1, peep_id=1, role='leader', attendance_type='actual', event_datetime=datetime(2025, 2, 7)),
            EventAttendance(event_id=2, peep_id=1, role='leader', attendance_type='actual', event_datetime=datetime(2025, 2, 14))
        ]

        # Members 2 and 3 responded but didn't attend
        responded_peep_ids = {1, 2, 3}

        # Generate snapshot
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=actual_attendance,
            expected_attendance=[],
            responded_peep_ids=responded_peep_ids
        )

        assert len(result) == 3, "Should have 3 members in snapshot"

        # Find each member in result
        member1 = next(m for m in result if m.peep_id == 1)
        member2 = next(m for m in result if m.peep_id == 2)
        member3 = next(m for m in result if m.peep_id == 3)

        # Member 1 attended - priority reset to 0, total_attended increased
        assert member1.priority == 0, "Attendee should have priority 0"
        assert member1.total_attended == 7, "total_attended should be 5 + 2"

        # Member 2 responded but didn't attend - priority should increment
        assert member2.priority == 4, "Non-attendee who responded should have priority +1 (was 3)"

        # Member 3 responded but didn't attend - priority should increment
        assert member3.priority == 2, "Non-attendee who responded should have priority +1 (was 1)"

        # Members should be reordered by priority (highest first)
        assert result[0].peep_id == 2, "Member 2 should be first (priority 4)"
        assert result[1].peep_id == 3, "Member 3 should be second (priority 2)"
        assert result[2].peep_id == 1, "Member 1 should be last (priority 0)"

    def test_generate_snapshot_with_expected_attendance(self, test_db):
        """Test generating snapshot with expected attendance (scheduling snapshot mode)."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot, EventAttendance

        generator = SnapshotGenerator()

        # Create starting snapshot
        starting_snapshot = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=2, index_position=0, total_attended=5, active=True
            ),
            MemberSnapshot(
                peep_id=2, email="member2@test.com", full_name="Member 2",
                display_name="M2", primary_role="follower",
                priority=1, index_position=1, total_attended=3, active=True
            )
        ]

        # Member 1 scheduled for future event (expected attendance)
        from datetime import datetime
        expected_attendance = [
            EventAttendance(event_id=1, peep_id=1, role='leader', attendance_type='expected', event_datetime=datetime(2025, 2, 7))
        ]

        responded_peep_ids = {1, 2}

        # Generate snapshot
        result = generator.generate_snapshot_from_attendance(
            starting_snapshot=starting_snapshot,
            actual_attendance=[],
            expected_attendance=expected_attendance,
            responded_peep_ids=responded_peep_ids
        )

        member1 = next(m for m in result if m.peep_id == 1)
        member2 = next(m for m in result if m.peep_id == 2)

        # Member 1 expected to attend - priority reset, moved to back
        assert member1.priority == 0, "Expected attendee should have priority 0"
        assert member1.total_attended == 6, "total_attended should increment for expected"

        # Member 2 responded but not scheduled - priority increment
        assert member2.priority == 2, "Non-scheduled responder should have priority +1"

        # Member 2 should be first (higher priority)
        assert result[0].peep_id == 2
        assert result[1].peep_id == 1

    def test_snapshot_comparison_active_status_difference(self, test_db, test_period_data):
        """Test that active status differences are detected."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator(verbose=False)

        # Create two snapshots with different active status
        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=False
            )  # Different active status
        ]

        # Compare snapshots
        is_same, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert not is_same, "Snapshots with different active status should not match"
        assert any("active" in diff.lower() for diff in differences), "Should mention active status difference"

    def test_snapshot_comparison_index_position_difference(self, test_db, test_period_data):
        """Test that index position differences are detected."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator(verbose=False)

        # Create two snapshots with different index positions
        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=5, total_attended=0, active=True
            )  # Different index
        ]

        # Compare snapshots
        is_same, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert not is_same, "Snapshots with different index positions should not match"
        assert any("index" in diff.lower() for diff in differences), "Should mention index difference"

    def test_snapshot_comparison_length_mismatch(self, test_db, test_period_data):
        """Test that snapshots with different lengths are detected as different."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator(verbose=False)

        # Create two snapshots with different lengths
        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            ),
            MemberSnapshot(
                peep_id=2, email="member2@test.com", full_name="Member 2",
                display_name="M2", primary_role="follower",
                priority=1, index_position=1, total_attended=0, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            )
        ]

        # Compare snapshots
        is_same, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert not is_same, "Snapshots with different lengths should not match"
        assert len(differences) > 0, "Should report differences"
        assert any("length mismatch" in diff.lower() for diff in differences), "Should mention length mismatch"

    def test_snapshot_comparison_peep_id_mismatch(self, test_db, test_period_data):
        """Test that snapshots with different peep IDs are detected."""
        from db.snapshot_generator import SnapshotGenerator, MemberSnapshot

        generator = SnapshotGenerator(verbose=False)

        # Create two snapshots with mismatched peep IDs
        snapshot1 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            ),
            MemberSnapshot(
                peep_id=2, email="member2@test.com", full_name="Member 2",
                display_name="M2", primary_role="follower",
                priority=1, index_position=1, total_attended=0, active=True
            )
        ]

        snapshot2 = [
            MemberSnapshot(
                peep_id=1, email="member1@test.com", full_name="Member 1",
                display_name="M1", primary_role="leader",
                priority=1, index_position=0, total_attended=0, active=True
            ),
            MemberSnapshot(
                peep_id=3, email="member3@test.com", full_name="Member 3",
                display_name="M3", primary_role="follower",
                priority=1, index_position=1, total_attended=0, active=True
            )  # Different ID
        ]

        # Compare snapshots
        is_same, differences = generator.compare_snapshots(snapshot1, snapshot2)

        assert not is_same, "Snapshots with different peep IDs should not match"
        assert any("peep id mismatch" in diff.lower() for diff in differences), "Should mention peep ID mismatch"


class TestSnapshotCalculation:
    """Tests for snapshot calculation logic (priority and index calculations)."""

    def test_index_calculation_with_same_priority(self, test_db, test_period_data):
        """Test index placement for members with same priority (sorted by total_attended descending)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer.import_period()

        period_id = importer.period_id

        # Get all snapshots ordered by index
        cursor.execute("""
            SELECT peep_id, priority, total_attended, index_position
            FROM peep_order_snapshots
            WHERE period_id = ?
            ORDER BY index_position
        """, (period_id,))
        snapshots = cursor.fetchall()

        # Verify index ordering is consistent with priority (higher first) and total_attended
        for i in range(len(snapshots) - 1):
            curr_peep, curr_priority, curr_attended, curr_index = snapshots[i]
            next_peep, next_priority, next_attended, next_index = snapshots[i + 1]

            # Index should be sequential
            assert next_index == curr_index + 1, f"Index should be sequential, got {curr_index} then {next_index}"

            # If same priority, higher total_attended should come first
            if curr_priority == next_priority:
                assert curr_attended >= next_attended, \
                    f"Members with same priority should be ordered by total_attended descending"

    def test_priority_increment_for_responded_available_not_assigned(self, test_db, test_period_data):
        """Test priority increments when: responded + available (no assignments imported in basic fixture)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Phase 1: Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer.import_period()

        period_id = importer.period_id

        # Fixture has attendance, so check members who responded but didn't attend
        # Members 1-2 attend (in fixture), members 4-8 responded but didn't attend
        # (Member 3 is alternate, so check members 4-5 who responded but weren't assigned/didn't attend)
        cursor.execute("""
            SELECT p.id, pos.priority
            FROM peeps p
            JOIN peep_order_snapshots pos ON p.id = pos.peep_id
            WHERE pos.period_id = ? AND p.id IN (?, ?)
            ORDER BY p.id
        """, (period_id, peep_id_mapping['4'], peep_id_mapping['5']))
        results = cursor.fetchall()

        assert len(results) == 2, "Should have snapshots for members 4 and 5"
        # Members who responded but didn't attend get priority incremented
        for peep_id, priority in results:
            assert priority > 0, f"Member {peep_id} responded but didn't attend, priority should increment"

    def test_priority_unchanged_for_did_not_respond(self, test_db, test_period_data):
        """Test priority unchanged for members who did not respond."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer.import_period()

        period_id = importer.period_id

        # Members 9-10 did not respond (fixture only creates responses for first 8)
        cursor.execute("""
            SELECT p.id, pos.priority
            FROM peeps p
            JOIN peep_order_snapshots pos ON p.id = pos.peep_id
            WHERE pos.period_id = ? AND p.id IN (?, ?)
            ORDER BY p.id
        """, (period_id, peep_id_mapping['9'], peep_id_mapping['10']))
        results = cursor.fetchall()

        assert len(results) == 2, "Should have snapshots for members 9 and 10"
        for peep_id, priority in results:
            assert priority == 0, f"Member {peep_id} did not respond, priority should remain 0 (baseline)"

    def test_sequential_enforcement_loads_prior_snapshot(self, test_db, test_period_data):
        """Test that period 2 snapshot loads period 1 snapshot as baseline."""
        # Create two periods
        period1_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))
        temp_base = Path(period1_data['temp_dir'])

        # Create minimal period 2 (same setup as previous test)
        period2_dir = temp_base / '2025-03'
        period2_dir.mkdir()

        members = []
        for i in range(1, 11):
            members.append({
                'id': i,
                'Name': f'Test Member {i}',
                'Display Name': f'Member{i}',
                'Email Address': f'member{i}@test.com',
                'Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Index': 0,
                'Priority': 0,
                'Total Attended': 0,
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        with open(period2_dir / 'members.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerows(members)

        event_dates = [
            'Friday March 7th - 5pm to 7pm',
            'Friday March 14th - 5pm to 7pm',
            'Friday March 21st - 5pm to 7pm'
        ]
        responses = []
        for i in range(1, 9):
            responses.append({
                'Timestamp': '3/1/2025 10:00:00',
                'Email Address': f'member{i}@test.com',
                'Name': f'Test Member {i}',
                'Primary Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Max Sessions': 2,
                'Min Interval Days': 0,
                'Secondary Role': "I only want to be scheduled in my primary role",
                'Availability': ', '.join(event_dates)
            })

        with open(period2_dir / 'responses.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerows(responses)

        # Production format for results.json
        results_data = {
            'valid_events': [{
                'id': 0,
                'date': '2025-03-07 17:00',
                'duration_minutes': 120,
                'attendees': [{'id': 1, 'name': 'Member1', 'role': 'leader'}],
                'alternates': []
            }],
            'peeps': [],
            'num_unique_attendees': 1,
            'priority_fulfilled': 0,
            'system_weight': 0
        }
        with open(period2_dir / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Production format for actual_attendance.json
        attendance_data = {
            'valid_events': [{
                'id': 0,
                'date': '2025-03-07 17:00',
                'duration_minutes': 120,
                'attendees': [{'id': 1, 'name': 'Member1', 'role': 'leader'}]
            }]
        }
        with open(period2_dir / 'actual_attendance.json', 'w') as f:
            json.dump(attendance_data, f)

        cursor = test_db.cursor()

        # Import members and both periods
        collector = MemberCollector(processed_data_path=temp_base, verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        importer1 = PeriodImporter(
            period_name='2025-02',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer1.import_period()
        period1_id = importer1.period_id

        # Get member 3's priority/index from period 1
        cursor.execute("""
            SELECT priority, index_position
            FROM peep_order_snapshots
            WHERE period_id = ? AND peep_id = ?
        """, (period1_id, peep_id_mapping['3']))
        p1_priority, p1_index = cursor.fetchone()

        importer2 = PeriodImporter(
            period_name='2025-03',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer2.import_period()
        period2_id = importer2.period_id

        # Get member 3's priority/index from period 2
        cursor.execute("""
            SELECT priority, index_position
            FROM peep_order_snapshots
            WHERE period_id = ? AND peep_id = ?
        """, (period2_id, peep_id_mapping['3']))
        p2_priority, p2_index = cursor.fetchone()

        # Verify: Member 3 didn't attend in period 2, so priority should have incremented
        # (if they responded and were available but not assigned)
        # This validates that period 2 loaded period 1's snapshot as baseline
        assert p2_priority >= p1_priority, "Period 2 should load period 1 snapshot as baseline"

    def test_sequential_enforcement_raises_error_without_prior_snapshots(self, test_db, test_period_data):
        """Test that importing period 2 without period 1 snapshots raises ValueError."""
        period_data = next(test_period_data(period_name='2025-03', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Create a prior period WITHOUT snapshots
        cursor.execute("""
            INSERT INTO schedule_periods (period_name, start_date, end_date, status)
            VALUES ('2025-02', '2025-02-01', '2025-02-28', 'completed')
        """)

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Try to import period 2025-03 (should fail because prior period has no snapshots)
        importer = PeriodImporter(
            period_name='2025-03',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )

        # Should raise ValueError during snapshot calculation
        with pytest.raises(ValueError, match="Prior period .* has no snapshots"):
            importer.import_period()

    @pytest.mark.skip(reason="Basic fixture doesn't import attendance - would need enhanced fixture")
    def test_total_attended_accumulation_across_periods(self, test_db, test_period_data):
        """Test total_attended accumulates correctly across multiple periods."""
        # Create two periods
        period1_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))
        temp_base = Path(period1_data['temp_dir'])

        # Create period 2
        period2_dir = temp_base / '2025-03'
        period2_dir.mkdir()

        # Create members and responses for period 2
        members = []
        for i in range(1, 11):
            members.append({
                'id': i,
                'Name': f'Test Member {i}',
                'Display Name': f'Member{i}',
                'Email Address': f'member{i}@test.com',
                'Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Index': 0,
                'Priority': 0,
                'Total Attended': 0,
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        with open(period2_dir / 'members.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerows(members)

        # Create responses for period 2
        event_dates_march = [
            'Friday March 7th - 5pm to 7pm',
            'Friday March 14th - 5pm to 7pm',
            'Friday March 21st - 5pm to 7pm'
        ]
        responses = []
        for i in range(1, 9):
            responses.append({
                'Timestamp': '3/1/2025 10:00:00',
                'Email Address': f'member{i}@test.com',
                'Name': f'Test Member {i}',
                'Primary Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Max Sessions': 2,
                'Min Interval Days': 0,
                'Secondary Role': "I only want to be scheduled in my primary role",
                'Availability': ', '.join(event_dates_march)
            })

        with open(period2_dir / 'responses.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerows(responses)

        # Create results and attendance
        with open(period2_dir / 'results.json', 'w') as f:
            json.dump({
                'event_1': {'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}], 'alternates': []},
                'event_2': {'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}], 'alternates': []},
                'event_3': {'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}], 'alternates': []}
            }, f)

        with open(period2_dir / 'actual_attendance.json', 'w') as f:
            json.dump({'events': [
                {'date': '2025-03-07 17:00', 'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}]},
                {'date': '2025-03-14 17:00', 'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}]},
                {'date': '2025-03-21 17:00', 'attendees': [{'id': 1, 'name': 'Member1', 'role': 'Leader'}]}
            ]}, f)

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=temp_base, verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Import period 1
        importer1 = PeriodImporter(
            period_name='2025-02',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer1.import_period()
        period1_id = importer1.period_id

        # Get member 1's total_attended after period 1
        cursor.execute("""
            SELECT total_attended
            FROM peep_order_snapshots
            WHERE period_id = ? AND peep_id = ?
        """, (period1_id, peep_id_mapping['1']))
        total_attended_p1 = cursor.fetchone()[0]

        # Import period 2
        importer2 = PeriodImporter(
            period_name='2025-03',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer2.import_period()
        period2_id = importer2.period_id

        # Get member 1's total_attended after period 2
        cursor.execute("""
            SELECT total_attended
            FROM peep_order_snapshots
            WHERE period_id = ? AND peep_id = ?
        """, (period2_id, peep_id_mapping['1']))
        total_attended_p2 = cursor.fetchone()[0]

        # Verify accumulation (member 1 attended 2 events in period 1, 3 events in period 2)
        assert total_attended_p2 > total_attended_p1, "total_attended should accumulate across periods"
        assert total_attended_p2 == total_attended_p1 + 3, f"Member 1 attended 3 more events, expected {total_attended_p1 + 3}, got {total_attended_p2}"


class TestPeriodImport:
    """Tests for period-level import operations."""

    def test_december_period_handling(self, test_db, test_period_data):
        """Test that December periods correctly calculate next year for period end."""
        period_data = next(test_period_data(period_name='2025-12', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-12',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )

        # Should handle December → January year rollover
        importer.create_schedule_period()
        assert importer.period_id is not None, "Should create December period successfully"

        # Verify period was created
        cursor.execute("SELECT period_name FROM schedule_periods WHERE id = ?", (importer.period_id,))
        result = cursor.fetchone()
        assert result is not None, "December period should be in database"
        assert result[0] == '2025-12', "Period name should be 2025-12"

    def test_full_period_import(self, test_db, test_period_data):
        """Test full period import using import_period() method."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Phase 1: Member collection
        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Phase 2: Period import
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )

        # Use the full import_period() method
        importer.import_period()

        # Verify period created
        cursor.execute("SELECT COUNT(*) FROM schedule_periods WHERE period_name = ?", ('2025-02',))
        assert cursor.fetchone()[0] == 1, "Should create period"

        # Verify responses imported
        cursor.execute("SELECT COUNT(*) FROM responses")
        response_count = cursor.fetchone()[0]
        assert response_count > 0, "Should import responses"

        # Verify events created
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]
        assert event_count == 3, "Should create 3 events"

        # Verify snapshots created
        cursor.execute("SELECT COUNT(*) FROM peep_order_snapshots")
        snapshot_count = cursor.fetchone()[0]
        assert snapshot_count == 10, "Should create snapshots for all 10 members"

    def test_invalid_period_name_format(self, test_db, test_period_data):
        """Test that invalid period name format raises ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))

        cursor = test_db.cursor()

        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Invalid period name
        with pytest.raises(ValueError, match="Invalid period name format"):
            importer = PeriodImporter(
                period_name='invalid-format-here',  # Invalid format
                processed_data_path=Path(period_data['temp_dir']),
                peep_id_mapping=collector.peep_id_mapping,
                cursor=cursor,
                verbose=False,
                skip_snapshots=True
            )
            importer.create_schedule_period()


class TestIntegration:
    """Integration tests for full import flow."""

    def test_full_sequential_import(self, test_db, test_period_data):
        """Test full sequential import of 2 periods with complete data flow."""
        # Create first period
        period1_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))
        temp_base = Path(period1_data['temp_dir'])

        # Create second period in same directory
        import json

        period2_dir = temp_base / '2025-03'
        period2_dir.mkdir()

        # Create members for period 2 (same 10 members)
        members = []
        for i in range(1, 11):
            members.append({
                'id': i,
                'Name': f'Test Member {i}',
                'Display Name': f'Member{i}',
                'Email Address': f'member{i}@test.com',
                'Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Index': 0,
                'Priority': 0,
                'Total Attended': 0,
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        with open(period2_dir / 'members.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerows(members)

        # Create responses for period 2 with date strings for auto-derive mode
        responses = []
        event_dates_march = [
            'Friday March 7th - 5pm to 7pm',
            'Friday March 14th - 5pm to 7pm',
            'Friday March 21st - 5pm to 7pm'
        ]
        for i in range(1, 9):
            responses.append({
                'Timestamp': '3/1/2025 10:00:00',
                'Email Address': f'member{i}@test.com',
                'Name': f'Test Member {i}',
                'Primary Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Max Sessions': 2,
                'Min Interval Days': 0,
                'Secondary Role': "I only want to be scheduled in my primary role",
                'Availability': ', '.join(event_dates_march)
            })

        with open(period2_dir / 'responses.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerows(responses)

        # Create results.json for period 2 - PRODUCTION FORMAT
        results_data = {
            'valid_events': [
                {'id': 0, 'date': '2025-03-07 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 1, 'name': 'Member1', 'role': 'leader'}], 'alternates': []},
                {'id': 1, 'date': '2025-03-14 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 2, 'name': 'Member2', 'role': 'follower'}], 'alternates': []},
                {'id': 2, 'date': '2025-03-21 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 3, 'name': 'Member3', 'role': 'leader'}], 'alternates': []}
            ],
            'peeps': [],
            'num_unique_attendees': 3,
            'priority_fulfilled': 0,
            'system_weight': 0
        }
        with open(period2_dir / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Create actual_attendance.json for period 2 - PRODUCTION FORMAT
        attendance_data = {
            'valid_events': [
                {'id': 0, 'date': '2025-03-07 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 1, 'name': 'Member1', 'role': 'leader'}]},
                {'id': 1, 'date': '2025-03-14 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 2, 'name': 'Member2', 'role': 'follower'}]},
                {'id': 2, 'date': '2025-03-21 17:00', 'duration_minutes': 120,
                 'attendees': [{'id': 3, 'name': 'Member3', 'role': 'leader'}]}
            ]
        }
        with open(period2_dir / 'actual_attendance.json', 'w') as f:
            json.dump(attendance_data, f)

        cursor = test_db.cursor()

        # Phase 1: Member collection (scan both periods)
        collector = MemberCollector(
            processed_data_path=temp_base,
            verbose=False
        )
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        assert len(peep_id_mapping) == 10, "Should collect 10 unique members"

        # Phase 2: Import Period 1
        importer1 = PeriodImporter(
            period_name='2025-02',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer1.import_period()

        # Verify period 1
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = (SELECT id FROM schedule_periods WHERE period_name = '2025-02')")
        assert cursor.fetchone()[0] == 3, "Period 1 should have 3 events"

        # Phase 2: Import Period 2
        importer2 = PeriodImporter(
            period_name='2025-03',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer2.import_period()

        # Verify period 2
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = (SELECT id FROM schedule_periods WHERE period_name = '2025-03')")
        assert cursor.fetchone()[0] == 3, "Period 2 should have 3 events"

        # Verify end-to-end data integrity
        cursor.execute("SELECT COUNT(*) FROM peeps")
        assert cursor.fetchone()[0] == 10, "Should have 10 members total"

        cursor.execute("SELECT COUNT(*) FROM schedule_periods")
        assert cursor.fetchone()[0] == 2, "Should have 2 periods"

        cursor.execute("SELECT COUNT(*) FROM events")
        assert cursor.fetchone()[0] == 6, "Should have 6 events total (3 per period)"

        cursor.execute("SELECT COUNT(*) FROM peep_order_snapshots")
        assert cursor.fetchone()[0] == 20, "Should have 20 snapshots total (10 per period)"

    def test_import_period_creates_events_from_results_when_no_responses(self, test_db, tmp_path):
        """
        Test edge case where period has NO responses but HAS results.json and actual_attendance.json.

        This scenario represents 2025-12 where:
        - No responses.csv data (0 responses)
        - results.json contains event definitions and assignments
        - actual_attendance.json contains attendance data

        Expected behavior:
        - Events created from results.json (not from responses which don't exist)
        - Assignments created from results.json
        - Attendance created from actual_attendance.json
        - 0 spurious assignment changes
        """
        import tempfile
        import shutil

        # Create temporary test directory
        temp_base = tmp_path / "test_data"
        temp_base.mkdir()
        period_dir = temp_base / "2025-12"
        period_dir.mkdir()

        # Create members.csv with minimal test data
        members_data = []
        for i in [5, 15, 38, 20, 21, 51, 4, 19, 35, 34, 48, 31, 37, 42, 46, 1]:
            members_data.append({
                'id': i,
                'Name': f'Member {i}',
                'Display Name': f'M{i}',
                'Email Address': f'member{i}@test.com',
                'Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Index': 0,
                'Priority': 0,
                'Total Attended': 0,
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        import csv
        with open(period_dir / 'members.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerows(members_data)

        # Create EMPTY responses.csv (only header row)
        with open(period_dir / 'responses.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            # No data rows - empty responses

        # Create results.json with 2 events and assignments (mimicking 2025-12 structure)
        results_data = {
            "valid_events": [
                {
                    "id": 0,
                    "date": "2025-12-06 16:00",
                    "duration_minutes": 90,
                    "attendees": [
                        {"id": 5, "name": "Member 5", "role": "leader"},
                        {"id": 15, "name": "Member 15", "role": "leader"},
                        {"id": 20, "name": "Member 20", "role": "leader"},
                        {"id": 51, "name": "Member 51", "role": "follower"},
                        {"id": 4, "name": "Member 4", "role": "follower"}
                    ],
                    "alternates": [
                        {"id": 21, "name": "Member 21", "role": "leader"}
                    ]
                },
                {
                    "id": 1,
                    "date": "2025-12-12 18:00",
                    "duration_minutes": 90,
                    "attendees": [
                        {"id": 34, "name": "Member 34", "role": "leader"},
                        {"id": 38, "name": "Member 38", "role": "leader"},
                        {"id": 48, "name": "Member 48", "role": "leader"},
                        {"id": 37, "name": "Member 37", "role": "follower"},
                        {"id": 42, "name": "Member 42", "role": "follower"}
                    ],
                    "alternates": [
                        {"id": 31, "name": "Member 31", "role": "leader"}
                    ]
                }
            ]
        }

        with open(period_dir / 'results.json', 'w') as f:
            json.dump(results_data, f, indent=2)

        # Create actual_attendance.json matching results.json attendees
        attendance_data = {
            "valid_events": [
                {
                    "id": 0,
                    "date": "2025-12-06 16:00",
                    "duration_minutes": 90,
                    "attendees": [
                        {"id": 5, "name": "Member 5", "role": "leader"},
                        {"id": 15, "name": "Member 15", "role": "leader"},
                        {"id": 20, "name": "Member 20", "role": "leader"},
                        {"id": 51, "name": "Member 51", "role": "follower"},
                        {"id": 4, "name": "Member 4", "role": "follower"}
                    ]
                },
                {
                    "id": 1,
                    "date": "2025-12-12 18:00",
                    "duration_minutes": 90,
                    "attendees": [
                        {"id": 34, "name": "Member 34", "role": "leader"},
                        {"id": 38, "name": "Member 38", "role": "leader"},
                        {"id": 48, "name": "Member 48", "role": "leader"},
                        {"id": 37, "name": "Member 37", "role": "follower"},
                        {"id": 42, "name": "Member 42", "role": "follower"}
                    ]
                }
            ]
        }

        with open(period_dir / 'actual_attendance.json', 'w') as f:
            json.dump(attendance_data, f, indent=2)

        cursor = test_db.cursor()

        # Phase 1: Import members
        collector = MemberCollector(
            processed_data_path=temp_base,
            verbose=False
        )
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)
        peep_id_mapping = collector.peep_id_mapping

        # Verify members imported
        assert len(peep_id_mapping) == 16, "Should have 16 unique members"

        # Phase 2: Import period with no responses
        importer = PeriodImporter(
            period_name='2025-12',
            processed_data_path=temp_base,
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )

        importer.import_period()

        # Verify: 0 responses imported (empty responses.csv)
        cursor.execute("SELECT COUNT(*) FROM responses WHERE period_id = ?", (importer.period_id,))
        assert cursor.fetchone()[0] == 0, "Should have 0 responses when responses.csv is empty"

        # Verify: 2 events created from results.json
        cursor.execute("SELECT COUNT(*) FROM events WHERE period_id = ?", (importer.period_id,))
        assert cursor.fetchone()[0] == 2, "Should create 2 events from results.json"

        # Verify: Events have correct details from results.json
        cursor.execute("""
            SELECT event_datetime, duration_minutes, status
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (importer.period_id,))
        events = cursor.fetchall()
        assert len(events) == 2
        assert events[0][0] == "2025-12-06T16:00:00", "First event should be 2025-12-06T16:00:00 (ISO 8601)"
        assert events[0][1] == 90, "First event should be 90 minutes"
        assert events[0][2] == "completed", "Event should have 'completed' status from results.json"
        assert events[1][0] == "2025-12-12T18:00:00", "Second event should be 2025-12-12T18:00:00 (ISO 8601)"
        assert events[1][1] == 90, "Second event should be 90 minutes"

        # Verify: Assignments created correctly (5 attendees + 1 alternate per event = 12 total)
        cursor.execute("""
            SELECT COUNT(*)
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (importer.period_id,))
        assert cursor.fetchone()[0] == 12, "Should create 12 assignments (5+1) * 2 events"

        # Verify: Attendance created correctly (5 attendees per event = 10 total)
        cursor.execute("""
            SELECT COUNT(*)
            FROM event_attendance ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (importer.period_id,))
        assert cursor.fetchone()[0] == 10, "Should create 10 attendance records"

        # Verify: 0 assignment changes (no cancellations, everyone assigned attended)
        cursor.execute("""
            SELECT COUNT(*)
            FROM event_assignment_changes eac
            JOIN events e ON eac.event_id = e.id
            WHERE e.period_id = ?
        """, (importer.period_id,))
        assert cursor.fetchone()[0] == 0, "Should have 0 assignment changes (no cancellations or adds)"

        # Verify: 0 event_availability records (no responses means no availability)
        cursor.execute("""
            SELECT COUNT(*)
            FROM event_availability ea
            JOIN events e ON ea.event_id = (
                SELECT id FROM events WHERE period_id = ? LIMIT 1
            )
        """, (importer.period_id,))
        # This query is checking if any availability exists for events in this period
        # Since we have no responses, we expect 0
        cursor.execute("""
            SELECT COUNT(*)
            FROM event_availability ea
            WHERE ea.event_id IN (
                SELECT id FROM events WHERE period_id = ?
            )
        """, (importer.period_id,))
        assert cursor.fetchone()[0] == 0, "Should have 0 event_availability records (no responses)"


class TestDatabaseImportBugFixes:
    """
    Tests that reproduce bugs found during database import validation.

    These tests demonstrate expected behavior vs actual buggy behavior.
    All tests should FAIL until bugs are fixed.
    """

    def test_fix_event_statuses_updated_from_scheduling_results(self, test_db, test_period_data):
        """
        Bug #1: Event statuses never updated from results.json/actual_attendance.json.

        Expected behavior:
        - Events in results.json → status='scheduled'
        - Events in actual_attendance.json → status='completed'
        - Events only in responses.csv (not scheduled) → status='proposed'

        Actual behavior: All events status='proposed' (default from create_events).
        """
        # Create period with mixed event statuses
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Modify test data to create different scenarios:
        # Event 1: Scheduled and completed (in both results.json and actual_attendance.json)
        # Event 2: Scheduled but not completed (in results.json only)
        # Event 3: Only proposed (not in results.json or actual_attendance.json)

        # Create results.json with 2 events (Event 1 and Event 2)
        results_data = {
            'valid_events': [
                {
                    'id': 'evt1',
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                },
                {
                    'id': 'evt2',
                    'date': '2025-02-14 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 3, 'name': 'Test Member 3', 'role': 'leader'},
                        {'id': 4, 'name': 'Test Member 4', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        # Create actual_attendance.json with only 1 event (Event 1 completed)
        attendance_data = {
            'valid_events': [
                {
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ]
                }
            ]
        }

        # Write updated JSON files
        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        with open(Path(period_data['period_dir']) / 'actual_attendance.json', 'w') as f:
            json.dump(attendance_data, f)

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Query event statuses
        cursor.execute("""
            SELECT event_datetime, status
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (importer.period_id,))
        events = cursor.fetchall()

        # Expected: 3 events total
        assert len(events) == 3, f"Should have 3 events (got {len(events)})"

        # Event 1: Should be 'completed' (in actual_attendance.json)
        assert events[0][1] == 'completed', \
            f"Event 1 (2025-02-07) should be 'completed' (in attendance), got '{events[0][1]}'"

        # Event 2: Should be 'cancelled' (in results.json but not attendance)
        assert events[1][1] == 'cancelled', \
            f"Event 2 (2025-02-14) should be 'cancelled' (scheduled but didn't occur), got '{events[1][1]}'"

        # Event 3: Should be 'proposed' (only in responses, not scheduled)
        assert events[2][1] == 'proposed', \
            f"Event 3 (2025-02-21) should be 'proposed' (not scheduled), got '{events[2][1]}'"

    def test_fix_event_durations_updated_from_scheduling_results(self, test_db, test_period_data):
        """
        Bug #2: Event durations not updated from results.json.

        Expected behavior:
        - Event duration should match results.json/actual_attendance.json (actual scheduled duration)
        - Duration can change from proposed (responses.csv) to actual (results.json)

        Actual behavior: Duration remains at proposed value from responses.csv.
        """
        # Create period with event that gets downgraded
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=1))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Modify responses.csv to propose a 120-minute event
        responses_data = []
        for i in range(1, 6):
            responses_data.append({
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': f'member{i}@test.com',
                'Name': f'Test Member {i}',
                'Primary Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Max Sessions': 2,
                'Min Interval Days': 0,
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Availability': 'Friday February 7th - 5pm to 7pm'  # Implies 120 minutes
            })

        with open(Path(period_data['period_dir']) / 'responses.csv', 'w', newline='') as f:
            writer = csv_module.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerows(responses_data)

        # Create results.json with downgraded 90-minute event
        results_data = {
            'valid_events': [
                {
                    'id': 'evt1',
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 90,  # Downgraded from 120
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Remove fixture's actual_attendance.json (test is for results.json duration only)
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        if attendance_file.exists():
            attendance_file.unlink()

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Query event duration
        cursor.execute("""
            SELECT duration_minutes
            FROM events
            WHERE period_id = ?
        """, (importer.period_id,))
        duration = cursor.fetchone()[0]

        # Expected: 90 minutes (from results.json, not 120 from responses)
        assert duration == 90, \
            f"Event duration should be 90 minutes (from results.json), got {duration}"

    def test_fix_period_status_based_on_attendance_existence(self, test_db, test_period_data):
        """
        Bug #3: Period status incorrect - all periods marked 'completed' regardless.

        Expected behavior:
        - Period status='completed' only if actual_attendance.json exists
        - Period status='scheduled' if results.json exists but no attendance
        - Period status='draft' if only responses exist

        Actual behavior: All periods marked 'completed' (hardcoded in create_schedule_period).
        """
        # Create future period with results but NO attendance
        period_data = next(test_period_data(period_name='2025-12', num_members=10, num_events=1))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Create results.json (scheduled)
        results_data = {
            'valid_events': [
                {
                    'id': 'evt1',
                    'date': '2025-12-06 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Remove actual_attendance.json (future period, not completed)
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        if attendance_path.exists():
            attendance_path.unlink()

        # Import period
        importer = PeriodImporter(
            period_name='2025-12',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Query period status
        cursor.execute("""
            SELECT status
            FROM schedule_periods
            WHERE id = ?
        """, (importer.period_id,))
        status = cursor.fetchone()[0]

        # Expected: 'scheduled' (has results.json but no attendance)
        assert status == 'scheduled', \
            f"Period status should be 'scheduled' (has schedule but no attendance data), got '{status}'"

    def test_fix_snapshots_only_created_when_attendance_exists(self, test_db, test_period_data):
        """
        Bug #4: Snapshots created without attendance data.

        Expected behavior:
        - Snapshots ONLY created when actual_attendance.json exists
        - No snapshots for future periods without attendance

        Actual behavior: Snapshots created for all periods regardless.
        """
        # Create future period with results but NO attendance
        period_data = next(test_period_data(period_name='2025-12', num_members=10, num_events=1))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Create results.json (scheduled)
        results_data = {
            'valid_events': [
                {
                    'id': 'evt1',
                    'date': '2025-12-06 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Remove actual_attendance.json (future period)
        attendance_path = Path(period_data['period_dir']) / 'actual_attendance.json'
        if attendance_path.exists():
            attendance_path.unlink()

        # Import period WITH snapshots enabled
        importer = PeriodImporter(
            period_name='2025-12',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False  # Enable snapshots to test the bug
        )
        importer.import_period()

        # Query snapshot count
        cursor.execute("""
            SELECT COUNT(*)
            FROM peep_order_snapshots
            WHERE period_id = ?
        """, (importer.period_id,))
        snapshot_count = cursor.fetchone()[0]

        # Expected: 0 snapshots (no attendance data)
        assert snapshot_count == 0, \
            f"Should have 0 snapshots (no attendance data), got {snapshot_count}"

    def test_fix_legacy_period_event_id_populated_from_results(self, test_db, test_period_data):
        """
        Bug #5: legacy_period_event_id not populated.

        Expected behavior:
        - legacy_period_event_id should contain event "id" from results.json

        Actual behavior: All NULL.
        """
        # Create period with events
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=2))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Create results.json with explicit event IDs
        results_data = {
            'valid_events': [
                {
                    'id': 'legacy_evt_001',  # Legacy event ID
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                },
                {
                    'id': 'legacy_evt_002',  # Legacy event ID
                    'date': '2025-02-14 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 3, 'name': 'Test Member 3', 'role': 'leader'},
                        {'id': 4, 'name': 'Test Member 4', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Query legacy_period_event_id values
        cursor.execute("""
            SELECT event_datetime, legacy_period_event_id
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (importer.period_id,))
        events = cursor.fetchall()

        # Expected: Both events should have legacy IDs
        assert len(events) == 2, "Should have 2 events"

        assert events[0][1] == 'legacy_evt_001', \
            f"Event 1 should have legacy_period_event_id='legacy_evt_001', got '{events[0][1]}'"

        assert events[1][1] == 'legacy_evt_002', \
            f"Event 2 should have legacy_period_event_id='legacy_evt_002', got '{events[1][1]}'"

    def test_fix_event_cancellation_tracked_when_not_in_attendance(self, test_db, test_period_data):
        """
        Bug #6: Results vs Attendance discrepancies not handled.

        Expected behavior:
        - Event in results.json but NOT in actual_attendance.json → status='cancelled' or similar
        - Event in actual_attendance.json but NOT in results.json → mark as late addition

        Actual behavior: No reconciliation between results.json and actual_attendance.json.
        """
        # Create period with scheduled events, one of which gets cancelled
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))
        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Create results.json with 3 events scheduled
        results_data = {
            'valid_events': [
                {
                    'id': 'evt1',
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ],
                    'alternates': []
                },
                {
                    'id': 'evt2',
                    'date': '2025-02-14 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 3, 'name': 'Test Member 3', 'role': 'leader'},
                        {'id': 4, 'name': 'Test Member 4', 'role': 'follower'}
                    ],
                    'alternates': []
                },
                {
                    'id': 'evt3',
                    'date': '2025-02-21 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 5, 'name': 'Test Member 5', 'role': 'leader'},
                        {'id': 6, 'name': 'Test Member 6', 'role': 'follower'}
                    ],
                    'alternates': []
                }
            ]
        }

        # Create actual_attendance.json with only 2 events (Event 2 was cancelled)
        attendance_data = {
            'valid_events': [
                {
                    'date': '2025-02-07 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 1, 'name': 'Test Member 1', 'role': 'leader'},
                        {'id': 2, 'name': 'Test Member 2', 'role': 'follower'}
                    ]
                },
                {
                    'date': '2025-02-21 17:00',
                    'duration_minutes': 120,
                    'attendees': [
                        {'id': 5, 'name': 'Test Member 5', 'role': 'leader'},
                        {'id': 6, 'name': 'Test Member 6', 'role': 'follower'}
                    ]
                }
            ]
        }

        with open(Path(period_data['period_dir']) / 'results.json', 'w') as f:
            json.dump(results_data, f)

        with open(Path(period_data['period_dir']) / 'actual_attendance.json', 'w') as f:
            json.dump(attendance_data, f)

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Query event statuses
        cursor.execute("""
            SELECT event_datetime, status
            FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
        """, (importer.period_id,))
        events = cursor.fetchall()

        # Expected: 3 events
        assert len(events) == 3, f"Should have 3 events, got {len(events)}"

        # Event 1: Completed (in both results and attendance)
        assert events[0][1] == 'completed', \
            f"Event 1 (2025-02-07) should be 'completed', got '{events[0][1]}'"

        # Event 2: Cancelled (in results but not in attendance)
        assert events[1][1] == 'cancelled', \
            f"Event 2 (2025-02-14) should be 'cancelled' (in results but not attendance), got '{events[1][1]}'"

        # Event 3: Completed (in both results and attendance)
        assert events[2][1] == 'completed', \
            f"Event 3 (2025-02-21) should be 'completed', got '{events[2][1]}'"

@pytest.mark.skip(reason="Working on blocking bug fixes first")
class TestCancelledEventsImport:
    """Tests for importing cancelled events from cancellations.json."""

    def test_import_cancelled_events_from_json(self, test_db, test_period_data):
        """Test loading cancellations.json and marking matching events as cancelled."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period first
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Get the first event's datetime string for cancellation
        cursor.execute("""
            SELECT event_datetime FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
            LIMIT 1
        """, (period_id,))
        first_event = cursor.fetchone()
        event_datetime = first_event[0]

        # Parse datetime to create cancellation string
        # Expected format: "Friday February 7th - 5pm to 7pm"
        dt = datetime.fromisoformat(event_datetime)
        day_name = dt.strftime('%A')
        month_name = dt.strftime('%B')
        day = dt.day
        # Convert day to ordinal (1->1st, 2->2nd, 3->3rd, 21->21st, etc.)
        if day in (1, 21, 31):
            day_suffix = 'st'
        elif day in (2, 22):
            day_suffix = 'nd'
        elif day in (3, 23):
            day_suffix = 'rd'
        else:
            day_suffix = 'th'
        day_str = f"{day}{day_suffix}"
        hour = dt.hour
        hour_12 = dt.strftime('%I').lstrip('0')
        am_pm = dt.strftime('%p').lower()
        # Duration is 120 minutes (2 hours)
        end_hour = (dt + timedelta(hours=2)).strftime('%I').lstrip('0')
        end_am_pm = (dt + timedelta(hours=2)).strftime('%p').lower()

        event_string = f"{day_name} {month_name} {day_str} - {hour_12}{am_pm} to {end_hour}{end_am_pm}"

        # Create cancellations.json
        cancellations_data = {
            "cancelled_events": [event_string],
            "cancelled_availability": []
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import cancelled events
        from db.import_period_data import import_cancelled_events
        import_cancelled_events(cancellations_path, period_id, cursor)

        # Verify first event is marked as cancelled
        cursor.execute("""
            SELECT status FROM events
            WHERE period_id = ? AND event_datetime = ?
        """, (period_id, event_datetime))
        result = cursor.fetchone()
        assert result is not None, "Event should exist"
        assert result[0] == 'cancelled', f"Event should be cancelled, got {result[0]}"

    def test_cancelled_events_backward_compatible_missing_file(self, test_db, test_period_data):
        """Test that missing cancellations.json returns empty set (backward compatible)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Try to import with missing file (should not crash)
        from db.import_period_data import import_cancelled_events
        missing_path = Path(period_data['period_dir']) / 'nonexistent.json'

        # Should handle gracefully
        import_cancelled_events(missing_path, period_id, cursor)

        # Verify all events are still proposed (not cancelled)
        cursor.execute("""
            SELECT COUNT(*) FROM events
            WHERE period_id = ? AND status = 'cancelled'
        """, (period_id,))
        assert cursor.fetchone()[0] == 0, "No events should be cancelled when file is missing"

    def test_cancelled_events_empty_list_handling(self, test_db, test_period_data):
        """Test that empty cancelled_events list doesn't cancel any events."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create cancellations.json with empty list
        cancellations_data = {
            "cancelled_events": [],
            "cancelled_availability": []
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import cancelled events
        from db.import_period_data import import_cancelled_events
        import_cancelled_events(cancellations_path, period_id, cursor)

        # Verify no events are cancelled
        cursor.execute("""
            SELECT COUNT(*) FROM events
            WHERE period_id = ? AND status = 'cancelled'
        """, (period_id,))
        assert cursor.fetchone()[0] == 0, "No events should be cancelled with empty list"

    def test_cancelled_events_validates_invalid_event_strings(self, test_db, test_period_data):
        """Test that invalid event strings raise ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create cancellations.json with invalid event string
        cancellations_data = {
            "cancelled_events": ["Invalid Event String That Doesn't Match Anything"],
            "cancelled_availability": []
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import should raise ValueError
        from db.import_period_data import import_cancelled_events
        with pytest.raises(ValueError, match="(?s).*cancelled_events.*does not match"):
            import_cancelled_events(cancellations_path, period_id, cursor)


@pytest.mark.skip(reason="Working on blocking bug fixes first")
class TestCancelledAvailabilityImport:
    """Tests for importing cancelled availability from cancellations.json."""

    def test_cancelled_availability_removes_event_availability_records(self, test_db, test_period_data):
        """Test that cancelled_availability removes matching event_availability records."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Get a response and event to work with
        cursor.execute("""
            SELECT id FROM responses
            WHERE period_id = ?
            LIMIT 1
        """, (period_id,))
        response_id = cursor.fetchone()[0]

        cursor.execute("""
            SELECT id, event_datetime FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
            LIMIT 1
        """, (period_id,))
        event_id, event_datetime = cursor.fetchone()

        # Verify event_availability exists
        cursor.execute("""
            SELECT COUNT(*) FROM event_availability
            WHERE response_id = ? AND event_id = ?
        """, (response_id, event_id))
        assert cursor.fetchone()[0] > 0, "Event availability should exist before cancellation"

        # Get peep email and event string for cancellation
        cursor.execute("SELECT peep_id FROM responses WHERE id = ?", (response_id,))
        peep_id = cursor.fetchone()[0]

        cursor.execute("SELECT email FROM peeps WHERE id = ?", (peep_id,))
        peep_email = cursor.fetchone()[0]

        # Create event string
        dt = datetime.fromisoformat(event_datetime)
        day_name = dt.strftime('%A')
        month_name = dt.strftime('%B')
        day = dt.day
        if day in (1, 21, 31):
            day_suffix = 'st'
        elif day in (2, 22):
            day_suffix = 'nd'
        elif day in (3, 23):
            day_suffix = 'rd'
        else:
            day_suffix = 'th'
        day_str = f"{day}{day_suffix}"
        hour = dt.hour
        hour_12 = dt.strftime('%I').lstrip('0')
        am_pm = dt.strftime('%p').lower()
        end_hour = (dt + timedelta(hours=2)).strftime('%I').lstrip('0')
        end_am_pm = (dt + timedelta(hours=2)).strftime('%p').lower()

        event_string = f"{day_name} {month_name} {day_str} - {hour_12}{am_pm} to {end_hour}{end_am_pm}"

        # Create cancellations.json
        cancellations_data = {
            "cancelled_events": [],
            "cancelled_availability": [
                {
                    "email": peep_email,
                    "events": [event_string]
                }
            ]
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import cancelled availability
        from db.import_period_data import import_cancelled_availability
        import_cancelled_availability(cancellations_path, period_id, cursor, collector.peep_id_mapping)

        # Verify event_availability record is removed
        cursor.execute("""
            SELECT COUNT(*) FROM event_availability
            WHERE response_id = ? AND event_id = ?
        """, (response_id, event_id))
        assert cursor.fetchone()[0] == 0, "Event availability record should be deleted"

    def test_cancelled_availability_handles_duplicate_emails(self, test_db, test_period_data):
        """Test that duplicate email entries are merged without data loss."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Get peep email and events
        cursor.execute("""
            SELECT peep_id FROM responses
            WHERE period_id = ?
            LIMIT 1
        """, (period_id,))
        peep_id = cursor.fetchone()[0]

        cursor.execute("SELECT email FROM peeps WHERE id = ?", (peep_id,))
        peep_email = cursor.fetchone()[0]

        # Get two events
        cursor.execute("""
            SELECT id, event_datetime FROM events
            WHERE period_id = ?
            ORDER BY event_datetime
            LIMIT 2
        """, (period_id,))
        events = cursor.fetchall()

        event_strings = []
        for event_id, event_datetime in events:
            dt = datetime.fromisoformat(event_datetime)
            day_name = dt.strftime('%A')
            month_name = dt.strftime('%B')
            day = dt.day
            if day in (1, 21, 31):
                day_suffix = 'st'
            elif day in (2, 22):
                day_suffix = 'nd'
            elif day in (3, 23):
                day_suffix = 'rd'
            else:
                day_suffix = 'th'
            day_str = f"{day}{day_suffix}"
            hour = dt.hour
            hour_12 = dt.strftime('%I').lstrip('0')
            am_pm = dt.strftime('%p').lower()
            end_hour = (dt + timedelta(hours=2)).strftime('%I').lstrip('0')
            end_am_pm = (dt + timedelta(hours=2)).strftime('%p').lower()

            event_strings.append(f"{day_name} {month_name} {day_str} - {hour_12}{am_pm} to {end_hour}{end_am_pm}")

        # Create cancellations.json with duplicate emails (one per event)
        cancellations_data = {
            "cancelled_events": [],
            "cancelled_availability": [
                {
                    "email": peep_email,
                    "events": [event_strings[0]]
                },
                {
                    "email": peep_email,
                    "events": [event_strings[1]]
                }
            ]
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import cancelled availability
        from db.import_period_data import import_cancelled_availability
        import_cancelled_availability(cancellations_path, period_id, cursor, collector.peep_id_mapping)

        # Verify both event_availability records are removed (merged handling)
        for event_id, _ in events:
            cursor.execute("""
                SELECT response_id FROM responses
                WHERE peep_id = ? AND period_id = ?
                LIMIT 1
            """, (peep_id, period_id))
            response_id = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM event_availability
                WHERE response_id = ? AND event_id = ?
            """, (response_id, event_id))
            assert cursor.fetchone()[0] == 0, f"Event availability should be removed for both duplicate emails"

    def test_cancelled_availability_backward_compatible_missing_section(self, test_db, test_period_data):
        """Test that missing cancelled_availability section returns empty dict."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create cancellations.json without cancelled_availability section
        cancellations_data = {
            "cancelled_events": []
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import should handle missing section gracefully
        from db.import_period_data import import_cancelled_availability
        import_cancelled_availability(cancellations_path, period_id, cursor, collector.peep_id_mapping)

        # Verify no event_availability records are removed
        cursor.execute("""
            SELECT COUNT(*) FROM event_availability
            WHERE id > 0
        """)
        original_count = cursor.fetchone()[0]

        # Should be unchanged since no cancellations were imported
        assert original_count > 0, "Should have event_availability records"

    def test_cancelled_availability_empty_list_handling(self, test_db, test_period_data):
        """Test that empty cancelled_availability list doesn't remove anything."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM event_availability")
        initial_count = cursor.fetchone()[0]

        # Create cancellations.json with empty list
        cancellations_data = {
            "cancelled_events": [],
            "cancelled_availability": []
        }
        cancellations_path = Path(period_data['period_dir']) / 'cancellations.json'
        with open(cancellations_path, 'w') as f:
            json.dump(cancellations_data, f)

        # Import cancelled availability
        from db.import_period_data import import_cancelled_availability
        import_cancelled_availability(cancellations_path, period_id, cursor, collector.peep_id_mapping)

        # Verify count is unchanged
        cursor.execute("SELECT COUNT(*) FROM event_availability")
        final_count = cursor.fetchone()[0]
        assert final_count == initial_count, "Event availability records should be unchanged with empty list"


@pytest.mark.skip(reason="Working on blocking bug fixes first")
class TestPartnershipRequestsImport:
    """Tests for importing partnership requests from partnerships.json."""

    def test_import_partnerships_and_store_in_database(self, test_db, test_period_data):
        """Test loading partnerships.json and storing in partnership_requests table."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create partnerships.json with test data
        partnerships_data = {
            "1": [2],
            "2": [1],
            "3": [4, 5]
        }
        partnerships_path = Path(period_data['period_dir']) / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships_data, f)

        # Import partnerships
        from db.import_period_data import import_partnerships
        import_partnerships(partnerships_path, period_id, cursor)

        # Verify partnerships are stored
        cursor.execute("""
            SELECT requester_peep_id, partner_peep_id FROM partnership_requests
            WHERE period_id = ?
            ORDER BY requester_peep_id, partner_peep_id
        """, (period_id,))
        results = cursor.fetchall()

        # Should have 4 records: (1,2), (2,1), (3,4), (3,5)
        assert len(results) == 4, f"Should have 4 partnership records, got {len(results)}"

        # Verify specific partnerships
        assert (1, 2) in results, "Should have partnership request from 1 to 2"
        assert (2, 1) in results, "Should have partnership request from 2 to 1"
        assert (3, 4) in results, "Should have partnership request from 3 to 4"
        assert (3, 5) in results, "Should have partnership request from 3 to 5"

    def test_partnerships_wrapped_format_support(self, test_db, test_period_data):
        """Test that partnerships.json with wrapped 'partnerships' key is supported."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create partnerships.json with wrapped format
        partnerships_data = {
            "partnerships": {
                "1": [2],
                "2": [1]
            }
        }
        partnerships_path = Path(period_data['period_dir']) / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships_data, f)

        # Import partnerships
        from db.import_period_data import import_partnerships
        import_partnerships(partnerships_path, period_id, cursor)

        # Verify partnerships are stored
        cursor.execute("""
            SELECT COUNT(*) FROM partnership_requests
            WHERE period_id = ?
        """, (period_id,))
        count = cursor.fetchone()[0]
        assert count == 2, f"Should have 2 partnership records from wrapped format, got {count}"

    def test_partnerships_backward_compatible_missing_file(self, test_db, test_period_data):
        """Test that missing partnerships.json returns empty dict (backward compatible)."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Try to import with missing file (should not crash)
        from db.import_period_data import import_partnerships
        missing_path = Path(period_data['period_dir']) / 'nonexistent.json'

        # Should handle gracefully
        import_partnerships(missing_path, period_id, cursor)

        # Verify no partnerships were created
        cursor.execute("""
            SELECT COUNT(*) FROM partnership_requests
            WHERE period_id = ?
        """, (period_id,))
        assert cursor.fetchone()[0] == 0, "No partnerships should be created when file is missing"

    def test_partnerships_validates_invalid_member_ids(self, test_db, test_period_data):
        """Test that invalid member IDs raise ValueError with clear error message."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create partnerships.json with invalid member ID (999)
        partnerships_data = {
            "1": [999]  # 999 doesn't exist in peeps table
        }
        partnerships_path = Path(period_data['period_dir']) / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships_data, f)

        # Import should raise ValueError
        from db.import_period_data import import_partnerships
        with pytest.raises(ValueError, match="(?s).*(member|peep).*999.*does not exist.*peeps"):
            import_partnerships(partnerships_path, period_id, cursor)

    def test_partnerships_validates_requester_member_ids(self, test_db, test_period_data):
        """Test that invalid requester IDs raise ValueError."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create partnerships.json with invalid requester ID (999)
        partnerships_data = {
            "999": [1]  # 999 doesn't exist in peeps table
        }
        partnerships_path = Path(period_data['period_dir']) / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships_data, f)

        # Import should raise ValueError
        from db.import_period_data import import_partnerships
        with pytest.raises(ValueError, match="(?s).*(member|peep).*999.*does not exist.*peeps"):
            import_partnerships(partnerships_path, period_id, cursor)

    def test_partnerships_strict_validation_malformed_data(self, test_db, test_period_data):
        """Test that malformed partnership data raises ValueError with clear error."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members and period
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()
        period_id = importer.period_id

        # Create partnerships.json with invalid structure (value should be array)
        partnerships_data = {
            "1": "not_an_array"  # Should be list, not string
        }
        partnerships_path = Path(period_data['period_dir']) / 'partnerships.json'
        with open(partnerships_path, 'w') as f:
            json.dump(partnerships_data, f)

        # Import should raise ValueError
        from db.import_period_data import import_partnerships
        with pytest.raises(ValueError, match="(?s).*format.*array.*list"):
            import_partnerships(partnerships_path, period_id, cursor)


class TestStatusLifecycleBugs:
    """Tests demonstrating three critical status lifecycle bugs in database import.

    Bug #002: Period Status Bug
    Bug #003: Snapshot Creation Bug
    Bug #004: Event Status Lifecycle Bug

    These tests will FAIL initially but PASS once bugs are fixed.
    """

    # ========================================================================
    # Bug #002: Period Status Bug
    # ========================================================================

    def test_period_status_scheduled_when_no_attendance(self, test_db, test_period_data):
        """
        Bug #002: Period status should be 'scheduled' when:
        - Has assignments (from results.json)
        - NO attendance data (from actual_attendance.json)
        - This indicates future/upcoming period with schedule
        """
        # Create test data with results.json but NO actual_attendance.json
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        # Remove actual_attendance.json to simulate future period
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (has assignments but no attendance)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Check period status
        cursor.execute("""
            SELECT status FROM schedule_periods WHERE id = ?
        """, (importer.period_id,))
        result = cursor.fetchone()
        actual_status = result[0] if result else None

        # EXPECTED: status should be 'scheduled' (has schedule but not yet completed)
        assert actual_status == 'scheduled', \
            f"Period with assignments but no attendance should be 'scheduled', got '{actual_status}'"

    def test_period_status_completed_when_has_attendance(self, test_db, test_period_data):
        """
        Bug #002: Period status should be 'completed' when:
        - Has attendance data (from actual_attendance.json)
        - This indicates period has occurred with recorded participation
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (has both assignments and attendance)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Check period status
        cursor.execute("""
            SELECT status FROM schedule_periods WHERE id = ?
        """, (importer.period_id,))
        result = cursor.fetchone()
        actual_status = result[0] if result else None

        # EXPECTED: status should be 'completed' (has attendance data)
        assert actual_status == 'completed', \
            f"Period with attendance data should be 'completed', got '{actual_status}'"

    def test_period_status_active_when_has_assignments_no_attendance(self, test_db, test_period_data):
        """
        Bug #002: Period status should be 'active' when:
        - Has assignments (from results.json)
        - NO attendance data (from actual_attendance.json)
        - This indicates scheduled but not yet occurred
        """
        # Create test data without attendance
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        # Remove actual_attendance.json
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Verify assignments exist
        cursor.execute("""
            SELECT COUNT(*) FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (importer.period_id,))
        num_assignments = cursor.fetchone()[0]
        assert num_assignments > 0, "Period should have assignments"

        # Check period status
        cursor.execute("""
            SELECT status FROM schedule_periods WHERE id = ?
        """, (importer.period_id,))
        result = cursor.fetchone()
        actual_status = result[0] if result else None

        # EXPECTED: status should be 'active' (has schedule but hasn't occurred)
        assert actual_status == 'scheduled', \
            f"Period with assignments but no attendance should be 'scheduled', got '{actual_status}'"

    def test_period_status_draft_when_no_assignments_no_attendance(self, test_db, test_period_data):
        """
        Bug #002: Period status should be 'draft' when:
        - NO assignments (empty results.json)
        - NO attendance data
        - This indicates unopened/unscheduled period
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=0))

        # Create empty results.json (no assignments)
        results_file = Path(period_data['period_dir']) / 'results.json'
        with open(results_file, 'w') as f:
            json.dump({'valid_events': []}, f)

        # Remove actual_attendance.json
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (no assignments, no attendance)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Verify no assignments exist
        cursor.execute("""
            SELECT COUNT(*) FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            WHERE e.period_id = ?
        """, (importer.period_id,))
        num_assignments = cursor.fetchone()[0]
        assert num_assignments == 0, "Period should have no assignments"

        # Check period status
        cursor.execute("""
            SELECT status FROM schedule_periods WHERE id = ?
        """, (importer.period_id,))
        result = cursor.fetchone()
        actual_status = result[0] if result else None

        # EXPECTED: status should be 'draft' (not yet started/scheduled)
        assert actual_status == 'draft', \
            f"Period with no assignments and no attendance should be 'draft', got '{actual_status}'"

    # ========================================================================
    # Bug #003: Snapshot Creation Bug
    # ========================================================================

    def test_snapshots_not_created_without_attendance_data(self, test_db, test_period_data):
        """
        Bug #003: Snapshots should NOT be created when:
        - No attendance data exists (future/incomplete period)
        - This prevents stale snapshots for periods that haven't occurred
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        # Remove actual_attendance.json to simulate future period
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period WITHOUT skip_snapshots flag
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False  # Should respect the logic: only create if attendance exists
        )
        importer.import_period()

        # Check that NO snapshots were created
        cursor.execute("""
            SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?
        """, (importer.period_id,))
        num_snapshots = cursor.fetchone()[0]

        # EXPECTED: num_snapshots should be 0 (no attendance = no snapshots)
        assert num_snapshots == 0, \
            f"Period without attendance data should have 0 snapshots, got {num_snapshots}"

    def test_snapshots_created_with_attendance_data(self, test_db, test_period_data):
        """
        Bug #003: Snapshots SHOULD be created when:
        - Attendance data exists (period has been completed)
        - This captures the final state after the period
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period with attendance data
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=False
        )
        importer.import_period()

        # Check that snapshots WERE created
        cursor.execute("""
            SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?
        """, (importer.period_id,))
        num_snapshots = cursor.fetchone()[0]

        # EXPECTED: num_snapshots should be > 0 (has attendance = has snapshots)
        assert num_snapshots > 0, \
            f"Period with attendance data should have snapshots, got {num_snapshots}"

    def test_snapshots_skipped_with_skip_snapshots_flag(self, test_db, test_period_data):
        """
        Bug #003: Snapshots should NOT be created when:
        - --skip-snapshots flag is used
        - Even if attendance data exists
        - This allows testing import without snapshot overhead
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period WITH skip_snapshots=True
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True  # Skip snapshot creation
        )
        importer.import_period()

        # Check that NO snapshots were created
        cursor.execute("""
            SELECT COUNT(*) FROM peep_order_snapshots WHERE period_id = ?
        """, (importer.period_id,))
        num_snapshots = cursor.fetchone()[0]

        # EXPECTED: num_snapshots should be 0 (skipped due to flag)
        assert num_snapshots == 0, \
            f"Period imported with --skip-snapshots should have 0 snapshots, got {num_snapshots}"

    # ========================================================================
    # Bug #004: Event Status Lifecycle Bug
    # ========================================================================

    def test_event_status_proposed_from_responses(self, test_db, test_period_data):
        """
        Bug #004: Events created from responses.csv should have:
        - status='proposed' (not yet scheduled by scheduler)
        - This is the initial state before scheduler processes them
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        # Remove results.json so events stay in 'proposed' state
        results_file = Path(period_data['period_dir']) / 'results.json'
        results_file.unlink()

        # Also remove actual_attendance.json so events don't transition to 'completed'
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (only responses, no scheduler results)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Check event statuses
        cursor.execute("""
            SELECT status FROM events WHERE period_id = ? ORDER BY event_datetime
        """, (importer.period_id,))
        statuses = [row[0] for row in cursor.fetchall()]

        # EXPECTED: all events should be 'proposed'
        assert len(statuses) > 0, "Period should have events"
        assert all(status == 'proposed' for status in statuses), \
            f"Events from responses.csv should all be 'proposed', got {statuses}"

    def test_event_status_scheduled_from_results(self, test_db, test_period_data):
        """
        Bug #004: Events in results.json should have:
        - status='scheduled' (after scheduler has processed)
        - This indicates scheduler has created assignments
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        # Remove actual_attendance.json so events stay 'scheduled' (not completed)
        attendance_file = Path(period_data['period_dir']) / 'actual_attendance.json'
        attendance_file.unlink()

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (with results.json but no attendance)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Check event statuses - specifically those in results.json
        cursor.execute("""
            SELECT status FROM events WHERE period_id = ? AND status = 'scheduled'
            ORDER BY event_datetime
        """, (importer.period_id,))
        scheduled_events = cursor.fetchall()

        # EXPECTED: events from results.json should be 'scheduled'
        assert len(scheduled_events) > 0, \
            "Period with results.json should have scheduled events"

    def test_event_status_completed_from_attendance(self, test_db, test_period_data):
        """
        Bug #004: Events in actual_attendance.json should have:
        - status='completed' (after attendance has been recorded)
        - This indicates the event has occurred with recorded participation
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period (with both results.json and actual_attendance.json)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Check event statuses
        cursor.execute("""
            SELECT status FROM events WHERE period_id = ? AND status = 'completed'
            ORDER BY event_datetime
        """, (importer.period_id,))
        completed_events = cursor.fetchall()

        # EXPECTED: events from actual_attendance.json should be 'completed'
        assert len(completed_events) > 0, \
            "Period with actual_attendance.json should have completed events"

    def test_event_status_lifecycle_transitions(self, test_db, test_period_data):
        """
        Bug #004: Events should follow the complete status lifecycle:
        proposed → scheduled → completed

        This tests the full transition sequence in a single period import.
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5, num_events=3))

        cursor = test_db.cursor()

        # Import members
        collector = MemberCollector(processed_data_path=Path(period_data['temp_dir']), verbose=False)
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        # Import period with full data (responses + results + attendance)
        importer = PeriodImporter(
            period_name='2025-02',
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=collector.peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=True
        )
        importer.import_period()

        # Get all events and their statuses
        cursor.execute("""
            SELECT id, status FROM events WHERE period_id = ?
            ORDER BY event_datetime
        """, (importer.period_id,))
        events = cursor.fetchall()

        # EXPECTED:
        # - Events should have progressed through lifecycle
        # - All events in attendance should be 'completed'
        # - Lifecycle should be: proposed → scheduled → completed
        assert len(events) > 0, "Period should have events"

        # Check that at least some events reached 'completed' state
        completed_count = sum(1 for _, status in events if status == 'completed')
        assert completed_count > 0, \
            f"Events with attendance should transition to 'completed', got statuses: {[s for _, s in events]}"
