"""Tests for member collection and database insertion (MemberCollector).

Tests cover:
- Member scanning from CSV files
- Peep ID mapping creation
- Database insertion
- Error handling for malformed/missing data
"""

import csv
from pathlib import Path
import pytest
from db.import_period_data import MemberCollector
from tests.db.helpers import assert_row_count
from tests.fixtures.data_specs import MemberSpec


@pytest.mark.db
class TestMemberCollection:
    """Tests for MemberCollector - scanning members and creating peep_id_mapping."""

    def test_scans_and_inserts_all_members(self, test_db, test_period_data, members_csv_builder):
        """MemberCollector scans all members and inserts them to database."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))
        period_dir = Path(period_data['period_dir'])
        cursor = test_db.cursor()

        members = [MemberSpec(csv_id=i, name=f"Test Member {i}") for i in range(1, 11)]
        members_csv_builder(period_dir, members)

        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        assert len(collector.peep_id_mapping) == 10, "peep_id_mapping should track all 10 members"
        assert_row_count(cursor, 'peeps', expected=10)

    @pytest.mark.revisit("Issue #030: Change invalid date handling to raise error")
    def test_handles_invalid_date_joined_gracefully(self, test_period_data, members_csv_builder):
        """MemberCollector logs warning for invalid date format but continues.

        REVISIT: This test documents current behavior (graceful handling).
        See issue #030 for planned behavior change: invalid dates should raise error.
        When implemented, rename to test_raises_error_for_invalid_date_joined.
        """
        period_data = next(test_period_data(period_name='2025-02', num_members=5))
        period_dir = Path(period_data['period_dir'])

        # Invalid date format currently logs warning but doesn't crash
        # Future behavior: should raise error instead
        members = [MemberSpec(
            csv_id=1,
            name="Member One",
            email="member1@test.com",
            date_joined="not-a-valid-date"
        )]
        members_csv_builder(period_dir, members)

        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )
        collector.scan_all_periods()

        assert len(collector.members) == 1

    def test_raises_error_when_member_missing_id(self, test_period_data):
        """MemberCollector raises ValueError when member has missing ID."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))
        members_path = Path(period_data['period_dir']) / 'members.csv'

        # Manual CSV write - builder can't create empty ID (required field)
        with open(members_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'id', 'Name', 'Display Name', 'Email Address', 'Role',
                'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'
            ])
            writer.writeheader()
            writer.writerow({
                'id': '',
                'Name': 'Test Member',
                'Display Name': 'TM',
                'Email Address': 'test@example.com',
                'Role': 'leader',
                'Index': '0',
                'Priority': '1',
                'Total Attended': '0',
                'Active': 'TRUE',
                'Date Joined': '2025-01-01'
            })

        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )

        with pytest.raises(ValueError, match="(?s)Invalid member data.*missing required.*id"):
            collector.scan_all_periods()

    def test_raises_error_when_members_csv_missing(self, test_period_data):
        """MemberCollector raises FileNotFoundError when members.csv is missing."""
        period_data = next(test_period_data(period_name='2025-02', num_members=10))
        members_file = Path(period_data['period_dir']) / 'members.csv'
        members_file.unlink()

        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )

        with pytest.raises(FileNotFoundError, match="Required file not found.*members.csv"):
            collector.scan_all_periods()

    def test_raises_error_when_multiple_members_missing_email(
        self, test_period_data, members_csv_builder
    ):
        """MemberCollector raises ValueError listing all members with missing emails."""
        period_data = next(test_period_data(period_name='2025-02', num_members=5))
        period_dir = Path(period_data['period_dir'])

        # Create members with empty emails (allowed by builder, rejected by collector)
        members = [
            MemberSpec(csv_id=1, name="Member One", email="", role="leader"),
            MemberSpec(csv_id=2, name="Member Two", email="", role="follower")
        ]
        members_csv_builder(period_dir, members)

        collector = MemberCollector(
            processed_data_path=Path(period_data['temp_dir']),
            verbose=False
        )

        with pytest.raises(ValueError, match=r"2 members are missing required email addresses"):
            collector.scan_all_periods()

    def test_updates_member_data_across_multiple_periods(
        self, test_db, test_period_data, members_csv_builder
    ):
        """MemberCollector updates member data when same member appears in multiple periods."""
        period1_data = next(test_period_data(period_name='2025-02', num_members=5))
        period2_data = next(test_period_data(period_name='2025-03', num_members=5))
        cursor = test_db.cursor()

        members_period1 = [
            MemberSpec(csv_id=1, name="Alice Original", email="alice@test.com", role="leader"),
            MemberSpec(csv_id=2, name="Bob", email="bob@test.com", role="follower")
        ]
        members_csv_builder(Path(period1_data['period_dir']), members_period1)

        members_period2 = [
            MemberSpec(csv_id=1, name="Alice Updated", email="alice@test.com", role="follower"),
            MemberSpec(csv_id=3, name="Carol", email="carol@test.com", role="leader")
        ]
        members_csv_builder(Path(period2_data['period_dir']), members_period2)

        collector = MemberCollector(
            processed_data_path=Path(period1_data['temp_dir']),
            verbose=False
        )
        collector.scan_all_periods()
        collector.insert_members_to_db(cursor)

        assert len(collector.peep_id_mapping) == 3
        assert_row_count(cursor, 'peeps', expected=3)

        from tests.db.helpers import get_single_value
        full_name = get_single_value(cursor, 'peeps', 'full_name', "email = 'alice@test.com'")
        primary_role = get_single_value(cursor, 'peeps', 'primary_role', "email = 'alice@test.com'")
        assert full_name == "Alice Updated"
        assert primary_role == "follower"
