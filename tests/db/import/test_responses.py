"""Tests for response import functionality (PeriodImporter.import_responses).

Tests cover:
- Response parsing and validation
- Email validation against member list
- Handling of missing/invalid fields
- Switch preference parsing
- Error handling for malformed data
"""

import csv
from pathlib import Path
import pytest
from tests.db.helpers import assert_response_count, get_single_value
from tests.fixtures.data_specs import ResponseSpec


@pytest.mark.db
class TestResponseImport:
    """Tests for response import and validation."""

    def test_imports_all_responses(self, importer_factory):
        """PeriodImporter imports all responses from responses.csv."""
        ctx = importer_factory()

        response_mapping = ctx.importer.import_responses()

        assert len(response_mapping) == 8, "Should import all responses from responses.csv"
        assert_response_count(ctx.cursor, ctx.period_id, expected=8)

    def test_handles_missing_responses_csv_gracefully(self, importer_factory):
        """PeriodImporter skips responses when responses.csv is missing."""
        ctx = importer_factory()

        responses_file = Path(ctx.period_data['period_dir']) / 'responses.csv'
        responses_file.unlink()

        ctx.importer.import_responses()

        assert_response_count(ctx.cursor, ctx.period_id, expected=0)

    def test_raises_error_when_response_email_not_in_peeps(self, importer_factory, responses_csv_builder):
        """PeriodImporter raises ValueError when response email not in peeps table."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # Create response with email not in peeps table
        responses = [ResponseSpec(
            email="unknown@example.com",
            name="Unknown Person",
            role="leader",
            availability=["Friday February 7th - 5pm to 7pm"]
        )]
        responses_csv_builder(period_dir, responses)

        with pytest.raises(ValueError, match=r"Email mismatch.*does not match any member"):
            ctx.importer.import_responses()

    @pytest.mark.parametrize("field_name,invalid_value", [
        ("Max Sessions", "not-a-number"),
        ("Min Interval Days", "invalid"),
    ])
    @pytest.mark.revisit("Issue #031: Change invalid numeric fields to raise error")
    def test_handles_invalid_numeric_field_gracefully(
        self, importer_factory, field_name, invalid_value
    ):
        """PeriodImporter defaults invalid numeric fields to 0.

        REVISIT: This test documents current behavior (graceful defaulting).
        See issue #031 for planned behavior change: invalid numeric values should raise error.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # Invalid numeric field - currently defaults to 0
        # Future behavior: should raise error instead
        responses_path = period_dir / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role',
                'Secondary Role', 'Max Sessions', 'Availability',
                'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ])
            writer.writeheader()
            row = {
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'leader',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Availability': 'Friday February 7th - 5pm to 7pm',
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Partnership Preference': '',
                'Questions or Comments': ''
            }
            row[field_name] = invalid_value
            writer.writerow(row)

        response_mapping = ctx.importer.import_responses()
        assert len(response_mapping) == 1, f"Should import response despite invalid {field_name}"

    @pytest.mark.parametrize("missing_field", [
        pytest.param("Email Address", id="missing_email"),
        pytest.param("Name", id="missing_name"),
    ])
    def test_raises_error_when_response_missing_required_field(
        self, importer_factory, missing_field
    ):
        """PeriodImporter raises ValueError when response missing required field.

        Tests both email and name required field validation.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # Manual CSV - create response with one required field empty
        responses_path = period_dir / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role',
                'Secondary Role', 'Max Sessions', 'Availability',
                'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ])
            writer.writeheader()
            writer.writerow({
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': '' if missing_field == 'Email Address' else 'test@example.com',
                'Name': '' if missing_field == 'Name' else 'Test Member',
                'Primary Role': 'leader',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Availability': 'Friday February 7th - 5pm to 7pm',
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Partnership Preference': '',
                'Questions or Comments': ''
            })

        with pytest.raises(ValueError, match="(?s)Invalid response.*Missing required field"):
            ctx.importer.import_responses()

    @pytest.mark.parametrize("secondary_role,expected_switch_pref", [
        pytest.param(
            "I only want to be scheduled in my primary role", 1,
            id="primary_only"
        ),
        pytest.param(
            "I'm happy to dance my secondary role if it lets me attend when my primary is full", 2,
            id="switch_if_primary_full"
        ),
        pytest.param(
            "I'm willing to dance my secondary role only if it's needed to enable filling a session", 3,
            id="switch_if_needed"
        ),
    ])
    def test_parses_switch_preference_correctly(
        self, importer_factory, responses_csv_builder, secondary_role, expected_switch_pref
    ):
        """PeriodImporter correctly parses Secondary Role field to switch preference.

        Tests all three switch preference values mapping from Secondary Role field.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # Create response with specific secondary role to test switch preference
        responses = [ResponseSpec(
            email="member1@test.com",
            name="Test Member 1",
            role="leader",
            secondary_role=secondary_role,
            availability=["Friday February 7th - 5pm to 7pm"]
        )]
        responses_csv_builder(period_dir, responses)

        ctx.importer.import_responses()

        switch_pref = get_single_value(ctx.cursor, 'responses', 'switch_preference', f'period_id = {ctx.period_id}')
        assert switch_pref is not None, "Should have imported response"

        assert switch_pref == expected_switch_pref, \
            f"Secondary role '{secondary_role}' should map to {expected_switch_pref}, got {switch_pref}"

    @pytest.mark.revisit("Issue #031: Change invalid timestamp to raise error")
    def test_handles_invalid_timestamp_gracefully(self, importer_factory):
        """PeriodImporter logs warning for invalid timestamp but continues.

        REVISIT: This test documents current behavior (graceful handling).
        See issue #031 for planned behavior change: invalid timestamps should raise error.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data['period_dir'])

        # Invalid timestamp - currently logs warning but doesn't crash
        # Future behavior: should raise error instead
        responses_path = period_dir / 'responses.csv'
        with open(responses_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'Email Address', 'Name', 'Primary Role',
                'Secondary Role', 'Max Sessions', 'Availability',
                'Event Duration', 'Session Spacing Preference',
                'Min Interval Days', 'Partnership Preference', 'Questions or Comments'
            ])
            writer.writeheader()
            writer.writerow({
                'Timestamp': 'not-a-valid-timestamp',
                'Email Address': 'member1@test.com',
                'Name': 'Test Member 1',
                'Primary Role': 'leader',
                'Secondary Role': 'I only want to be scheduled in my primary role',
                'Max Sessions': '2',
                'Min Interval Days': '7',
                'Availability': 'Friday February 7th - 5pm to 7pm',
                'Event Duration': '',
                'Session Spacing Preference': '',
                'Partnership Preference': '',
                'Questions or Comments': ''
            })

        response_mapping = ctx.importer.import_responses()
        assert len(response_mapping) == 1, "Should still import response despite bad timestamp"
