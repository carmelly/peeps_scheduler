"""Tests for partnership requests import (partnerships.json).

Tests cover:
- Partnership data loading and storage
- Format variations (wrapped and unwrapped)
- Validation of member IDs
- Backward compatibility with missing files
- Strict validation of malformed data
"""

import json
from pathlib import Path
import pytest
from peeps_scheduler.db.import_period_data import import_partnerships
from tests.db.helpers import assert_partnership_count


@pytest.mark.db
class TestPartnershipRequests:
    """Tests for importing partnership requests from partnerships.json."""

    def test_import_partnerships_and_store_in_database(self, importer_factory, partnerships_json_builder):
        """PeriodImporter loads partnerships.json and stores in partnership_requests table."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses (to ensure members exist)
        ctx.importer.import_responses()

        # Create partnerships.json with test data
        partnerships_data = {"1": [2], "2": [1], "3": [4, 5]}
        partnerships_json_builder(period_dir, partnerships_data)

        # Import partnerships
        partnerships_path = period_dir / "partnerships.json"
        import_partnerships(partnerships_path, ctx.period_id, ctx.cursor)

        # Verify partnerships are stored
        assert_partnership_count(ctx.cursor, 4, period_id=ctx.period_id)

        # Multi-row query for partnership verification
        ctx.cursor.execute(
            """
            SELECT requester_peep_id, partner_peep_id FROM partnership_requests
            WHERE period_id = ?
            ORDER BY requester_peep_id, partner_peep_id
        """,
            (ctx.period_id,),
        )
        results = ctx.cursor.fetchall()

        # Verify specific partnerships
        assert (1, 2) in results, "Should have partnership request from 1 to 2"
        assert (2, 1) in results, "Should have partnership request from 2 to 1"
        assert (3, 4) in results, "Should have partnership request from 3 to 4"
        assert (3, 5) in results, "Should have partnership request from 3 to 5"

    def test_partnerships_wrapped_format_support(self, importer_factory):
        """PeriodImporter handles partnerships.json with wrapped 'partnerships' key."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses
        ctx.importer.import_responses()

        # Create partnerships.json with wrapped format
        partnerships_data = {"partnerships": {"1": [2], "2": [1]}}
        partnerships_path = period_dir / "partnerships.json"
        with open(partnerships_path, "w") as f:
            json.dump(partnerships_data, f)

        # Import partnerships
        import_partnerships(partnerships_path, ctx.period_id, ctx.cursor)

        # Verify partnerships are stored
        assert_partnership_count(ctx.cursor, 2, period_id=ctx.period_id)

    def test_partnerships_backward_compatible_missing_file(self, importer_factory):
        """PeriodImporter handles missing partnerships.json gracefully."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Remove partnerships.json to test backward compatibility
        (period_dir / "partnerships.json").unlink(missing_ok=True)

        # Import period - should handle missing file gracefully
        ctx.importer.import_period()

        # Verify no partnerships were created
        assert_partnership_count(ctx.cursor, 0, period_id=ctx.importer.period_id)

    @pytest.mark.parametrize("partnerships_data,id_type", [
        pytest.param({"1": [999]}, "partner", id="invalid_partner"),
        pytest.param({"999": [1]}, "requester", id="invalid_requester"),
    ])
    def test_partnerships_invalid_member_id_raises(
        self, importer_factory, partnerships_data, id_type
    ):
        """PeriodImporter raises ValueError when member ID doesn't exist.

        Tests both invalid partner IDs and invalid requester IDs.
        """
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses
        ctx.importer.import_responses()

        # Create partnerships.json with invalid member ID (999)
        partnerships_path = period_dir / "partnerships.json"
        with open(partnerships_path, "w") as f:
            json.dump(partnerships_data, f)

        # Import should raise ValueError
        with pytest.raises(
            ValueError, match="(?s).*(partner|requester).*999.*not found.*peeps"
        ):
            import_partnerships(partnerships_path, ctx.period_id, ctx.cursor)

    def test_partnerships_strict_validation_malformed_data(self, importer_factory):
        """PeriodImporter raises ValueError for malformed partnership data."""
        ctx = importer_factory()
        period_dir = Path(ctx.period_data["period_dir"])

        # Import responses
        ctx.importer.import_responses()

        # Create partnerships.json with invalid structure (value should be array)
        partnerships_data = {"1": "not_an_array"}  # Should be list
        partnerships_path = period_dir / "partnerships.json"
        with open(partnerships_path, "w") as f:
            json.dump(partnerships_data, f)

        # Import should raise ValueError
        with pytest.raises(ValueError, match="must be a list"):
            import_partnerships(partnerships_path, ctx.period_id, ctx.cursor)

    def test_partnerships_with_csv_id_mapping(self, importer_factory, partnerships_json_builder):
        """PeriodImporter correctly maps CSV IDs to database peep IDs using peep_id_mapping."""
        ctx = importer_factory(create_period=False)
        period_dir = Path(ctx.period_data["period_dir"])

        # Import period to create structure
        ctx.importer.import_period()

        # Create partnerships using CSV IDs (1, 2, 3 from test data)
        partnerships_json_builder(
            period_dir,
            {
                "1": [2, 3],     # CSV ID 1 partners with CSV IDs 2 and 3
                "2": [1]         # CSV ID 2 partners with CSV ID 1
            }
        )

        # Import partnerships with CSV ID mapping
        partnerships_path = period_dir / "partnerships.json"
        count = import_partnerships(
            partnerships_path,
            ctx.importer.period_id,
            ctx.cursor,
            peep_id_mapping=ctx.importer.peep_id_mapping
        )

        # Should have imported 3 partnerships (1→2, 1→3, 2→1)
        assert count == 3, f"Should import 3 partnerships, got {count}"
        assert_partnership_count(ctx.cursor, expected=3, period_id=ctx.importer.period_id)
