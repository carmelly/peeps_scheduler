"""Import-specific test fixtures.

This conftest provides fixtures for database import testing, building on the
shared fixtures from tests/conftest.py and tests/fixtures/.
"""

import sqlite3
from pathlib import Path
from typing import NamedTuple
import pytest
from peeps_scheduler.db.import_period_data import MemberCollector, PeriodImporter


class ImporterContext(NamedTuple):
    """Return type for importer_factory.

    Attributes:
        importer: PeriodImporter instance
        cursor: Database cursor for assertions
        period_data: Test period data dict (temp_dir, period_dir, etc.)
        peep_id_mapping: Mapping of email to peep ID
        period_id: ID of created period (None if create_period=False)
    """
    importer: PeriodImporter
    cursor: sqlite3.Cursor
    period_data: dict
    peep_id_mapping: dict
    period_id: int | None


@pytest.fixture
def importer_factory(test_db, test_period_data):
    """Factory for creating PeriodImporter instances with flexible configuration.

    Args:
        period_name: Period name (default: '2025-02')
        num_members: Number of test members (default: 10)
        create_period: Whether to create the period (default: True)
        skip_snapshots: Whether to skip snapshot generation (default: True)
        peep_id_mapping: Existing peep_id_mapping to reuse (default: None, creates new)

    Returns:
        ImporterContext with: importer, cursor, period_data, peep_id_mapping, period_id

    Examples:
        ctx = importer_factory()  # defaults
        ctx = importer_factory(create_period=False)  # for import_period() tests
        ctx = importer_factory(skip_snapshots=False)  # for snapshot tests
        ctx = importer_factory(period_name='2025-03')  # different period
        ctx2 = importer_factory(period_name='2025-03', peep_id_mapping=ctx1.peep_id_mapping)  # multi-period
    """
    def _create(period_name='2025-02', num_members=10, create_period=True, skip_snapshots=True, peep_id_mapping=None):
        period_data = next(test_period_data(period_name=period_name, num_members=num_members))
        cursor = test_db.cursor()

        # If peep_id_mapping provided, reuse it (for multi-period tests)
        # Otherwise, collect members from scratch
        if peep_id_mapping is None:
            collector = MemberCollector(
                processed_data_path=Path(period_data['temp_dir']),
                verbose=False
            )
            collector.scan_all_periods()
            collector.insert_members_to_db(cursor)
            peep_id_mapping = collector.peep_id_mapping

        importer = PeriodImporter(
            period_name=period_name,
            processed_data_path=Path(period_data['temp_dir']),
            peep_id_mapping=peep_id_mapping,
            cursor=cursor,
            verbose=False,
            skip_snapshots=skip_snapshots
        )

        period_id = None
        if create_period:
            period_id = importer.create_schedule_period()

        return ImporterContext(
            importer=importer,
            cursor=cursor,
            period_data=period_data,
            peep_id_mapping=peep_id_mapping,
            period_id=period_id
        )

    return _create


@pytest.fixture
def imported_period(importer_factory):
    """Fully imported period with events and attendance.

    Returns:
        ImporterContext with fully imported period

    Example:
        def test_full_period(imported_period):
            cursor = imported_period.cursor
            period_id = imported_period.period_id
            # Test against fully imported period
    """
    ctx = importer_factory()
    ctx.importer.import_events()
    ctx.importer.import_attendance()
    return ctx
