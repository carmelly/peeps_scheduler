"""Database query and assertion helpers for test suite.

These helpers reduce boilerplate in database tests by providing reusable
query patterns and assertion functions.
"""

import sqlite3
from typing import Any

# ============================================================================
# Query Helpers
# ============================================================================

def get_table_count(cursor: sqlite3.Cursor, table: str, where: str | None = None) -> int:
    """Get row count from table with optional WHERE clause.

    Args:
        cursor: Database cursor
        table: Table name
        where: Optional WHERE clause (without "WHERE" keyword)

    Returns:
        Number of rows matching criteria

    Example:
        count = get_table_count(cursor, 'peeps')
        count = get_table_count(cursor, 'events', 'period_id = 1')
    """
    query = f"SELECT COUNT(*) FROM {table}"
    if where:
        query += f" WHERE {where}"

    cursor.execute(query)
    return cursor.fetchone()[0]


def get_period_id(cursor: sqlite3.Cursor, period_name: str) -> int | None:
    """Lookup period ID by name.

    Args:
        cursor: Database cursor
        period_name: Period name (e.g., "2025-02")

    Returns:
        Period ID if found, None otherwise
    """
    cursor.execute(
        "SELECT id FROM schedule_periods WHERE period_name = ?",
        (period_name,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_event_by_datetime(
    cursor: sqlite3.Cursor,
    period_id: int,
    datetime_str: str
) -> int | None:
    """Lookup event ID by period and datetime.

    Args:
        cursor: Database cursor
        period_id: Period ID
        datetime_str: Event datetime in format "YYYY-MM-DD HH:MM"

    Returns:
        Event ID if found, None otherwise
    """
    cursor.execute(
        "SELECT id FROM events WHERE period_id = ? AND event_datetime = ?",
        (period_id, datetime_str)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_peep_by_email(cursor: sqlite3.Cursor, email: str) -> int | None:
    """Lookup peep ID by email.

    Args:
        cursor: Database cursor
        email: Email address

    Returns:
        Peep ID if found, None otherwise
    """
    cursor.execute("SELECT id FROM peeps WHERE email = ?", (email,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_single_value(
    cursor: sqlite3.Cursor,
    table: str,
    column: str,
    where: str
) -> Any | None:
    """Get single column value from table.

    Args:
        cursor: Database cursor
        table: Table name
        column: Column to select
        where: WHERE clause (without "WHERE" keyword)

    Returns:
        Column value if found, None otherwise

    Example:
        status = get_single_value(cursor, 'schedule_periods', 'status', 'id = 1')
    """
    cursor.execute(f"SELECT {column} FROM {table} WHERE {where}")
    row = cursor.fetchone()
    return row[0] if row else None


def get_attendance_participation_mode(cursor: sqlite3.Cursor, event_id: int, peep_id: int) -> str | None:
    """Get participation_mode for a specific attendance record.

    Args:
        cursor: Database cursor
        event_id: Event ID
        peep_id: Peep ID

    Returns:
        Participation mode if found, None otherwise

    Example:
        mode = get_attendance_participation_mode(cursor, 1, 5)
    """
    cursor.execute(
        "SELECT participation_mode FROM event_attendance WHERE event_id = ? AND peep_id = ?",
        (event_id, peep_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_snapshot_priority(cursor: sqlite3.Cursor, period_id: int, peep_id: int) -> int | None:
    """Get priority value from snapshot for a specific member in a period.

    Args:
        cursor: Database cursor
        period_id: Period ID
        peep_id: Peep ID

    Returns:
        Priority value if found, None otherwise

    Example:
        priority = get_snapshot_priority(cursor, 1, 5)
    """
    cursor.execute(
        "SELECT priority FROM peep_order_snapshots WHERE period_id = ? AND peep_id = ?",
        (period_id, peep_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_snapshot_index(cursor: sqlite3.Cursor, period_id: int, peep_id: int) -> int | None:
    """Get index_position from snapshot for a specific member in a period.

    Args:
        cursor: Database cursor
        period_id: Period ID
        peep_id: Peep ID

    Returns:
        Index position if found, None otherwise

    Example:
        index = get_snapshot_index(cursor, 1, 5)
    """
    cursor.execute(
        "SELECT index_position FROM peep_order_snapshots WHERE period_id = ? AND peep_id = ?",
        (period_id, peep_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_event_id_by_datetime(cursor: sqlite3.Cursor, period_id: int, datetime_prefix: str) -> int | None:
    """Get event ID by datetime prefix (e.g., '2025-02-07').

    Args:
        cursor: Database cursor
        period_id: Period ID
        datetime_prefix: Event datetime prefix for pattern matching

    Returns:
        Event ID if found, None otherwise

    Example:
        event_id = get_event_id_by_datetime(cursor, 1, '2025-02-07')
    """
    cursor.execute(
        "SELECT id FROM events WHERE period_id = ? AND event_datetime LIKE ?",
        (period_id, f"{datetime_prefix}%")
    )
    row = cursor.fetchone()
    return row[0] if row else None


def get_assignment_count_by_type(cursor: sqlite3.Cursor, event_id: int, assignment_type: str) -> int:
    """Get count of assignments for an event by type (attendee/alternate).

    Args:
        cursor: Database cursor
        event_id: Event ID
        assignment_type: Assignment type ('assigned' or 'alternate')

    Returns:
        Count of assignments

    Example:
        count = get_assignment_count_by_type(cursor, 1, 'assigned')
    """
    cursor.execute(
        "SELECT COUNT(*) FROM event_assignments WHERE event_id = ? AND assignment_type = ?",
        (event_id, assignment_type)
    )
    return cursor.fetchone()[0]


# ============================================================================
# Assertion Helpers
# ============================================================================

def assert_row_count(
    cursor: sqlite3.Cursor,
    table: str,
    expected: int,
    where: str | None = None,
    msg: str | None = None
) -> None:
    """Assert table row count matches expected value.

    Args:
        cursor: Database cursor
        table: Table name
        expected: Expected row count
        where: Optional WHERE clause
        msg: Optional custom assertion message

    Example:
        assert_row_count(cursor, 'peeps', 10)
        assert_row_count(cursor, 'events', 3, 'period_id = 1')
    """
    actual = get_table_count(cursor, table, where)
    if msg is None:
        where_clause = f" WHERE {where}" if where else ""
        msg = f"Expected {expected} rows in {table}{where_clause}, found {actual}"
    assert actual == expected, msg


def assert_period_status(
    cursor: sqlite3.Cursor,
    period_id: int,
    expected_status: str
) -> None:
    """Assert period has expected status.

    Args:
        cursor: Database cursor
        period_id: Period ID
        expected_status: Expected status value

    Example:
        assert_period_status(cursor, 1, 'imported')
    """
    cursor.execute(
        "SELECT status FROM schedule_periods WHERE id = ?",
        (period_id,)
    )
    row = cursor.fetchone()
    assert row is not None, f"Period {period_id} not found"
    actual_status = row[0]
    assert actual_status == expected_status, \
        f"Expected period {period_id} status '{expected_status}', found '{actual_status}'"


def assert_event_status(
    cursor: sqlite3.Cursor,
    event_id: int,
    expected_status: str
) -> None:
    """Assert event has expected status.

    Args:
        cursor: Database cursor
        event_id: Event ID
        expected_status: Expected status value

    Example:
        assert_event_status(cursor, 1, 'scheduled')
    """
    cursor.execute(
        "SELECT status FROM events WHERE id = ?",
        (event_id,)
    )
    row = cursor.fetchone()
    assert row is not None, f"Event {event_id} not found"
    actual_status = row[0]
    assert actual_status == expected_status, \
        f"Expected event {event_id} status '{expected_status}', found '{actual_status}'"


def assert_event_count(
    cursor: sqlite3.Cursor,
    period_id: int,
    expected: int,
    status: str | None = None
) -> None:
    """Assert number of events in period.

    Args:
        cursor: Database cursor
        period_id: Period ID
        expected: Expected event count
        status: Optional status filter ('scheduled', 'cancelled', etc.)

    Example:
        assert_event_count(cursor, 1, 3)
        assert_event_count(cursor, 1, 0, status='cancelled')
    """
    where = f'period_id = {period_id}'
    if status:
        where += f" AND status = '{status}'"

    assert_row_count(
        cursor,
        'events',
        expected,
        where=where,
        msg=f"Expected {expected} events in period {period_id}" + (f" with status '{status}'" if status else "")
    )


def assert_assignment_count(
    cursor: sqlite3.Cursor,
    event_id: int,
    expected: int,
    assignment_type: str | None = None
) -> None:
    """Assert number of assignments for event.

    Args:
        cursor: Database cursor
        event_id: Event ID
        expected: Expected assignment count
        assignment_type: Optional assignment type filter ('assigned' or 'alternate')

    Example:
        assert_assignment_count(cursor, 1, 4)
        assert_assignment_count(cursor, 1, 2, assignment_type='assigned')
    """
    where = f'event_id = {event_id}'
    if assignment_type:
        where += f" AND assignment_type = '{assignment_type}'"

    assert_row_count(
        cursor,
        'event_assignments',
        expected,
        where=where,
        msg=f"Expected {expected} assignments for event {event_id}"
    )


def assert_peep_assignment_count(
    cursor: sqlite3.Cursor,
    peep_id: int,
    expected: int
) -> None:
    """Assert number of assignments for a specific member.

    Args:
        cursor: Database cursor
        peep_id: Peep ID
        expected: Expected assignment count

    Example:
        assert_peep_assignment_count(cursor, 1, 3)
    """
    assert_row_count(
        cursor,
        'event_assignments',
        expected,
        where=f'peep_id = {peep_id}',
        msg=f"Expected {expected} assignments for peep {peep_id}"
    )


def assert_attendance_count(
    cursor: sqlite3.Cursor,
    event_id: int,
    expected: int
) -> None:
    """Assert number of attendance records for event.

    Args:
        cursor: Database cursor
        event_id: Event ID
        expected: Expected attendance count

    Example:
        assert_attendance_count(cursor, 1, 2)
    """
    assert_row_count(
        cursor,
        'event_attendance',
        expected,
        where=f'event_id = {event_id}',
        msg=f"Expected {expected} attendance records for event {event_id}"
    )


def assert_snapshot_count(
    cursor: sqlite3.Cursor,
    period_id: int,
    expected: int
) -> None:
    """Assert number of snapshots for period.

    Args:
        cursor: Database cursor
        period_id: Period ID
        expected: Expected snapshot count

    Example:
        assert_snapshot_count(cursor, 1, 3)
    """
    assert_row_count(
        cursor,
        'peep_order_snapshots',
        expected,
        where=f'period_id = {period_id}',
        msg=f"Expected {expected} snapshots for period {period_id}"
    )


def assert_response_count(
    cursor: sqlite3.Cursor,
    period_id: int,
    expected: int
) -> None:
    """Assert number of responses for period.

    Args:
        cursor: Database cursor
        period_id: Period ID
        expected: Expected response count

    Example:
        assert_response_count(cursor, 1, 8)
    """
    assert_row_count(
        cursor,
        'responses',
        expected,
        where=f'period_id = {period_id}',
        msg=f"Expected {expected} responses for period {period_id}"
    )


def assert_partnership_count(
    cursor: sqlite3.Cursor,
    expected: int,
    period_id: int | None = None
) -> None:
    """Assert number of partnership records.

    Args:
        cursor: Database cursor
        expected: Expected partnership count
        period_id: Optional period ID filter

    Example:
        assert_partnership_count(cursor, 4)
        assert_partnership_count(cursor, 2, period_id=1)
    """
    where = None
    if period_id:
        where = f'period_id = {period_id}'

    assert_row_count(
        cursor,
        'partnership_requests',
        expected,
        where=where,
        msg=f"Expected {expected} partnership records" + (f" for period {period_id}" if period_id else "")
    )
