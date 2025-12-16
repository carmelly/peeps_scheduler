import pytest
import datetime
import sqlite3
import tempfile
import os
from pathlib import Path
from models import Peep, Event, Role, SwitchPreference


@pytest.fixture
def test_db():
    """Create an isolated test database with schema applied."""
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')

    # Load schema file
    schema_path = Path(__file__).parent.parent / 'db' / 'schema.sql'
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Split into individual statements
    statements = []
    current_statement = []
    for line in schema_sql.split('\n'):
        if 'sqlite_sequence' in line.lower():
            continue
        current_statement.append(line)
        if line.strip().endswith(');'):
            statements.append('\n'.join(current_statement))
            current_statement = []

    # Separate CREATE TABLE from CREATE INDEX statements
    table_statements = [s for s in statements if 'CREATE TABLE' in s]
    index_statements = [s for s in statements if 'CREATE INDEX' in s]
    other_statements = [s for s in statements if 'CREATE TABLE' not in s and 'CREATE INDEX' not in s]

    conn = sqlite3.connect(db_path)

    # Execute in correct order: tables first, then indexes
    for stmt in table_statements + other_statements:
        if stmt.strip():
            conn.execute(stmt)

    for stmt in index_statements:
        if stmt.strip():
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError:
                # Skip indexes that reference non-existent tables
                pass

    conn.commit()

    yield conn

    # Cleanup
    conn.close()
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def test_db_path():
    """Create an isolated test database and return its path."""
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')

    # Apply schema (filter out sqlite_sequence which is auto-created)
    schema_path = Path(__file__).parent.parent / 'db' / 'schema.sql'
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Remove sqlite_sequence table creation (reserved for internal use)
    lines = schema_sql.split('\n')
    filtered_lines = [line for line in lines if 'sqlite_sequence' not in line.lower()]

    # Separate CREATE INDEX from CREATE TABLE statements
    # CREATE INDEX must come AFTER CREATE TABLE
    index_statements = []
    other_statements = []
    current_statement = []

    for line in filtered_lines:
        current_statement.append(line)
        # SQL statements end with semicolon
        if line.strip().endswith(';'):
            statement = '\n'.join(current_statement)
            if statement.strip().upper().startswith('CREATE INDEX'):
                index_statements.append(statement)
            else:
                other_statements.append(statement)
            current_statement = []

    # Reconstruct schema with CREATE TABLE first, then CREATE INDEX
    schema_sql = '\n'.join(other_statements + index_statements)

    conn = sqlite3.connect(db_path)
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def peep_factory():
    """Factory for creating test peeps with sensible defaults."""
    def _create(id=1, role=Role.LEADER, **kwargs):
        defaults = {
            'full_name': f'TestPeep{id}',
            'display_name': f'TestPeep{id}',
            'email': f'peep{id}@test.com',
            'availability': [1],
            'event_limit': 2,
            'priority': 0,
            'responded': True,
            'switch_pref': SwitchPreference.PRIMARY_ONLY,
            'index': 0,
            'total_attended': 0,
            'min_interval_days': 0,
            'active': True,
            'date_joined': '2025-01-01'
        }
        defaults.update(kwargs)
        return Peep(id=id, role=role, **defaults)
    return _create


@pytest.fixture
def event_factory():
    """Factory for creating test events with sensible defaults."""
    def _create(id=1, duration_minutes=120, **kwargs):
        defaults = {
            'date': datetime.datetime(2025, 1, 15, 18, 0)
        }
        defaults.update(kwargs)
        return Event(id=id, duration_minutes=duration_minutes, **defaults)
    return _create


@pytest.fixture
def test_period_data():
    """Generate complete test period data for import testing."""
    import json
    import csv
    import tempfile
    import shutil

    def _create(period_name='2025-02', num_members=10, num_events=3):
        """Create test CSV/JSON files for a period in a temporary directory."""
        # Create temporary directory for period data
        temp_dir = tempfile.mkdtemp()
        period_dir = Path(temp_dir) / period_name
        period_dir.mkdir(parents=True)

        # Generate test members
        members = []
        for i in range(1, num_members + 1):
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

        # Write members.csv
        with open(period_dir / 'members.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'])
            writer.writeheader()
            writer.writerows(members)

        # Generate test responses with date strings for auto-derive event mode
        responses = []
        event_dates = [
            'Friday February 7th - 5pm to 7pm',
            'Friday February 14th - 5pm to 7pm',
            'Friday February 21st - 5pm to 7pm'
        ][:num_events]

        for i in range(1, min(num_members, 8) + 1):  # First 8 members respond
            # Use actual date strings with time ranges for auto-derive mode
            availability_str = ', '.join(event_dates)
            responses.append({
                'Timestamp': '2/1/2025 10:00:00',
                'Email Address': f'member{i}@test.com',
                'Name': f'Test Member {i}',
                'Primary Role': 'Leader' if i % 2 == 1 else 'Follower',
                'Max Sessions': 2,
                'Min Interval Days': 0,
                'Secondary Role': "I only want to be scheduled in my primary role",
                'Availability': availability_str
            })

        # Write responses.csv
        with open(period_dir / 'responses.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Timestamp', 'Email Address', 'Name', 'Primary Role', 'Max Sessions', 'Min Interval Days', 'Secondary Role', 'Availability'])
            writer.writeheader()
            writer.writerows(responses)

        # Note: Events will be auto-derived from availability strings by extract_events()

        # Generate test assignments (results.json)
        assignments = {}
        for event_idx in range(num_events):
            event_key = f'event_{event_idx + 1}'
            assignments[event_key] = {
                'attendees': [
                    {'id': 1, 'name': 'Member1', 'role': 'Leader'},
                    {'id': 2, 'name': 'Member2', 'role': 'Follower'}
                ],
                'alternates': [
                    {'id': 3, 'name': 'Member3', 'role': 'Leader'}
                ]
            }

        # Write results.json
        with open(period_dir / 'results.json', 'w') as f:
            json.dump(assignments, f, indent=2)

        # Generate test attendance (actual_attendance.json)
        attendance = {
            'events': []
        }
        base_date = datetime.datetime(2025, 2, 7, 17, 0)  # Feb 7, 5pm
        for event_idx in range(num_events):
            event_date = base_date + datetime.timedelta(days=7 * event_idx)
            attendance['events'].append({
                'date': event_date.strftime('%Y-%m-%d %H:%M'),
                'attendees': [
                    {'id': 1, 'name': 'Member1', 'role': 'Leader'},
                    {'id': 2, 'name': 'Member2', 'role': 'Follower'}
                ]
            })

        # Write actual_attendance.json
        with open(period_dir / 'actual_attendance.json', 'w') as f:
            json.dump(attendance, f, indent=2)

        # Write empty notes.json
        with open(period_dir / 'notes.json', 'w') as f:
            json.dump([], f, indent=2)

        yield {
            'temp_dir': temp_dir,
            'period_dir': period_dir,
            'period_name': period_name,
            'num_members': num_members,
            'num_events': num_events
        }

        # Cleanup
        shutil.rmtree(temp_dir)

    return _create
