"""
Comprehensive tests for schema validation in db/validate.py.

Each test creates a broken schema and verifies that validate_schema() detects the issue
with a clear, actionable error message.

Test Coverage Areas:
1. Foreign Key Constraints (6 required relationships)
2. Migration Table & Version (migration 011)
3. Critical Indexes (7 required indexes)
4. Column Data Types (critical fields)
5. Error Messages (clear and specific)
"""

import pytest
import sqlite3
from peeps_scheduler.db.validate import validate_schema

@pytest.fixture
def broken_schema_db():
    """
    Create an in-memory SQLite database with broken schema for testing.

    Returns a function that creates in-memory DBs with specific issues for testing.
    Caller can customize the schema to inject specific problems.

    Usage:
        conn = broken_schema_db(skip_fk_constraint='event_assignments_event_id')
    """
    def _create(
        skip_fk_constraints=None,
        skip_indexes=None,
        wrong_column_types=None,
        skip_migration_table=False,
        skip_migration_011=False
    ):
        """
        Create test database with intentional schema issues.

        Args:
            skip_fk_constraints: List of FK constraints to NOT create, e.g.
                ['event_assignments.event_id->events.id']
            skip_indexes: List of index names to NOT create, e.g.
                ['idx_event_assignments_peep_id']
            wrong_column_types: Dict of column type changes, e.g.
                {'events.id': 'TEXT'}  # Will create as TEXT instead of correct type
            skip_migration_table: If True, don't create __migrations_applied__ table
            skip_migration_011: If True, don't insert migration 011

        Returns:
            sqlite3.Connection: In-memory database connection
        """
        # Create in-memory database
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()

        # Create peeps table (base dependency)
        cursor.execute("""
            CREATE TABLE peeps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                primary_role TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                date_joined DATE,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                CHECK(primary_role IN ('leader', 'follower'))
            )
        """)

        # Create schedule_periods table (base dependency)
        cursor.execute("""
            CREATE TABLE schedule_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_name TEXT UNIQUE NOT NULL,
                display_name TEXT,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                status TEXT DEFAULT 'draft',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                CHECK(start_date <= end_date),
                CHECK(status IN ('draft', 'scheduled', 'completed'))
            )
        """)

        # Create events table
        if not (wrong_column_types and 'events.id' in wrong_column_types):
            cursor.execute("""
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_id INTEGER NOT NULL,
                    legacy_period_event_id INTEGER,
                    event_datetime DATETIME NOT NULL,
                    duration_minutes INTEGER NOT NULL,
                    status TEXT DEFAULT 'proposed',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
        # Create events with wrong id column type
            cursor.execute(f"""
                CREATE TABLE events (
                    id {wrong_column_types['events.id']},
                    period_id INTEGER NOT NULL,
                    legacy_period_event_id INTEGER,
                    event_datetime DATETIME NOT NULL,
                    duration_minutes INTEGER NOT NULL,
                    status TEXT DEFAULT 'proposed',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # Build event_assignments CREATE statement with conditional FKs
        ea_fks = []
        if not (skip_fk_constraints and 'event_assignments.event_id->events.id' in skip_fk_constraints):
            ea_fks.append("FOREIGN KEY (event_id) REFERENCES events(id)")
        if not (skip_fk_constraints and 'event_assignments.peep_id->peeps.id' in skip_fk_constraints):
            ea_fks.append("FOREIGN KEY (peep_id) REFERENCES peeps(id)")

        ea_fk_clause = ", " + ", ".join(ea_fks) if ea_fks else ""

        cursor.execute(f"""
            CREATE TABLE event_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                peep_id INTEGER NOT NULL,
                assigned_role TEXT NOT NULL,
                assignment_type TEXT NOT NULL,
                assignment_order INTEGER,
                alternate_position INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                {ea_fk_clause}
            )
        """)

        # Create responses table
        cursor.execute("""
            CREATE TABLE responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peep_id INTEGER NOT NULL,
                period_id INTEGER NOT NULL,
                response_role TEXT NOT NULL,
                switch_preference INTEGER DEFAULT 1,
                max_sessions INTEGER NOT NULL,
                min_interval_days INTEGER DEFAULT 0,
                partnership_preference TEXT,
                organizer_comments TEXT,
                instructor_comments TEXT,
                response_timestamp DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create event_availability table with optional FK constraints
        ea_fk = ""
        if not (skip_fk_constraints and 'event_availability.event_id->events.id' in skip_fk_constraints):
            ea_fk = ", FOREIGN KEY (event_id) REFERENCES events(id)"

        cursor.execute(f"""
            CREATE TABLE event_availability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                response_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL
                {ea_fk}
            )
        """)

        # Create peep_order_snapshots table with optional FK constraint
        pos_fk = ""
        if not (skip_fk_constraints and 'peep_order_snapshots.peep_id->peeps.id' in skip_fk_constraints):
            pos_fk = ", FOREIGN KEY (peep_id) REFERENCES peeps(id)"

        cursor.execute(f"""
            CREATE TABLE peep_order_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peep_id INTEGER NOT NULL,
                period_id INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                index_position INTEGER NOT NULL,
                total_attended INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                active BOOLEAN NOT NULL DEFAULT 1
                {pos_fk}
            )
        """)

        # Create partnership_requests table with optional FK constraints
        pr_fks = []
        if not (skip_fk_constraints and 'partnership_requests.period_id->schedule_periods.id' in skip_fk_constraints):
            pr_fks.append("FOREIGN KEY (period_id) REFERENCES schedule_periods(id)")
        if not (skip_fk_constraints and 'partnership_requests.requester_peep_id->peeps.id' in skip_fk_constraints):
            pr_fks.append("FOREIGN KEY (requester_peep_id) REFERENCES peeps(id)")
        if not (skip_fk_constraints and 'partnership_requests.partner_peep_id->peeps.id' in skip_fk_constraints):
            pr_fks.append("FOREIGN KEY (partner_peep_id) REFERENCES peeps(id)")

        pr_fk_clause = ", " + ", ".join(pr_fks) if pr_fks else ""

        cursor.execute(f"""
            CREATE TABLE partnership_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id INTEGER NOT NULL,
                requester_peep_id INTEGER NOT NULL,
                partner_peep_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                {pr_fk_clause}
            )
        """)

        # Create migration table (optional)
        if not skip_migration_table:
            cursor.execute("""
                CREATE TABLE __migrations_applied__ (
                    filename TEXT PRIMARY KEY,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # Insert migration 013 (optional)
            if not skip_migration_011:
                cursor.execute("""
                    INSERT INTO __migrations_applied__ (filename)
                    VALUES ('013_create_partnership_requests_table.sql')
                """)

        # Create indexes (optional)
        skip_indexes = skip_indexes or []

        if 'idx_event_assignments_event_id' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_event_assignments_event_id
                ON event_assignments(event_id)
            """)

        if 'idx_event_assignments_peep_id' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_event_assignments_peep_id
                ON event_assignments(peep_id)
            """)

        if 'idx_availability_response' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_availability_response
                ON event_availability(response_id)
            """)

        if 'idx_event_availability_event_id' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_event_availability_event_id
                ON event_availability(event_id)
            """)

        if 'idx_snapshots_peep_id' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_snapshots_peep_id
                ON peep_order_snapshots(peep_id)
            """)

        if 'idx_partnerships_period_requester' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_partnerships_period_requester
                ON partnership_requests(period_id, requester_peep_id)
            """)

        if 'idx_partnerships_period_partner' not in skip_indexes:
            cursor.execute("""
                CREATE INDEX idx_partnerships_period_partner
                ON partnership_requests(period_id, partner_peep_id)
            """)

        conn.commit()
        return conn

    return _create


# =============================================================================
# TESTS: Foreign Key Constraints
# =============================================================================

@pytest.mark.db
class TestForeignKeyConstraints:
    """Tests for detecting missing foreign key constraints."""

    @pytest.mark.parametrize("fk_constraint,table_name,column_name", [
        ('event_assignments.event_id->events.id', 'event_assignments', 'event_id'),
        ('event_assignments.peep_id->peeps.id', 'event_assignments', 'peep_id'),
        ('event_availability.event_id->events.id', 'event_availability', 'event_id'),
        ('peep_order_snapshots.peep_id->peeps.id', 'peep_order_snapshots', 'peep_id'),
        ('partnership_requests.requester_peep_id->peeps.id', 'partnership_requests', 'requester_peep_id'),
    ])
    def test_missing_foreign_key_constraint(self, fk_constraint, table_name, column_name, broken_schema_db):
        """Test detection of missing foreign key constraints."""
        conn = broken_schema_db(skip_fk_constraints=[fk_constraint])
        issues = validate_schema(conn)

        assert len(issues) > 0, f"Should detect missing FK constraint {fk_constraint}"
        assert any(
            'foreign key' in issue.lower() and table_name in issue.lower() and column_name in issue.lower()
            for issue in issues
        ), f"Expected FK constraint error for {fk_constraint}, got: {issues}"

        conn.close()

    def test_all_required_fks_present(self, broken_schema_db):
        """
        Test that schema with all required FKs passes validation (positive test).

        This test should PASS because all FKs are present.
        """
        conn = broken_schema_db()  # No issues

        issues = validate_schema(conn)

        # Should have no FK-related issues
        fk_issues = [issue for issue in issues if 'foreign key' in issue.lower()]
        assert len(fk_issues) == 0, f"Should not report FK issues when all FKs present, got: {fk_issues}"

        conn.close()


# =============================================================================
# TESTS: Migration Table & Version
# =============================================================================

@pytest.mark.db
class TestMigrationValidation:
    """Tests for detecting missing migration table and missing migration 011."""

    def test_missing_migrations_table(self, broken_schema_db):
        """
        Test detection of missing __migrations_applied__ table
        """
        conn = broken_schema_db(skip_migration_table=True)

        issues = validate_schema(conn)

        assert len(issues) > 0, "Should detect missing migrations table"
        assert any(
                'migrations table' in issue.lower() or '__migrations_applied__' in issue
                for issue in issues
            ), f"Expected migrations table error, got: {issues}"

        conn.close()

    def test_missing_migration_011(self, broken_schema_db):
        """
        Test detection of missing migration 013 in __migrations_applied__ table
        """
        conn = broken_schema_db(skip_migration_011=True)
        issues = validate_schema(conn)

        assert len(issues) > 0, "Should detect missing migration 013"
        assert any(
                '013' in issue or 'migration' in issue.lower() and 'partnership' in issue.lower()
                for issue in issues
            ), f"Expected migration 013 error, got: {issues}"

        conn.close()

    def test_migrations_table_present_with_011(self, broken_schema_db):
        """
        Test that schema with migrations table and migration 013 passes (positive test).
        """
        conn = broken_schema_db()  # Both present
        issues = validate_schema(conn)

        # Should have no migration-related issues
        migration_issues = [issue for issue in issues if 'migration' in issue.lower()]
        assert len(migration_issues) == 0, f"Should not report migration issues when present, got: {migration_issues}"

        conn.close()


# =============================================================================
# TESTS: Critical Indexes
# =============================================================================

@pytest.mark.db
class TestCriticalIndexes:
    """Tests for detecting missing critical indexes."""

    @pytest.mark.parametrize("index_name,table_name,search_keyword", [
        ('idx_event_assignments_event_id', 'event_assignments', 'event_id'),
        ('idx_event_assignments_peep_id', 'event_assignments', 'peep_id'),
        ('idx_availability_response', 'event_availability', 'response'),
        ('idx_event_availability_event_id', 'event_availability', 'event_id'),
        ('idx_snapshots_peep_id', 'peep_order_snapshots', 'peep_id'),
        ('idx_partnerships_period_requester', 'partnership_requests', 'requester'),
    ])
    def test_missing_critical_index(self, index_name, table_name, search_keyword, broken_schema_db):
        """Test detection of missing critical indexes."""
        conn = broken_schema_db(skip_indexes=[index_name])
        issues = validate_schema(conn)

        assert len(issues) > 0, f"Should detect missing index {index_name}"
        assert any(
            'index' in issue.lower() and table_name in issue.lower() and search_keyword in issue.lower()
            for issue in issues
        ), f"Expected index error for {index_name}, got: {issues}"

        conn.close()

    def test_all_critical_indexes_present(self, broken_schema_db):
        """
        Test that schema with all critical indexes passes (positive test).
        """
        conn = broken_schema_db()  # All indexes present
        issues = validate_schema(conn)

        # Should have no index-related issues
        index_issues = [issue for issue in issues if 'index' in issue.lower()]
        assert len(index_issues) == 0, f"Should not report index issues when all present, got: {index_issues}"

        conn.close()


# =============================================================================
# TESTS: Error Message Quality
# =============================================================================

@pytest.mark.db
class TestErrorMessageQuality:
    """Tests for clear, actionable error messages."""

    def test_error_messages_are_specific_not_generic(self, broken_schema_db):
        """
        Test that error messages identify specific constraints, not generic errors.

        Bad: "Schema validation failed"
        Good: "Missing foreign key constraint: event_assignments.event_id -> events.id"
        """
        conn = broken_schema_db(
            skip_fk_constraints=['event_assignments.event_id->events.id'],
            skip_indexes=['idx_event_assignments_event_id']
        )
        issues = validate_schema(conn)

        # Verify error messages are specific
        assert len(issues) >= 2, "Should report at least 2 issues"

        for issue in issues:
                assert len(issue) > 20, f"Error message too generic: '{issue}'"
                # Should mention what's missing and what constraint/index
                assert any(
                    word in issue.lower()
                    for word in ['missing', 'foreign key', 'index', 'constraint']
                ), f"Error message should be specific: '{issue}'"

        conn.close()

    def test_error_messages_suggest_solution(self, broken_schema_db):
        """
        Test that error messages are actionable and suggest what's wrong.
        """
        conn = broken_schema_db(
            skip_fk_constraints=['event_assignments.peep_id->peeps.id']
        )
        issues = validate_schema(conn)

        assert len(issues) > 0

        # Each issue should mention:
        # 1. What's missing or wrong
        # 2. Which table/column is affected
        # 3. What it should reference (for FKs)
        for issue in issues:
                assert any(table in issue for table in ['event_assignments', 'peep_id']), \
                    f"Error should mention affected table/column: '{issue}'"

        conn.close()

    def test_multiple_errors_grouped_logically(self, broken_schema_db):
        """
        Test that when multiple errors exist, they're grouped logically in output.
        """
        conn = broken_schema_db(
            skip_fk_constraints=[
                'event_assignments.event_id->events.id',
                'event_assignments.peep_id->peeps.id'
            ],
            skip_indexes=[
                'idx_event_assignments_event_id',
                'idx_event_assignments_peep_id'
            ]
        )
        issues = validate_schema(conn)

        # Should report multiple issues for event_assignments
        assert len(issues) >= 4, f"Should report at least 4 issues, got {len(issues)}"

        # Issues for event_assignments should be together
        event_assignment_issues = [
                i for i, issue in enumerate(issues)
                if 'event_assignments' in issue.lower()
            ]
        assert len(event_assignment_issues) >= 4, \
                f"Expected at least 4 event_assignments issues"

        conn.close()


# =============================================================================
# TESTS: Complete Schema Validation
# =============================================================================

@pytest.mark.db
class TestCompleteSchemaValidation:
    """Tests for full schema validation across multiple issue types."""

    def test_detects_all_foreign_key_issues(self, broken_schema_db):
        """
        Test that validate_schema detects ALL missing foreign keys.
        """
        conn = broken_schema_db(
            skip_fk_constraints=[
                'event_assignments.event_id->events.id',
                'event_assignments.peep_id->peeps.id',
                'event_availability.event_id->events.id',
                'peep_order_snapshots.peep_id->peeps.id',
                'partnership_requests.period_id->schedule_periods.id',
                'partnership_requests.requester_peep_id->peeps.id',
                'partnership_requests.partner_peep_id->peeps.id'
            ]
        )
        issues = validate_schema(conn)

        # Should detect all 7 missing FKs
        fk_issues = [issue for issue in issues if 'foreign key' in issue.lower()]
        assert len(fk_issues) >= 7, \
                f"Expected at least 7 FK issues, got {len(fk_issues)}: {fk_issues}"

        conn.close()

    def test_detects_all_index_issues(self, broken_schema_db):
        """
        Test that validate_schema detects ALL missing indexes.
        """
        conn = broken_schema_db(
            skip_indexes=[
                'idx_event_assignments_event_id',
                'idx_event_assignments_peep_id',
                'idx_availability_response',
                'idx_event_availability_event_id',
                'idx_snapshots_peep_id',
                'idx_partnerships_period_requester',
                'idx_partnerships_period_partner'
            ]
        )
        issues = validate_schema(conn)

        # Should detect all 7 missing indexes
        index_issues = [issue for issue in issues if 'index' in issue.lower()]
        assert len(index_issues) >= 7, \
                f"Expected at least 7 index issues, got {len(index_issues)}: {index_issues}"

        conn.close()

    def test_combined_validation_issues(self, broken_schema_db):
        """
        Test validation with mixed issues: FKs, indexes, and migrations.
        """
        conn = broken_schema_db(
            skip_fk_constraints=['event_assignments.event_id->events.id'],
            skip_indexes=['idx_event_assignments_event_id'],
            skip_migration_011=True
        )
        issues = validate_schema(conn)

        # Should detect FK, index, and migration issues
        assert len(issues) >= 3, f"Expected at least 3 issues, got {len(issues)}: {issues}"

        fk_issues = [i for i in issues if 'foreign key' in i.lower()]
        index_issues = [i for i in issues if 'index' in i.lower()]
        migration_issues = [i for i in issues if 'migration' in i.lower()]

        assert len(fk_issues) >= 1, f"Should report FK issue: {issues}"
        assert len(index_issues) >= 1, f"Should report index issue: {issues}"
        assert len(migration_issues) >= 1, f"Should report migration issue: {issues}"

        conn.close()

    def test_returns_empty_list_for_valid_schema(self, broken_schema_db):
        """
        Test that validate_schema returns empty list for valid schema.
        """
        conn = broken_schema_db()  # No issues
        issues = validate_schema(conn)

        # Should return empty list or list with only non-critical warnings
        assert isinstance(issues, list), f"Should return list, got {type(issues)}"

        # Only allow informational messages, no errors
        error_issues = [
                i for i in issues
                if not any(word in i.lower() for word in ['info', 'note', 'optional'])
            ]
        assert len(error_issues) == 0, \
                f"Valid schema should have no error issues, got: {error_issues}"

        conn.close()


# =============================================================================
# INTEGRATION TESTS: Realistic Scenarios
# =============================================================================

@pytest.mark.db
class TestRealisticScenarios:
    """Integration tests with realistic broken schema scenarios."""

    def test_corrupted_migration_state(self, broken_schema_db):
        """
        Test scenario: Database upgraded partially, migration 011 not completed.
        """
        conn = broken_schema_db(
            skip_fk_constraints=['partnership_requests.requester_peep_id->peeps.id'],
            skip_migration_011=True
        )
        issues = validate_schema(conn)

        # Should detect both the incomplete migration AND the FK issue it causes
        assert len(issues) >= 2, \
                f"Incomplete migration should trigger multiple issues: {issues}"

        migration_issues = [i for i in issues if 'migration' in i.lower()]
        assert len(migration_issues) >= 1, f"Should report migration issue: {issues}"

        conn.close()

    def test_missing_critical_infrastructure(self, broken_schema_db):
        """
        Test scenario: Multiple critical pieces missing (catastrophic failure).
        """
        conn = broken_schema_db(
            skip_fk_constraints=[
                'event_assignments.event_id->events.id',
                'event_assignments.peep_id->peeps.id',
                'event_availability.event_id->events.id'
            ],
            skip_indexes=[
                'idx_event_assignments_event_id',
                'idx_event_assignments_peep_id',
                'idx_event_availability_event_id'
            ],
            skip_migration_table=True
        )
        issues = validate_schema(conn)

        # Should detect numerous issues
        assert len(issues) >= 7, \
                f"Should detect multiple missing pieces: {issues}"

        # Should mention both migration table AND structural issues
        assert any('migration' in i.lower() for i in issues), \
                f"Should mention missing migration table: {issues}"
        assert any('foreign key' in i.lower() for i in issues), \
                f"Should mention missing FKs: {issues}"
        assert any('index' in i.lower() for i in issues), \
                f"Should mention missing indexes: {issues}"

        conn.close()


# =============================================================================
# TESTS: Edge Cases
# =============================================================================

@pytest.mark.db
class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_handles_closed_connection_gracefully(self):
        """
        Test that validate_schema handles closed connection gracefully.
        """
        # Create and immediately close a connection
        conn = sqlite3.connect(':memory:')
        conn.close()

        # Should handle gracefully (raise meaningful error)
        try:
            issues = validate_schema(conn)
            assert False, "Should have raised an error for closed connection"
        except Exception as e:
            # Should raise meaningful error about closed connection
            assert 'closed' in str(e).lower() or 'cannot operate' in str(e).lower() or 'database' in str(e).lower()

    def test_validates_empty_but_structurally_valid_database(self, broken_schema_db):
        """
        Test schema validation on empty database with correct structure.
        """
        conn = broken_schema_db()  # All schema correct, no data
        issues = validate_schema(conn)

        # Should pass validation even with no data
        assert len(issues) == 0 or all(
                'warning' in i.lower() or 'info' in i.lower()
                for i in issues
            ), f"Empty but valid database should pass: {issues}"

        conn.close()
