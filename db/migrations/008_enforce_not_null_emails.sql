-- Migration 008: Enforce NOT NULL constraints on email fields
--
-- Assumes all data will be reimported with proper synthetic emails.
-- This migration just ensures database schema enforces email requirements.

BEGIN TRANSACTION;

-- For peeps table, recreate with explicit NOT NULL constraint
-- (The constraint already exists, but making it explicit for clarity)
CREATE TABLE peeps_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    primary_role TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,                 -- Enforce NOT NULL
    date_joined DATE,
    active BOOLEAN NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CHECK(primary_role IN ('leader', 'follower'))
);

-- Copy existing data if any (will fail if there are NULL emails)
INSERT INTO peeps_new
SELECT * FROM peeps;

-- Replace old table
DROP TABLE peeps;
ALTER TABLE peeps_new RENAME TO peeps;

-- Recreate indexes
CREATE INDEX idx_peeps_email ON peeps(email);
CREATE INDEX idx_peeps_active ON peeps(active);

-- Add CHECK constraints to raw data tables to prevent blank emails at import
-- Raw members: Email Address cannot be NULL or empty
ALTER TABLE raw_members ADD COLUMN email_check_constraint TEXT
    CHECK("Email Address" IS NOT NULL AND TRIM("Email Address") != '');

-- Raw responses: Email Address cannot be NULL or empty
ALTER TABLE raw_responses ADD COLUMN email_check_constraint TEXT
    CHECK("Email Address" IS NOT NULL AND TRIM("Email Address") != '');

-- Note: SQLite doesn't support adding CHECK constraints to existing columns,
-- so we'll enforce this at the application level during import

COMMIT;