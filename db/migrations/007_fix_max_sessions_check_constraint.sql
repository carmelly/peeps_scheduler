-- Migration 007: Fix max_sessions CHECK constraint to allow 0 values
--
-- The current constraint requires max_sessions > 0, but users can legitimately
-- set max_sessions to 0 to indicate they don't want to be scheduled.

-- SQLite doesn't support ALTER TABLE DROP CONSTRAINT, so we need to:
-- 1. Create new table with corrected constraint
-- 2. Copy data
-- 3. Drop old table
-- 4. Rename new table

BEGIN TRANSACTION;

-- Create new responses table with correct constraint
CREATE TABLE responses_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,                    -- Renamed from member_id
    period_id INTEGER NOT NULL,
    response_role TEXT NOT NULL,                 -- Role for this period (may differ from primary)
    switch_preference INTEGER DEFAULT 1,         -- SwitchPreference enum value
    max_sessions INTEGER NOT NULL,               -- Event limit for period
    min_interval_days INTEGER DEFAULT 0,         -- Cooldown between events
    partnership_preference TEXT,                 -- Free text partnership requests
    organizer_comments TEXT,                     -- Comments for organizers
    instructor_comments TEXT,                    -- Comments for instructor
    response_timestamp DATETIME,                 -- When response was submitted
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(peep_id, period_id),
    CHECK(max_sessions >= 0),                    -- Changed from > 0 to >= 0
    CHECK(min_interval_days >= 0),
    CHECK(switch_preference IN (1, 2, 3)),
    CHECK(response_role IN ('leader', 'follower'))
);

-- Copy all existing data
INSERT INTO responses_new
SELECT * FROM responses;

-- Drop old table and rename new one
DROP TABLE responses;
ALTER TABLE responses_new RENAME TO responses;

-- Recreate indexes that may have been lost
CREATE INDEX idx_responses_peep_period ON responses(peep_id, period_id);
CREATE INDEX idx_responses_period ON responses(period_id);

COMMIT;