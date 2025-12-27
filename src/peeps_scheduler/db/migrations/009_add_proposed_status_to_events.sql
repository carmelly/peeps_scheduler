-- Migration 009: Add 'proposed' status to events table
--
-- Adds 'proposed' status to support event lifecycle:
-- proposed → scheduled → completed
--
-- This allows tracking events from availability parsing through
-- scheduler selection to actual completion.

BEGIN TRANSACTION;

-- Create new events table with updated status constraint
CREATE TABLE events_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,     -- Global unique event ID
    period_id INTEGER NOT NULL,               -- Foreign key to schedule_periods
    legacy_period_event_id INTEGER,           -- Original per-period ID (0,1,2,3...)
    event_datetime DATETIME NOT NULL,         -- Event date and time
    duration_minutes INTEGER NOT NULL,        -- 60, 90, or 120 minutes
    status TEXT DEFAULT 'proposed',           -- proposed, scheduled, cancelled, completed
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(period_id, legacy_period_event_id),
    UNIQUE(event_datetime),                    -- No two events at same time

    CHECK(duration_minutes IN (60, 90, 120)),
    CHECK(status IN ('proposed', 'scheduled', 'cancelled', 'completed'))
);

-- Copy existing data (if any)
INSERT INTO events_new
SELECT * FROM events
WHERE 1=0; -- Don't copy anything - table should be empty after restore

-- Drop old table and rename new one
DROP TABLE events;
ALTER TABLE events_new RENAME TO events;

-- Recreate indexes
CREATE INDEX idx_events_period ON events(period_id);
CREATE INDEX idx_events_datetime ON events(event_datetime);

COMMIT;