-- Migration 005: Replace member_period_snapshots with peep_order_snapshots
--
-- This migration replaces the old member_period_snapshots table with a cleaner
-- peep_order_snapshots design that removes the redundant role field (role is
-- already stored in peeps.primary_role) and focuses on period completion snapshots.

-- Drop the old table
DROP TABLE IF EXISTS member_period_snapshots;

-- Create the new peep_order_snapshots table
CREATE TABLE peep_order_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,

    -- State after ALL real results for this completed period
    priority INTEGER NOT NULL,
    index_position INTEGER NOT NULL,
    total_attended INTEGER NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(peep_id, period_id),

    CHECK(priority >= 0),
    CHECK(index_position >= 0),
    CHECK(total_attended >= 0)
);

-- Create indexes for common query patterns
CREATE INDEX idx_snapshots_period ON peep_order_snapshots(period_id);
CREATE INDEX idx_snapshots_peep ON peep_order_snapshots(peep_id);