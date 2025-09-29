-- Migration 011: Backup corrupted snapshots before regeneration
--
-- Creates a backup table to preserve the original corrupted snapshot data
-- before regenerating with correct priority logic.

-- Create backup table with same structure as peep_order_snapshots
CREATE TABLE peep_order_snapshots_backup (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,

    -- State after ALL real results for this completed period
    priority INTEGER NOT NULL,
    index_position INTEGER NOT NULL,
    total_attended INTEGER NOT NULL,

    active BOOLEAN NOT NULL DEFAULT 1,
    notes TEXT,

    -- Backup metadata
    backup_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    backup_reason TEXT DEFAULT 'Pre-regeneration backup of corrupted priority calculations',

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),

    CHECK(priority >= 0),
    CHECK(index_position >= 0),
    CHECK(total_attended >= 0)
);

-- Copy all existing snapshots to backup
INSERT INTO peep_order_snapshots_backup (
    peep_id, period_id, priority, index_position, total_attended,
    active, notes
)
SELECT
    peep_id, period_id, priority, index_position, total_attended,
    active, notes
FROM peep_order_snapshots;

-- Create index for efficient querying of backup data
CREATE INDEX idx_backup_snapshots_period_peep ON peep_order_snapshots_backup(period_id, peep_id);
CREATE INDEX idx_backup_snapshots_timestamp ON peep_order_snapshots_backup(backup_timestamp);