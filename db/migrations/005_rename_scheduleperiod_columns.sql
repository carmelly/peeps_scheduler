PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

-- Rename old table
ALTER TABLE SchedulePeriods RENAME TO _SchedulePeriods_old;

-- Create new table with renamed columns and nullable date fields
CREATE TABLE SchedulePeriods (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	start_date DATE,
	end_date DATE,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	finalized BOOLEAN DEFAULT 0,
	notes TEXT,
	snapshot_before TEXT,
	snapshot_after TEXT,
	snapshot_final TEXT
);

-- Copy data into new structure
INSERT INTO SchedulePeriods (
	id, name, start_date, end_date, timestamp,
	finalized, notes, snapshot_before, snapshot_after, snapshot_final
)
SELECT
	id, label, start_date, end_date, created_at,
	finalized, notes, snapshot_before, snapshot_after, snapshot_final
FROM _SchedulePeriods_old;

-- Drop old table
DROP TABLE _SchedulePeriods_old;

COMMIT;

PRAGMA foreign_keys=on;