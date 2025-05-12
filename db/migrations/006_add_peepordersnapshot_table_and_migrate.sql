PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

-- Create PeepOrderSnapshots table
CREATE TABLE IF NOT EXISTS PeepOrderSnapshots (
	id INTEGER PRIMARY KEY,
	timestamp DATETIME NOT NULL,
	data TEXT NOT NULL
);

-- Add foreign key fields to SchedulePeriods
ALTER TABLE SchedulePeriods ADD COLUMN snapshot_before_id INTEGER REFERENCES PeepOrderSnapshots(id);
ALTER TABLE SchedulePeriods ADD COLUMN snapshot_after_id INTEGER REFERENCES PeepOrderSnapshots(id);
ALTER TABLE SchedulePeriods ADD COLUMN snapshot_final_id INTEGER REFERENCES PeepOrderSnapshots(id);

-- Insert snapshots from existing JSON columns with midnight timestamps
INSERT INTO PeepOrderSnapshots (timestamp, data)
SELECT DATETIME(start_date, '-1 day'), snapshot_before
FROM SchedulePeriods
WHERE snapshot_before IS NOT NULL;

UPDATE SchedulePeriods
SET snapshot_before_id = (
	SELECT id FROM PeepOrderSnapshots
	WHERE DATE(timestamp) = DATE(SchedulePeriods.start_date, '-1 day')
	  AND data = SchedulePeriods.snapshot_before
	LIMIT 1
)
WHERE snapshot_before IS NOT NULL;

INSERT INTO PeepOrderSnapshots (timestamp, data)
SELECT DATETIME(start_date), snapshot_after
FROM SchedulePeriods
WHERE snapshot_after IS NOT NULL;

UPDATE SchedulePeriods
SET snapshot_after_id = (
	SELECT id FROM PeepOrderSnapshots
	WHERE DATE(timestamp) = DATE(SchedulePeriods.start_date)
	  AND data = SchedulePeriods.snapshot_after
	LIMIT 1
)
WHERE snapshot_after IS NOT NULL;

INSERT INTO PeepOrderSnapshots (timestamp, data)
SELECT DATETIME(end_date), snapshot_final
FROM SchedulePeriods
WHERE snapshot_final IS NOT NULL;

UPDATE SchedulePeriods
SET snapshot_final_id = (
	SELECT id FROM PeepOrderSnapshots
	WHERE DATE(timestamp) = DATE(SchedulePeriods.end_date)
	  AND data = SchedulePeriods.snapshot_final
	LIMIT 1
)
WHERE snapshot_final IS NOT NULL;

-- Recreate SchedulePeriods table without old snapshot columns
ALTER TABLE SchedulePeriods RENAME TO _SchedulePeriods_old;

CREATE TABLE SchedulePeriods (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	start_date DATE,
	end_date DATE,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	finalized BOOLEAN DEFAULT 0,
	notes TEXT,
	snapshot_before_id INTEGER REFERENCES PeepOrderSnapshots(id),
	snapshot_after_id INTEGER REFERENCES PeepOrderSnapshots(id),
	snapshot_final_id INTEGER REFERENCES PeepOrderSnapshots(id)
);

INSERT INTO SchedulePeriods (
	id, name, start_date, end_date, timestamp, finalized, notes,
	snapshot_before_id, snapshot_after_id, snapshot_final_id
)
SELECT
	id, name, start_date, end_date, timestamp, finalized, notes,
	snapshot_before_id, snapshot_after_id, snapshot_final_id
FROM _SchedulePeriods_old;

DROP TABLE _SchedulePeriods_old;

COMMIT;

PRAGMA foreign_keys=on;
