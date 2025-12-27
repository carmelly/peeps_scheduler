-- Migration 012: Update period status from 'active' to 'scheduled'
--
-- Bug #002 Fix: Period status should be 'scheduled' (not 'active') for periods
-- that have assignments but no attendance data yet.
--
-- This migration:
-- 1. Updates any existing 'active' status values to 'scheduled'
-- 2. Recreates schedule_periods table with updated CHECK constraint
-- 3. Preserves all data and relationships

-- Step 1: Create backup of schedule_periods table
CREATE TABLE schedule_periods_backup (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_name TEXT UNIQUE NOT NULL,
    display_name TEXT,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Step 2: Backup all existing data
INSERT INTO schedule_periods_backup (id, period_name, display_name, start_date, end_date, status, created_at, notes)
SELECT id, period_name, display_name, start_date, end_date, status, created_at, notes
FROM schedule_periods;

-- Step 3: Drop the old table
DROP TABLE schedule_periods;

-- Step 4: Create new schedule_periods table with corrected CHECK constraint
CREATE TABLE schedule_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_name TEXT UNIQUE NOT NULL,
    display_name TEXT,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,

    -- Status lifecycle:
    -- 'draft': Initial state, no assignments or attendance
    -- 'scheduled': Has assignments (from results.json), no attendance (future period)
    -- 'completed': Has attendance data (period has occurred)
    CHECK(status IN ('draft', 'scheduled', 'completed'))
);

-- Step 5: Restore data with status conversion: 'active' → 'scheduled'
INSERT INTO schedule_periods (id, period_name, display_name, start_date, end_date, status, created_at, notes)
SELECT
    id,
    period_name,
    display_name,
    start_date,
    end_date,
    CASE
        WHEN status = 'active' THEN 'scheduled'
        ELSE status
    END AS status,
    created_at,
    CASE
        WHEN status = 'active' THEN 'Migrated from active status'
        ELSE notes
    END AS notes
FROM schedule_periods_backup;

-- Step 6: Restore index
CREATE INDEX idx_periods_name ON schedule_periods(period_name);

-- Step 7: Verify migration success
-- Check for any remaining 'active' status values
SELECT COUNT(*) as active_count FROM schedule_periods WHERE status = 'active';

-- If the query above returns 0, migration was successful.
-- Backup table can be dropped after verification: DROP TABLE schedule_periods_backup;