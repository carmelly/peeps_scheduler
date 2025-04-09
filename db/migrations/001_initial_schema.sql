-- 001_initial_schema.sql

-- SchedulePeriod: A specific scheduling window (e.g., April 2025)
CREATE TABLE SchedulePeriod (
	id INTEGER PRIMARY KEY,
	label TEXT NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	finalized BOOLEAN DEFAULT 0,  -- Indicates if results are locked in
	notes TEXT,
	snapshot_before TEXT,         -- JSON: list of {peep_id, index, priority}
	snapshot_after TEXT,          -- Used for scheduling the next period
	snapshot_final TEXT           -- Used after confirming actual attendance
);

-- Peeps: People who can be scheduled into events
CREATE TABLE Peeps (
	id INTEGER PRIMARY KEY,
	full_name TEXT NOT NULL,
	display_name TEXT,
	email TEXT UNIQUE,
	primary_role TEXT CHECK (primary_role IN ('leader', 'follower')),
	date_joined DATE
);

-- Events: Individual events/classes within a schedule period
CREATE TABLE Events (
	id INTEGER PRIMARY KEY,
	schedule_id INTEGER NOT NULL REFERENCES SchedulePeriod(id),
	name TEXT,
	datetime DATETIME NOT NULL,    -- ISO format datetime
	duration_minutes INTEGER DEFAULT 120,
	min_per_role INTEGER NOT NULL,
	max_per_role INTEGER NOT NULL
);

-- Responses: Peep-specific input for a schedule period
CREATE TABLE Responses (
	id INTEGER PRIMARY KEY,
	schedule_id INTEGER NOT NULL REFERENCES SchedulePeriod(id),
	peep_id INTEGER NOT NULL REFERENCES Peeps(id),
	submitted_at DATETIME NOT NULL,
	min_interval_days INTEGER NOT NULL,
	max_sessions INTEGER NOT NULL,
	availability TEXT NOT NULL,   -- JSON list of event IDs
	comment_teacher TEXT,
	comment_organizers TEXT,
	UNIQUE(schedule_id, peep_id)
);

-- AttendanceRecord: Tracks who was assigned, confirmed, skipped, etc.
CREATE TABLE AttendanceRecord (
	id INTEGER PRIMARY KEY,
	event_id INTEGER NOT NULL REFERENCES Events(id),
	peep_id INTEGER NOT NULL REFERENCES Peeps(id),
	role TEXT NOT NULL CHECK (role IN ('leader', 'follower')),
	status TEXT NOT NULL CHECK (status IN ('preliminary', 'confirmed', 'absent', 'alternate', 'skipped')),
	UNIQUE(event_id, peep_id)
);

-- Helpful indexes for lookup speed
CREATE INDEX idx_responses_schedule ON Responses(schedule_id);
CREATE INDEX idx_attendance_event ON AttendanceRecord(event_id);
