CREATE TABLE Peeps (
	id INTEGER PRIMARY KEY,
	full_name TEXT NOT NULL,
	display_name TEXT,
	email TEXT UNIQUE,
	primary_role TEXT CHECK (primary_role IN ('leader', 'follower')),
	date_joined DATE
, active BOOLEAN DEFAULT 1);
CREATE TABLE Events (
	id INTEGER PRIMARY KEY,
	schedule_id INTEGER NOT NULL REFERENCES SchedulePeriod(id),
	name TEXT,
	datetime DATETIME NOT NULL,    -- ISO format datetime
	duration_minutes INTEGER DEFAULT 120,
	min_per_role INTEGER NOT NULL,
	max_per_role INTEGER NOT NULL
);
CREATE TABLE AttendanceRecord (
	id INTEGER PRIMARY KEY,
	event_id INTEGER NOT NULL REFERENCES Events(id),
	peep_id INTEGER NOT NULL REFERENCES Peeps(id),
	role TEXT NOT NULL CHECK (role IN ('leader', 'follower')),
	status TEXT NOT NULL CHECK (status IN ('preliminary', 'confirmed', 'absent', 'alternate', 'skipped')),
	UNIQUE(event_id, peep_id)
);
CREATE TABLE PeepOrderSnapshots (
	id INTEGER PRIMARY KEY,
	timestamp DATETIME NOT NULL,
	data TEXT NOT NULL
);
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
CREATE TABLE Responses (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scheduleperiod_id INTEGER NOT NULL,
	peep_id INTEGER NOT NULL,
	timestamp TEXT,
	role TEXT,
	availability TEXT,
	min_interval_days INTEGER,
	max_sessions INTEGER,
	raw_data TEXT,
	FOREIGN KEY(scheduleperiod_id) REFERENCES scheduleperiod(id),
	FOREIGN KEY(peep_id) REFERENCES peeps(id)
);
CREATE TABLE sqlite_sequence(name,seq);


CREATE INDEX idx_responses_schedule ON Responses(schedule_id);
CREATE INDEX idx_attendance_event ON AttendanceRecord(event_id);
CREATE TABLE __migrations_applied__ (
			filename TEXT PRIMARY KEY,
			applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
