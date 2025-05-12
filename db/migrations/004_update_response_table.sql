-- Recreate response table to match desired schema
DROP TABLE IF EXISTS Responses;

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