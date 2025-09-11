CREATE INDEX idx_raw_actual_attendance_period ON raw_actual_attendance(period_name);
CREATE INDEX idx_raw_members_email ON raw_members("Email Address");
CREATE INDEX idx_raw_members_period ON raw_members(period_name);
CREATE INDEX idx_raw_output_period ON raw_output(period_name);
CREATE INDEX idx_raw_responses_email ON raw_responses("Email Address");
CREATE INDEX idx_raw_responses_period ON raw_responses(period_name);
CREATE INDEX idx_raw_results_period ON raw_results(period_name);
CREATE TABLE __migrations_applied__ (
				filename TEXT PRIMARY KEY,
				applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
			);
CREATE TABLE raw_actual_attendance (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL UNIQUE,
	actual_attendance_json TEXT,  -- Complete JSON as text
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0);
CREATE TABLE raw_members (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL,
	csv_id TEXT,
	Name TEXT,
	"Display Name" TEXT,  -- Preserve spaces in column names
	"Email Address" TEXT,
	Role TEXT,
	"Index" INTEGER,
	Priority INTEGER,
	"Total Attended" INTEGER,
	Active TEXT,
	"Date Joined" TEXT,
	raw_data TEXT,  -- Full row as JSON for any extra fields
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0,
	UNIQUE(period_name, csv_id)  -- Prevent duplicate imports
);
CREATE TABLE raw_output (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL UNIQUE,
	output_json TEXT,  -- Complete JSON as text
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0);
CREATE TABLE raw_responses (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL,
	row_number INTEGER,
	Timestamp TEXT,
	"Email Address" TEXT,
	Name TEXT,
	"Primary Role" TEXT,
	"Secondary Role" TEXT,
	"Max Sessions" TEXT,
	Availability TEXT,
	"Min Interval Days" TEXT,
	"Preferred gap between sessions?" TEXT,
	"Partnership Preference" TEXT,
	"Questions or Comments for Organizers" TEXT,
	"Questions or Comments for Leilani" TEXT,
	raw_data TEXT,  -- Full row as JSON for schema variations
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0,
	UNIQUE(period_name, row_number, Name, "Email Address")  -- Prevent duplicate imports
);
CREATE TABLE raw_results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL UNIQUE,
	results_json TEXT,  -- Complete JSON as text
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0);