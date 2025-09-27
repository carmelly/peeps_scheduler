CREATE INDEX idx_assignments_event_type ON event_assignments(event_id, assignment_type);
CREATE INDEX idx_availability_response ON event_availability(response_id);
CREATE INDEX idx_eac_changed_at ON event_assignment_changes(changed_at);
CREATE INDEX idx_eac_event ON event_assignment_changes(event_id);
CREATE INDEX idx_event_attendance_event ON event_attendance(event_id);
CREATE INDEX idx_event_attendance_mode ON event_attendance(participation_mode);
CREATE INDEX idx_event_attendance_peep ON event_attendance(peep_id);
CREATE INDEX idx_events_datetime ON events(event_datetime);
CREATE INDEX idx_events_period ON events(period_id);
CREATE INDEX idx_peeps_active ON peeps(active);
CREATE INDEX idx_peeps_email ON peeps(email);
CREATE INDEX idx_raw_actual_attendance_period ON raw_actual_attendance(period_name);
CREATE INDEX idx_raw_members_email ON raw_members("Email Address");
CREATE INDEX idx_raw_members_period ON raw_members(period_name);
CREATE INDEX idx_raw_output_period ON raw_output(period_name);
CREATE INDEX idx_raw_responses_email ON raw_responses("Email Address");
CREATE INDEX idx_raw_responses_period ON raw_responses(period_name);
CREATE INDEX idx_raw_results_period ON raw_results(period_name);
CREATE INDEX idx_responses_peep_period ON responses(peep_id, period_id);
CREATE INDEX idx_responses_period ON responses(period_id);
CREATE INDEX idx_snapshots_peep ON peep_order_snapshots(peep_id);
CREATE INDEX idx_snapshots_period ON peep_order_snapshots(period_id);
CREATE TABLE __migrations_applied__ (
				filename TEXT PRIMARY KEY,
				applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
			);
CREATE TABLE event_assignment_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,

    change_type TEXT NOT NULL,             -- 'cancel', 'promote_alternate', 'add', 'add_alternate', 'change_role'
    change_source TEXT DEFAULT 'organizer', -- 'organizer', 'member', 'system', etc.
    change_reason TEXT,                    -- e.g., 'no_show_push_to_back', 'timely_cancel_with_alternate'
    policy_applied TEXT,                   -- optional: name/key of fairness policy applied
    notes TEXT,

    changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,

    FOREIGN KEY (event_id) REFERENCES events(id),

    CHECK(change_type IN (
        'cancel',                  -- Someone cancelled/was removed
        'promote_alternate',       -- Move alternate to attendee
        'add_alternate',           -- Add someone as alternate
        'add',                     -- Add someone (attendee or alternate)
        'change_role'              -- Switch leader/follower
    ))
);
CREATE TABLE event_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    peep_id INTEGER NOT NULL,                    -- Renamed from member_id
    assigned_role TEXT NOT NULL,                 -- "leader" or "follower"
    assignment_type TEXT NOT NULL,               -- "attendee" or "alternate"
    assignment_order INTEGER,                    -- Order of assignment (for attendees)
    alternate_position INTEGER,                  -- Position in alternate list
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id) REFERENCES events(id),
    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    UNIQUE(event_id, peep_id),

    CHECK(assigned_role IN ('leader', 'follower')),
    CHECK(assignment_type IN ('attendee', 'alternate'))
);
CREATE TABLE event_attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    peep_id INTEGER NOT NULL,                    -- Renamed from member_id

    -- Link to final scheduler output if they had one (NULL for volunteer_fill)
    event_assignment_id INTEGER,

    -- What the scheduler said (nullable)
    expected_role TEXT,                          -- "leader" or "follower"
    expected_type TEXT,                          -- "attendee" or "alternate"

    -- What happened
    actual_role TEXT,                            -- "leader" or "follower"
    attendance_status TEXT NOT NULL,             -- "attended", "cancelled", or "no_show"
    participation_mode TEXT NOT NULL,            -- "scheduled", "alternate_promoted", or "volunteer_fill"
    last_minute_cancel BOOLEAN,                  -- true if cancelled within cutoff

    check_in_time DATETIME,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id) REFERENCES events(id),
    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (event_assignment_id) REFERENCES event_assignments(id),

    UNIQUE(event_id, peep_id),

    CHECK(attendance_status IN ('attended', 'cancelled', 'no_show')),
    CHECK(participation_mode IN ('scheduled', 'alternate_promoted', 'volunteer_fill')),
    CHECK(expected_type IS NULL OR expected_type IN ('attendee', 'alternate')),
    CHECK(expected_role IS NULL OR expected_role IN ('leader', 'follower')),
    CHECK(actual_role IS NULL OR actual_role IN ('leader', 'follower'))
);
CREATE TABLE event_availability (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    response_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,

    FOREIGN KEY (response_id) REFERENCES responses(id),
    FOREIGN KEY (event_id) REFERENCES events(id),
    UNIQUE(response_id, event_id)
);
CREATE TABLE "events" (
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
CREATE TABLE peep_order_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,

    -- State after ALL real results for this completed period
    priority INTEGER NOT NULL,
    index_position INTEGER NOT NULL,
    total_attended INTEGER NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT, active BOOLEAN NOT NULL DEFAULT 1,

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(peep_id, period_id),

    CHECK(priority >= 0),
    CHECK(index_position >= 0),
    CHECK(total_attended >= 0)
);
CREATE TABLE "peeps" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    primary_role TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,                 -- Enforce NOT NULL
    date_joined DATE,
    active BOOLEAN NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CHECK(primary_role IN ('leader', 'follower'))
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
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0, email_check_constraint TEXT
    CHECK("Email Address" IS NOT NULL AND TRIM("Email Address") != ''),
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
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0, email_check_constraint TEXT
    CHECK("Email Address" IS NOT NULL AND TRIM("Email Address") != ''),
	UNIQUE(period_name, row_number, Name, "Email Address")  -- Prevent duplicate imports
);
CREATE TABLE raw_results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	period_name TEXT NOT NULL UNIQUE,
	results_json TEXT,  -- Complete JSON as text
	imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
, data_quality_notes TEXT DEFAULT '', reconstructed_flag INTEGER DEFAULT 0);
CREATE TABLE "responses" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,                    -- Renamed from member_id
    period_id INTEGER NOT NULL,
    response_role TEXT NOT NULL,                 -- Role for this period (may differ from primary)
    switch_preference INTEGER DEFAULT 1,         -- SwitchPreference enum value
    max_sessions INTEGER NOT NULL,               -- Event limit for period
    min_interval_days INTEGER DEFAULT 0,         -- Cooldown between events
    partnership_preference TEXT,                 -- Free text partnership requests
    organizer_comments TEXT,                     -- Comments for organizers
    instructor_comments TEXT,                    -- Comments for instructor
    response_timestamp DATETIME,                 -- When response was submitted
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(peep_id, period_id),
    CHECK(max_sessions >= 0),                    -- Changed from > 0 to >= 0
    CHECK(min_interval_days >= 0),
    CHECK(switch_preference IN (1, 2, 3)),
    CHECK(response_role IN ('leader', 'follower'))
);
CREATE TABLE schedule_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_name TEXT UNIQUE NOT NULL,    -- "2025-09", "2024-10", etc.
    display_name TEXT,                   -- "May 2025"
    start_date DATE NOT NULL,            -- Period start (when available)
    end_date DATE NOT NULL,              -- Period end (when available) 
    status TEXT DEFAULT 'draft',         -- draft, active, completed
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CHECK(start_date <= end_date),
    CHECK(status IN ('draft', 'active', 'completed'))
);