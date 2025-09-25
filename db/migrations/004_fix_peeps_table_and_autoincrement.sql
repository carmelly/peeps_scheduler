-- Fix peeps table: add AUTOINCREMENT and rename members -> peeps
-- Since tables are empty, we can simply drop and recreate with correct schema

-- Drop existing empty tables (in dependency order)
DROP TABLE IF EXISTS event_assignment_changes;
DROP TABLE IF EXISTS event_attendance;
DROP TABLE IF EXISTS event_assignments;
DROP TABLE IF EXISTS event_availability;
DROP TABLE IF EXISTS responses;
DROP TABLE IF EXISTS member_period_snapshots;
DROP TABLE IF EXISTS members;

-- Create peeps table (renamed from members) with AUTOINCREMENT
CREATE TABLE peeps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Now with AUTOINCREMENT for new peeps
    full_name TEXT NOT NULL,                -- From "Name" column
    display_name TEXT NOT NULL,             -- From "Display Name" column
    primary_role TEXT NOT NULL,             -- From "Role" column
    email TEXT UNIQUE NOT NULL,             -- From "Email Address" column
    date_joined DATE,                       -- From "Date Joined" column
    active BOOLEAN NOT NULL DEFAULT 1,      -- From "Active" column
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CHECK(primary_role IN ('leader', 'follower'))
);

-- Recreate other tables with peep_id references
CREATE TABLE member_period_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peep_id INTEGER NOT NULL,                    -- Renamed from member_id
    period_id INTEGER NOT NULL,
    role TEXT NOT NULL,                          -- "leader" or "follower"
    priority INTEGER NOT NULL,                   -- Scheduling priority
    index_position INTEGER NOT NULL,             -- Position in priority list
    total_attended INTEGER NOT NULL,             -- Historical attendance count
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (peep_id) REFERENCES peeps(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(peep_id, period_id),

    CHECK(role IN ('leader', 'follower')),
    CHECK(priority >= 0),
    CHECK(index_position >= 0),
    CHECK(total_attended >= 0)
);

CREATE TABLE responses (
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

    CHECK(response_role IN ('leader', 'follower')),
    CHECK(switch_preference IN (1, 2, 3)),
    CHECK(max_sessions > 0),
    CHECK(min_interval_days >= 0)
);

CREATE TABLE event_availability (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    response_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,

    FOREIGN KEY (response_id) REFERENCES responses(id),
    FOREIGN KEY (event_id) REFERENCES events(id),
    UNIQUE(response_id, event_id)
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

-- Recreate event_assignment_changes table (unchanged)
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

-- Recreate indexes with updated names
CREATE INDEX idx_peeps_email ON peeps(email);
CREATE INDEX idx_peeps_active ON peeps(active);
CREATE INDEX idx_member_snapshots_period ON member_period_snapshots(period_id);
CREATE INDEX idx_responses_period_peep ON responses(period_id, peep_id);
CREATE INDEX idx_assignments_event_type ON event_assignments(event_id, assignment_type);
CREATE INDEX idx_availability_response ON event_availability(response_id);
CREATE INDEX idx_event_attendance_event ON event_attendance(event_id);
CREATE INDEX idx_event_attendance_peep ON event_attendance(peep_id);
CREATE INDEX idx_event_attendance_mode ON event_attendance(participation_mode);
CREATE INDEX idx_eac_event ON event_assignment_changes(event_id);
CREATE INDEX idx_eac_changed_at ON event_assignment_changes(changed_at);