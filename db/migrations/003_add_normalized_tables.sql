-- Step 1 - Design Schema
-- Design normalized relational schema with proper foreign key relationships
-- Enable efficient queries for scheduling algorithms and analytics

CREATE TABLE members (
    id INTEGER PRIMARY KEY,              -- Preserve CSV IDs (manually maintained)
    full_name TEXT NOT NULL,             -- From "Name" column
    display_name TEXT NOT NULL,          -- From "Display Name" column  
    primary_role TEXT NOT NULL,          -- From "Role" column 
    email TEXT UNIQUE NOT NULL,          -- From "Email Address" column
    date_joined DATE,                    -- From "Date Joined" column
    active BOOLEAN NOT NULL DEFAULT 1,   -- From "Active" column
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    CHECK(primary_role IN ('leader', 'follower'))
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

CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,     -- Global unique event ID
    period_id INTEGER NOT NULL,               -- Foreign key to schedule_periods
    legacy_period_event_id INTEGER,           -- Original per-period ID (0,1,2,3...)
    event_datetime DATETIME NOT NULL,         -- Event date and time
    duration_minutes INTEGER NOT NULL,        -- 60, 90, or 120 minutes
    status TEXT DEFAULT 'scheduled',           -- scheduled, cancelled, completed
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(period_id, legacy_period_event_id),
    UNIQUE(event_datetime),                    -- No two events at same time

    CHECK(duration_minutes IN (60, 90, 120)),
    CHECK(status IN ('scheduled', 'cancelled', 'completed'))
);

-- Represents member status (priority, order) at the beginning of the period
CREATE TABLE member_period_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,
    role TEXT NOT NULL,                       -- "leader" or "follower" 
    priority INTEGER NOT NULL,                -- Scheduling priority
    index_position INTEGER NOT NULL,          -- Position in priority list
    total_attended INTEGER NOT NULL,          -- Historical attendance count
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (member_id) REFERENCES members(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(member_id, period_id),

    CHECK(role IN ('leader', 'follower')),
    CHECK(priority >= 0),
    CHECK(index_position >= 0),
    CHECK(total_attended >= 0)
);

CREATE TABLE responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id INTEGER NOT NULL,
    period_id INTEGER NOT NULL,
    response_role TEXT NOT NULL,             -- Role for this period (may differ from primary)
    switch_preference INTEGER DEFAULT 1,     -- SwitchPreference enum value
    max_sessions INTEGER NOT NULL,           -- Event limit for period
    min_interval_days INTEGER DEFAULT 0,     -- Cooldown between events
    partnership_preference TEXT,             -- Free text partnership requests
    organizer_comments TEXT,                 -- Comments for organizers
    instructor_comments TEXT,                -- Comments for instructor
    response_timestamp DATETIME,             -- When response was submitted
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (member_id) REFERENCES members(id),
    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    UNIQUE(member_id, period_id),

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
    member_id INTEGER NOT NULL,
    assigned_role TEXT NOT NULL,            -- "leader" or "follower"
    assignment_type TEXT NOT NULL,          -- "attendee" or "alternate"
    assignment_order INTEGER,               -- Order of assignment (for attendees)
    alternate_position INTEGER,             -- Position in alternate list
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (event_id) REFERENCES events(id),
    FOREIGN KEY (member_id) REFERENCES members(id),
    UNIQUE(event_id, member_id),

    CHECK(assigned_role IN ('leader', 'follower')),
    CHECK(assignment_type IN ('attendee', 'alternate'))
);

-- Actual attendance of event usually differs from assignments
CREATE TABLE event_attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    member_id INTEGER NOT NULL,

    -- Link to final scheduler output if they had one (NULL for volunteer_fill)
    event_assignment_id INTEGER,

    -- What the scheduler said (nullable)
    expected_role TEXT,                 -- "leader" or "follower"
    expected_type TEXT,                 -- "attendee" or "alternate"

    -- What happened
    actual_role TEXT,                   -- "leader" or "follower"
    attendance_status TEXT NOT NULL,    -- "attended", "cancelled", or "no_show"
    participation_mode TEXT NOT NULL,   -- "scheduled", "alternate_promoted", or "volunteer_fill"
    last_minute_cancel BOOLEAN,         -- true if cancelled within cutoff

    check_in_time DATETIME,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (event_id) REFERENCES events(id),
    FOREIGN KEY (member_id) REFERENCES members(id),
    FOREIGN KEY (event_assignment_id) REFERENCES event_assignments(id),

    UNIQUE(event_id, member_id),

    CHECK(attendance_status IN ('attended', 'cancelled', 'no_show')),
    CHECK(participation_mode IN ('scheduled', 'alternate_promoted', 'volunteer_fill')),
    CHECK(expected_type IS NULL OR expected_type IN ('attendee', 'alternate')),
    CHECK(expected_role IS NULL OR expected_role IN ('leader', 'follower')),
    CHECK(actual_role IS NULL OR actual_role IN ('leader', 'follower'))
);

-- Tracks changes to event assignments after initial scheduling
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

-- Indexes for performance
CREATE INDEX idx_members_email ON members(email);
CREATE INDEX idx_members_active ON members(active);
CREATE INDEX idx_events_period ON events(period_id);
CREATE INDEX idx_events_datetime ON events(event_datetime);
CREATE INDEX idx_member_snapshots_period ON member_period_snapshots(period_id);
CREATE INDEX idx_responses_period_member ON responses(period_id, member_id);
CREATE INDEX idx_assignments_event_type ON event_assignments(event_id, assignment_type);
CREATE INDEX idx_availability_response ON event_availability(response_id);
CREATE INDEX idx_event_attendance_event ON event_attendance(event_id);
CREATE INDEX idx_event_attendance_member ON event_attendance(member_id);
CREATE INDEX idx_event_attendance_mode ON event_attendance(participation_mode);
CREATE INDEX idx_eac_event ON event_assignment_changes(event_id);
CREATE INDEX idx_eac_changed_at ON event_assignment_changes(changed_at);
