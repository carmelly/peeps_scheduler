-- Migration 013: Create partnership_requests table (Phase 1 Feature)
--
-- Adds support for partnership requests where members can request to be partnered
-- with specific other members for event scheduling.
--
-- Table stores partnership preferences that guide the scheduler's pairing logic.

CREATE TABLE partnership_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_id INTEGER NOT NULL,
    requester_peep_id INTEGER NOT NULL,
    partner_peep_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (period_id) REFERENCES schedule_periods(id),
    FOREIGN KEY (requester_peep_id) REFERENCES peeps(id),
    FOREIGN KEY (partner_peep_id) REFERENCES peeps(id),

    -- Ensure each request is unique (same request only once per period)
    UNIQUE(period_id, requester_peep_id, partner_peep_id),

    -- Prevent self-partnerships
    CHECK(requester_peep_id != partner_peep_id)
);

-- Create index for efficient querying
CREATE INDEX idx_partnerships_period_requester ON partnership_requests(period_id, requester_peep_id);
CREATE INDEX idx_partnerships_period_partner ON partnership_requests(period_id, partner_peep_id);
