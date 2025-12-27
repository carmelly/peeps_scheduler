-- Migration 010: Add peep_id to event_assignment_changes
-- This allows us to track which specific person each change applies to

-- Add peep_id column to event_assignment_changes
ALTER TABLE event_assignment_changes ADD COLUMN peep_id INTEGER REFERENCES peeps(id);

-- Add index for efficient lookups
CREATE INDEX idx_eac_peep ON event_assignment_changes(peep_id);