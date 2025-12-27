-- Migration 006: Add active field to peep_order_snapshots
--
-- This migration adds an 'active' boolean field to track member active status
-- at the time each snapshot was created. This captures historical active/inactive
-- states from the raw member data.

ALTER TABLE peep_order_snapshots ADD COLUMN active BOOLEAN NOT NULL DEFAULT 1;