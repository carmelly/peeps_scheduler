-- 002_add_active_column.sql
-- Add 'active' column to Peeps to indicate whether they are still eligible for scheduling

ALTER TABLE Peeps ADD COLUMN active BOOLEAN DEFAULT 1;
