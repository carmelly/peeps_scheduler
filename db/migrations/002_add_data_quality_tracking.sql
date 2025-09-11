-- Step 0.1: Add Data Quality Tracking Columns
-- Add metadata columns to track data reconstruction and quality issues

-- Add data quality columns to raw_members
ALTER TABLE raw_members ADD COLUMN data_quality_notes TEXT DEFAULT '';
ALTER TABLE raw_members ADD COLUMN reconstructed_flag INTEGER DEFAULT 0;

-- Add data quality columns to raw_responses  
ALTER TABLE raw_responses ADD COLUMN data_quality_notes TEXT DEFAULT '';
ALTER TABLE raw_responses ADD COLUMN reconstructed_flag INTEGER DEFAULT 0;

-- Add data quality columns to raw_results
ALTER TABLE raw_results ADD COLUMN data_quality_notes TEXT DEFAULT '';
ALTER TABLE raw_results ADD COLUMN reconstructed_flag INTEGER DEFAULT 0;

-- Add data quality columns to raw_output
ALTER TABLE raw_output ADD COLUMN data_quality_notes TEXT DEFAULT '';
ALTER TABLE raw_output ADD COLUMN reconstructed_flag INTEGER DEFAULT 0;

-- Add data quality columns to raw_actual_attendance
ALTER TABLE raw_actual_attendance ADD COLUMN data_quality_notes TEXT DEFAULT '';
ALTER TABLE raw_actual_attendance ADD COLUMN reconstructed_flag INTEGER DEFAULT 0;
