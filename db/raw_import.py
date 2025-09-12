#!/usr/bin/env python3
"""
Raw Data Importer for Step 0 of Database Migration

Imports historical CSV/JSON files into raw tables with zero transformation.
Preserves original field names and data formats exactly as they appear in files.
Schema based on 2025-09 structure with backfill for older periods.

Usage:
	python db/raw_import.py [period_name]
	python db/raw_import.py --all
	python db/raw_import.py 2025-09
"""

import argparse
import csv
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from data_manager import get_data_manager
import constants

# Use constants and DataManager for paths
DB_PATH = constants.DEFAULT_DB_PATH
data_manager = get_data_manager()
# Use processed data path for historical imports
DATA_PATH = str(data_manager.get_processed_data_path())

def setup_logging():
	"""Configure logging for import operations."""
	logging.basicConfig(
		level=logging.INFO,
		format='%(asctime)s - %(levelname)s - %(message)s'
	)

def get_db_connection():
	"""Get database connection with row factory for easier access."""
	conn = sqlite3.connect(DB_PATH)
	conn.row_factory = sqlite3.Row
	return conn

def import_members_csv(period_name: str, csv_path: str, conn: sqlite3.Connection, notes_data: dict = None):
	"""Import members.csv into raw_members table with data quality metadata."""
	if not os.path.exists(csv_path):
		logging.warning(f"No members.csv found for {period_name}")
		return 0
	
	# Extract data quality info from notes for members.csv
	data_quality_notes = ""
	reconstructed_flag = 0
	
	if notes_data and 'files' in notes_data and 'members.csv' in notes_data['files']:
		file_info = notes_data['files']['members.csv']
		quality_info = []
		
		# Handle missing files with explanation
		if file_info.get('status') == 'missing':
			if 'reason' in file_info:
				quality_info.append(f"Status: missing - {file_info['reason']}")
			else:
				quality_info.append("Status: missing")
		else:
			if 'source' in file_info:
				quality_info.append(f"Source: {file_info['source']}")
			
			if 'changes' in file_info and file_info['changes']:
				changes = "; ".join(file_info['changes'])
				quality_info.append(f"Changes: {changes}")
			
			if 'data_quality' in file_info:
				quality_info.append(f"Quality: {file_info['data_quality']}")
		
		data_quality_notes = "; ".join(quality_info)
		reconstructed_flag = 1 if file_info.get('reconstructed', False) else 0
	
	with open(csv_path, 'r', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		
		imported_count = 0
		for row in reader:
			# Store complete row as JSON in raw_data
			raw_data = json.dumps(dict(row))
			
			# Extract known fields, Python keys match CSV exactly
			# Handle missing fields gracefully for older periods
			values = {
				'period_name': period_name,
				'csv_id': row.get('id', ''),
				'Name': row.get('Name', ''),
				'Display_Name': row.get('Display Name', ''),
				'Email_Address': row.get('Email Address', ''),
				'Role': row.get('Role', ''),
				'Index': int(row.get('Index', 0)) if row.get('Index', '').strip() else None,
				'Priority': int(row.get('Priority', 0)) if row.get('Priority', '').strip() else None,
				'Total_Attended': int(row.get('Total Attended', 0)) if row.get('Total Attended', '').strip() else None,
				'Active': row.get('Active', ''),
				'Date_Joined': row.get('Date Joined', ''),
				'raw_data': raw_data
			}
			
			# Add data quality metadata to values
			values['data_quality_notes'] = data_quality_notes
			values['reconstructed_flag'] = reconstructed_flag
			
			try:
				conn.execute("""
					INSERT OR REPLACE INTO raw_members (
						period_name, csv_id, Name, "Display Name", "Email Address", Role,
						"Index", Priority, "Total Attended", Active, "Date Joined", raw_data,
						data_quality_notes, reconstructed_flag
					) VALUES (
						:period_name, :csv_id, :Name, :Display_Name, :Email_Address, :Role,
						:Index, :Priority, :Total_Attended, :Active, :Date_Joined, :raw_data,
						:data_quality_notes, :reconstructed_flag
					)
				""", values)
				imported_count += 1
			except sqlite3.Error as e:
				logging.error(f"Error importing member row {row.get('Name', 'unknown')}: {e}")
				continue
	
	logging.info(f"Imported {imported_count} members for {period_name}")
	return imported_count

def import_responses_csv(period_name: str, csv_path: str, conn: sqlite3.Connection, notes_data: dict = None):
	"""Import responses.csv into raw_responses table with data quality metadata."""
	if not os.path.exists(csv_path):
		logging.warning(f"No responses.csv found for {period_name}")
		return 0
	
	# Extract data quality info from notes for responses.csv
	data_quality_notes = ""
	reconstructed_flag = 0
	
	if notes_data and 'files' in notes_data and 'responses.csv' in notes_data['files']:
		file_info = notes_data['files']['responses.csv']
		quality_info = []
		
		# Handle missing files with explanation
		if file_info.get('status') == 'missing':
			if 'reason' in file_info:
				quality_info.append(f"Status: missing - {file_info['reason']}")
			else:
				quality_info.append("Status: missing")
		else:
			if 'source' in file_info:
				quality_info.append(f"Source: {file_info['source']}")
			
			if 'changes' in file_info and file_info['changes']:
				changes = "; ".join(file_info['changes'])
				quality_info.append(f"Changes: {changes}")
			
			if 'data_quality' in file_info:
				quality_info.append(f"Quality: {file_info['data_quality']}")
		
		data_quality_notes = "; ".join(quality_info)
		reconstructed_flag = 1 if file_info.get('reconstructed', False) else 0
	
	with open(csv_path, 'r', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		
		imported_count = 0
		for row_num, row in enumerate(reader):
			# Store complete row as JSON in raw_data
			raw_data = json.dumps(dict(row))
			
			# Extract known fields based on 2025-09 schema, Python keys match CSV exactly
			# Handle missing fields gracefully for older periods
			values = {
				'period_name': period_name,
				'row_number': row_num,
				'Timestamp': row.get('Timestamp', ''),
				'Email_Address': row.get('Email Address', ''),
				'Name': row.get('Name', ''),
				'Primary_Role': row.get('Primary Role', ''),
				'Secondary_Role': row.get('Secondary Role', ''),
				'Max_Sessions': row.get('Max Sessions', ''),
				'Availability': row.get('Availability', ''),
				'Min_Interval_Days': row.get('Min Interval Days', ''),
				'Preferred_gap_between_sessions': row.get('Preferred gap between sessions?', ''),
				'Partnership_Preference': row.get('Partnership Preference', ''),
				'Questions_or_Comments_for_Organizers': row.get('Questions or Comments for Organizers', ''),
				'Questions_or_Comments_for_Leilani': row.get('Questions or Comments for Leilani', ''),
				'raw_data': raw_data
			}
			
			# Add data quality metadata to values
			values['data_quality_notes'] = data_quality_notes
			values['reconstructed_flag'] = reconstructed_flag
			
			try:
				conn.execute("""
					INSERT OR REPLACE INTO raw_responses (
						period_name, row_number, Timestamp, "Email Address", Name, "Primary Role", 
						"Secondary Role", "Max Sessions", Availability, "Min Interval Days",
						"Preferred gap between sessions?", "Partnership Preference", 
						"Questions or Comments for Organizers", "Questions or Comments for Leilani", raw_data,
						data_quality_notes, reconstructed_flag
					) VALUES (
						:period_name, :row_number, :Timestamp, :Email_Address, :Name, :Primary_Role,
						:Secondary_Role, :Max_Sessions, :Availability, :Min_Interval_Days,
						:Preferred_gap_between_sessions, :Partnership_Preference,
						:Questions_or_Comments_for_Organizers, :Questions_or_Comments_for_Leilani, :raw_data,
						:data_quality_notes, :reconstructed_flag
					)
				""", values)
				imported_count += 1
			except sqlite3.Error as e:
				logging.error(f"Error importing response row {row_num}: {e}")
				continue
	
	logging.info(f"Imported {imported_count} responses for {period_name}")
	return imported_count

def import_json_file(period_name: str, json_path: str, table_name: str, json_column: str, conn: sqlite3.Connection, notes_data: dict = None):
	"""Import JSON file into specified raw table with data quality metadata."""
	if not os.path.exists(json_path):
		logging.warning(f"No {os.path.basename(json_path)} found for {period_name}")
		return False
	
	with open(json_path, 'r', encoding='utf-8') as f:
		json_content = f.read()
	
	# Extract data quality info from notes
	filename = os.path.basename(json_path)
	data_quality_notes = ""
	reconstructed_flag = 0
	
	if notes_data and 'files' in notes_data and filename in notes_data['files']:
		file_info = notes_data['files'][filename]
		quality_info = []
		
		# Handle missing files with explanation
		if file_info.get('status') == 'missing':
			if 'reason' in file_info:
				quality_info.append(f"Status: missing - {file_info['reason']}")
			else:
				quality_info.append("Status: missing")
		else:
			if 'source' in file_info:
				quality_info.append(f"Source: {file_info['source']}")
			
			if 'changes' in file_info and file_info['changes']:
				changes = "; ".join(file_info['changes'])
				quality_info.append(f"Changes: {changes}")
			
			if 'data_quality' in file_info:
				quality_info.append(f"Quality: {file_info['data_quality']}")
		
		data_quality_notes = "; ".join(quality_info)
		reconstructed_flag = 1 if file_info.get('reconstructed', False) else 0
	
	try:
		conn.execute(f"""
			INSERT OR REPLACE INTO {table_name} (
				period_name, {json_column}, data_quality_notes, reconstructed_flag
			) VALUES (?, ?, ?, ?)
		""", (period_name, json_content, data_quality_notes, reconstructed_flag))
		logging.info(f"Imported {os.path.basename(json_path)} for {period_name}")
		return True
	except sqlite3.Error as e:
		logging.error(f"Error importing {json_path}: {e}")
		return False

def load_notes_data(period_path: str):
	"""Load and parse notes.json if it exists."""
	notes_path = os.path.join(period_path, 'notes.json')
	if not os.path.exists(notes_path):
		return None
	
	try:
		with open(notes_path, 'r', encoding='utf-8') as f:
			return json.load(f)
	except (json.JSONDecodeError, IOError) as e:
		logging.warning(f"Could not parse notes.json: {e}")
		return None

def import_period(period_name: str):
	"""Import all files for a single period."""
	period_path = os.path.join(DATA_PATH, period_name)
	if not os.path.exists(period_path):
		logging.error(f"Period directory not found: {period_path}")
		return False
	
	logging.info(f"Importing period: {period_name}")
	
	# Load notes data for data quality metadata
	notes_data = load_notes_data(period_path)
	if notes_data:
		logging.info(f"Found notes.json for {period_name}, will apply data quality metadata")
	
	conn = get_db_connection()
	try:
		# Import CSV files
		members_count = import_members_csv(
			period_name, 
			os.path.join(period_path, 'members.csv'),
			conn,
			notes_data
		)
		
		responses_count = import_responses_csv(
			period_name,
			os.path.join(period_path, 'responses.csv'), 
			conn,
			notes_data
		)
		
		# Import JSON files
		results_imported = import_json_file(
			period_name,
			os.path.join(period_path, 'results.json'),
			'raw_results',
			'results_json',
			conn,
			notes_data
		)
		
		output_imported = import_json_file(
			period_name,
			os.path.join(period_path, 'output.json'),
			'raw_output', 
			'output_json',
			conn,
			notes_data
		)
		
		# Import actual_attendance.json (manually edited final status)
		actual_attendance_imported = import_json_file(
			period_name,
			os.path.join(period_path, 'actual_attendance.json'),
			'raw_actual_attendance',
			'actual_attendance_json',
			conn,
			notes_data
		)
		
		conn.commit()
		logging.info(f"✅ Successfully imported {period_name}: {members_count} members, {responses_count} responses")
		return True
		
	except Exception as e:
		conn.rollback()
		logging.error(f"❌ Failed to import {period_name}: {e}")
		return False
	finally:
		conn.close()

def get_available_periods():
	"""Get list of available period directories."""
	if not os.path.exists(DATA_PATH):
		return []
	
	periods = []
	for item in os.listdir(DATA_PATH):
		period_path = os.path.join(DATA_PATH, item)
		if os.path.isdir(period_path) and not item.startswith('.') and item != 'README.md':
			periods.append(item)
	
	return sorted(periods)

def get_ordered_periods():
	"""Get periods in proper import order: pre period first, then chronological."""
	available = get_available_periods()
	
	# Separate pre periods from regular periods
	pre_periods = [p for p in available if p.startswith('pre-')]
	regular_periods = [p for p in available if not p.startswith('pre-')]
	
	# Sort both groups
	pre_periods.sort()
	regular_periods.sort()
	
	# Return pre periods first, then regular periods
	return pre_periods + regular_periods

def main():
	setup_logging()
	
	parser = argparse.ArgumentParser(description="Import historical data into raw tables")
	parser.add_argument('period', nargs='?', help='Period name to import (e.g., 2025-09)')
	parser.add_argument('--all', action='store_true', help='Import all available periods')
	parser.add_argument('--list', action='store_true', help='List available periods')
	
	args = parser.parse_args()
	
	# Ensure migrations are run first
	if os.path.exists(DB_PATH):
		logging.info("Database exists, checking for new migrations...")
	else:
		logging.info("Creating new database and running migrations...")
	
	# Import and run migrations
	try:
		from migrate import run_migrations
		run_migrations()
	except ImportError:
		logging.error("Could not import migrate module. Run from project root.")
		sys.exit(1)
	
	available_periods = get_available_periods()
	
	if args.list:
		print("Available periods:")
		for period in available_periods:
			print(f"  {period}")
		return
	
	if args.all:
		success_count = 0
		ordered_periods = get_ordered_periods()
		logging.info(f"Importing {len(ordered_periods)} periods in order: {', '.join(ordered_periods)}")
		
		for period in ordered_periods:
			if import_period(period):
				success_count += 1
		logging.info(f"✅ Import complete: {success_count}/{len(ordered_periods)} periods imported successfully")
	
	elif args.period:
		if args.period not in available_periods:
			logging.error(f"Period '{args.period}' not found. Available: {', '.join(available_periods)}")
			sys.exit(1)
		import_period(args.period)
	
	else:
		parser.print_help()
		print(f"\nAvailable periods: {', '.join(available_periods)}")

if __name__ == "__main__":
	main()
