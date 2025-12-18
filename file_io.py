import csv
import datetime
import json
import logging
import os
import sys
from models import EventSequence, Peep, Event, Role, SwitchPreference
import constants

# -- Constants --

PEEPS_CSV_FIELDS = [
	'id', 'Name', 'Display Name', 'Email Address', 'Role',
	'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined'
]

RESPONSES_CSV_FIELDS = [
	'Name', 'Email Address', 'Primary Role', 'Secondary Role',
	'Max Sessions', 'Availability', 'Min Interval Days'
]

def normalize_email(email):
	"""
	Normalize email for matching. Removes dots from Gmail addresses only.
	Gmail ignores dots, so john.smith@gmail.com == johnsmith@gmail.com.
	"""
	if not email:
		return ""

	email = email.strip().lower()

	if email.endswith('@gmail.com'):
		local, domain = email.rsplit('@', 1)
		local = local.replace('.', '')
		return f"{local}@{domain}"

	return email

# -- CSV-related --

def load_csv(filename, required_columns=[]):
	"""Load CSV file and validate required columns, trimming whitespace from headers and values."""
	with open(filename, newline='', encoding='utf-8') as csvfile:
		# Read the first line (fieldnames), trim whitespace
		reader = csv.reader(csvfile)
		try:
			raw_fieldnames = next(reader)
		except StopIteration:
			return []

		fieldnames = [name.strip() for name in raw_fieldnames]

		# Check required columns
		missing = set(required_columns) - set(fieldnames)
		if required_columns and missing:
			raise ValueError(f"Missing required column(s): {missing}")

		# Rebuild DictReader with cleaned headers
		dict_reader = csv.DictReader(csvfile, fieldnames=fieldnames)
		rows = []
		
		# Replace smart quotes (’) with ASCII quotes (') and normalize whitespace
		def _normalize_text(s):
			s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
			# Normalize multiple spaces to single space
			import re
			s = re.sub(r'\s+', ' ', s)
			return s

		# Strip whitespace, normalize quotes and whitespace for every value
		for row in dict_reader:
			cleaned = {k: _normalize_text(v.strip()) if v else "" for k, v in row.items()}
			rows.append(cleaned)

		return rows

def load_peeps(peeps_csv_path):
	"""Load and convert peep rows from CSV into Peep instances. Validates unique emails."""
	rows = load_csv(peeps_csv_path, PEEPS_CSV_FIELDS)
	peeps = [Peep.from_csv(row) for row in rows]
	emails = []

	for peep in peeps:
		email = normalize_email(peep.email)
		if peep.active:
			if not email:
				raise ValueError(f"Active peep '{peep.full_name}' is missing an email.")
			emails.append(email)
		elif email:
			emails.append(email)

	dupes = {email for email in emails if emails.count(email) > 1}
	if dupes:
		raise ValueError(f"Duplicate email(s) found in peeps: {sorted(dupes)}")

	return peeps

def load_responses(response_csv_path):
	"""Load and parse response rows from responses.csv."""
	return load_csv(response_csv_path, RESPONSES_CSV_FIELDS)

def save_peeps_csv(peeps: list[Peep], filename):
	"""Save updated peeps to a new CSV called members_updated.csv in the same folder as the original."""
	output_path = os.path.join(os.path.dirname(filename), "members_updated.csv")
	with open(output_path, "w", newline='', encoding='utf-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=PEEPS_CSV_FIELDS)
		writer.writeheader()
		for peep in peeps:
			writer.writerow(peep.to_csv())
	logging.info(f"Updated peeps saved to {output_path}")

# -- JSON-related --

def load_json(filename):
	"""Load data from a JSON file."""
	try:
		with open(filename, "r") as f:
			return json.load(f)
	except FileNotFoundError:
		return None

def save_json(data, filename):
	"""Save data to a JSON file, handling Enums and datetime."""
	def custom_serializer(obj):
		if hasattr(obj, "value"):
			return obj.value
		if isinstance(obj, datetime.datetime):
			return obj.strftime(constants.DATE_FORMAT)
		if isinstance(obj, datetime.date):
			return obj.isoformat()
		return str(obj)

	with open(filename, "w") as f:
		json.dump(data, f, indent=4, default=custom_serializer)

def load_cancelled_events(data_folder, year=None):
	"""
	Load cancelled events from cancelled_events.json in the data folder.

	Parses event date strings immediately and returns parsed event_ids.

	Args:
		data_folder: Path to the data folder containing cancelled_events.json
		year: Year to use for parsing event dates (required for correct parsing)

	Returns:
		set: Set of parsed event_id strings in format "YYYY-MM-DD HH:MM".
		     Returns empty set if file doesn't exist (backward compatible).

	Raises:
		Exception: If cancelled_events.json exists but contains invalid JSON
		ValueError: If any event string cannot be parsed
	"""
	cancelled_file = os.path.join(data_folder, "cancelled_events.json")

	# Return empty set if file doesn't exist (backward compatible)
	if not os.path.exists(cancelled_file):
		return set()

	# File exists - parse it and raise errors if malformed
	try:
		with open(cancelled_file, "r") as f:
			data = json.load(f)
	except json.JSONDecodeError as e:
		raise Exception(f"invalid cancelled_events.json: {e}") from e

	# Handle missing or null 'cancelled_events' key
	cancelled_event_strings = data.get("cancelled_events")
	if cancelled_event_strings is None:
		return set()

	# Parse event strings immediately and return event_ids
	parsed_event_ids = set()
	for event_str in cancelled_event_strings:
		try:
			event_id, _, _ = parse_event_date(event_str, year=year)
			parsed_event_ids.add(event_id)
		except Exception as e:
			raise ValueError(f"Cannot parse event string in cancelled_events.json: '{event_str}' - {e}") from e

	return parsed_event_ids

def load_data_from_json(filename):
	"""Load peeps and events from an existing output JSON file."""
	json_data = load_json(filename)
	response_data = json_data['responses']
	event_data = json_data['events']
	peeps_data = json_data['peeps']

	events = [Event.from_dict(e) for e in event_data] if event_data else []
	peeps = [Peep(**p) for p in peeps_data] if peeps_data else []

	sorted_peeps = sorted(peeps, key=lambda peep: peep.index)
	if not Peep.is_peeps_list_sorted_by_priority(sorted_peeps):
		raise ValueError("Peeps data is not sorted by priority (highest to lowest). Check input file data integrity.")

	return sorted_peeps, events

def save_event_sequence(sequence: EventSequence, filename):
	"""Serialize and save an EventSequence to JSON."""
	save_json(sequence.to_dict(), filename)
	logging.info(f"Saved event sequence to {filename}")

# -- Response conversion --

def convert_to_json(response_csv_path, peeps_csv_path, output_json_path, year=None):
	"""Main function: convert responses and members CSVs into output.json."""
	peeps = load_peeps(peeps_csv_path)
	response_rows = load_responses(response_csv_path)
	event_map = extract_events(response_rows, year=year)
	updated_peeps, responses_data = process_responses(response_rows, peeps, event_map, year=year)

	output = {
		"responses": responses_data,
		"events": [event.to_dict() for event in event_map.values()],
		"peeps": [peep.to_dict() for peep in updated_peeps],
	}
	save_json(output, output_json_path)

def extract_events(rows, year=None):
	"""
	Extract events from responses.csv.

	Supports two modes:
	1. Event rows (backward compatibility): Rows with Name starting with "Event:"
	2. Auto-derive from availability: If no Event rows, scan availability strings

	Args:
		rows: List of response rows
		year: Optional year for date parsing. Defaults to current year if not provided.

	Returns:
		dict: event_map with event_id as key and Event object as value
	"""
	event_map = {}
	event_counter = 0

	# First, check if there are Event rows (backward compatibility)
	event_rows = [row for row in rows if row.get("Name", "").startswith("Event:")]

	if event_rows:
		# Use Event rows (old format)
		for row in event_rows:
			name = row.get("Name", "")
			parts = name.split("Event: ", 1)
			if len(parts) < 2 or not parts[1]:
				raise ValueError(f"Malformed event row: missing date in 'Name' field -> {name}")

			date_str = parts[1].strip()
			event_id, duration_from_str, display_name = parse_event_date(date_str, year=year)

			# For old format Event rows, duration comes from Event Duration column
			duration_str = row.get("Event Duration", "")
			if duration_str:
				try:
					duration = int(duration_str)
				except ValueError:
					raise ValueError(f"Invalid Event Duration value: {duration_str}")
			elif duration_from_str is not None:
				# New format Event row with time range
				duration = duration_from_str
			else:
				raise ValueError(f"Missing Event Duration for event row: {name}")

			if duration not in constants.CLASS_CONFIG:
				raise ValueError(f"Duration {duration} not in CLASS_CONFIG")

			event = Event.from_dict({
				"id": event_counter,
				"date": event_id,
				"duration_minutes": duration,
			})
			event.id = event_counter
			event_map[event_id] = event
			event_counter += 1

	else:
		# Auto-derive events from availability strings
		unique_events = {}  # event_id -> (duration, display_name)

		for row in rows:
			name = row.get("Name", "")
			if not name:
				continue

			availability_str = row.get("Availability", "")
			if not availability_str:
				continue

			# Split availability by comma and parse each date string
			date_strings = [s.strip() for s in availability_str.split(",") if s.strip()]

			for date_str in date_strings:
				try:
					event_id, duration, display_name = parse_event_date(date_str, year=year)

					if duration is None:
						raise ValueError(f"Cannot auto-derive event duration from '{date_str}' - time range required")

					if duration not in constants.CLASS_CONFIG:
						raise ValueError(f"Duration {duration} minutes not in CLASS_CONFIG (valid: {list(constants.CLASS_CONFIG.keys())})")

					# Track unique events
					if event_id not in unique_events:
						unique_events[event_id] = (duration, display_name)
					else:
						# Verify consistency if same event appears multiple times
						existing_duration, _ = unique_events[event_id]
						if existing_duration != duration:
							raise ValueError(f"Inconsistent duration for event {event_id}: {existing_duration} vs {duration}")

				except ValueError as e:
					logging.warning(f"Skipping invalid availability string '{date_str}' for {name}: {e}")
					continue

		# Create Event objects from unique events
		for event_id, (duration, display_name) in sorted(unique_events.items()):
			event = Event.from_dict({
				"id": event_counter,
				"date": event_id,
				"duration_minutes": duration,
			})
			event.id = event_counter
			event_map[event_id] = event
			event_counter += 1

	return event_map

def process_responses(rows, peeps, event_map, year=None):
	"""Update peep objects from responses and return updated peeps and response summaries."""
	responses_data = []

	for row in rows:
		name = row.get("Name", "")
		if not name or name.startswith("Event:"):
			continue

		email = normalize_email(row.get("Email Address", ""))
		if not email:
			raise ValueError(f"Missing email for row: {name}")

		peep = next((p for p in peeps if normalize_email(p.email) == email), None)
		if not peep:
			raise ValueError(f"No matching peep found for email: {email} (row: {name})")
		if not peep.active: 
			raise ValueError(f"Response from inactive peep: {peep.full_name} (ID {peep.id}) — please activate them in the members spreadsheet.")

		peep.role = Role.from_string(row["Primary Role"])
		peep.event_limit = int(row["Max Sessions"])
		peep.min_interval_days = int(row["Min Interval Days"])
		peep.switch_pref = SwitchPreference.from_string(row["Secondary Role"])
		peep.responded = True

		available_strs = [s.strip() for s in row.get("Availability", "").split(",") if s.strip()]
		for date_str in available_strs:
			event_id, _, _ = parse_event_date(date_str, year=year)
			event = event_map.get(event_id)
			if not event:
				logging.warning(f"{name} listed availability for unknown event: {event_id}")
				continue
			peep.availability.append(event.id)

		responses_data.append({
			"timestamp": row["Timestamp"],
			"name": name,
			"role": peep.role,
			"switch_pref": peep.switch_pref,
			"max_sessions": peep.event_limit,
			"available_dates": available_strs,
		})

	return peeps, responses_data

# -- Miscellaneous --

def parse_time_range(time_str):
	"""
	Parse a time range string and return start time, end time, and duration.

	Handles formats like:
	- "5:30pm to 7pm"
	- "5pm to 6:30pm"
	- "4pm to 5:30pm"

	Returns:
		tuple: (start_time_str, end_time_str, duration_minutes)
		Example: ("17:30", "19:00", 90)
	"""
	import re

	# Split on " to "
	parts = time_str.lower().split(" to ")
	if len(parts) != 2:
		raise ValueError(f"Invalid time range format (expected 'X to Y'): {time_str}")

	start_str, end_str = parts[0].strip(), parts[1].strip()

	def parse_time(t):
		"""Parse a single time like '5:30pm' or '5pm' into 24-hour format."""
		# Match patterns like "5:30pm", "5pm", "17:30"
		match = re.match(r'^(\d{1,2})(?::(\d{2}))?([ap]m)?$', t)
		if not match:
			raise ValueError(f"Invalid time format: {t}")

		hour = int(match.group(1))
		minute = int(match.group(2)) if match.group(2) else 0
		meridiem = match.group(3)

		if meridiem:
			if meridiem == 'pm' and hour != 12:
				hour += 12
			elif meridiem == 'am' and hour == 12:
				hour = 0

		if hour < 0 or hour > 23 or minute < 0 or minute > 59:
			raise ValueError(f"Time out of range: {t}")

		return datetime.time(hour, minute)

	start_time = parse_time(start_str)
	end_time = parse_time(end_str)

	# Calculate duration in minutes
	start_minutes = start_time.hour * 60 + start_time.minute
	end_minutes = end_time.hour * 60 + end_time.minute

	if end_minutes <= start_minutes:
		raise ValueError(f"End time must be after start time: {time_str}")

	duration = end_minutes - start_minutes

	return (start_time.strftime("%H:%M"), end_time.strftime("%H:%M"), duration)

def parse_event_date(date_str, year=None):
	"""
	Parse an event date string and return event ID, duration (if present), and display name.

	Supports two formats:
	1. New format with time range: "Friday January 9th - 5:30pm to 7pm"
	2. Old format (backward compatibility): "Friday October 17 - 5pm"

	Args:
		date_str: Date string to parse
		year: Optional year to use. Defaults to current year if not provided.

	Returns:
		tuple: (event_id, duration_minutes or None, display_name)
		Example: ("2026-01-09 17:30", 90, "Friday January 9th - 5:30pm to 7pm")
	"""
	import re

	if year is None:
		year = datetime.datetime.now().year

	# Strip any trailing notes in parentheses
	date_str = date_str.split('(')[0].strip()
	display_name = date_str

	# Check if this is a time range format (contains " to ")
	has_time_range = " to " in date_str

	if has_time_range:
		# New format: "Friday January 9th - 5:30pm to 7pm"
		# Split on " - " to separate date from time range
		parts = date_str.split(" - ")
		if len(parts) != 2:
			raise ValueError(f"Invalid event date format (expected 'Date - Time Range'): {date_str}")

		date_part = parts[0].strip()
		time_range_part = parts[1].strip()

		# Remove ordinal suffixes (st, nd, rd, th) from date
		date_part = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_part)

		# Parse the date part (e.g., "Friday January 9")
		try:
			dt = datetime.datetime.strptime(date_part, "%A %B %d")
		except ValueError as e:
			raise ValueError(f"Invalid date format in '{date_str}': {e}")

		# Parse the time range to get start time and duration
		start_time_str, end_time_str, duration = parse_time_range(time_range_part)

		# Set year from parameter or current year
		dt = dt.replace(year=year)

		# Combine date with start time
		start_hour, start_minute = map(int, start_time_str.split(':'))
		dt = dt.replace(hour=start_hour, minute=start_minute)

		event_id = dt.strftime("%Y-%m-%d %H:%M")
		return (event_id, duration, display_name)

	else:
		# Old format: "Friday October 17 - 5pm" (backward compatibility)
		# Remove ordinal suffixes if present
		date_str_normalized = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

		try:
			dt = datetime.datetime.strptime(date_str_normalized, constants.DATESTR_FORMAT)
		except ValueError:
			# Try without ordinals in original
			dt = datetime.datetime.strptime(date_str, constants.DATESTR_FORMAT)

		dt = dt.replace(year=year)
		event_id = dt.strftime("%Y-%m-%d %H:%M")
		return (event_id, None, display_name)
