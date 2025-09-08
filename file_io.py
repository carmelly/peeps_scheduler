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
	'Max Sessions', 'Availability'
]

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
		
		# Replace smart quotes (’) with ASCII quotes ('))
		def _normalize_quotes(s):
			return s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
		
		# Strip whitespace and normalize quotes for every value 
		for row in dict_reader:
			cleaned = {k: _normalize_quotes(v.strip()) if v else "" for k, v in row.items()}
			rows.append(cleaned)

		return rows

def load_peeps(peeps_csv_path):
	"""Load and convert peep rows from CSV into Peep instances. Validates unique emails."""
	rows = load_csv(peeps_csv_path, PEEPS_CSV_FIELDS)
	peeps = [Peep.from_csv(row) for row in rows]
	emails = []

	for peep in peeps:
		email = peep.email.lower()
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

def load_data_from_json(filename):
	"""Load peeps and events from an existing output JSON file."""
	json_data = load_json(filename)
	response_data = json_data['responses']
	event_data = json_data['events']
	peeps_data = json_data['peeps']

	events = [Event.from_dict(e) for e in event_data] if event_data else []
	peeps = [Peep(**p) for p in peeps_data] if peeps_data else []

	sorted_peeps = sorted(peeps, key=lambda peep: peep.index)
	assert Peep.is_peeps_list_sorted_by_priority(peeps), "Peeps are not sorted by priority; check input file."

	return sorted_peeps, events

def save_event_sequence(sequence: EventSequence, filename):
	"""Serialize and save an EventSequence to JSON."""
	save_json(sequence.to_dict(), filename)
	logging.info(f"Saved event sequence to {filename}")

# -- Response conversion --

def convert_to_json(response_csv_path, peeps_csv_path, output_json_path):
	"""Main function: convert responses and members CSVs into output.json."""
	peeps = load_peeps(peeps_csv_path)
	response_rows = load_responses(response_csv_path)
	event_map = extract_events(response_rows)
	updated_peeps, responses_data = process_responses(response_rows, peeps, event_map)

	output = {
		"responses": responses_data,
		"events": [event.to_dict() for event in event_map.values()],
		"peeps": [peep.to_dict() for peep in updated_peeps],
	}
	save_json(output, output_json_path)

def extract_events(rows):
	"""Parse 'Event:' rows from responses to construct Event objects."""
	event_map = {}
	event_counter = 0

	for row in rows:
		name = row.get("Name", "")
		if not name or not name.startswith("Event:"):
			continue

		parts = name.split("Event: ", 1)
		if len(parts) < 2 or not parts[1]:
			raise ValueError(f"Malformed event row: missing date in 'Name' field -> {name}")

		date_str = parts[1].strip()
		event_date = parse_event_date(date_str)

		duration_str = row.get("Event Duration", "")
		if not duration_str:
			raise ValueError(f"Missing Event Duration for event row: {name}")
		try:
			duration = int(duration_str)
		except ValueError:
			raise ValueError(f"Invalid Event Duration value: {duration_str}")
		if duration not in constants.CLASS_CONFIG:
			raise ValueError(f"Duration {duration} not in CLASS_CONFIG")

		event = Event.from_dict({
			"id": event_counter,
			"date": event_date,
			"duration_minutes": duration,
		})
		event.id = event_counter
		event_map[event_date] = event
		event_counter += 1

	return event_map

def process_responses(rows, peeps, event_map):
	"""Update peep objects from responses and return updated peeps and response summaries."""
	responses_data = []

	for row in rows:
		name = row.get("Name", "")
		if not name or name.startswith("Event:"):
			continue

		email = row.get("Email Address", "").lower()
		if not email:
			raise ValueError(f"Missing email for row: {name}")

		peep = next((p for p in peeps if p.email.lower() == email), None)
		if not peep:
			raise ValueError(f"No matching peep found for email: {email} (row: {name})")
		if not peep.active: 
			raise ValueError(f"Response from inactive peep: {peep.full_name} (ID {peep.id}) — please activate them in the members spreadsheet.")

		peep.role = Role(normalize_role(row["Primary Role"]))
		peep.event_limit = int(row["Max Sessions"])
		peep.min_interval_days = int(row.get("Min Interval Days", 0))
		peep.switch_pref = SwitchPreference.from_string(row["Secondary Role"])
		peep.responded = True

		available_strs = [s for s in row.get("Availability", "").split(",") if s]
		for date_str in available_strs:
			date_id = parse_event_date(date_str)
			event = event_map.get(date_id)
			if not event:
				raise ValueError(f"{name} listed availability for unknown event: {date_id}")
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

def parse_event_date(date_str):
	"""
	Parse an event date string and return a formatted datetime string.
	Assumes the event is in the current year.
	TODO: fix for next year if date has passed, but right now we're testing
	with old dates.

	Expected input format: "Weekday Month Day - H[AM/PM]" (e.g., "March 5 - 4PM")
	Output format: "YYYY-MM-DD HH:MM"
	"""
	date_str = date_str.split('(')[0].strip()
	dt = datetime.datetime.strptime(date_str, constants.DATESTR_FORMAT)
	dt = dt.replace(year=datetime.datetime.now().year)
	return dt.strftime("%Y-%m-%d %H:%M")

 
def normalize_role(value: str) -> str:
	""" 
	Transitional input normalizer for CSV/web inputs
	Accepts legacy aliases during the migration window.
	"""
	v = value.strip().lower()
	# temporary alias support while CSVs still say "lead"/"follow"
	if v == "lead":
		return "leader"
	if v == "follow":
		return "follower"
	# canonical values
	if v in ("leader", "follower"):
		return v
	raise ValueError(f"Unknown role: {value!r}")
