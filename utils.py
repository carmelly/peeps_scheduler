from collections import defaultdict
import json
import csv
import logging
import os
import sys
import datetime 
import itertools
from constants import DATE_FORMAT, DATESTR_FORMAT
import constants
from models import EventSequence, Peep, Event, Role, SwitchPreference

def parse_event_date(date_str):
	"""
	Parse an event date string and return a formatted datetime string.
	Assumes the event is in the current year.
	TODO: fix for next year if date has passed, but right now we're testing
	with old dates.

	Expected input format: "Weekday Month Day - H[AM/PM]" (e.g., "March 5 - 4PM")
	Output format: "YYYY-MM-DD HH:MM"
	"""
	# Strip out any parenthetical content
	date_str = date_str.split('(')[0].strip()

	# Parse the cleaned string
	dt = datetime.datetime.strptime(date_str, DATESTR_FORMAT)

	# Use current year
	dt = dt.replace(year=datetime.datetime.now().year)

	return dt.strftime("%Y-%m-%d %H:%M")

# Load CSV data
def load_csv(filename, required_columns=[]):
	
	with open(filename, newline='', encoding='utf-8') as csvfile:
		reader = csv.DictReader(csvfile)
		missing = set(required_columns) - set(reader.fieldnames)
		if required_columns: 
			if missing:
				logging.critical(f"Missing required columns in {filename}: {missing}")
				sys.exit() 
		return list(reader)

def convert_to_json(response_csv_path, peeps_csv_path, output_json_path):
	# Load peeps
	with open(peeps_csv_path, "r", newline='', encoding="utf-8") as f:
		reader = csv.DictReader(f)
		peeps = [Peep.from_csv(row) for row in reader]

	# Load responses
	with open(response_csv_path, "r", newline='', encoding="utf-8") as f:
		reader = csv.DictReader(f)
		rows = list(reader)

	# First pass: extract event definitions
	event_map = {}
	event_counter = 0

	for row in rows:
		name = row["Name"].strip()
		if name.startswith("Event:"):
			parts = name.split("Event: ", 1)
			if len(parts) < 2 or not parts[1].strip():
				raise ValueError(f"Malformed event row: missing date in 'Name' field -> {name}")
			
			date_str = parts[1].strip()
			event_date = parse_event_date(date_str)

			duration_str = row.get("Event Duration", "").strip()
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
			event.id = event_counter  # assign unique ID
			event_counter += 1

			event_map[event_date] = event  # keyed by date string

	# Second pass: apply availability to peeps
	responses_data = []

	for row in rows:
		name = row["Name"].strip()
		if name.startswith("Event:"): #skip events
			continue

		email = row.get("Email Address", "").strip().lower()
		if not email:
			raise ValueError(f"Missing email for row: {name}")
		
		peep = next((p for p in peeps if p.email.lower() == email), None)
		if not peep:
			raise ValueError(f"No matching peep found for email: {email} (row: {name})")

		peep.role = Role.from_string(row['Primary Role'])
		peep.event_limit = int(row['Max Sessions'])
		peep.min_interval_days = int(row.get('Min Interval Days', 0))  # Default to 0 if not specified
		peep.switch_pref = SwitchPreference.from_string(row['Secondary Role'])
		peep.responded = True 
		
		available_str = row.get("Availability", "").split(",")
		available_str = [s.strip() for s in available_str if s.strip()]
		for date_str in available_str:
			date_id  = parse_event_date(date_str)
			event = event_map.get(date_id)
			if not event:
				raise ValueError(f"{name} listed availability for unknown event: {date_id}")
			peep.availability.append(event.id)

		responses_data.append({
			"timestamp": row['Timestamp'],
			"name": name,
			"role": peep.role,
			"switch_pref": peep.switch_pref, 
			"max_sessions": peep.event_limit,
			"available_dates": available_str,
		})
	
	# Output to JSON
	output = {
		"responses": responses_data,
		"events": [event.to_dict() for event in event_map.values()],
		"peeps": [peep.to_dict() for peep in peeps]
	}
	save_json(output, output_json_path)

def generate_event_permutations(events):
	"""Generates all possible permutations of event sequences as a list of event ids."""

	if not events:
		return []
	event_ids = [event.id for event in events]
	index_sequences = list(itertools.permutations(event_ids, len(event_ids)))

	logging.debug(f"Total permutations: {len(index_sequences)}")
	return index_sequences

def generate_test_data(num_events, num_peeps, output_filename):

	# Generate events
	start_date = datetime.date.today()
	events = [Event.generate_test_event(i, start_date) for i in range(num_events)]
	event_ids = [event.id for event in events]

	# Generate peeps
	peeps = [Peep.generate_test_peep(i, i-1, event_ids) for i in range(num_peeps)]
	# sort by priority and fix index 
	peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)
	for i, peep in enumerate(peeps): 
		peep.index = i 

	# Generate dummy responses (optional, for completeness)
	responses = []
	for peep in peeps:
		responses.append({
			"timestamp": datetime.datetime.now().strftime(DATE_FORMAT),
			"name": peep.name,
			"preferred_role": peep.role,
			"max_sessions": peep.event_limit,
			"available_dates": [event.formatted_date() for event in events if event.id in peep.availability],
		})

	# Format output JSON (matches output.json structure)
	output = {
		"responses": responses,
		"events": [event.to_dict() for event in events],
		"peeps": [peep.to_dict() for peep in peeps]
	}

	save_json(output, output_filename)
	logging.info(f"Generated test data saved to {output_filename}.")

def load_data_from_json(filename):
	json_data = load_json(filename)
	response_data = json_data['responses'] # don't really need this but could help debugging
	event_data = json_data['events']
	peeps_data = json_data['peeps']

	events = [Event.from_dict(e) for e in event_data] if event_data else []
	peeps = [Peep(**p) for p in peeps_data] if peeps_data else []

	# sort peeps by their current index from the csv, which represents
	# their current order in the priority queue
	sorted_peeps = sorted(peeps, key=lambda peep: peep.index)
	assert is_sorted_by_priority(peeps), "Peeps are not sorted by priority; check input file." 

	return sorted_peeps, events

def is_sorted_by_priority(peeps): 
	 return all(peeps[i].priority >= peeps[i + 1].priority for i in range(len(peeps)-1))

def save_json(data, filename):
	"""Save data to a JSON file, handling Enums and datetime."""
	def custom_serializer(obj):
		if hasattr(obj, "value"):  # For Enums like Role
			return obj.value
		if isinstance(obj, datetime.datetime):
			return obj.strftime(DATE_FORMAT)
		if isinstance(obj, datetime.date):
			return obj.isoformat()
		return str(obj)  # Fallback for other non-serializable objects
	
	with open(filename, "w") as f:
		json.dump(data, f, indent=4, default=custom_serializer)


def load_json(filename):
	"""Load data from a JSON file."""
	try:
		with open(filename, "r") as f:
			return json.load(f)
	except FileNotFoundError:
		return None


def setup_logging(verbose=False):
	stream_log_level = logging.DEBUG if verbose else logging.INFO
	
	# stream level is set by the verbose arg 
	stream_handler = logging.StreamHandler()
	stream_handler.setLevel(stream_log_level)

	# file level is alway DEBUG
	file_handler = logging.FileHandler('debug.log')
	file_handler.setLevel(logging.DEBUG)

	logging.basicConfig(
		level=logging.DEBUG,
		format='%(asctime)s - %(levelname)s - %(message)s',
		handlers=[stream_handler, file_handler]
		)

def save_event_sequence(sequence: EventSequence, filename):
	data = {
		"valid_events": [
			{
				"id": event.id,
				"date": event.date.strftime(DATE_FORMAT),
				"duration_minutes": event.duration_minutes, 
				"attendees": [
					{
						"id": peep.id,
						"name": peep.name,
						"role": peep.role.value
					}
					for peep in event.attendees
				],
				"alternates": [
					{
						"id": peep.id,
						"name": peep.name,
						"role": peep.role.value
					}
					for peep in event.alt_leaders + event.alt_followers
				],
				"leaders_string": event.get_participants_str(Role.LEADER), 
				"followers_string": event.get_participants_str(Role.FOLLOWER), 

			}
			for event in sequence.valid_events
		],
		"peeps": [peep.to_dict() for peep in sequence.peeps],
		"num_unique_attendees": sequence.num_unique_attendees,
		"system_weight": sequence.system_weight
	}
	save_json(data, filename)
	logging.info(f"Saved event sequence to {filename}")

def apply_event_results( result_json, members_csv):
	from models import Peep, Event

	peep_rows = load_csv(members_csv)
	fresh_peeps = []
	for row in peep_rows:
		peep = Peep(
			id=row['id'],
			name=row['Name'],
			display_name=row['Display Name'],
			email=row['Email Address'],
			role=row['Role'],
			index=int(row['Index']),
			priority=int(row['Priority']),
			total_attended=int(row['Total Attended']),
			availability=[],
			event_limit=0,
			min_interval_days=0, 
			active = row['Active'], 
			date_joined = row['Date Joined']
		)
		fresh_peeps.append(peep)

	with open(result_json, "r") as f:
		result_data = json.load(f)

	event_data = result_data['valid_events']
	events = []
	for e in event_data:
		event = Event(
			id=e['id'],
			duration_minutes=e['duration_minutes'], 
			date=datetime.datetime.strptime(e['date'], DATE_FORMAT),
			min_role=0,
			max_role=0
		)
		for peep_info in e['attendees']:
			for peep in fresh_peeps:
				if peep.id == peep_info['id']:
					role = Role.from_string(peep_info['role'])
					event.add_attendee(peep, role)
		events.append(event)

	sequence = EventSequence(events, fresh_peeps)
	sequence.valid_events = events  # Mark them valid (since they came from results.json)
	
	# Only update actual attendees, alts are not considered now 
	for event in sequence.valid_events:
		Peep.update_event_attendees(fresh_peeps, event)
	sequence.finalize() 
	
	return sequence.peeps

def save_peeps_csv(peeps, filename):
	"""Save updated peeps to a new CSV called members_updated.csv in the same folder as the original."""
	filename = os.path.join(os.path.dirname(filename), "members_updated.csv")
	
	fieldnames = ['id', 'Name', 'Display Name', 'Email Address', 'Role', 'Index', 'Priority', 'Total Attended', 'Active', 'Date Joined']
	with open(filename, "w", newline='', encoding='utf-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for peep in peeps:
			writer.writerow({
				'id': peep.id,
				'Name': peep.full_name,
				'Display Name': peep.display_name, 
				'Email Address': peep.email, 
				'Role': peep.role.value,
				'Index': peep.index,
				'Priority': peep.priority,
				'Total Attended': peep.total_attended, 
				'Active': peep.active,
				'Date Joined': peep.date_joined
			})
	logging.info(f"Updated peeps saved to {filename}")
