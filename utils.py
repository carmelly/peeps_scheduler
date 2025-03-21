import json
import csv
import logging
from globals import Globals
from models import Peep, Event, Role
import datetime 
import itertools

def parse_event_date(date_str):
	"""
	Parse an event date string and return a formatted datetime string.
	Assumes the event is in the current year.
	TODO: fix for next year if date has passed, but right now we're testing
	with old dates.

	Expected input format: "Weeday Month Day - H[AM/PM]" (e.g., "March 5 - 4PM")
	Output format: "YYYY-MM-DD HH:MM"
	"""
	dt = datetime.datetime.strptime(f"{date_str}",f"{Globals.datestr_format}" )
	dt = dt.replace(year=datetime.datetime.now().year)
	return dt.strftime("%Y-%m-%d %H:%M")

# Load CSV data
def load_csv(filename):
	with open(filename, newline='', encoding='utf-8') as csvfile:
		return list(csv.DictReader(csvfile))

def convert_to_json(responses_file, members_file, output_file):
	peeps_data = load_csv(members_file)
	responses_data = load_csv(responses_file)

	unique_peeps = {}
	unique_events = {}
	jsonData = []
	event_counter = 0

	# Process members data
	for row in peeps_data:
		id, name, role, index, priority, total_attended = row['id'], row['Name'].strip(), row['Role'], row['Index'], row['Priority'], row['Total Attended']
		

		if id not in unique_peeps:
			unique_peeps[id] = {
				"id": id,
				"name": name,
				"role": role,
				"index": int(index),
				"priority": int(priority),
				"total_attended": int(total_attended),
				"availability": [],
			}

	# Process responses
	for row in responses_data:
		name, preferred_role, max_sessions, available_dates = row['Name'].strip(), row['Preferred Role'], row['Max Sessions'], row['Availability']
		min_interval_days = int(row.get('Min Interval Days', 0))  # Default to 0 if not specified
		matched_peeps = [peep for peep in unique_peeps.values() if peep['name'].lower() == name.lower()]

		if not matched_peeps:
			matched_peeps = [peep for peep in unique_peeps.values() if peep['name'].split()[0].lower() == name.split()[0].lower()]

		if len(matched_peeps) == 1:
			peep = matched_peeps[0]
			peep['event_limit'] = max_sessions
			peep['min_interval_days'] = min_interval_days	

			event_ids = []
			for event in available_dates.split(', '):
				if event:
					if event not in unique_events:
						unique_events[event] = {
							"id": event_counter,
							"date": parse_event_date(event),
						}
						event_counter += 1
					event_ids.append(unique_events[event]['id'])

			peep['availability'] = list(set(peep['availability'] + event_ids))
			jsonData.append({
				"timestamp": row['Timestamp'],
				"name": name,
				"preferred_role": preferred_role,
				"max_sessions": max_sessions,
				"available_dates": available_dates.split(', '),
			})
		else:
			print(f"Error: {len(matched_peeps)} matches found for '{name}', skipping.")

	output = {
		"responses": jsonData,
		"events": list(unique_events.values()),
		"peeps": list(unique_peeps.values())
	}

	with open(output_file, 'w', encoding='utf-8') as f:
		json.dump(output, f, indent=2)

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
			"timestamp": datetime.datetime.now().strftime(Globals.date_format),
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
			return obj.strftime(Globals.date_format)
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


def setup_logging():
	logging.basicConfig(level=logging.DEBUG,
			format='%(asctime)s - %(levelname)s - %(message)s',
			handlers=[
				logging.StreamHandler(),
				logging.FileHandler('debug.log')
			])

	logging.getLogger().handlers[0].setLevel(logging.INFO)
	logging.getLogger().handlers[1].setLevel(logging.DEBUG)
