from collections import defaultdict
import json
import logging
import datetime 
import itertools
from constants import DATE_FORMAT, DATESTR_FORMAT
from file_io import load_csv, save_json
from models import EventSequence, Peep, Event, Role, SwitchPreference

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


