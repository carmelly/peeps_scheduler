from collections import defaultdict
import json
import logging
import datetime
import itertools
from peeps_scheduler.constants import DATE_FORMAT, DATESTR_FORMAT
from peeps_scheduler.file_io import load_csv, save_json, normalize_email
from peeps_scheduler.models import EventSequence, Peep, Event, Role, SwitchPreference

def generate_event_permutations(events):
	"""Generates all possible permutations of event sequences as a list of event ids."""

	if not events:
		return []
	event_ids = [event.id for event in events]
	index_sequences = list(itertools.permutations(event_ids, len(event_ids)))

	logging.debug(f"Total permutations: {len(index_sequences)}")
	return index_sequences

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

def apply_event_results( result_json, members_csv, responses_csv):
	from peeps_scheduler.models import Peep, Event
	import os

	peep_rows = load_csv(members_csv)
	fresh_peeps = []
	for row in peep_rows:
		peep = Peep(
			id=row['id'],
			full_name=row['Name'],
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

	# Process responses to mark who responded
	responded_emails = set()
	if responses_csv and os.path.exists(responses_csv):
		response_rows = load_csv(responses_csv)
		for row in response_rows:
			email = normalize_email(row.get('Email Address', ''))
			if email:  # Only add non-empty emails
				responded_emails.add(email)
		logging.debug(f"Found {len(responded_emails)} unique respondents in {responses_csv}")
	else: 
		logging.debug("No responses file provided or file does not exist; skipping response processing.")

	# Set responded flag based on email match
	for peep in fresh_peeps:
		if peep.email and normalize_email(peep.email) in responded_emails:
			peep.responded = True
			logging.debug(f"Marked peep {peep.id} ({peep.email}) as responded")
		else:
			peep.responded = False

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


