import json
import logging
from models import Peep, Event
import datetime
import itertools

def generate_event_permutations(events):
    """Generates all possible permutations of event sequences as a list of event ids."""
    
    if not events:
        return []
    event_ids = [event.id for event in events]
    index_sequences = list(itertools.permutations(event_ids, len(event_ids)))

    logging.debug(f"Total permutations: {len(index_sequences)}")
    return index_sequences

def initialize_data (generate_events=True, generate_peeps=True):
	num_events = 10
	num_peeps = 30
	event_filename = "test_events.json"
	peep_filename = "test_peeps.json"

  # Load or generate events
	if generate_events:
		start_date = datetime.date.today()
		events = [Event.generate_test_event(i, start_date) for i in range(num_events)]
		save_json([event.to_dict() for event in events], event_filename)
		logging.info(f"Saved {len(events)} Events to {event_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		event_data = load_json(event_filename)
		events = [Event.from_dict(e) for e in event_data] if event_data else []

	# Load or generate peeps
	if generate_peeps:
		peeps = [Peep.generate_test_peep(i, i - 1, num_events) for i in range(num_peeps)]
		save_json([peep.__dict__ for peep in peeps], peep_filename)
		logging.info(f"Saved {len(peeps)} Peeps to {peep_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		peep_data = load_json(peep_filename)
		peeps = [Peep(**p) for p in peep_data] if peep_data else []

	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)
	return sorted_peeps, events

def initialize_data_from_json(): 
	output_json = 'data/novice_peeps_output.json'

	json_data = load_json(output_json)
	response_data = json_data['responses'] # don't really need this but could help debugging 
	event_data = json_data['events']
	peeps_data = json_data['peeps']
		
	events = [Event.from_dict(e) for e in event_data] if event_data else []
	peeps = [Peep(**p) for p in peeps_data] if peeps_data else []

	# sort peeps by their current index from the csv, which represents 
	# their current order in the priority queue 
	# TODO: if we want to be super crazy we can check that the priorities are in descending order 
	sorted_peeps = sorted(peeps, key=lambda peep: peep.index)
	return sorted_peeps, events

def save_json(data, filename):
	"""Save data to a JSON file."""
	with open(filename, "w") as f:
		json.dump(data, f, indent=4)


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
