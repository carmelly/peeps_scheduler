import argparse
import copy
import logging
import time
from globals import Globals
from models import Role, Peep, Event, EventSequence
import utils

def evaluate_all_event_sequences(og_peeps, og_events):
	"""Generates and evaluates all possible event sequences based on peep availability and role limits."""

	def balance_roles(event, winners):
		"""Ensures leaders and followers are balanced within an event."""
		leaders = event.leaders
		followers = event.followers
		if len(leaders) != len(followers):
			larger_group = leaders if len(leaders) > len(followers) else followers
			while len(leaders) != len(followers):
				if not larger_group:
					logging.warning(f"Unable to balance roles for event {event.id}.")
					break
				#TODO: save removed to an alternates list
				peep_to_remove = larger_group.pop()
				event.attendees = [entry for entry in event.attendees if entry[0] != peep_to_remove]
				winners.remove(peep_to_remove)

		assert len(event.leaders) == len(event.followers) >= event.min_role
		assert len(event.leaders) <= event.max_role

	def evaluate_sequence(sequence):
		"""Evaluates an event sequence by assigning peeps to events and updating priorities and list order."""
		for event in sequence.events:
			winners = []
			losers = []

			# Add peeps to event
			for peep in sequence.peeps:
				if peep.can_attend(event):
					event.add_attendee(peep)
					winners.append(peep)
				else:
					losers.append(peep)

			if event.is_valid():  # If we have enough to fill the event
				balance_roles(event, winners)
				Peep.update_event_attendees(sequence.peeps, winners, event)
				sequence.valid_events.append(event)
			else:
				event.attendees.clear()

		# End of sequence, update peeps who didn't make it
		sequence.finalize()

	# Create all event permutations
	event_perm = utils.generate_event_permutations(og_events)
	event_map = {event.id: event for event in og_events}  # Faster lookup
	event_sequences = []

	for perm in event_perm:
		# Create an EventSequence object for this permutation
		events = [copy.deepcopy(event_map[id]) for id in perm]
		event_sequence = EventSequence(events, copy.deepcopy(og_peeps))

		# Evaluate the EventSequence
		evaluate_sequence(event_sequence)

		# Only include sequences with valid events
		if event_sequence.valid_events:
			event_sequences.append(event_sequence)

	return event_sequences

def apply_results(results_file, members_file):
	logging.info(f"Applying {results_file} to update {members_file}")
	# Apply results to fresh member list
	updated_peeps = utils.apply_event_results(results_file, members_file)
	utils.save_peeps_csv(updated_peeps, members_file)
	logging.info("Updated members.csv ready for Google Sheets upload.")

def run_scheduler(generate_test_data, load_from_csv):
	# should we generate new lists? otherwise read from file
	data_folder = Globals.data_folder
	output_json = f'data/{data_folder}/output.json'
	if generate_test_data: 
		output_json = f'data/test_data/output.json'
		logging.info(f"Generating test data and saving to {output_json}")
		utils.generate_test_data(5, 30, output_json)
	elif load_from_csv: 
		responses_csv = f'data/{Globals.data_folder}/responses.csv'
		peeps_csv = f'data/{Globals.data_folder}/members.csv'
		output_json = f'data/{Globals.data_folder}/output.json'
		logging.info(f"Loading data from {peeps_csv} and {responses_csv} and saving to {output_json}")
		utils.convert_to_json(responses_csv, peeps_csv, output_json)
		
	# load from json, which already sorts by index and checks priority order
	logging.info(f"Loading data from {output_json}")
	peeps, events = utils.load_data_from_json(output_json)

	logging.debug("Initial Peeps")
	logging.debug(Peep.peeps_str(peeps))

	# remove events from consideration that dont have enough peeps with availability 
	sanitized_events = Event.sanitize_events(events, peeps)
	logging.info(f"Sanitized Events: {len(sanitized_events)}/{len(events)}")

	if len(sanitized_events) > Globals.max_events:
		logging.warning(
			f"Found {len(sanitized_events)} events with enough available peeps; this results in too many permutations to compute. "
			f"Removing events with high overlap and low priority."
			)
		sanitized_events = Event.remove_high_overlap_events(sanitized_events, peeps, Globals.max_events)

	# process all event sequences, assigning peeps in order and determining valid events
	logging.info(f"Evaluating all sequences...")
	start_time = time.perf_counter()  # Start timing
	event_sequences = evaluate_all_event_sequences(peeps, sanitized_events)
	end_time = time.perf_counter()  # End timing
	elapsed_time = end_time - start_time
	logging.info(f"Evaluation complete. Elapsed time: {elapsed_time:.2f} seconds")

	# remove duplicate sequences
	unique_sequences = EventSequence.get_unique_sequences(event_sequences)
	logging.info(f"Found {len(unique_sequences)} unique sequences")

	# sort by unique attendees (desc) and system weight (desc)
	#TODO: another tiebreaker could be total spaces filled, because peeps that get to go more than once wouldn't be counted in unique_attendees 
	sorted_unique = sorted(unique_sequences, key=lambda sequence: (-sequence.num_unique_attendees, -sequence.system_weight))

	best_sequence = sorted_unique[0] if sorted_unique else None
	if best_sequence:
		#TODO: instead of logging to terminal and file, add a command to print results.json in a readable format separately
		logging.info(f"Best {best_sequence}")
		# Save results
		result_json = f"data/{data_folder}/results.json"
		utils.save_event_sequence(best_sequence, result_json)

		logging.debug("Final Peeps:")
		logging.debug(Peep.peeps_str(best_sequence.peeps))
	else:
		logging.info(f"No sequence found; couldn't fill any events.")

def main():
	utils.setup_logging() 

	parser = argparse.ArgumentParser(description="Peeps Event Scheduler CLI")
	subparsers = parser.add_subparsers(dest='command')

	# Run command
	run_parser = subparsers.add_parser('run', help='Run the scheduler')
	run_parser.add_argument('--generate-tests', action='store_true', help='Generate test data')
	run_parser.add_argument('--load-from-csv', action='store_true', help='Load data from CSV')

	# Apply results command
	apply_parser = subparsers.add_parser('apply-results', help='Apply final results to members CSV')
	apply_parser.add_argument('--results-file', required=True, help='Path to results JSON file')
	apply_parser.add_argument('--members-file', required=True, help='Path to members CSV')

	args = parser.parse_args()

	# Routing logic
	if args.command == 'run':
		run_scheduler(generate_test_data=args.generate_tests, load_from_csv=args.load_from_csv)
	elif args.command == 'apply-results':
		apply_results(args.results_file, args.members_file)
	else:
		parser.print_help()
		
if __name__ == "__main__":
	main()
