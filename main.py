import os
import argparse
import logging
import utils
from scheduler import Scheduler

def apply_results(period_folder):
	actual_attendance_file = os.path.join(period_folder, "actual_attendance.json")
	members_file = os.path.join(period_folder, "members.csv")
	responses_file = os.path.join(period_folder, "responses.csv")

	# Check that required files exist
	if not os.path.exists(actual_attendance_file):
		logging.error(f"Actual attendance file not found: {actual_attendance_file}")
		return False
	if not os.path.exists(members_file):
		logging.error(f"Members file not found: {members_file}")
		return False

	# responses.csv is optional but we'll warn if missing
	if not os.path.exists(responses_file):
		logging.warning(f"Responses file not found: {responses_file} - priority will not be updated for non-attendees who responded")
		responses_file = None

	logging.info(f"Applying {actual_attendance_file} to update {members_file}")
	if responses_file:
		logging.info(f"Using responses file: {responses_file}")

	# Apply results to fresh member list
	updated_peeps = utils.apply_event_results(actual_attendance_file, members_file, responses_file)
	from file_io import save_peeps_csv
	save_peeps_csv(updated_peeps, members_file)
	logging.info("Updated members.csv ready for Google Sheets upload.")
	return True

def main():
	
	
	# Default from environment if available
	default_data_folder = os.getenv("DATA_FOLDER")

	parser = argparse.ArgumentParser(description="Peeps Event Scheduler CLI")
	parser.add_argument('--verbose', action='store_true', help='Enable verbose (DEBUG) logging')
	
	subparsers = parser.add_subparsers(dest='command')
	
	# Run command
	run_parser = subparsers.add_parser('run', help='Run the scheduler')
	run_parser.add_argument('--generate-tests', action='store_true', help='Generate test data')
	run_parser.add_argument('--load-from-csv', action='store_true', help='Load data from CSV')
	run_parser.add_argument('--data-folder', type=str, default=default_data_folder, required=(default_data_folder is None), help='Path to data folder')
	run_parser.add_argument('--max-events', type=int, default=7, help='Maximum number of events to schedule')

	# Apply results command
	apply_parser = subparsers.add_parser('apply-results', help='Apply actual attendance to update members CSV')
	apply_parser.add_argument('--period-folder', required=True, help='Path to period folder containing actual_attendance.json, members.csv, and responses.csv')

	args = parser.parse_args()
	utils.setup_logging(verbose=args.verbose)

	# Routing logic
	if args.command == 'run':
		scheduler = Scheduler(data_folder=args.data_folder, max_events=args.max_events)
		scheduler.run(generate_test_data=args.generate_tests, load_from_csv=args.load_from_csv)
	elif args.command == 'apply-results':
		apply_results(args.period_folder)
	else:
		parser.print_help()
		
if __name__ == "__main__":
	main()
