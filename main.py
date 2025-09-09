import os
import argparse
import logging
import utils
from scheduler import Scheduler
from db_imports import import_members, import_responses, import_period 

def apply_results(results_file, members_file):
	logging.info(f"Applying {results_file} to update {members_file}")
	# Apply results to fresh member list
	updated_peeps = utils.apply_event_results(results_file, members_file)
	utils.save_peeps_csv(updated_peeps, members_file)
	logging.info("Updated members.csv ready for Google Sheets upload.")

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
	apply_parser = subparsers.add_parser('apply-results', help='Apply final results to members CSV')
	apply_parser.add_argument('--results-file', required=True, help='Path to results JSON file')
	apply_parser.add_argument('--members-file', required=True, help='Path to members CSV')

	# Import entire period to db command 
	import_parser = subparsers.add_parser('import-period', help='Import all files for scheduling period')
	import_parser.add_argument('--slug', required=True, help='slug for scheduling period')
	import_parser.add_argument('--dry-run', action='store_true', help='Preview changes without modifying the database')

	args = parser.parse_args()
	utils.setup_logging(verbose=args.verbose)

	# Routing logic
	if args.command == 'run':
		scheduler = Scheduler(data_folder=args.data_folder, max_events=args.max_events)
		scheduler.run(generate_test_data=args.generate_tests, load_from_csv=args.load_from_csv)
	elif args.command == 'apply-results':
		apply_results(args.results_file, args.members_file)
	elif args.command == 'import-period':
		import_period(args.slug, dry_run=args.dry_run)
	else:
		parser.print_help()
		
if __name__ == "__main__":
	main()
