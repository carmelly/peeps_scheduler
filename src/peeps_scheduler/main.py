import os
import argparse
import logging
from peeps_scheduler import utils
from peeps_scheduler.scheduler import Scheduler
from peeps_scheduler.data_manager import get_data_manager

def apply_results(period_folder, results_filename="actual_attendance.json", logger=None):
    """
    Apply actual attendance results to update members CSV.

    Args:
            period_folder: Path to period folder containing files
            results_filename: Name of results JSON file (default: actual_attendance.json)
            logger: Logger instance (optional, will use root logger if not provided)
    """
    if logger is None:
        logger = logging.getLogger("cli")

    dm = get_data_manager()
    period_path = dm.get_period_path(period_folder)
    actual_attendance_file = os.path.join(period_path, results_filename)
    members_file = os.path.join(period_path, "members.csv")
    responses_file = os.path.join(period_path, "responses.csv")

    # Check that required files exist
    if not os.path.exists(actual_attendance_file):
        logger.error(f"Actual attendance file not found: {actual_attendance_file}")
        return False
    if not os.path.exists(members_file):
        logger.error(f"Members file not found: {members_file}")
        return False

    # responses.csv is optional but we'll warn if missing
    if not os.path.exists(responses_file):
        logger.warning(
            f"Responses file not found: {responses_file} - priority will not be updated for non-attendees who responded"
        )
        responses_file = None

    logger.info(f"Applying {actual_attendance_file} to update {members_file}")
    if responses_file:
        logger.info(f"Using responses file: {responses_file}")

    # Apply results to fresh member list
    updated_peeps = utils.apply_event_results(actual_attendance_file, members_file, responses_file)
    from peeps_scheduler.file_io import save_peeps_csv

    save_peeps_csv(updated_peeps, members_file)
    logger.info("Updated members.csv ready for Google Sheets upload.")
    return True

def main():
    # Default from environment if available
    default_data_folder = os.getenv("DATA_FOLDER")

    parser = argparse.ArgumentParser(description="Peeps Event Scheduler CLI")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose (DEBUG) logging")

    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the scheduler")
    run_parser.add_argument("--generate-tests", action="store_true", help="Generate test data")
    run_parser.add_argument("--load-from-csv", action="store_true", help="Load data from CSV")
    run_parser.add_argument(
        "--data-folder",
        type=str,
        default=default_data_folder,
        required=(default_data_folder is None),
        help="Path to data folder",
    )
    run_parser.add_argument(
        "--max-events", type=int, default=7, help="Maximum number of events to schedule"
    )
    run_parser.add_argument(
        "--cancellations-file",
        type=str,
        default="cancellations.json",
        help="Filename of cancellations JSON (default: cancellations.json)",
    )
    run_parser.add_argument(
        "--partnerships-file",
        type=str,
        default="partnerships.json",
        help="Filename of partnerships JSON (default: partnerships.json)",
    )

    # Apply results command
    apply_parser = subparsers.add_parser(
        "apply-results", help="Apply actual attendance to update members CSV"
    )
    apply_parser.add_argument(
        "--period-folder",
        required=True,
        help="Path to period folder containing actual_attendance.json, members.csv, and responses.csv",
    )
    apply_parser.add_argument(
        "--results-file",
        default="actual_attendance.json",
        help="Filename of results JSON (default: actual_attendance.json)",
    )

    # Availability report command
    availability_parser = subparsers.add_parser(
        "availability-report", help="Generate availability report from responses"
    )
    availability_parser.add_argument(
        "--data-folder",
        type=str,
        default=default_data_folder,
        required=(default_data_folder is None),
        help="Path to data folder",
    )
    availability_parser.add_argument(
        "--cancellations-file",
        type=str,
        default="cancellations.json",
        help="Filename of cancellations JSON (default: cancellations.json)",
    )

    args = parser.parse_args()
    logger = utils.setup_logging(verbose=args.verbose)

    # Routing logic
    if args.command == "run":
        scheduler = Scheduler(
            data_folder=args.data_folder,
            max_events=args.max_events,
            cancellations_file=args.cancellations_file,
            partnerships_file=args.partnerships_file,
        )
        scheduler.run(generate_test_data=args.generate_tests, load_from_csv=args.load_from_csv)
    elif args.command == "apply-results":
        apply_results(args.period_folder, args.results_file, logger=logger)
    elif args.command == "availability-report":
        from peeps_scheduler.availability_report import run_availability_report

        run_availability_report(args.data_folder, cancellations_file=args.cancellations_file)
    else:
        parser.print_help()
		
if __name__ == "__main__":
	main()
