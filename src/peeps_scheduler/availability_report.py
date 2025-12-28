from collections import defaultdict
from peeps_scheduler.data_manager import DataManager, get_data_manager
from peeps_scheduler.models import Role, SwitchPreference
from peeps_scheduler.file_io import load_cancellations, load_csv, normalize_email, parse_event_date

def parse_availability(responses_file, members_file, cancelled_event_ids=None, cancelled_availability=None, year=None):
	members = {normalize_email(row["Email Address"]): row for row in load_csv(members_file)}
	cancelled_event_ids = cancelled_event_ids or set()
	cancelled_availability = cancelled_availability or {}
	availability = defaultdict(lambda: {"leader": [], "follower": [], "leader_fill": [], "follower_fill": []})
	unavailable = []
	responders = set()

	# Validate cancelled availability emails exist
	unknown_emails = set(cancelled_availability.keys()) - set(members.keys())
	if unknown_emails:
		raise ValueError(f"unknown email(s) in cancelled availability: {sorted(unknown_emails)}")

	# Collect all events from responses
	all_event_ids = set()
	for row in load_csv(str(responses_file)):
		dates = [d.strip() for d in row.get("Availability", "").split(",") if d.strip()]
		for date in dates:
			try:
				event_id, _, _ = parse_event_date(date, year=year)
				all_event_ids.add(event_id)
			except Exception as e:
				raise ValueError(f"cannot parse availability date '{date}': {e}") from e

	# Validate cancelled events exist
	unknown_cancelled = cancelled_event_ids - all_event_ids
	if unknown_cancelled:
		raise ValueError(f"cancelled events not found in responses: {sorted(unknown_cancelled)}")

	# Validate cancelled availability events exist
	for email, event_ids in cancelled_availability.items():
		unknown_event_ids = event_ids - all_event_ids
		if unknown_event_ids:
			raise ValueError(
				f"cancelled availability events not found in responses for {email}: {sorted(unknown_event_ids)}"
			)

	# Build cancelled availability details for display
	cancelled_availability_details = {}
	for email, event_ids in cancelled_availability.items():
		member = members.get(email)
		if member:
			display_name = member["Display Name"]
			cancelled_availability_details[display_name] = sorted(event_ids)

	# Process availability from responses
	for row in load_csv(str(responses_file)):
		email = normalize_email(row["Email Address"])
		role = Role.from_string(row["Primary Role"].strip())
		switch_pref = SwitchPreference.from_string(row["Secondary Role"].strip())
		dates = [d.strip() for d in row.get("Availability", "").split(",") if d.strip()]

		member = members.get(email)
		if not member:
			print(f"WARNING: Skipping unmatched email: {email}")
			continue
		if email in responders:
			print(f"WARNING: Duplicate email: {email}")
			continue
		responders.add(email)

		# Filter out cancelled events
		available_dates = []
		for date in dates:
			event_id, _, _ = parse_event_date(date, year=year)
			# Skip cancelled events and cancelled availability for this person
			if event_id not in cancelled_event_ids and event_id not in cancelled_availability.get(email, set()):
				available_dates.append(date)

		# If no available dates after filtering, mark as unavailable
		if not available_dates:
			unavailable.append(member["Display Name"])
			continue

		# Add person to each available date
		for date in available_dates:
			availability[date][role.value].append(member["Display Name"])
			if switch_pref != SwitchPreference.PRIMARY_ONLY:
				availability[date][f"{role.opposite().value}_fill"].append(member["Display Name"])

	non_responders = [
		member["Display Name"] for email, member in members.items()
		if email not in responders and member.get("Active", "TRUE").upper() == "TRUE"
	]

	return availability, unavailable, non_responders, cancelled_event_ids, cancelled_availability_details

def print_availability(availability, unavailable, non_responders, year=None, cancelled_events=None, cancelled_availability_details=None):
	cancelled_events = cancelled_events or set()
	cancelled_availability_details = cancelled_availability_details or {}

	print("="*80)
	print("AVAILABILITY REPORT")
	print("="*80)

	if cancelled_events or cancelled_availability_details:
		print()

		# Show cancelled events first
		if cancelled_events:
			print("CANCELLED EVENTS:")
			for event_id in sorted(cancelled_events):
				print(f"  - {event_id}")

		# Show cancelled availability
		if cancelled_availability_details:
			print("\nCANCELLED AVAILABILITY (excluded from above):")
			for name in sorted(cancelled_availability_details.keys()):
				events = cancelled_availability_details[name]
				events_str = ", ".join(sorted(events))
				print(f"  - {name}: {events_str}")

	for date in sorted(availability.keys(), key=lambda d:parse_event_date(d, year=year)[0]):
		print(f"\n{date}")
		print(f"    Leaders  ({len(availability[date]['leader'])}): {', '.join(availability[date]['leader'])} ( + {', '.join(availability[date]['leader_fill'])})")
		print(f"    Followers({len(availability[date]['follower'])}): {', '.join(availability[date]['follower'])} ( + {', '.join(availability[date]['follower_fill'])})")

	print("\nNo availability:")
	for name in sorted(unavailable):
		print(f"  - {name}")

	print("\nDid not respond:")
	for name in sorted(non_responders):
		print(f"  - {name}")

def run_availability_report(data_folder, cancellations_file='cancellations.json'):
	"""Generate and print availability report for a given data period."""
	dm = get_data_manager()
	period_path = dm.get_period_path(data_folder)
	responses_file = period_path / "responses.csv"
	members_file = period_path / "members.csv"

	# Extract year from data_folder (e.g., "2026-01" -> 2026)
	# Handle both absolute paths and folder names
	from pathlib import Path
	folder_name = Path(data_folder).name
	try:
		year = int(folder_name[:4]) if folder_name and len(folder_name) >= 4 and folder_name[:4].isdigit() else None
	except (ValueError, TypeError):
		year = None

	cancellations_path = period_path / cancellations_file
	cancelled_event_ids, cancelled_availability = load_cancellations(str(cancellations_path), year=year)
	availability, unavailable, non_responders, cancelled_events, cancelled_availability_details = parse_availability(
		responses_file,
		members_file,
		cancelled_event_ids=cancelled_event_ids,
		cancelled_availability=cancelled_availability,
		year=year
	)

	print_availability(
		availability,
		unavailable,
		non_responders,
		year=year,
		cancelled_events=cancelled_events,
		cancelled_availability_details=cancelled_availability_details
	)
