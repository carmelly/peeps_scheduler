import json
import logging
from file_io import load_peeps, load_responses, parse_event_date
import utils
from managers import DbManager
from models import Peep

DB_PATH = "db/peeps_scheduler.db"

def import_period(slug: str, dry_run: bool = False):
	"""
	Run members + responses as one atomic import.
	If any step fails, everything rolls back (when not dry_run).
	"""
	members_path = f"db/imports/{slug}/data/members.csv"
	responses_path = f"db/imports/{slug}/data/responses.csv"

	with DbManager(DB_PATH) as db:
		with db.transaction(dry_run=dry_run):
			db_peeps = _import_members(db, members_path)
			period_id, db_events, db_responses = _import_responses(db, responses_path, "May 2025")

	results = { 
		"source_files": [
			members_path, 
			responses_path
		],
		"period_id": period_id, 
		"row_counts": {
			"peeps": db_peeps, 
			"events": db_events, 
			"responses": db_responses
		}
	}
	results_str = json.dumps(results, indent=2)
	pretty_results = results_str[1:-1].strip() 

	print('\nData for manifest')
	if dry_run:
		print('PREVIEW; rolled back')
	print('------------')
	print(f"  {pretty_results}")

	return results 

def _import_members(db: "DbManager", csv_path: str) -> int:
	"""
	Inner step: no transactions here. Uses the outer transaction in import_period.
	Raises on unexpected errors to trigger full rollback.
	"""
	peeps = load_peeps(csv_path)
	matched, updated, created = [], [], []

	for peep in peeps:
		result = db.upsert_peep(peep)  # "created" | "updated" | "unchanged"
		if result == "created":
			created.append(peep)
		elif result == "updated":
			updated.append(peep)
		else:
			matched.append(peep)

	db_total_peeps = db.count_peeps()

	print(f"📋 Import summary for {csv_path}:")
	print(f"   Matched (no changes): {len(matched)}")
	print(f"   Updated: {len(updated)}")
	print(f"   Created: {len(created)}")

	return db_total_peeps

def _import_responses(db: "DbManager", responses_csv_path: str, period_name: str) -> tuple[int, int, int]:
	"""
	Inner step: no transactions here. Uses the outer transaction in import_period.
	- Validates period name and aborts on duplicate
	- Builds Events from unique availability dates
	- Inserts SchedulePeriod, Events, Responses
	- Verifies counts and returns (period_id, events_count, responses_count)
	Raises ValueError for user-correctable issues to trigger full rollback.
	"""
	if not period_name:
		raise ValueError("❌ Schedule name is required.")
	elif db.get_period_id_by_name(period_name): 
		# Prevent duplicate period 
		raise ValueError(f"❌ SchedulePeriod '{period_name}' already exists.")

	# Expected CSV columns
	columns = ['Timestamp', 'Email Address', 'Name', 'Role', 'Min Interval', 'Max Sessions', 'Availability']
	rows = load_responses(responses_csv_path, columns)

	# Peeps lookup by email (lowercased)
	peep_lookup = {}
	for peep in db.get_all_peeps():
		if peep and peep.email:
			peep_lookup[peep.email.lower()] = peep

	# Build planned inserts
	event_map = {}  # date_str -> event dict with explicit id
	responses = []

	# Choose next event id once; we’re inside the outer txn
	next_event_id = db.next_event_id()

	for row in rows:
		email = (row["Email Address"] or "").strip().lower()
		name = (row["Name"] or "").strip()
		peep = peep_lookup.get(email)
		if not peep:
			raise ValueError(f"❌ Could not match: {name} <{email}>")

		available_dates = [d.strip() for d in (row["Availability"] or "").split(",") if d.strip()]
		for date_str in available_dates:
			if date_str not in event_map:
				event_map[date_str] = {
					"id": next_event_id,
					"name": date_str,
					"date": parse_event_date(date_str),
					"min_role": 4,
					"max_role": 8,
				}
				next_event_id += 1

		responses.append({
			"timestamp": row["Timestamp"],
			"peep_id": peep.id,
			"role": row["Role"],
			"max_sessions": int(row["Max Sessions"]),
			"min_interval_days": int(row["Min Interval"]),
			"availability": str([event_map[d]["id"] for d in available_dates]),
			"raw_data": str(row),
		})

	# Inserts
	period_id = db.create_schedule_period(period_name)
	db.bulk_insert_events(period_id, list(event_map.values()))
	db.bulk_insert_responses(period_id, responses)

	# Post-insert counts (still in outer txn)
	db_events = db.count_events_for_period(period_id)
	db_responses = db.count_responses_for_period(period_id)

	# Sanity
	assert db_events == len(event_map)
	assert db_responses == len(responses)

	print(f"\n✅ Created SchedulePeriod '{period_name}'")
	print(f"   Events added: {len(event_map)}")
	print(f"   Responses recorded: {len(responses)}")

	return period_id, db_events, db_responses