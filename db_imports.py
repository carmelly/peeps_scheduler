import logging
from file_io import load_peeps, load_responses, parse_event_date
import utils
from managers import DbManager
from models import Peep

DB_PATH = "db/peeps_scheduler.db"

def import_period(slug, dry_run=False): 
	# derive filenames from slug 
	members_path = f"db/imports/{slug}/data/members.csv"
	responses_path = f"db/imports/{slug}/data/responses.csv"

	db_peeps = import_members(members_path, dry_run)
	period_id, db_events, db_responses = import_responses(responses_path, "May 2025", dry_run) or (None, None, None)

	
	print('\nData for manifest')
	if dry_run: 
		print('PREVIEW; rolled back')
	print('------------')
	print("Source files:") 
	print(f"   {members_path}")
	print(f"   {responses_path}")
	print(f"SchedulePeriod id: {period_id}") 
	print("DB row counts:") 
	print(f"   peeps: {db_peeps}")
	print(f"   events: {db_events}")
	print(f"   responses: {db_responses}")


def import_members(csv_path, dry_run=False):
	with DbManager(DB_PATH) as db: 
		peeps = load_peeps(csv_path)
		matched, updated, created = [], [], []

		with db.transaction(dry_run):
			for peep in peeps:
				result = db.upsert_peep(peep)  # "created"/"updated"/"unchanged"
				if result == "created":
					created.append(peep)
				elif result == "updated":
					updated.append(peep)
				else:
					matched.append(peep)
			db_total_peeps = db.count_peeps()

		# After context exits:
		# - persisted when dry_run=False
		# - rolled back when dry_run=True
		print(f"\n📋 Import summary for {csv_path}:")
		print(f"   Matched (no changes): {len(matched)}")
		print(f"   Updated: {len(updated)}")
		print(f"   Created: {len(created)}")
		if not dry_run:
			print(f"   Peeps in DB (post-commit): {db_total_peeps}")
		else:
			print(f"   Peeps in DB (preview, rolled back): {db_total_peeps}")
		return db_total_peeps
 
def import_responses(responses_csv_path, period_name, dry_run=False, replace=False):
	with DbManager(DB_PATH) as db: 

		# basic validation
		if not period_name:
			print("❌ Schedule name is required.")
			return None, None, None
	
		# load csv with expected columns
		columns =  ['Timestamp', 'Email Address', "Name", 'Role', 'Min Interval','Max Sessions', 'Availability']
		rows = load_responses(responses_csv_path, columns)
		responses = []
		event_map = {}
		peep_lookup = {}

		# build lookup of peeps by lowercase email
		for peep in db.get_all_peeps():
			if peep.email:
				peep_lookup[peep.email.lower()] = peep

		# get last used event id 
		event_counter = db.next_event_id()

		existing = db.get_period_id_by_name(period_name)
		if existing:
			print(f"❌ SchedulePeriod name '{period_name}' already exists. Choose a different name.")
			return None, None, None
		
		# process CSV rows
		for row in rows:
			email = row["Email Address"].strip().lower()
			name = row["Name"].strip()
			peep = peep_lookup.get(email)
			if not peep:
				print(f"❌ Could not match: {name} <{email}>")
				return None, None, None

			available_dates = [d.strip() for d in row["Availability"].split(",") if d.strip()]
			for date_str in available_dates:
				if date_str not in event_map:
					event_map[date_str] = {
						"id": event_counter,
						"name": date_str,
						"date": parse_event_date(date_str),
						"min_role": 4,
						"max_role": 8,
					}
					event_counter += 1
		
			# create an Event for each new date we encounter
			available_dates = [d.strip() for d in row["Availability"].split(",") if d.strip()]
			for date_str in available_dates:
				if date_str not in event_map:
					event_map[date_str] = {
						"id": event_counter,
						"name": date_str, 
						"date": parse_event_date(date_str),
						"min_role": 4,
						"max_role": 8
					}
					event_counter += 1

			# build response dict
			responses.append({
				"timestamp": row["Timestamp"],
				"peep_id": peep.id, 
				"role": row["Role"],
				"max_sessions": int(row["Max Sessions"]),
				"min_interval_days": int(row["Min Interval"]),
				"availability": str([event_map[d]["id"] for d in available_dates]),
				"raw_data": str(row)
				})

		with db.transaction(dry_run=dry_run):
			period_id = db.create_schedule_period(period_name)
			db.bulk_insert_events(period_id, list(event_map.values()))
			db.bulk_insert_responses(period_id, responses)

			db_events = db.count_events_for_period(period_id)
			db_responses = db.count_responses_for_period(period_id)

			assert db_events == len(event_map)
			assert db_responses == len(responses)

		print(f"\n✅ Created SchedulePeriod '{period_name}'")
		print(f"   Events added: {len(event_map)}")
		print(f"   Responses recorded: {len(responses)}")

		return (period_id, db_events, db_responses)
