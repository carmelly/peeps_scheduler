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

	import_members(members_path, dry_run)
	import_responses(responses_path, "May 2025", dry_run)# hardcode name for now


def import_members(csv_path, dry_run=False):
	db = DbManager(DB_PATH)
	conn = db.conn
	cur = conn.cursor()

	# peeps = members_csv.get_all_peeps()
	peeps = load_peeps(csv_path)
	matched = []
	updated = [] 
	created = [] 

	for peep in peeps: 
		match = db.get_peep_by_id(peep.id)  

		if match:
			existing = Peep.from_db_dict(match)
			changes = (
				(peep.full_name != existing.full_name) or
				(peep.display_name != existing.display_name) or
				(peep.email != existing.email) or
				(peep.role != existing.role) or
				(peep.active != existing.active) or
				((peep.date_joined if peep.date_joined else None) != existing.date_joined)
			)
			if changes:
				if not dry_run:
					db.update_peep(peep)
				updated.append(peep)
			else:
				matched.append(peep)
		else:
			if not dry_run:
				db.create_peep(peep.to_db_dict())
			created.append(peep)

	if dry_run:
		print("\n🧪 Dry run: Loaded the following Peeps from CSV:")
		for p in peeps:
			print(f"  - Peep({p.id:>3}): {p.display_name}")

	if not dry_run:
		conn.commit()
	conn.close()

	print(f"\n📋 Import summary for {csv_path}:")
	print(f"   Matched (no changes): {len(matched)}")
	print(f"   Updated: {len(updated)}")
	for p in updated:
		print(f"      - Peep({p.id:>3}): {p.display_name}")
	print(f"   Created: {len(created)}")
	for p in created:
		print(f"      - Peep({p.id:>3}): {p.display_name}")

def import_responses(responses_csv_path, period_name, dry_run=False):
	db = DbManager(DB_PATH)
	conn = db.conn
	cur = conn.cursor() # TODO: move logic to DbManager so we won't need this here

	# check for existing and unique name
	if not period_name:
			print("❌ Schedule name is required.")
			return
	cur.execute("SELECT COUNT(*) FROM scheduleperiods WHERE name = ?", (period_name,))
	if cur.fetchone()[0] > 0:
		print(f"❌ SchedulePeriod name '{period_name}' already exists. Choose a different name.")
		conn.close()
		return
	
	# read csv
	columns =  ['Timestamp', 'Email Address', "Name", 'Role', 'Min Interval','Max Sessions', 'Availability']
	rows = load_responses(responses_csv_path, columns)
	responses = []
	event_map = {}
	peep_lookup = {}

	# build lookup of all peeps by lowercase email
	for peep in db.get_all_peeps():
		email = peep["email"]
		if email:
			peep_lookup[email.lower()] = Peep.from_db_dict(peep)

	# get last used event id 
	cur.execute("SELECT MAX(id) FROM events")
	max_event_id = cur.fetchone()[0] or -1
	event_counter = max_event_id + 1

	# process responses
	for row in rows:
		# find peep in db by email; bail if not matched
		email = row["Email Address"].strip().lower()
		name = row["Name"].strip()
		peep = peep_lookup.get(email)
		if not peep:
			logging.critical(f"No matching peep for: {name} <{email}>")
			print(f"❌ Could not match: {name} <{email}>")
			conn.close()
			return

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

	if dry_run:
		print(f"\n🧪 Dry run: would insert SchedulePeriod '{period_name}' with {len(event_map)} events and {len(responses)} responses.")
		return

	# Insert SchedulePeriod
	cur.execute("INSERT INTO SchedulePeriods (name) VALUES (?)", (period_name,))
	period_id = cur.lastrowid

	# Insert Events
	event_id_map = {}
	for event in event_map.values():
		cur.execute("INSERT INTO Events (id, schedule_id, name, datetime, min_per_role, max_per_role) VALUES (?, ?, ?, ?, ?, ?)",
			(event["id"], period_id, event["name"], event["date"], event["min_role"], event["max_role"]))
		event_id_map[event["date"]] = cur.lastrowid

	# Insert Responses
	for r in responses:
		cur.execute("""
			INSERT INTO Responses (scheduleperiod_id, peep_id, timestamp, role, availability, min_interval_days, max_sessions, raw_data)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		""", (
			period_id,
			r["peep_id"],
			r["timestamp"],
			r["role"],
			r["availability"],
			r["min_interval_days"],
			r["max_sessions"],
			r["raw_data"]
		))

	conn.commit()
	conn.close()

	print(f"\n✅ Created SchedulePeriod '{period_name}'")
	print(f"   Events added: {len(event_map)}")
	print(f"   Responses recorded: {len(responses)}")
