import csv
import os
import sqlite3
import logging
import utils
from constants import DATE_FORMAT
from datetime import datetime
from managers import CsvManager, DbManager

DB_PATH = "db/peeps_scheduler.db"

def import_members(csv_path, dry_run=False):
	conn = sqlite3.connect(DB_PATH)
	db = DbManager(conn)
	members_csv = CsvManager(csv_path)

	peeps = members_csv.get_all_peeps()
	matched = []
	updated = [] 
	created = [] 

	for peep in peeps: 
		existing = db.get_peep_by_id(peep.id)

		if existing:
			changes = (
				(peep.name != existing.name) or
				(peep.display_name != existing.display_name) or
				((peep.email.lower() if peep.email else None) != (existing.email.lower() if existing.email else None)) or
				(peep.role != existing.role) or
				(peep.active != existing.active) or
				(peep.date_joined != existing.date_joined)
			)
			if changes:
				if not dry_run:
					db.update_peep(peep)
				updated.append(peep)
			else:
				matched.append(peep)
		else:
			if not dry_run:
				db.create_peep(peep)
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
	conn = sqlite3.connect(DB_PATH)
	cur = conn.cursor() # TODO: move logic to DbManager so we won't need this here
	db = DbManager(conn)
	responses_csv = CsvManager(responses_csv_path)

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
	rows = responses_csv.load_rows(required_columns=['Timestamp','Name','Email Address','Role','Max Sessions','Availability','Min Interval'])
	responses = []
	event_map = {}
	peep_lookup = {}

	# build lookup of all peeps by lowercase email
	for peep in db.get_all_peeps():
		if peep.email:
			peep_lookup[peep.email.lower()] = peep

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
					"date": utils.parse_event_date(date_str),
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
