import argparse
import json
import sqlite3
from constants import DATE_FORMAT
from datetime import datetime
from managers import DbManager
from models import Role

DB_PATH = "db/peeps_scheduler.db"

def list_peeps_by_snapshot(conn):
	db = DbManager(conn)
	cur = conn.cursor()
	cur.execute("""
		SELECT id, timestamp, data FROM PeepOrderSnapshots
		ORDER BY timestamp DESC LIMIT 1
	""")
	row = cur.fetchone()
	if not row:
		print("No peep order snapshots found.")
		return

	snapshot_id, timestamp, peeps_json = row
	try:
		snapshot_peeps = json.loads(peeps_json)
	except json.JSONDecodeError:
		print("Invalid JSON in snapshot.")
		return

	print(f"\n👥 Latest Peep Order Snapshot: ({timestamp})")
	for p in sorted(snapshot_peeps, key=lambda x: x["index"]):
		peep = db.get_peep_by_id(p["peep_id"])
		if not peep:
			continue
		print(f"  {p['index']:>2}. {peep.display_name} ({peep.role.value})  id={peep.id}  priority={p['priority']}")

def attendance_report(conn):
	cur = conn.cursor()
	cur.execute("""
		SELECT p.id, p.display_name, p.primary_role, COUNT(a.id) AS attended
		FROM peeps p
		LEFT JOIN AttendanceRecords a ON a.peep_id = p.id
		GROUP BY p.id, p.display_name, p.primary_role
		ORDER BY attended DESC, p.display_name
	""")
	rows = cur.fetchall()
	print("\n📊 Attendance Report:")
	for row in rows:
		peep_id, name, role, attended = row
		print(f"  {name} ({role})  id={peep_id}  sessions attended: {attended}")

def availability_report(conn):
	cur = conn.cursor()
	cur.execute("""
		SELECT sp.id FROM SchedulePeriods sp
		ORDER BY sp.id DESC LIMIT 1
	""")
	latest_period = cur.fetchone()
	if not latest_period:
		print("No schedule periods found.")
		return

	scheduleperiod_id = latest_period[0]

	cur.execute("SELECT id, display_name FROM peeps WHERE active = 1")
	all_peeps = {row[0]: {"name": row[1]} for row in cur.fetchall()}

	cur.execute("SELECT peep_id, role, availability FROM Responses WHERE scheduleperiod_id = ?", (scheduleperiod_id,))
	response_rows = cur.fetchall()

	# Map event_id -> date
	cur.execute("SELECT id, datetime FROM events WHERE schedule_id = ?", (scheduleperiod_id,))
	event_dates = {eid: date for eid, date in cur.fetchall()}

	availability = {}
	responders = set()

	for peep_id, role, availability_json in response_rows:
		if peep_id not in all_peeps:
			continue
		responders.add(all_peeps[peep_id]["name"])
		try:
			available = json.loads(availability_json)
		except json.JSONDecodeError:
			continue
		for eid in available:
			date = event_dates.get(eid)
			if not date:
				continue
			if date not in availability:
				availability[date] = {"Lead": [], "Follow": []}
			# role = all_peeps[peep_id]["role"]
			availability[date][role].append(all_peeps[peep_id]["name"])

	non_responders = sorted(set(p["name"] for p in all_peeps.values()) - responders)

	print("\n📅 Availability Report:")
	for date in sorted(availability.keys()):
		formatted_date = datetime.strptime(date, DATE_FORMAT).strftime("%A %b %d")
		leaders = availability[date].get("Lead", [])
		followers = availability[date].get("Follow", [])
		print(f"  {formatted_date}:")
		print(f"    Leaders  ({len(leaders)}): {', '.join(leaders)}")
		print(f"    Followers({len(followers)}): {', '.join(followers)}")

	print("\n🚫 Did not respond:")
	for name in non_responders:
		print(f"  - {name}")

def run():
	parser = argparse.ArgumentParser(description="Peeps Scheduler Reports")
	parser.add_argument("report", choices=["peep-order", "attendance", "availability"], help="Report type to generate")
	args = parser.parse_args()

	conn = sqlite3.connect(DB_PATH)

	if args.report == "peep-order":
		list_peeps_by_snapshot(conn)
	elif args.report == "attendance":
		attendance_report(conn)
	elif args.report == "availability":
		availability_report(conn)

	conn.close()

if __name__ == "__main__":
	run()
