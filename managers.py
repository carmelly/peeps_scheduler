import logging
import csv
from file_io import normalize_role
from models import Peep, Event, Role
from constants import DATE_FORMAT
from contextlib import contextmanager

import datetime
import sqlite3

# Data type coercers 
def _as_int(v):
	return None if v is None or v == "" else int(v)

def _as_bool(v):
	if v in (True, False):
		return bool(v)
	if isinstance(v, str):
		return v.strip().lower() in {"1", "true", "t", "yes", "y"}
	return bool(v)

def _as_date(v, date_format):
	if not v:
		return None
	return datetime.datetime.strptime(v, date_format).date()

def _as_role(v: str):
	if v is None:
		return None
	# accept Role or str
	if isinstance(v, Role):
		return v
	val = v.strip()
	return Role(normalize_role(val))

class DbManager:
	def __init__(self, db_path: str):
		"""
		Open a new SQLite connection from a filesystem path.
		This class owns the connection lifecycle.
		"""
		self.conn = sqlite3.connect(db_path)
		self.conn.row_factory = sqlite3.Row

	def __enter__(self) -> "DbManager":
		return self

	def __exit__(self, exc_type, exc, tb) -> None:
		try:
			if exc is None:
				self.conn.commit()
			else:
				self.conn.rollback()
		finally:
			self.conn.close()
	
	@contextmanager
	def transaction(self, dry_run: bool = False):
		# Ensure FK constraints are enforced
		self.conn.execute("PRAGMA foreign_keys = ON")
		self.conn.execute("BEGIN IMMEDIATE")
		try:
			yield
			if dry_run:
				self.conn.rollback()
			else:
				self.conn.commit()
		except Exception:
			self.conn.rollback()
			raise
	
	def get_peep_by_id(self, id) -> Peep:
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE id = ?", (id,))
		row = cur.fetchone()
		return Peep.from_db_row(row) if row else None
	
	def get_peep_by_email(self, email) -> Peep:
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE LOWER(email) = ?", (email.lower(),))
		row = cur.fetchone()
		return Peep.from_db_row(row) if row else None

	def get_all_peeps(self) -> list[Peep]:
		cur = self.conn.cursor()
		cur.execute("""
			SELECT id, full_name, display_name, email, primary_role, active, date_joined
			FROM peeps
			ORDER BY id
		""")
		return [Peep.from_db_row(r) if r else None for r in cur.fetchall()]

	def upsert_peep(self, peep: Peep) -> str:
		"""
		Update if exists (by id), else insert. 
		Returns "updated" or "created".
		"""
		# call your existing get/update/create methods internally

	def count_peeps(self) -> int:
		cur = self.conn.execute("SELECT COUNT(*) FROM Peeps")
		return cur.fetchone()[0]
	
	def upsert_peep(self, peep: "Peep") -> str:
		"""
		Update an existing peep if it has changes, otherwise insert a new one.
		Returns:
			"updated" if an existing row was changed
			"created" if a new row was inserted
			"unchanged" if nothing needed (optional to use)
		"""
		existing = self.get_peep_by_id(peep.id)
		if existing:
			changes = (
				(peep.full_name != existing.full_name)
				or (peep.display_name != existing.display_name)
				or (peep.email != existing.email)
				or (peep.role != existing.role)
				or (peep.active != existing.active)
				or ((peep.date_joined if peep.date_joined else None) != existing.date_joined)
			)
			if changes:
				self.update_peep(peep)  # assumes update_peep accepts a Peep
				return "updated"
			return "unchanged"
		else:
			self.create_peep(peep.to_db_dict())
			return "created"
	
	def create_peep(self, peep_dict):
		cur = self.conn.cursor()
		cur.execute("""
			INSERT INTO peeps (id, full_name, display_name, email, primary_role, active, date_joined)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		""", (
			peep_dict["id"],
			peep_dict["full_name"],
			peep_dict["display_name"],
			peep_dict["email"],
			peep_dict["primary_role"],
			peep_dict["active"],
			peep_dict["date_joined"],
		))
		# return the row in the standard read shape
		return self.get_peep_by_id(peep_dict["id"])

	# accepts either dict or Peep; stores Role.value
	def update_peep(self, peep):
		d = peep if isinstance(peep, dict) else {
			"id": peep.id,
			"full_name": peep.name,
			"display_name": getattr(peep, "display_name", peep.name),
			"email": peep.email,
			"primary_role": (peep.role.value if hasattr(peep, "role") else peep.role),
			"active": peep.active,
			"date_joined": peep.date_joined,
		}
		cur = self.conn.cursor()
		cur.execute("""
			UPDATE peeps
			SET full_name = ?, display_name = ?, email = ?, primary_role = ?, active = ?, date_joined = ?
			WHERE id = ?
		""", (
			d["full_name"],
			d.get("display_name") or d["full_name"],
			d.get("email"),
			d["primary_role"],
			d["active"],
			d.get("date_joined"),
			d["id"],
		))
		if cur.rowcount == 0:
			raise LookupError(f"peep id {d['id']} not found")
		return self.get_peep_by_id(d["id"])

	def get_events_by_period(self, scheduleperiod_id):
		cur = self.conn.cursor()
		cur.execute("SELECT id, scheduleperiod_id, event_id, date, min_role, max_role FROM events WHERE scheduleperiod_id = ?", (scheduleperiod_id,))
		return [self._event_row_to_dict(row) for row in cur.fetchall()]

	def get_period_id_by_name(self, name: str) -> int | None:
		cur = self.conn.execute("SELECT id FROM SchedulePeriods WHERE name = ?", (name,))
		row = cur.fetchone()
		return int(row["id"]) if row else None

	def create_schedule_period(self, name: str) -> int:
		cur = self.conn.execute("INSERT INTO SchedulePeriods (name) VALUES (?)", (name,))
		return int(cur.lastrowid)

	def bulk_insert_events(self, period_id: int, events: list[dict]) -> int:
		# events: [{"id": int, "name": str, "date": str|datetime, "min_role": int, "max_role": int}]
		self.conn.executemany(
			"INSERT INTO Events (id, schedule_id, name, datetime, min_per_role, max_per_role) VALUES (?, ?, ?, ?, ?, ?)",
			[(e["id"], period_id, e["name"], e["date"], e["min_role"], e["max_role"]) for e in events]
		)
		return len(events)

	def bulk_insert_responses(self, period_id: int, rows: list[dict]) -> int:
		# rows: [{"peep_id": int, "timestamp": str, "role": str, "availability": str, "min_interval_days": int, "max_sessions": int, "raw_data": str}]
		self.conn.executemany(
			"""INSERT INTO Responses (scheduleperiod_id, peep_id, timestamp, role, availability, min_interval_days, max_sessions, raw_data)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
			[(period_id, r["peep_id"], r["timestamp"], r["role"], r["availability"],
			r["min_interval_days"], r["max_sessions"], r["raw_data"]) for r in rows]
		)
		return len(rows)

	def count_events_for_period(self, period_id: int) -> int:
		cur = self.conn.execute("SELECT COUNT(*) AS c FROM Events WHERE schedule_id = ?", (period_id,))
		return int(cur.fetchone()[0])

	def count_responses_for_period(self, period_id: int) -> int:
		cur = self.conn.execute("SELECT COUNT(*) AS c FROM Responses WHERE scheduleperiod_id = ?", (period_id,))
		return int(cur.fetchone()[0])

	def delete_period(self, period_id: int) -> None:
		# Order matters: children → parent
		self.conn.execute("DELETE FROM Responses WHERE scheduleperiod_id = ?", (period_id,))
		# (Future) self.conn.execute("DELETE FROM AttendanceRecords WHERE scheduleperiod_id = ?", (period_id,))
		self.conn.execute("DELETE FROM Events WHERE schedule_id = ?", (period_id,))
		# (Future) self.conn.execute("DELETE FROM PeepOrderSnapshots WHERE scheduleperiod_id = ?", (period_id,))
		self.conn.execute("DELETE FROM SchedulePeriods WHERE id = ?", (period_id,))

	def next_event_id(self) -> int:
		"""
		Return the next Events.id value (MAX(id)+1). Safe when called inside a transaction.
		"""
		cur = self.conn.execute("SELECT COALESCE(MAX(id), -1) FROM Events")
		return int(cur.fetchone()[0]) + 1
