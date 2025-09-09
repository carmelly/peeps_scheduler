import logging
import csv
from file_io import normalize_role
from models import Peep, Event, Role
from constants import DATE_FORMAT
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

	def _peep_row_to_dict(self, row):
		d = dict(row)
		return {
			"id": _as_int(d.get("id")),
			"full_name": (d.get("full_name") or "").strip(),
			"display_name": (d.get("display_name") or "").strip(),
			"email": (d.get("email") or None),
			"primary_role": _as_role(d.get("primary_role")).value if d.get("primary_role") else None,
			"active": _as_bool(d.get("active")),
			"date_joined": d.get("date_joined") or None,  # keep string as-is or parse with _as_date if you prefer
		}

	def _event_row_to_dict(self, row):
		d = dict(row)
		return {
			"id": _as_int(d.get("event_id") or d.get("id")),
			"scheduleperiod_id": _as_int(d.get("scheduleperiod_id")),
			"date": _as_date(d.get("date"), DATE_FORMAT),
			"min_role": _as_int(d.get("min_role")),
			"max_role": _as_int(d.get("max_role")),
		}
	
	def get_peep_by_id(self, id):
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE id = ?", (id,))
		row = cur.fetchone()
		return self._peep_row_to_dict(row) if row else None
	
	def get_peep_by_email(self, email):
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE LOWER(email) = ?", (email.lower(),))
		row = cur.fetchone()
		return self._peep_row_to_dict(row) if row else None

	def get_all_peeps(self):
		cur = self.conn.cursor()
		cur.execute("""
			SELECT id, full_name, display_name, email, primary_role, active, date_joined
			FROM peeps
			ORDER BY id
		""")
		return [self._peep_row_to_dict(r) for r in cur.fetchall()]

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
