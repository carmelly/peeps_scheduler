import logging
import csv
from models import Peep, Event, Role
from constants import DATE_FORMAT
import datetime

class DbManager:
	def __init__(self, conn):
		self.conn = conn

	# Peeps
	def peep_from_db_row(self, row):
		columns = ['id', 'full_name', 'display_name', 'email', 'primary_role', 'active', 'date_joined']
		data = dict(zip(columns, row))
		peep = Peep(
			id = int(data['id']),
			name = data['full_name'],
			email = data['email'] or None,
			role = data['primary_role'],
			active = bool(data['active']),
			date_joined = data['date_joined']
		)
		peep.display_name = data['display_name'].strip() # TODO: make this an official part of the Peep class
		return peep 
	
	def get_peep_by_id(self, id):
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE id = ?", (id,))
		row = cur.fetchone()
		return self.peep_from_db_row(row) if row else None
	
	def get_peep_by_email(self, email):
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps WHERE LOWER(email) = ?", (email.lower(),))
		row = cur.fetchone()
		return self.peep_from_db_row(row) if row else None

	def get_all_peeps(self):
		cur = self.conn.cursor()
		cur.execute("SELECT id, full_name, display_name, email, primary_role, active, date_joined FROM peeps")
		rows = cur.fetchall()
		return [self.peep_from_db_row(row) for row in rows]

	def create_peep(self, peep):
		cur = self.conn.cursor()
		cur.execute("""
			INSERT INTO peeps (id, full_name, display_name, email, primary_role, active, date_joined)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		""", (
			peep.id,
			peep.name,
			peep.name,
			peep.email,
			peep.role.value,
			peep.active,
			peep.date_joined
		))
		self.conn.commit()

	def update_peep(self, peep):
		cur = self.conn.cursor()
		cur.execute("""
			UPDATE peeps
			SET full_name = ?, display_name = ?, email = ?, primary_role = ?, active = ?, date_joined = ?
			WHERE id = ?
		""", (
			peep.name,
			peep.name,
			peep.email,
			peep.role.value,
			peep.active,
			peep.date_joined,
			peep.id
		))
		self.conn.commit()

	# Events
	def event_from_db_row(self, row):
		columns = ['id', 'scheduleperiod_id', 'event_id', 'date', 'min_role', 'max_role']
		data = dict(zip(columns, row))
		return Event(
			id=int(data['event_id']),
			date=datetime.datetime.strptime(data['date'], DATE_FORMAT),
			min_role=int(data['min_role']),
			max_role=int(data['max_role'])
		)

	def get_events_by_period(self, scheduleperiod_id):
		cur = self.conn.cursor()
		cur.execute("SELECT id, scheduleperiod_id, event_id, date, min_role, max_role FROM event WHERE scheduleperiod_id = ?", (scheduleperiod_id,))
		rows = cur.fetchall()
		return [self.event_from_db_row(row) for row in rows]


class CsvManager:
	def __init__(self, file_path):
		self.file_path = file_path

	def load_rows(self, required_columns=None):
		with open(self.file_path, newline='', encoding='utf-8') as csvfile:
			reader = csv.DictReader(csvfile)
			if required_columns:
				missing = set(required_columns) - set(reader.fieldnames)
				if missing:
					raise ValueError(f"Missing required columns in {self.file_path}: {missing}")
			return list(reader)

	def get_peep_from_csv_row(self, row):
		peep = Peep(
			id  =int(row['id']),
			name = row['Name'].strip(),
			email = row['Email Address'].strip() or None,
			role = row['Role'].strip().lower(),
			active = bool(row['Active'] == 'TRUE'),
			date_joined = row['Date Joined'] or None
		)
		peep.display_name = row['Display Name'].strip() # TODO: make this an official part of the Peep class
		return peep

	def get_all_peeps(self): 
		required_columns = ['id','Name','Display Name','Email Address','Role', 'Active', 'Date Joined']
		rows = self.load_rows(required_columns)
		return [self.get_peep_from_csv_row(row) for row in rows]
