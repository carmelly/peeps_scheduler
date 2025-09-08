import sqlite3
import pytest
from managers import DbManager, _as_int, _as_bool, _as_date, _as_role
from constants import DATE_FORMAT
from models import Role

def _init_temp_db(path: str) -> None:
	conn = sqlite3.connect(path)
	try:
		conn.executescript("""
			PRAGMA foreign_keys = ON;

			CREATE TABLE peeps (
				id INTEGER PRIMARY KEY,
				full_name TEXT NOT NULL,
				display_name TEXT NOT NULL,
				email TEXT,
				primary_role TEXT,
				active INTEGER NOT NULL DEFAULT 1,
				date_joined TEXT
			);
		""")
		conn.execute("INSERT INTO peeps (id, full_name, display_name, email, primary_role, active, date_joined) VALUES (1,'Alice Example','Alice','a@example.com','leader',1,'2024-01-01');")
		conn.execute("INSERT INTO peeps (id, full_name, display_name, email, primary_role, active, date_joined) VALUES (2,'Bob Sample','Bob','b@example.com','follower',1,'2024-02-02');")
		conn.commit()
	finally:
		conn.close()

@pytest.fixture(scope="class")
def temp_db_path(tmp_path_factory):
	db_path = tmp_path_factory.mktemp("ps") / "test_ps.db"
	_init_temp_db(str(db_path))
	return str(db_path)

class TestCoercers:
	def test_as_int(self):
		assert _as_int(None) is None
		assert _as_int("") is None
		assert _as_int("5") == 5
		assert _as_int(0) == 0
		assert _as_int(12) == 12

	def test_as_bool(self):
		assert _as_bool(True) is True
		assert _as_bool(False) is False
		assert _as_bool(1) is True
		assert _as_bool(0) is False
		assert _as_bool(None) is False
		assert _as_bool("true") is True
		assert _as_bool("TRUE") is True
		assert _as_bool("yes") is True
		assert _as_bool("y") is True
		assert _as_bool("1") is True
		assert _as_bool("false") is False
		assert _as_bool("no") is False
		assert _as_bool("n") is False
		assert _as_bool("0") is False

	def test_as_date(self):
		assert _as_date(None, DATE_FORMAT) is None
		d = _as_date("2024-03-04 15:00", DATE_FORMAT)
		assert d.year == 2024 and d.month == 3 and d.day == 4

	def test_as_role(self):
		# accepts Role enum
		assert _as_role(Role.LEADER) == Role.LEADER
		assert _as_role(None) == None 
		# rejects unknown
		with pytest.raises(ValueError):
			_as_role("notarole")

@pytest.mark.usefixtures("temp_db_path")
class TestDbManager:
	def test_get_all_peeps_returns_dicts(self, temp_db_path):
		with DbManager(temp_db_path) as db:
			rows = db.get_all_peeps()
			assert isinstance(rows, list)
			assert all(isinstance(r, dict) for r in rows)
			assert {"id","full_name","display_name","email","primary_role","active","date_joined"} <= set(rows[0].keys())

	def test_get_peep_by_email_casefold(self, temp_db_path):
		with DbManager(temp_db_path) as db:
			row = db.get_peep_by_email("A@EXAMPLE.COM")
			assert row["id"] == 1
			assert row["primary_role"] == "leader"

	def test_context_commit_on_success(self, temp_db_path):
		with DbManager(temp_db_path) as db:
			db.create_peep({
				"id": 3,
				"full_name": "Carol Commit",
				"display_name": "Carol",
				"email": "c@example.com",
				"primary_role": "leader",
				"active": True,
				"date_joined": "2024-03-03",
			})
		# verify committed
		with sqlite3.connect(temp_db_path) as conn:
			assert conn.execute("SELECT COUNT(*) FROM peeps WHERE id=3").fetchone()[0] == 1

	def test_context_rollback_on_exception(self, temp_db_path):
		with pytest.raises(RuntimeError):
			with DbManager(temp_db_path) as db:
				db.create_peep({
					"id": 4,
					"full_name": "Dave Rollback",
					"display_name": "Dave",
					"email": "d@example.com",
					"primary_role": "Follower",
					"active": True,
					"date_joined": "2024-04-04",
				})
				raise RuntimeError("boom")
		# verify rolled back
		with sqlite3.connect(temp_db_path) as conn:
			assert conn.execute("SELECT COUNT(*) FROM peeps WHERE id=4").fetchone()[0] == 0

	def test_update_peep_full_field(self, temp_db_path):
		with DbManager(temp_db_path) as db:
			updated = db.update_peep({
				"id": 2,
				"full_name": "Bob Updated",
				"display_name": "Bobby",
				"email": "b@example.com",
				"primary_role": "Follower",
				"active": True,
				"date_joined": "2024-02-02",
			})
			assert updated["full_name"] == "Bob Updated"
			assert updated["display_name"] == "Bobby"

	def test_update_peep_not_found_raises(self, temp_db_path):
		with DbManager(temp_db_path) as db:
			with pytest.raises(LookupError):
				db.update_peep({
					"id": 999,
					"full_name": "Ghost",
					"display_name": "Ghost",
					"email": None,
					"primary_role": "leader",
					"active": True,
					"date_joined": None,
				})

from managers import CsvManager

# Helpers to create temporary CSVs for tests
def _write_csv(tmp_path, name, header, rows):
	p = tmp_path / name
	with open(p, "w", encoding="utf-8", newline="") as f:
		f.write(",".join(header) + "\n")
		for r in rows:
			f.write(",".join(r) + "\n")
	return str(p)

class TestCsvManager:
	def test_load_rows_success(self, tmp_path):
		# valid file with all required columns
		header = ["id","Name","Display Name","Email Address","Role","Active","Date Joined"]
		rows = [
			["1","Alice Example","Alice","a@example.com","Leader","TRUE","2024-01-01"],
			["2","Bob Sample","Bob","b@example.com","Follower","FALSE","2024-02-02"],
		]
		path = _write_csv(tmp_path, "peeps_ok.csv", header, rows)

		csvm = CsvManager(path)
		out = csvm.load_rows(required_columns=header)
		assert isinstance(out, list)
		assert len(out) == 2
		assert out[0]["Name"] == "Alice Example"

	def test_load_rows_missing_columns_raises(self, tmp_path):
		# missing Display Name column
		header = ["id","Name","Email Address","Role","Active","Date Joined"]
		rows = [
			["1","Alice Example","a@example.com","Leader","TRUE","2024-01-01"],
		]
		path = _write_csv(tmp_path, "peeps_missing.csv", header, rows)

		csvm = CsvManager(path)
		with pytest.raises(ValueError) as ei:
			csvm.load_rows(required_columns=['id','Name','Display Name','Email Address','Role','Active','Date Joined'])
		msg = str(ei.value)
		assert "Missing required columns" in msg
		assert "Display Name" in msg

	def test_get_peep_from_csv_row_coercion(self, tmp_path):
		# exercise _as_int, _as_bool, role parsing, trimming
		header = ["id","Name","Display Name","Email Address","Role","Active","Date Joined"]
		rows = [
			["003","  Carol  ","  Car  ","","Leader","TRUE",""],  # empty email, empty date
		]
		path = _write_csv(tmp_path, "one.csv", header, rows)

		csvm = CsvManager(path)
		row = csvm.load_rows(required_columns=header)[0]
		peep = csvm.get_peep_from_csv_row(row)

		assert peep["id"] == 3					# _as_int
		assert peep["full_name"] == "Carol"		# strip
		assert peep["display_name"] == "Car"	# strip
		assert peep["email"] is None			# empty -> None
		assert peep["primary_role"] == "leader"	# role value
		assert peep["active"] is True			# TRUE -> True
		assert peep["date_joined"] is None		# empty -> None

	def test_get_all_peeps_multirow(self, tmp_path):
		header = ["id","Name","Display Name","Email Address","Role","Active","Date Joined"]
		rows = [
			["1","Alice Example","Alice","a@example.com","Leader","TRUE","2024-01-01"],
			["2","Bob Sample","Bob","b@example.com","Follower","FALSE","2024-02-02"],
		]
		path = _write_csv(tmp_path, "many.csv", header, rows)

		csvm = CsvManager(path)
		peeps = csvm.get_all_peeps()
		assert isinstance(peeps, list)
		assert len(peeps) == 2

		# spot check shapes
		assert {"id","full_name","display_name","email","primary_role","active","date_joined"} <= set(peeps[0].keys())
		assert peeps[0]["primary_role"] in {"leader","follower"}
		assert peeps[0]["active"] in {True, False}
