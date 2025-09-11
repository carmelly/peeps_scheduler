import os
import sqlite3
import subprocess

DB_PATH = "db/peeps_scheduler.db"
MIGRATIONS_PATH = "db/migrations"
SCHEMA_PATH = "db/schema.sql"

# Resolve the local path to sqlite3.exe
SQLITE_EXE = os.path.join(os.path.dirname(__file__), "sqlite3.exe")

def run_migrations():
	conn = sqlite3.connect(DB_PATH)
	cur = conn.cursor()

	# Ensure tracking table exists
	cur.execute("""
		CREATE TABLE IF NOT EXISTS __migrations_applied__ (
			filename TEXT PRIMARY KEY,
			applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	""")

	# Read already-applied migrations
	cur.execute("SELECT filename FROM __migrations_applied__")
	applied = set(row[0] for row in cur.fetchall())

	# Apply new migrations
	files = sorted(f for f in os.listdir(MIGRATIONS_PATH) if f.endswith(".sql"))
	for file in files:
		if file in applied:
			continue
		with open(os.path.join(MIGRATIONS_PATH, file), "r", encoding="utf-8") as f:
			sql = f.read()
			print(f"🔁 Applying {file}...")
			cur.executescript(sql)
			cur.execute("INSERT INTO __migrations_applied__ (filename) VALUES (?)", (file,))

	conn.commit()
	conn.close()

	# Generate updated schema.sql using local sqlite3.exe
	print("🧬 Generating schema.sql...")
	try:
		schema_output = subprocess.check_output(
			[SQLITE_EXE, DB_PATH, ".schema"],
			text=True
		)
		with open(SCHEMA_PATH, "w", encoding="utf-8") as f:
			f.write(schema_output)
		print("✅ Migrations complete and schema.sql updated.")
	except FileNotFoundError:
		print("⚠️ Could not generate schema.sql — sqlite3.exe not found.")

if __name__ == "__main__":
	run_migrations()
