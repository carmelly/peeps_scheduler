import os
import sqlite3
import subprocess
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import get_data_manager
import constants

# Use DataManager for database path
DB_PATH = get_data_manager().get_database_path()
MIGRATIONS_PATH = Path("db/migrations")
SCHEMA_PATH = Path(constants.SCHEMA_PATH)

# Resolve the local path to sqlite3.exe
SQLITE_EXE = os.path.join(os.path.dirname(__file__), "sqlite3.exe")

def is_initial_migration(pending_files):
	"""Check if this is the very first migration (001_)."""
	return len(pending_files) > 0 and pending_files[0].startswith('001_')

def create_auto_backup(pending_files, db_existed_before):
	"""Create an automatic backup before applying migrations."""
	try:
		# Skip backup if no database existed before and this is initial migration
		if not db_existed_before and is_initial_migration(pending_files):
			print("ℹ️ Skipping backup for initial migration (no database existed)")
			return True
		
		# Import backup functionality
		backup_module_path = Path(__file__).parent / "backup.py"
		if backup_module_path.exists():
			import sys
			sys.path.insert(0, str(Path(__file__).parent))
			from backup import backup
			
			# Create descriptive backup name
			if len(pending_files) == 1:
				backup_label = f"pre_migration_{pending_files[0].replace('.sql', '')}"
			else:
				first_num = pending_files[0].split('_')[0]
				last_num = pending_files[-1].split('_')[0] 
				backup_label = f"pre_migrations_{first_num}_to_{last_num}"
			
			return backup(backup_label=backup_label, auto=True)
		else:
			print("⚠️ backup.py not found - skipping auto-backup")
			return True
	except Exception as e:
		print(f"⚠️ Auto-backup failed: {e}")
		print("Continuing with migrations...")
		return True  # Don't block migrations for backup failures

def validate_migration_file(filepath):
	"""Basic validation of migration file."""
	try:
		with open(filepath, 'r', encoding='utf-8') as f:
			content = f.read().strip()
		
		if not content:
			return False, "Migration file is empty"
		
		# Basic SQL validation - check for obvious issues
		content_upper = content.upper()
		if 'DROP DATABASE' in content_upper:
			return False, "Migration contains dangerous DROP DATABASE statement"
		
		return True, "OK"
	except Exception as e:
		return False, f"Error reading migration file: {e}"

def apply_migration(filepath, conn):
	"""Apply a single migration within a transaction."""
	filename = os.path.basename(filepath)
	
	# Validate migration before applying
	is_valid, validation_msg = validate_migration_file(filepath)
	if not is_valid:
		raise Exception(f"Migration validation failed: {validation_msg}")
	
	try:
		# Begin transaction
		conn.execute("BEGIN")
		
		# Read and execute migration
		with open(filepath, "r", encoding="utf-8") as f:
			sql = f.read()
		
		print(f"🔁 Applying {filename}...")
		conn.executescript(sql)
		
		# Record migration as applied
		conn.execute(
			"INSERT INTO __migrations_applied__ (filename) VALUES (?)", 
			(filename,)
		)
		
		# Commit transaction
		conn.execute("COMMIT")
		print(f"✅ Successfully applied {filename}")
		return True
		
	except sqlite3.Error as e:
		# Rollback on any database error
		try:
			conn.execute("ROLLBACK")
		except:
			pass  # Rollback might fail if transaction wasn't started
		raise Exception(f"Database error in {filename}: {e}")
	except Exception as e:
		# Rollback on any other error
		try:
			conn.execute("ROLLBACK")
		except:
			pass
		raise Exception(f"Error applying {filename}: {e}")

def run_migrations():
	"""Run all pending migrations with safety features."""
	
	# Check for migration files BEFORE connecting to database
	if not os.path.exists(MIGRATIONS_PATH):
		print(f"⚠️ Migrations directory not found: {MIGRATIONS_PATH}")
		return False
		
	files = sorted(f for f in os.listdir(MIGRATIONS_PATH) if f.endswith(".sql"))
	if not files:
		print("✅ No migration files found.")
		return True
	
	# Check if database exists before we connect (and potentially create it)
	db_existed_before = DB_PATH.exists()
	
	try:
		# Connect to database (this will create it if it doesn't exist)
		conn = sqlite3.connect(DB_PATH)
		
		# Ensure tracking table exists
		conn.execute("""
			CREATE TABLE IF NOT EXISTS __migrations_applied__ (
				filename TEXT PRIMARY KEY,
				applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
			)
		""")
		conn.commit()

		# Read already-applied migrations
		cur = conn.cursor()
		cur.execute("SELECT filename FROM __migrations_applied__")
		applied = set(row[0] for row in cur.fetchall())

		# Get pending migrations
		pending_files = [f for f in files if f not in applied]
		
		if not pending_files:
			print("✅ No pending migrations.")
			conn.close()
			return True
		
		print(f"📋 Found {len(pending_files)} pending migrations:")
		for f in pending_files:
			print(f"  - {f}")
		print()
		
		# Create backup before applying migrations (only if needed)
		print("🛡️ Creating pre-migration backup...")
		backup_success = create_auto_backup(pending_files, db_existed_before)
		if not backup_success:
			print("⚠️ Backup failed, but continuing with migrations...")
		
		# Apply pending migrations (abort on first failure)
		for i, filename in enumerate(pending_files, 1):
			filepath = os.path.join(MIGRATIONS_PATH, filename)
			try:
				print(f"[{i}/{len(pending_files)}] ", end="")
				apply_migration(filepath, conn)
			except Exception as e:
				conn.close()
				print(f"❌ Migration failed: {filename}")
				print(f"Error: {e}")
				print(f"\n⚠️ Migration aborted. Database restored to pre-migration state.")
				print(f"Fix the migration and run again.")
				return False
		
		conn.close()
		
		# All migrations succeeded
		print(f"\n✅ Successfully applied {len(pending_files)} migrations.")
		
		# Generate updated schema.sql using local sqlite3.exe
		print("🧬 Generating schema.sql...")
		try:
			schema_output = subprocess.check_output(
				[SQLITE_EXE, str(DB_PATH), ".schema"],
				text=True
			)
			with open(SCHEMA_PATH, "w", encoding="utf-8") as f:
				f.write(schema_output)
			print("✅ schema.sql updated.")
		except FileNotFoundError:
			print("⚠️ Could not generate schema.sql — sqlite3.exe not found.")
			print("   Download from https://sqlite.org/download.html and place in db/ folder.")
		except subprocess.CalledProcessError as e:
			print(f"⚠️ Error generating schema.sql: {e}")
		
		return True
		
	except sqlite3.Error as e:
		print(f"❌ Database connection error: {e}")
		return False
	except Exception as e:
		print(f"❌ Unexpected error: {e}")
		return False

if __name__ == "__main__":
	success = run_migrations()
	if not success:
		print("\n⚠️ Migration failed. Check errors above.")
		exit(1)
	else:
		print("\n🎉 All migrations completed successfully!")
