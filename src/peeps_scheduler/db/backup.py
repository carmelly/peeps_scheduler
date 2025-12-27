import shutil
from datetime import datetime
from pathlib import Path
import sys
import os
import sqlite3

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler import constants

# Database configuration
data_manager = get_data_manager()
DB_PATH = data_manager.get_database_path()
SCHEMA_PATH = Path(constants.SCHEMA_PATH)

# Use DataManager for backup paths
BACKUP_DIR = data_manager.get_db_backups_path()
BACKUP_DIR.mkdir(exist_ok=True)

def update_schema_file():
	"""Update schema.sql with current database structure."""
	if not DB_PATH.exists():
		return False
	
	try:
		conn = sqlite3.connect(DB_PATH)
		cursor = conn.cursor()
		
		# Get all table creation statements and indexes
		cursor.execute("""
			SELECT sql FROM sqlite_master 
			WHERE type IN ('table', 'index') 
			AND name NOT LIKE 'sqlite_%'
			AND sql IS NOT NULL
			ORDER BY type, name
		""")
		
		schema_statements = [row[0] + ';' for row in cursor.fetchall()]
		conn.close()
		
		# Write to schema.sql
		with open(SCHEMA_PATH, 'w', encoding='utf-8') as f:
			f.write('\n'.join(schema_statements))
		
		return True
	except Exception as e:
		print(f"WARNING: Could not update schema.sql: {e}")
		return False

def list_backups():
	"""List all available backups with file sizes and dates."""
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("FOLDER: No backups found.")
	else:
		print("FOLDER: Available backups:")
		for b in backups:
			try:
				size_mb = b.stat().st_size / (1024 * 1024)
				# Parse timestamp from filename (YYYYMMDD-HHMM_name.db)
				name_parts = b.stem.split('_', 1)
				if len(name_parts) >= 1:
					timestamp_str = name_parts[0]
					try:
						parsed_date = datetime.strptime(timestamp_str, '%Y%m%d-%H%M')
						date_str = parsed_date.strftime('%Y-%m-%d %H:%M')
					except ValueError:
						date_str = 'unknown date'
				else:
					date_str = 'unknown date'
				print(f"- {b.name} ({size_mb:.1f}MB, {date_str})")
			except OSError as e:
				print(f"- {b.name} (error reading file: {e})")

def backup(backup_label=None, auto=False):
	"""Create a backup with optional label. If auto=True, don't prompt for input."""
	if backup_label is None:
		if auto:
			backup_label = 'auto_migration'
		else:
			backup_label = input("Enter a name for this backup: ").strip().replace(" ", "_") or "backup"
	
	timestamp = datetime.now().strftime("%Y%m%d-%H%M")
	backup_filename = f"{timestamp}_{backup_label}.db"
	backup_path = BACKUP_DIR / backup_filename

	if not DB_PATH.exists():
		if auto:
			print("INFO: No existing database to backup.")
			return True  # Not an error for auto-backup
		else:
			print("ERROR: Could not find database at db/peeps_scheduler.db")
			return False

	try:
		# Verify source database size
		source_size = DB_PATH.stat().st_size
		
		# Copy with error handling
		shutil.copy2(DB_PATH, backup_path)
		
		# Verify backup was created and has correct size
		if not backup_path.exists():
			print(f"ERROR: Backup file was not created: {backup_path}")
			return False
			
		backup_size = backup_path.stat().st_size
		if backup_size != source_size:
			print(f"ERROR: Backup file size mismatch. Source: {source_size}, Backup: {backup_size}")
			return False
		
		size_mb = backup_size / (1024 * 1024)
		if auto:
			print(f"BACKUP: Auto-backup created: {backup_filename} ({size_mb:.1f}MB)")
		else:
			print(f"SUCCESS: Backup saved to: {backup_path} ({size_mb:.1f}MB)")
		
		# Update schema.sql after successful backup to reflect current state
		if update_schema_file():
			print("FILE: Updated schema.sql to reflect current database state")
		
		return True
		
	except OSError as e:
		print(f"ERROR: Failed to create backup: {e}")
		return False
	except Exception as e:
		print(f"ERROR: Unexpected error during backup: {e}")
		return False

def restore():
	"""Restore database from a backup with confirmation."""
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("ERROR: No backups found.")
		return False

	print("\nAvailable backups:")
	for i, b in enumerate(backups):
		try:
			size_mb = b.stat().st_size / (1024 * 1024)
			print(f"[{i+1}] {b.name} ({size_mb:.1f}MB)")
		except OSError:
			print(f"[{i+1}] {b.name} (error reading file)")

	choice = input("\nEnter the number of the backup to restore: ").strip()
	if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
		print("ERROR: Invalid selection.")
		return False

	selected = backups[int(choice) - 1]
	
	# Verify backup file before proceeding
	try:
		backup_size = selected.stat().st_size
		if backup_size == 0:
			print(f"ERROR: Backup file {selected.name} is empty and cannot be restored.")
			return False
	except OSError as e:
		print(f"ERROR: Cannot access backup file: {e}")
		return False
	
	print(f"\nWARNING: This will overwrite db/peeps_scheduler.db with {selected.name}")
	confirm = input("Type 'yes' to confirm: ").strip().lower()

	if confirm == 'yes':
		try:
			# Restore the backup data
			print("PACKAGE: Restoring data from backup...")
			shutil.copy2(selected, DB_PATH)
			
			# Verify restore succeeded
			if DB_PATH.exists():
				restored_size = DB_PATH.stat().st_size
				if restored_size == backup_size:
					print("SUCCESS: Restore complete.")
					
					# Update schema.sql after successful restore to reflect new state
					if update_schema_file():
						print("FILE: Updated schema.sql to reflect restored database state")
					
					return True
				else:
					print(f"ERROR: Restore failed - file size mismatch.")
					return False
			else:
				print("ERROR: Restore failed - database file not found after restore.")
				return False
				
		except OSError as e:
			print(f"ERROR: Failed to restore backup: {e}")
			return False
		except Exception as e:
			print(f"ERROR: Unexpected error during restore: {e}")
			return False
	else:
		print("ERROR: Restore cancelled.")
		return False

if __name__ == "__main__":
	if "--restore" in sys.argv:
		restore()
	elif "--list" in sys.argv:
		list_backups()
	else:
		backup()
