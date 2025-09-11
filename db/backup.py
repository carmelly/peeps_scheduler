import shutil
from datetime import datetime
from pathlib import Path
import sys
import os

# Base path of this script (e.g., db/)
BASE_PATH = Path(__file__).resolve().parent
DB_PATH = BASE_PATH / "peeps_scheduler.db"
BACKUP_DIR = BASE_PATH / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

def list_backups():
	"""List all available backups with file sizes and dates."""
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("📦 No backups found.")
	else:
		print("📁 Available backups:")
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
			print("ℹ️ No existing database to backup.")
			return True  # Not an error for auto-backup
		else:
			print("❌ Could not find database at db/peeps_scheduler.db")
			return False

	try:
		# Verify source database size
		source_size = DB_PATH.stat().st_size
		
		# Copy with error handling
		shutil.copy2(DB_PATH, backup_path)
		
		# Verify backup was created and has correct size
		if not backup_path.exists():
			print(f"❌ Backup file was not created: {backup_path}")
			return False
			
		backup_size = backup_path.stat().st_size
		if backup_size != source_size:
			print(f"❌ Backup file size mismatch. Source: {source_size}, Backup: {backup_size}")
			return False
		
		size_mb = backup_size / (1024 * 1024)
		if auto:
			print(f"🛡️ Auto-backup created: {backup_filename} ({size_mb:.1f}MB)")
		else:
			print(f"✅ Backup saved to: {backup_path} ({size_mb:.1f}MB)")
		return True
		
	except OSError as e:
		print(f"❌ Failed to create backup: {e}")
		return False
	except Exception as e:
		print(f"❌ Unexpected error during backup: {e}")
		return False

def restore():
	"""Restore database from a backup with confirmation."""
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("❌ No backups found.")
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
		print("❌ Invalid selection.")
		return False

	selected = backups[int(choice) - 1]
	
	# Verify backup file before proceeding
	try:
		backup_size = selected.stat().st_size
		if backup_size == 0:
			print(f"❌ Backup file {selected.name} is empty and cannot be restored.")
			return False
	except OSError as e:
		print(f"❌ Cannot access backup file: {e}")
		return False
	
	print(f"\n⚠️ This will overwrite db/peeps_scheduler.db with {selected.name}")
	confirm = input("Type 'yes' to confirm: ").strip().lower()

	if confirm == 'yes':
		try:
			shutil.copy2(selected, DB_PATH)
			
			# Verify restore succeeded
			if DB_PATH.exists():
				restored_size = DB_PATH.stat().st_size
				if restored_size == backup_size:
					print("✅ Restore complete.")
					return True
				else:
					print(f"❌ Restore failed - file size mismatch.")
					return False
			else:
				print("❌ Restore failed - database file not found after restore.")
				return False
				
		except OSError as e:
			print(f"❌ Failed to restore backup: {e}")
			return False
		except Exception as e:
			print(f"❌ Unexpected error during restore: {e}")
			return False
	else:
		print("❌ Restore cancelled.")
		return False

if __name__ == "__main__":
	if "--restore" in sys.argv:
		restore()
	elif "--list" in sys.argv:
		list_backups()
	else:
		backup()
