import shutil
from datetime import datetime
from pathlib import Path
import sys

# Base path of this script (e.g., db/)
BASE_PATH = Path(__file__).resolve().parent
DB_PATH = BASE_PATH / "peeps_scheduler.db"
BACKUP_DIR = BASE_PATH / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

def list_backups():
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("📦 No backups found.")
	else:
		print("📁 Available backups:")
		for b in backups:
			print(f"- {b.name}")

def backup():
	backup_label = input("Enter a name for this backup: ").strip().replace(" ", "_") or "backup"
	timestamp = datetime.now().strftime("%Y%m%d-%H%M")
	backup_filename = f"{timestamp}_{backup_label}.db"
	backup_path = BACKUP_DIR / backup_filename

	if DB_PATH.exists():
		shutil.copy2(DB_PATH, backup_path)
		print(f"✅ Backup saved to: {backup_path}")
	else:
		print("❌ Could not find database at db/peeps_scheduler.db")

def restore():
	backups = sorted(BACKUP_DIR.glob("*.db"))
	if not backups:
		print("❌ No backups found.")
		return

	print("\nAvailable backups:")
	for i, b in enumerate(backups):
		print(f"[{i+1}] {b.name}")

	choice = input("\nEnter the number of the backup to restore: ").strip()
	if not choice.isdigit() or not (1 <= int(choice) <= len(backups)):
		print("❌ Invalid selection.")
		return

	selected = backups[int(choice) - 1]
	print(f"\n⚠️ This will overwrite db/peeps_scheduler.db with {selected.name}")
	confirm = input("Type 'yes' to confirm: ").strip().lower()

	if confirm == "yes":
		shutil.copy2(selected, DB_PATH)
		print("✅ Restore complete.")
	else:
		print("❌ Restore cancelled.")

if __name__ == "__main__":
	if "--restore" in sys.argv:
		restore()
	elif "--list" in sys.argv:
		list_backups()
	else:
		backup()
