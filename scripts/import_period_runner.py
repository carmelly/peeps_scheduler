import json
import shutil
from pathlib import Path

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from db_imports import import_period

def save_manifest(manifest: dict, path: str):
	# Normalize all string paths inside manifest to forward slashes
	def normalize(v):
		if isinstance(v, str):
			return Path(v).as_posix()  # always forward slashes
		if isinstance(v, dict):
			return {k: normalize(val) for k, val in v.items()}
		if isinstance(v, list):
			return [normalize(val) for val in v]
		return v

	normalized = normalize(manifest)

	with open(path, "w", encoding="utf-8") as f:
		json.dump(normalized, f, indent=2)

DB_PATH = Path("db/peeps_scheduler.db")

def run_import_with_preview(slug: str, manifest_path: Path):
	# load manifest
	manifest = json.loads(manifest_path.read_text())
	pre_backup = Path(manifest["db_backups"]["pre"])
	post_backup = Path(manifest["db_backups"].get("post") or f"db/backups/period_{slug}_post_import.db")

	# 1. Restore pre backup
	shutil.copy(pre_backup, DB_PATH)
	print(f"✅ Restored DB from {pre_backup}")

	# 2. Run dry run
	print("\n--- Dry run preview ---\n")
	import_period(slug, dry_run=True)

	# 3. Confirm
	choice = input("\nProceed with import? [y/N]: ").strip().lower()
	if choice != "y":
		print("❌ Import cancelled. DB remains at pre-backup state.")
		return

	# 4. Run real import
	result = import_period(slug)

	# 5. Save post-backup
	shutil.copy(DB_PATH, post_backup)
	print(f"💾 Saved post-import backup to {post_backup}")

	# 6. Update and save manifest
	manifest["db_backups"]["post"] = str(post_backup)
	manifest.update(result)

	save_manifest(manifest, manifest_path)
	print(f"📝 Updated manifest.json with post-backup path")

if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument("--slug", required=True, help="Schedule period slug (e.g. 2025-05)")
	args = parser.parse_args()
	run_import_with_preview(args.slug, Path(f"db/imports/{args.slug}/manifest.json"))
