#!/usr/bin/env python3
"""
Create a ZIP archive of the current Git repo, including:
- Working directory files (tracked + untracked but not ignored)
- The .git directory itself

Excluded:
- Anything matched by .gitignore, .git/info/exclude, or global ignore rules

Result:
- A single ZIP in the repo root:
  * repo-<commit>.zip                (if clean)
  * repo-<commit>-dirty-YYYYMMDD-HHMM.zip  (if dirty)
- Any previous repo-*.zip files in the root are removed first
"""

import os
import subprocess
import zipfile
from pathlib import Path
import glob
from datetime import datetime

def _run(cmd):
	return subprocess.check_output(cmd).decode().strip()

def _is_dirty():
	# Non-empty porcelain output means staged/unstaged changes or untracked files
	return bool(_run(["git", "status", "--porcelain"]))

def create_repo_zip():
	# repo root
	repo_root = Path(_run(["git", "rev-parse", "--show-toplevel"]))

	# short commit id
	short_sha = _run(["git", "rev-parse", "--short", "HEAD"])

	# remove old zips in root
	for old in glob.glob(str(repo_root / "repo-*.zip")):
		os.remove(old)

	# decide filename
	if _is_dirty():
		ts = datetime.now().strftime("%Y%m%d-%H%M")
		output_name = f"repo-{short_sha}-dirty-{ts}.zip"
	else:
		output_name = f"repo-{short_sha}.zip"

	output_path = repo_root / output_name

	# tracked + untracked but not ignored
	files = _run(["git", "ls-files", "--cached", "--others", "--exclude-standard"]).splitlines()

	# add .git explicitly
	files.append(".git")

	print(f"Creating archive: {output_path}")

	with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
		for f in files:
			path = repo_root / f
			if path.is_file():
				zf.write(path, arcname=f)
			elif path.is_dir() and f == ".git":
				# walk .git fully
				for dirpath, _, filenames in os.walk(path):
					for filename in filenames:
						fullpath = Path(dirpath) / filename
						relpath = fullpath.relative_to(repo_root)
						zf.write(fullpath, arcname=str(relpath))

	print("Done:", output_path)

if __name__ == "__main__":
	create_repo_zip()
