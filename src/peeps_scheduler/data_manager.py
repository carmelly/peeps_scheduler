"""
Data management module for Novice Peeps scheduling system.
Handles path management for private data submodule and period archiving.
"""
from pathlib import Path
from typing import List
from peeps_scheduler import constants

class DataManager:
	"""Manages data paths and period archiving for the scheduling system."""
	
	def __init__(self, submodule_root: str = None):
		"""
		Initialize DataManager with submodule root path.
		
		Args:
			submodule_root: Path to the private data submodule (defaults to constants.PRIVATE_DATA_ROOT)
		"""
		if submodule_root is None:
			submodule_root = constants.PRIVATE_DATA_ROOT
		self.submodule_root = Path(submodule_root)
		self.original_path = self.submodule_root / "original"
		self.processed_path = self.submodule_root / "processed"
		self.db_backups_path = self.submodule_root / "db_backups"
		self.db_path = self.submodule_root / "peeps_scheduler.db"
		
		# Ensure directories exist
		self._ensure_directories()
	
	def _ensure_directories(self) -> None:
		"""Create required directories if they don't exist."""
		for path in [self.original_path, self.processed_path, self.db_backups_path]:
			path.mkdir(parents=True, exist_ok=True)
	
	# Path accessors for clean integration with existing code
	def get_original_data_path(self, period_slug: str = None) -> Path:
		"""Get path to original data directory for a period."""
		if period_slug:
			return self.original_path / period_slug
		return self.original_path
	
	def get_processed_data_path(self, period_slug: str = None) -> Path:
		"""Get path to processed data directory for a period."""
		if period_slug:
			return self.processed_path / period_slug
		return self.processed_path
	
	def get_db_backups_path(self) -> Path:
		"""Get path to database backups directory."""
		return self.db_backups_path
	
	def get_database_path(self) -> Path:
		"""Get path to the main database file."""
		return self.db_path
	
	# Period archiving utilities
	def get_period_path(self, period_slug: str) -> Path:
		"""Get the working path for a period (where you put CSVs and run scheduler)."""
		return self.get_original_data_path(period_slug)
	
	def ensure_period_exists(self, period_slug: str) -> Path:
		"""Ensure period directory exists and return path."""
		period_path = self.get_period_path(period_slug)
		period_path.mkdir(exist_ok=True)
		return period_path
	
	def list_periods(self) -> List[str]:
		"""
		List all periods in original data.
		
		Returns:
			List of period slugs
		"""
		periods = []
		for item in self.original_path.iterdir():
			if item.is_dir():
				periods.append(item.name)
		
		return sorted(periods)


# Global instance for easy access throughout the codebase
_data_manager = None

def get_data_manager() -> DataManager:
	"""Get the global DataManager instance."""
	global _data_manager
	if _data_manager is None:
		_data_manager = DataManager()
	return _data_manager
