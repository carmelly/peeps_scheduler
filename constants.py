import os

DATE_FORMAT = "%Y-%m-%d %H:%M"
DATESTR_FORMAT = "%A %B %d - %I%p"

CLASS_CONFIG = {
	60: {
		"price": 120.0, 
		"min_role": 2,
		"max_role": 3,
		"allow_downgrade": False
	},
	90: {
		"price": 195.0,
		"min_role": 4,
		"max_role": 5,
		"allow_downgrade": True
	},
	120: {
		"price": 260.0,
		"min_role": 6,
		"max_role": 7,
		"allow_downgrade": True
	}
}

ABS_MIN_ROLE = min(config["min_role"] for config in CLASS_CONFIG.values() if config["allow_downgrade"])
ABS_MAX_ROLE = max(config["max_role"] for config in CLASS_CONFIG.values())

# Private data submodule root - can be overridden by environment
PRIVATE_DATA_ROOT = os.getenv("PEEPS_DATA_PATH", "peeps_data")

# Database paths
SCHEMA_PATH = "db/schema.sql"
DEFAULT_DB_PATH = "peeps_data/peeps_scheduler.db"

# Optional data files
PARTNERSHIPS_FILE = "partnerships.json"
