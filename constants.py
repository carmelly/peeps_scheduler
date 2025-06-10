DATE_FORMAT = "%Y-%m-%d %H:%M"
DATESTR_FORMAT = "%A %B %d - %I%p"

CLASS_CONFIG = {
	90: {
		"price": 195.0,
		"min_role": 4,
		"max_role": 4,
	},
	120: {
		"price": 260.0,
		"min_role": 5,
		"max_role": 8,
	}
}
ABS_MIN_ROLE = min(config["min_role"] for config in CLASS_CONFIG.values())
ABS_MAX_ROLE = max(config["max_role"] for config in CLASS_CONFIG.values())