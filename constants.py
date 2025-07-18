DATE_FORMAT = "%Y-%m-%d %H:%M"
DATESTR_FORMAT = "%A %B %d - %I%p"

CLASS_CONFIG = {
	60: {
		"price": 120.0, #TODO: Not sure if this is correct, or what the role limits should be 
		"min_role": 2,
		"max_role": 3,
	},
	90: {
		"price": 195.0,
		"min_role": 4,
		"max_role": 5,
	},
	120: {
		"price": 260.0,
		"min_role": 6,
		"max_role": 7,
	}
}
ABS_MIN_ROLE = min(config["min_role"] for config in CLASS_CONFIG.values())
ABS_MAX_ROLE = max(config["max_role"] for config in CLASS_CONFIG.values())