import csv
import json
from collections import defaultdict
from datetime import datetime

def parse_event_date(date_str):
	"""
	Parse an event date string and return a formatted datetime string.
	Assumes the event is in the current year.
	TODO: fix for next year if date has passed, but right now we're testing 
	with old dates. 

	Expected input format: "Month Day - H[AM/PM]" (e.g., "March 5 - 4PM")
	Output format: "YYYY-MM-DD HH:MM"
	"""
	current_year = datetime.now().year
	dt = datetime.strptime(f"{current_year} {date_str}", "%Y %B %d - %I%p")
	return dt.strftime("%Y-%m-%d %H:%M")

# Load CSV data
def load_csv(filename):
	with open(filename, newline='', encoding='utf-8') as csvfile:
		return list(csv.DictReader(csvfile))

def convert_to_json(responses_file, members_file, output_file):
	peeps_data = load_csv(members_file)
	responses_data = load_csv(responses_file)
	
	unique_peeps = {}
	unique_events = {}
	jsonData = []
	event_counter = 0
	
	# Process members data
	for row in peeps_data:
		id, name, role, index, priority, total_attended = row['id'], row['Name'].strip(), row['Role'], row['Index'], row['Priority'], row['Total Attended']
		if id not in unique_peeps:
			unique_peeps[id] = {
				"id": id,
				"name": name,
				"role": role,
				"index": int(index),
				"priority": int(priority),
				"total_attended": int(total_attended),
				"availability": [],
				"event_limit": 0
			}
	
	# Process responses
	for row in responses_data:
		name, preferred_role, max_sessions, available_dates = row['Name'].strip(), row['Preferred Role'], row['Max Sessions'], row['Availability']
		matched_peeps = [peep for peep in unique_peeps.values() if peep['name'].lower() == name.lower()]
		
		if not matched_peeps:
			matched_peeps = [peep for peep in unique_peeps.values() if peep['name'].split()[0].lower() == name.split()[0].lower()]
		
		if len(matched_peeps) == 1:
			peep = matched_peeps[0]
			peep['event_limit'] = max_sessions
			
			event_ids = []
			for event in available_dates.split(', '):
				if event: 
					if event not in unique_events:
						unique_events[event] = {
							"id": event_counter,
							"date": parse_event_date(event),
						}
						event_counter += 1
					event_ids.append(unique_events[event]['id'])
			
			peep['availability'] = list(set(peep['availability'] + event_ids))
			jsonData.append({
				"timestamp": row['Timestamp'],
				"name": name,
				"preferred_role": preferred_role,
				"max_sessions": max_sessions,
				"available_dates": available_dates.split(', '),
				"comments": row.get('Comments', '')
			})
		else:
			print(f"Error: {len(matched_peeps)} matches found for '{name}', skipping.")
	
	output = {
		"responses": jsonData,
		"events": list(unique_events.values()),
		"peeps": list(unique_peeps.values())
	}
	
	with open(output_file, 'w', encoding='utf-8') as f:
		json.dump(output, f, indent=2)
	
if __name__ == "__main__":
	responses_csv = 'data/Novice Peeps Scheduling - March Responses.csv'
	peeps_csv = 'data/Novice Peeps Scheduling - All Novice Peeps Members.csv'
	output_json = 'data/novice_peeps_output.json'

	convert_to_json(responses_csv, peeps_csv, output_json)


