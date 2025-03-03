import datetime
import copy
import itertools
import math
import random
import json 

class Globals:
	verbosity = 1
	leader = "Leader"
	follower = "Follower"

class Event:
	def __init__(self, **args):
		self.id = args.get("id", 0)
		self.date = args.get("date", datetime.date(1900, 1, 1))
		self.min_role = args.get("min_role", 5)
		self.max_role = args.get("max_role", 8)
		self.leaders = []
		self.followers = []

	def role(self, key):
		return self.leaders if key == Globals.leader else self.followers

	@classmethod
	def generate_test_event(cls, event_id, start_date):
		"""Generate a random test event within one month on allowed days & times."""
		allowed_days = [2, 4, 5]  # Wednesday (2), Friday (4), Saturday (5)
		allowed_times = {
			2: [16, 17, 18, 19],  # Wed: 4-7 PM
			4: [16, 17, 18, 19],  # Fri: 4-7 PM
			5: [11, 19]           # Sat: 11 AM, 7 PM
		}

		# Pick a random day in the next 30 days that matches allowed days
		while True:
			rand_day = start_date + datetime.timedelta(days=random.randint(1, 30))
			if rand_day.weekday() in allowed_days:
				break

		event_hour = random.choice(allowed_times[rand_day.weekday()])
		event_datetime = datetime.datetime(rand_day.year, rand_day.month, rand_day.day, event_hour)
		
		return cls(
			id=event_id,
			date=event_datetime,
			min_role=random.randint(3, 5),
			max_role=random.randint(6, 8),
		)

	def to_dict(self):
		return {
			"id": self.id,
			"date": self.date.strftime("%Y-%m-%d %H:%M"), 
			"min_role": self.min_role,
			"max_role": self.max_role,
		}

	@staticmethod
	def from_dict(data):
		"""Convert dictionary data back into an Event object."""
		data["date"] = datetime.datetime.strptime(data["date"], "%Y-%m-%d %H:%M") 
		return Event(**data)

	def __str__(self):
		date_str =  self.date.strftime("%Y-%m-%d %H:%M"), 
		return f"Event({self.id}): date: {date_str}, min: {self.min_role}, max: {self.max_role}, L: {{ {', '.join(str(peep.id) for peep in self.leaders)} }}, F: {{ {', '.join(str(peep.id) for peep in self.followers)} }}"

class Peep:
	def __init__(self, **args):
		self.id = args.get("id", 0)
		self.index = args.get("index", 0)
		self.name = args.get("name", "") 
		self.priority = args.get("priority", 0)
		self.availability = args.get("availability", [])
		self.event_limit = args.get("event_limit", 1)
		self.role = args.get("role", "")
		self.num_events = 0

	@classmethod
	def generate_test_peep(cls, id, index, event_count):
		
		"""Generate a test Peep with random values"""
		data = {
            "id": id, 
			"index": index,
            "name": f"Person{id}",
            "priority": random.randint(0, 3),# Priority between 0 and 3
            # "availability": sorted(random.sample(range(event_count), random.randint(0, event_count))),
            "event_limit": random.randint(1, 3),
            "role": random.choice([Globals.leader, Globals.follower])
        }
		
		# Generate random event availability
		available_events = list(range(event_count))  # Event indices (0, 1, 2, ...)
		random.shuffle(available_events)
		availability = sorted(available_events[:random.randint(0, event_count)])  # Random subset
		data.update({"availability": availability})

		return cls(**data)
	
	# full representation of a Peep, can be used as a constructor 
	def _repr__(self):
		return (f"Peep(id={self.id}, name='{self.name}', priority={self.priority}, "
				f"availability={self.availability}, event_limit={self.event_limit}, role={self.role}, "
				f"total_attended={self.total_attended}, cur_scheduled={self.cur_scheduled}, "
				f"num_events={self.num_events}, index={self.index})")

	# Simplified tostring for easier testing 
	def __str__(self):
		role_str = "L" if self.role == Globals.leader else "F"
		return (f"Peep: id: {self.id}, p: {self.priority}, limit: {self.event_limit}, "
				f"a: {self.availability}, role: {role_str}")
	
class EventSequence:
	def __init__(self, events, peeps):
		self.events = events
		self.peeps = peeps
		self.num_unique_attendees = 0
		self.system_weight = 0
		self.valid_events = []

	def __str__(self):
		result = (f"EventSequence: "
			f"og events: {{ {', '.join(str(event.id) for event in self.events)} }}, "
			f"valid events: {{ {', '.join(str(event.id) for event in self.valid_events)} }}, " 
			f"unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, system_weight {self.system_weight}\n"
		)
		for event in self.valid_events:
			result += f"    {event}\n"
		return result

def save_json(data, filename):
    """Save data (list of dicts) to a JSON file."""
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def load_json(filename):
    """Load data from a JSON file."""
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
	
def initialize_data (generate_events=True, generate_peeps=True):
	num_events = 7
	num_peeps = 28
	event_filename = "test_events.json"
	peep_filename = "test_peeps.json"

  # Load or generate events
	if generate_events:
		start_date = datetime.date.today()
		events = [Event.generate_test_event(i, start_date) for i in range(num_events)]
		save_json([event.to_dict() for event in events], event_filename)
		print(f"Saved {len(events)} Events to {event_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		event_data = load_json(event_filename)
		events = [Event.from_dict(e) for e in event_data] if event_data else []

	# Load or generate peeps
	if generate_peeps:
		peeps = [Peep.generate_test_peep(i, i - 1, num_events) for i in range(num_peeps)]
		save_json([peep.__dict__ for peep in peeps], peep_filename)
		print(f"Saved {len(peeps)} Peeps to {peep_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		peep_data = load_json(peep_filename)
		peeps = [Peep(**p) for p in peep_data] if peep_data else []

	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)
	return sorted_peeps, events
	
	# if generate: 
	# 	#generate peeps list randomly 
	# 	num_peeps = 28
	# 	peeps = [Peep.generate_test_peep(i, i-1, num_events) for i in range(num_peeps)]
	# else: 
	# 	# read peeps list from json 
	# 	with open("test_peeps.json", "r") as file:
	# 		data = json.load(file)
	# 	peeps = [Peep(**peep) for peep in data]
    
	# # sort peeps by priority while keeping their relative ordering 
	# sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority) 
	# return sorted_peeps, events

def sim(og_peeps, og_events):
	# generate all permutations of events
	def generate_event_sequences():
		event_sequences = []
		
		if not len(og_events):
			return []
		indices = [i for i in range(len(og_events)) ]
		# brute force. there are better ways...
		index_sequences = list(itertools.permutations(indices, len(indices)))

		for index_sequence in index_sequences:
			event_sequence = []
			for event_index in index_sequence:
				event_sequence.append(copy.deepcopy(og_events[event_index]))
			event_sequences.append(EventSequence(event_sequence, copy.deepcopy(og_peeps)))

		return event_sequences

	event_sequences = generate_event_sequences() 

	# can someone go to this event
	def eval_event(peep, event):
		# meets the person's availability
		if event.id not in peep.availability:
			return False

		# space for the role
		if len(event.role(peep.role)) >= event.max_role:
			return False

		# personal limit for month
		if peep.num_events >= peep.event_limit:
			return False

		return True

	def eval_eventsequence(sequence):
		for event in sequence.events:
			# always sort: this shouldn't actually do anything unless you start manipulating priorities within a sequence(TBD?) 
			sorted_peeps = sorted(sequence.peeps, reverse=True, key=lambda peep: peep.priority)
			winners = []
			losers = []

			# add peeps to event 
			for peep in sorted_peeps:
				if eval_event(peep, event):
					event.role(peep.role).append(peep)
					winners.append(peep)
				else:
					losers.append(peep)

			# event is valid: apply changes, otherwise, do nothing and the sequence's peeps apply to the next event
			if len(event.leaders) >= event.min_role and len(event.followers) >= event.min_role:
				# find the floor to balance leaders and followers
				if len(event.leaders) != len(event.followers):
					bigger_list = event.leaders if len(event.leaders) > len(event.followers) else event.followers
					num_to_pop = abs(len(event.leaders) - len(event.followers))
					while num_to_pop > 0:
						winners.remove(bigger_list.pop(len(bigger_list) - 1))
						num_to_pop -= 1

				assert(len(event.leaders) == len(event.followers))
				assert(len(event.leaders) >= event.min_role and len(event.leaders) <= event.max_role)
				assert(len(event.followers) >= event.min_role and len(event.followers) <= event.max_role)

				def scoot(peep):
					sorted_peeps.remove(peep)
					sorted_peeps.append(peep)
					
				# apply failure
				for peep in losers:
					scoot(peep)

				# apply success
				for peep in winners:
					peep.num_events += 1
					peep.priority = 0
					scoot(peep)
	
				sequence.peeps = sorted_peeps
				sequence.valid_events.append(event)
			else:
				event.leaders = []
				event.followers = []
				pass

		# end of sequence, update
		for peep in sequence.peeps:
			# didn't make it to any, increase prio for next scheduling
			if peep.num_events <= 0:
				peep.priority += 1
			else:
				sequence.num_unique_attendees += 1

			# track fitness of result
			sequence.system_weight += peep.priority

	for sequence in event_sequences:
		eval_eventsequence(sequence)

	event_sequences = [sequence for sequence in event_sequences if len(sequence.valid_events)]

	# sort by unique attendees and whichever result has the least system weight
	sorted_sequences = sorted(event_sequences, reverse=True, key=lambda sequence: (sequence.num_unique_attendees, sequence.system_weight))
	return sorted_sequences

def print_peeps(peeps): 
	for peep in peeps: 
		print(f"   {peep}")

def main():
	# should we generate new lists? otherwise read from file 
	generate_events = False 
	generate_peeps = False 
	peeps, events = initialize_data(generate_events, generate_peeps)

	# remove events where there are not enough of any given role
	def sanitize_events(events):
		valid_events = []
		removed_events = []
		for event in events:
			num_leaders = 0
			num_followers = 0
			for peep in peeps:
				if event.id in peep.availability:
					if peep.role == Globals.leader:
						num_leaders += 1
					else:
						num_followers += 1

			if not (num_leaders < event.min_role or num_followers < event.min_role):
				valid_events.append(event)
			else:
				removed_events.append(event)

		return valid_events

	sanitized_events = sanitize_events(events)

	print("=====")
	print("Initial State")
	print_peeps(peeps)
	print("=====")
	print(f"Sanitized Events: {len(sanitized_events)}/{len(events)}")
	print("=====")

	sorted_sequences = sim(peeps, sanitized_events)
	
	if len(sorted_sequences):

		print(f"winner winner chicken dinner:" ) 
		print(f"{sorted_sequences[0]}")
		print("=====")
		print("Final State")
		for peep in sorted_sequences[0].peeps:
			print(f"   {peep}")
	else:
		print("No Winner")

	def save_json(peeps, filename="test_peeps.json"):
		"""Save the list of peeps as a JSON file."""
		with open(filename, "w") as f:
			json.dump([peep.__dict__ for peep in peeps], f, indent=4)
		print(f"Saved {len(peeps)} Peeps to {filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	
if __name__ == "__main__":
	for i in range(1):
		main()
