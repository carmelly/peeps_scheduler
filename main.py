import datetime
import copy
import itertools
import math
import random
import json 

class Globals:
	verbosity = 1
	days_between_events = 7
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

	def is_valid(self): 
		""" Event is valid if we have enough leaders and enough followers to fill the minimum per role """
		return( len(self.leaders) >= self.min_role and len(self.followers) >= self.min_role) 
	
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
		valid_days = [start_date + datetime.timedelta(days=i) for i in range(1, 31) if (start_date + datetime.timedelta(days=i)).weekday() in allowed_days]
		rand_day = random.choice(valid_days)

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

	def __repr__(self): 
		return str(self)
	def __str__(self):
		date_str =  self.date.strftime("%Y-%m-%d %H:%M") 
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
	
	@staticmethod
	def peeps_str(peeps):
		"""Return a string representation of a list of Peeps."""
		result =  f"Peeps[{len(peeps)}]:\n" 
		result += f"\n".join(f"   {peep}" for peep in peeps)
		return result

	# full representation of a Peep, can be used as a constructor 
	def __repr__(self):
		return (f"Peep(id={self.id}, name='{self.name}', priority={self.priority}, "
				f"availability={self.availability}, event_limit={self.event_limit}, role={self.role}, "
				f"total_attended={self.total_attended}, cur_scheduled={self.cur_scheduled}, "
				f"num_events={self.num_events}, index={self.index})")

	# Simplified tostring for easier testing 
	def __str__(self):
		role_str = "L" if self.role == Globals.leader else "F"
		return (f"Peep({self.id:>3}): p: {self.priority}, limit: {self.event_limit}, "
				f"role: {role_str}, a: {self.availability}")
	
class EventSequence:
	def __init__(self, events, peeps):
		self.events = events
		self.peeps = peeps
		self.num_unique_attendees = 0
		self.system_weight = 0
		self.valid_events = []

	def __repr__(self):
		return (', '.join(str(event.id) for event in self.events))
	def __str__(self):
		result = (f"EventSequence: "
			f"og events: {{ {', '.join(str(event.id) for event in self.events)} }}, "
			f"valid events: {{ {', '.join(str(event.id) for event in self.valid_events)} }}, " 
			f"unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, system_weight {self.system_weight}\n"
		)
		result += f"\t"
		result += "\n\t".join(str(event) for event in self.valid_events)
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


def can_attend(peep, event):
	"""Checks if a peep can attend an event based on peep availaility, role limit and personal event limit """
	
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

def update_event_attendees(peeps, winners):
	"""For all successful attendees, reset priority and send to the back of the line  """
	for peep in winners:
		peep.num_events += 1
		peep.priority = 0  # Reset priority after successful attendance
		peeps.remove(peep)
		peeps.append(peep)  # Move successful peeps to the end

def finalize_sequence(sequence):
	"""Finalizes a sequence by increasing priority for unsuccessful peeps and tracking metrics."""
	for peep in sequence.peeps:
		if peep.num_events == 0:
			peep.priority += 1  # Increase priority if not assigned to any event
		else:
			sequence.num_unique_attendees += 1

		sequence.system_weight += peep.priority  # Track total system priority weight

def generate_event_sequences(events, peeps):
	days = 7

	def gen(): 
		def gen2(events2, days_gap): 
			min_date_gap  = days_gap * 24 - 1
			if len(events2) == 1:
				return [events2]

			result = [] 
			for a in events2:
				remainder = []
				for x in events2: 
					date_gap = abs((a.date - x.date)/datetime.timedelta(hours=1))
					if x != a and date_gap >= min_date_gap: 
						remainder.append(x)
				z = gen2(remainder, days_gap) 

				for t in z:
					result.append([a] + t)

			return result

		seqs = gen2(events, days)
		event_sequences = [] 
		for seq in seqs: 
			event_sequences.append(EventSequence(copy.deepcopy(seq), copy.deepcopy(peeps)))

		return event_sequences
			
	def with_date_check(days_gap):
		min_date_gap  = days_gap * 24 - 1
		"""Generates all possible permutations of event sequences. TODO: sanitize for date spread """
		def generate_permutations(events, path=[], results=[]):
			if not events:
				results.append(EventSequence(copy.deepcopy(path), copy.deepcopy(peeps)))
				return results

			# if the next event to be evaluated conflicts with any previous event, 
			# bail early on this permutation 
			next_event = events[0]
			conflict = False 
			for event in path: 
				date_gap = abs((event.date - next_event.date)/datetime.timedelta(hours=1))
				if event.id == 1 and next_event.id == 5: 
					print (date_gap)

				if date_gap < min_date_gap: 
					conflict = True 

			if conflict: 
				events.pop(0)
				generate_permutations(events, path, results)
				return results

			# if not events: 
				# results.append(EventSequence(copy.deepcopy(path), copy.deepcopy(peeps)))
			
			for i in range(len(events)):
				generate_permutations(events[:i] + events[i+1:], path + [events[i]], results)

			return results

		sequences = generate_permutations(events)

		# print("Final valid sequences:")
		# for seq in valid_sequences:
		# 	print([e.id for e in seq.events])

		print(f"Total valid sequences generated: {len(sequences)}")
		return sequences

	def without_date_check():
		"""Generates all possible permutations of event sequences."""
		event_sequences = []
		
		if not len(events):
			return []
		indices = [i for i in range(len(events)) ]
		# brute force. there are better ways...
		index_sequences = list(itertools.permutations(indices, len(indices)))

		for index_sequence in index_sequences:
			event_sequence = []
			for event_index in index_sequence:
				event_sequence.append(copy.deepcopy(events[event_index]))
			event_sequences.append(EventSequence(event_sequence, copy.deepcopy(peeps)))

		# print("Final event sequences:")
		# for seq in event_sequences:
		# 	print([e.id for e in seq.events])

		print(f"Total event sequences generated: {len(event_sequences)}")
		return event_sequences
	
	all_seqs = without_date_check()
	
	print (f"Days between events {days}")
	# filtered_seqs = with_date_check(days)
	filtered_seqs = gen()
	print(f"Total valid sequences generated: {len(filtered_seqs)}")

	# for i in range(len(all_seqs)): 
	# 	for k in range(len(all_seqs[i].events)): 
	# 		assert all_seqs[i].events[k].id == filtered_seqs[i].events[k].id
	
	return filtered_seqs

def evaluate_all_event_sequences(og_peeps, og_events):
	"""Generates and evaluates all possible event sequences based on peep availability and role limits."""
    
	
	
	def balance_roles(event, winners):
		"""Ensures leaders and followers are balanced within an event."""
		if len(event.leaders) != len(event.followers):
			larger_group = event.leaders if len(event.leaders) > len(event.followers) else event.followers
			while len(event.leaders) != len(event.followers):
				winners.remove(larger_group.pop())

		assert len(event.leaders) == len(event.followers) >= event.min_role
		assert len(event.leaders) <= event.max_role

	def evaluate_sequence(sequence):
		"""Evaluates an event sequence by assigning peeps to events and updating priorities and list order."""
		for event in sequence.events:
			# always sort: this shouldn't actually do anything unless you start manipulating priorities within a sequence(TBD?) 
			sorted_peeps = sorted(sequence.peeps, reverse=True, key=lambda peep: peep.priority)
			winners = []
			losers = []

			# add peeps to event 
			for peep in sorted_peeps:
				if can_attend(peep, event):
					event.role(peep.role).append(peep)
					winners.append(peep)
				else:
					losers.append(peep)

			if event.is_valid(): # if we have enough to fill event
				balance_roles(event, winners)
				update_event_attendees(sorted_peeps, winners)
				sequence.peeps = sorted_peeps
				sequence.valid_events.append(event)
			else:
				event.leaders.clear()
				event.followers.clear() 

		# end of sequence, update peeps who didn't make it 
		finalize_sequence(sequence)
		
	# create all the permutation 
	event_sequences = generate_event_sequences(og_events, og_peeps)
	# process every permutation 
	for sequence in event_sequences:
		evaluate_sequence(sequence)
	# only include the sequences that had valid events
	event_sequences = [sequence for sequence in event_sequences if len(sequence.valid_events)]
	return event_sequences 
	

def main():
	# should we generate new lists? otherwise read from file 
	generate_events = False 
	generate_peeps = False 
	
	# remove events where there are not enough available to fill roles
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

	peeps, events = initialize_data(generate_events, generate_peeps)
	sanitized_events = sanitize_events(events)

	if Globals.verbosity > 0: 
		print("=====")
	if Globals.verbosity > 1: 
		print("Initial State")
		print(Peep.peeps_str(peeps))
		print("=====")
	if Globals.verbosity > 0: 
		print(f"Sanitized Events: {len(sanitized_events)}/{len(events)}")
		print("=====")

	event_sequences = evaluate_all_event_sequences(peeps, sanitized_events)

	# sort by unique attendees and whichever result has the least system weight
	sorted_sequences = sorted(event_sequences, reverse=True, key=lambda sequence: (sequence.num_unique_attendees, sequence.system_weight))
	best_sequence = sorted_sequences[0] if sorted_sequences else None

	if Globals.verbosity > 0: 
		if best_sequence:
			print(f"{best_sequence}")
			print("=====")
			if Globals.verbosity > 1: 
				print("Final State")
				print(Peep.peeps_str(best_sequence.peeps))
		else:
			print("No Winner")

if __name__ == "__main__":
	for i in range(1):
		main()
