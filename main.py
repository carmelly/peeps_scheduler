from datetime import date
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
		self.id = id
		self.date = date(1900, 1, 1)
		# min/max peps required per role
		self.min_role = 0
		self.max_role = 0
		# current assignments
		self.leaders = []
		self.followers = []

		for key, value in args.items():
			setattr(self, key, value)

	def role(self, key):
		return self.leaders if key == Globals.leader else self.followers

	def __str__(self):
		return f"Event({self.id}): name: {self.date}, date: {self.date}, min: {self.min_role}, max: {self.max_role}"

class Peep:
	def __init__(self, **args):
		self.num_events = 0
		for key, value in args.items():
			setattr(self, key, value)

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
	
	def to_json(self):
		return json.dumps(self.__dict__, indent=4)

	@staticmethod
	def parse_json(json_str):
		data = json.loads(json_str)
		return Peep(**data)
	
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
			f"unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, system_weight {self.system_weight}"
		)
		for event in self.valid_events:
			result += f"\n   Event: {event.id}, L: {{ {', '.join(str(peep.id) for peep in event.leaders)} }}, F: {{ {', '.join(str(peep.id) for peep in event.followers)} }}"
		return result

# read peeps from the google docs
def read_doc(generate=False):
	num_events = 7
	events = [ Event(id=i, min_role=5, max_role=7) for i in range(num_events) ]

	if generate: 
		#generate peeps list randomly 
		num_peeps = 28
		peeps = [Peep.generate_test_peep(i, i-1, num_events) for i in range(num_peeps)]
	else: 
	# read peeps list from json 
		with open("test_peeps.json", "r") as file:
			data = json.load(file)
		peeps = [Peep(**peep) for peep in data]
    
	# sort peeps by priority while keeping their relative ordering 
	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority) 
	return sorted_peeps, events

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
	generate = True # should we generate a new list? otherwise read from file 
	peeps, events = read_doc(generate)

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

	# Ask if user wants to save -- useful for saving "interesting" results to debug 
	# save_choice = input("\nDo you want to save the initial Peeps to a JSON file? (yes/no): ").strip().lower()
	# if save_choice in ["yes", "y"]:
	# 	save_json(peeps)
		
	# if we generated a new list, save it, overwriting previous test json
	# if the result was interesting, rename test_peeps.json to something else before running again 
	if(generate):
		save_json(peeps)
	
if __name__ == "__main__":
	for i in range(1):
		main()
