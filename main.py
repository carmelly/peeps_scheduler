from datetime import date
import copy
import itertools
import math
import random

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

	def Role(self, key):
		return self.leaders if key == Globals.leader else self.followers

	def __str__(self):
		return f"Event({self.id}): name: {self.date}, date: {self.date}, min: {self.min_role}, max: {self.max_role}"

class Peep:
	def __init__(self, **args):
		self.id = id
		self.name = "Dummy"
		self.priority = 0
		# event ids
		self.availability = []
		# repeat count per scheduling epoch
		self.event_limit = 2
		self.role = Globals.leader 
		self.total_attended = 0
		self.cur_scheduled = 0
		self.num_events = 0
		self.index = 0
		for key, value in args.items():
			setattr(self, key, value)

	def __str__(self):
		return f"Peep(id={self.id}, name=\"{self.name}\", prio={self.priority}, availability={self.availability}, role=Globals.{self.role})" 

class EventSequence:
	def __init__(self, events, peeps):
		self.events = events
		self.peeps = peeps
		self.num_unique_attendees = 0
		self.system_weight = 0
		self.valid_events = []

	def __str__(self):
		result = f"EventSequence: {{ {', '.join(str(event.id) for event in self.events)} }}, unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, system_weight {self.system_weight}"
		for event in self.events:
			result += f"\n   Event: {event.id}, L: {{ {', '.join(str(peep.id) for peep in event.leaders)} }}, F: {{ {', '.join(str(peep.id) for peep in event.followers)} }}"
		return result

# read peeps from the google docs
def read_doc():
	num_events = 3
	events = [ Event(id=i, min_role=3, max_role=3) for i in range(num_events) ]

	num_peeps = 15 
	peeps = [ Peep(id=i, name=f"person{i}") for i in range(num_peeps) ]
	# make half leaders, half followers
	for i in range(math.floor(num_peeps / 2)):
		peeps[i].role = Globals.follower	

	# randomize priority
	for peep in peeps:
		peep.priority = random.randint(0, 3)

	# randomize availability
	for peep in peeps:
		# how many events can this person attend
		attend_count = random.randint(0, len(events) - 1)
		event_indices = [i for i in range(len(events)) ]
		# remove a random index
		while attend_count and event_indices:
			peep.availability.append(event_indices.pop(random.randint(0, len(event_indices) - 1)))
			attend_count -= 1
		peep.availability.sort()

	# Peep(id=0, name="person0", prio=3, availability=[0, 2], role=Globals.Follower)
	# Peep(id=2, name="person2", prio=3, availability=[1], role=Globals.Follower)   
	# Peep(id=3, name="person3", prio=3, availability=[0, 2], role=Globals.Follower)
	# Peep(id=8, name="person8", prio=3, availability=[], role=Globals.Leader)      
	# Peep(id=12, name="person12", prio=3, availability=[], role=Globals.Leader)    
	# Peep(id=4, name="person4", prio=2, availability=[], role=Globals.Follower)    
	# Peep(id=5, name="person5", prio=2, availability=[0, 2], role=Globals.Follower)
	# Peep(id=10, name="person10", prio=2, availability=[0, 1], role=Globals.Leader)
	# Peep(id=14, name="person14", prio=2, availability=[1], role=Globals.Leader)   
	# Peep(id=1, name="person1", prio=1, availability=[], role=Globals.Follower)
	# Peep(id=13, name="person13", prio=1, availability=[1], role=Globals.Leader)
	# Peep(id=6, name="person6", prio=0, availability=[], role=Globals.Follower)
	# Peep(id=7, name="person7", prio=0, availability=[], role=Globals.Leader)
	# Peep(id=9, name="person9", prio=0, availability=[], role=Globals.Leader)
	# Peep(id=11, name="person11", prio=0, availability=[], role=Globals.Leader)

	# stable sort
	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)
	return sorted_peeps, events

def sim(og_peeps, og_events):
	# generate all permutations of events
	def generate_event_sequences():
		event_sequences = []
		indices = [i for i in range(len(og_events)) ]
		# brute force.  wanna fight about it? 
		index_sequences = list(itertools.permutations(indices, len(indices)))

		for index_sequence in index_sequences:
			event_sequence = []
			for event_index in index_sequence:
				event_sequence.append(copy.deepcopy(og_events[event_index]))
			event_sequences.append(EventSequence(event_sequence, copy.deepcopy(og_peeps)))

		return event_sequences

	event_sequences = generate_event_sequences() 

	print(f"NumEventSequences: {len(event_sequences)}")

	# can someone go to this event
	def eval_event(peep, event):
		# meets the person's availability
		if event.id not in peep.availability:
			return False

		# space for the role
		if len(event.Role(peep.role)) >= event.max_role:
			return False

		# limit for month
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
					event.Role(peep.role).append(peep)
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

				# for each peep, put them to the end of the rr queue
				# because leaders and followers are technically two separate lists (unless someone changes their role), the relative order to leaders/followers doesn't matter
				def apply_success(peep):
					peep.num_events += 1
					peep.priority = 0
					# scootch
					sorted_peeps.remove(peep)
					sorted_peeps.append(peep)

				for peep in losers:
					# scootch
					sorted_peeps.remove(peep)
					sorted_peeps.append(peep)

				for peep in winners:
					apply_success(peep)

				sequence.peeps = sorted_peeps
				sequence.valid_events.append(event)
			else:
				event.leaders = []
				event.followers = []
				pass

		# end of sequence, update
		for peep in sequence.peeps:
			# heuristic: "at least once a month" for "num_events"
			# didn't make it to any, increase prio for next month
			if peep.num_events <= 0:
				peep.priority += 1
			else:
				sequence.num_unique_attendees += 1

			# track fitness of result
			sequence.system_weight += peep.priority


	for sequence in event_sequences:
		eval_eventsequence(sequence)

	sorted_sequences = sorted(event_sequences, reverse=True, key=lambda sequence: sequence.num_unique_attendees)
	return sorted_sequences

def main():
	peeps, events = read_doc()

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
	print("Peeps:")
	# for p in peeps:
	# 	print(p)

	print(f"Events: {len(sanitized_events)}/{len(events)}")
	# for e in sanitized_events:
	# 	print(e)

	sorted_sequences = sim(peeps, sanitized_events)
	
	if len(sorted_sequences):
		print("InitialState")
		for peep in peeps:
			print(f"   {peep}")

		print(f"winner winner chicken dinner: {sorted_sequences[0]}")
		for peep in sorted_sequences[0].peeps:
			print(f"   {peep}")
	else:
		print("failure")
			
	# for sequence in sorted_sequences:
	# 	print(f"   {sequence}")
	
if __name__ == "__main__":
	for i in range(1):
		main()
