import datetime
import copy
import itertools
import random
import json 
import sys
import logging 
from csv_loader import convert_to_json

class Globals:
	verbosity = 1
	days_between_events = 3
	max_events = 7 # we can't compute permutations for more events than this at a time 
	leader = "Leader"
	follower = "Follower"

class Event:
	def __init__(self, **kwargs):
		self.id = kwargs.get("id", 0)
		self.date = kwargs.get("date", None)
		self.min_role = kwargs.get("min_role", 5)
		self.max_role = kwargs.get("max_role", 8)
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

	@classmethod
	def sanitize_events(cls, events, peeps):
		"""Sanitize events to ensure there are enough leaders and followers to fill roles."""
		valid_events = []
		removed_events = []
		for event in events:
			num_leaders = sum(1 for peep in peeps if event.id in peep.availability and peep.role == Globals.leader)
			num_followers = sum(1 for peep in peeps if event.id in peep.availability and peep.role == Globals.follower)

			if num_leaders >= event.min_role and num_followers >= event.min_role:
				valid_events.append(event)
			else:
				removed_events.append(event)
		
		return valid_events
	
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

	@staticmethod
	def events_conflict(event1, event2, days_between_events):
		hours_gap = (days_between_events * 24) - 1 # allow event at the same time with proper days apart 
		"""Returns True if events are too close together based on the required gap."""
		date_gap_hours = abs((event1.date - event2.date) /datetime.timedelta(hours=1))
		return date_gap_hours < hours_gap

	def __repr__(self):
		""" Used for logging at DEBUG level - detailed format """
		return (f"Event(event_id={self.id}, date={self.date}, "
                f"min_role={self.min_role}, max_role={self.max_role}, "
                f"leaders=[{self.get_leaders_str()}], followers=[{self.get_leaders_str()}])")
	
	def __str__(self):
		""" Used for logging at INFO level - concise format """
		return f"Event {self.id} on {self.date.strftime('%Y-%m-%d %H:%M')}"
	
	def get_leaders_str(self):
		"""Returns a comma-separated string of first names of leaders."""
		leaders = [attendee.name.split()[0] for attendee in self.leaders]
		return f"Leaders({len(self.leaders)}): " + ", ".join(leaders)

	def get_followers_str(self):
		"""Returns a comma-separated string of first names of followers."""
		followers = [attendee.name.split()[0] for attendee in self.followers]
		return  f"Followers({len(self.followers)}): " + ", ".join(followers)
	
class Peep:
	def __init__(self, **kwargs):
		self.id = str(kwargs.get("id", "")).strip()  # Ensure ID is a string and trimmed
		self.name = str(kwargs.get("name", "")).strip()
		self.role = str(kwargs.get("role", ""))
		self.index = int(kwargs.get("index", 0) or 0)  # Handles empty or missing values
		self.priority = int(kwargs.get("priority", 0) or 0)
		self.total_attended = int(kwargs.get("total_attended", 0) or 0)
		self.availability = list(kwargs.get("availability", []))  # Ensure list format
		self.event_limit = int(kwargs.get("event_limit", 0) or 0)
		self.num_events = 0 # always start at 0, gets incremented during the run 

	def can_attend(self, event):
		"""Checks if a peep can attend an event based on peep availability, role limit, and personal event limit."""
		# meets the person's availability
		if event.id not in self.availability:
			return False

		# space for the role
		if len(event.role(self.role)) >= event.max_role:
			return False

		# personal limit for the month
		if self.num_events >= self.event_limit:
			return False

		return True
	
	@staticmethod
	def update_event_attendees(peeps, winners):
		"""For all successful attendees, reset priority and send to the back of the line."""
		for peep in winners:
			peep.num_events += 1
			peep.priority = 0  # Reset priority after successful attendance
			peeps.remove(peep)
			peeps.append(peep)  # Move successful peeps to the end

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

	def __key__(self):
		"""
        Returns a tuple that uniquely identifies the EventSequence.

        The key is based on:
        1. Event IDs: The unique identifier of each valid event in the sequence.
        2. Leader IDs: A sorted list of leader IDs for each event.
        3. Follower IDs: A sorted list of follower IDs for each event.

        The combination of these factors ensures that two EventSequence objects are considered equal if they contain 
		the same events with the same leaders and followers.

        Returns:
            tuple: A tuple of (event ID, sorted leader IDs, sorted follower IDs) for each event in the sequence.
        """
		return tuple(
			(event.id, tuple(sorted(peep.id for peep in event.leaders)), tuple(sorted(peep.id for peep in event.followers)))
			for event in self.valid_events
		)

	def __eq__(self, other):
		"""Check equality based on the event sequence key."""
		if isinstance(other, EventSequence):
			return self.__key__() == other.__key__()
		return False

	def __hash__(self):
		"""Generate a hash value based on the event sequence key."""
		return hash(self.__key__())

	@staticmethod
	def get_unique_sequences(sequences): 
		"""
        Returns a list of unique EventSequences based on their valid events.
		"""
		return list({sequence: sequence for sequence in sequences}.values())
	
	def has_conflict(self, days_between_events): 
		events = self.valid_events 
		for i, event_a in enumerate(events): 
			other_events = events[:i] + events[i + 1:]
			for event_b in other_events: 
				if Event.events_conflict(event_a, event_b, days_between_events): 
					return True 
		return False 
		
	@staticmethod
	def pop_until_no_conflict(sequences, days_between_events): 
		"""Pops event sequences with conflicts until one without conflict is found.

		This method pops event sequences from the front of the list while they contain 
		conflicting events, and stops once a sequence without conflicts is found.
		"""
		removed = [] 
		while sequences and EventSequence.has_conflict(sequences[0], days_between_events): 
			removed.append(sequences.pop(0))
		return removed

		
	def __repr__(self):
		return (', '.join(str(event.id) for event in self.events))
	def __str__(self):
		result = (f"EventSequence: "
			f"valid events: {{ {', '.join(str(event.id) for event in self.valid_events)} }}, " 
			f"unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, system_weight {self.system_weight}"
		)
		result += f"\t"
		for event in self.valid_events: 
			result += f"\n\t{event}"
			result += f"\n\t  {event.get_leaders_str()}"
			result += f"\n\t  {event.get_followers_str()}"

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
		logging.info(f"Saved {len(events)} Events to {event_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		event_data = load_json(event_filename)
		events = [Event.from_dict(e) for e in event_data] if event_data else []

	# Load or generate peeps
	if generate_peeps:
		peeps = [Peep.generate_test_peep(i, i - 1, num_events) for i in range(num_peeps)]
		save_json([peep.__dict__ for peep in peeps], peep_filename)
		logging.info(f"Saved {len(peeps)} Peeps to {peep_filename}.\n"
		 	f"Rename before next run if you want to keep this generated test data.")
	else:
		peep_data = load_json(peep_filename)
		peeps = [Peep(**p) for p in peep_data] if peep_data else []

	sorted_peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)
	return sorted_peeps, events

def finalize_sequence(sequence):
	"""Finalizes a sequence by increasing priority for unsuccessful peeps and tracking metrics."""
	for peep in sequence.peeps:
		if peep.num_events == 0:
			peep.priority += 1  # Increase priority if not assigned to any event
		else:
			sequence.num_unique_attendees += 1

		sequence.system_weight += peep.priority  # Track total system priority weight

def generate_event_permutations(events):
    """Generates all possible permutations of event sequences as a list of event ids."""
    
    if not events:
        return []
    event_ids = [event.id for event in events]
    index_sequences = list(itertools.permutations(event_ids, len(event_ids)))

    logging.debug(f"Total permutations: {len(index_sequences)}")
    return index_sequences

def evaluate_all_event_sequences(og_peeps, og_events):
    """Generates and evaluates all possible event sequences based on peep availability and role limits."""
    
    def balance_roles(event, winners):
        """Ensures leaders and followers are balanced within an event."""
        if len(event.leaders) != len(event.followers):
            larger_group = event.leaders if len(event.leaders) > len(event.followers) else event.followers
            while len(event.leaders) != len(event.followers):
                if not larger_group:
                    logging.warning(f"Unable to balance roles for event {event.id}.")
                    break
                winners.remove(larger_group.pop())

        assert len(event.leaders) == len(event.followers) >= event.min_role
        assert len(event.leaders) <= event.max_role

    def evaluate_sequence(sequence):
        """Evaluates an event sequence by assigning peeps to events and updating priorities and list order."""
        for event in sequence.events:
            winners = []
            losers = []

            # Add peeps to event 
            for peep in sequence.peeps:
                if peep.can_attend(event):
                    event.role(peep.role).append(peep)
                    winners.append(peep)
                else:
                    losers.append(peep)

            if event.is_valid():  # If we have enough to fill the event
                balance_roles(event, winners)
                Peep.update_event_attendees(sequence.peeps, winners)
                sequence.valid_events.append(event)
            else:
                event.leaders.clear()
                event.followers.clear() 

        # End of sequence, update peeps who didn't make it 
        finalize_sequence(sequence)
        
    # Create all event permutations
    event_perm = generate_event_permutations(og_events)
    event_map = {event.id: event for event in og_events}  # Faster lookup
    event_sequences = [] 

    for perm in event_perm: 
        # Create an EventSequence object for this permutation 
        events = [copy.deepcopy(event_map[id]) for id in perm] 
        event_sequence = EventSequence(events, copy.deepcopy(og_peeps))

        # Evaluate the EventSequence 
        evaluate_sequence(event_sequence)

        # Only include sequences with valid events
        if event_sequence.valid_events:
            event_sequences.append(event_sequence)
    
    return event_sequences
	
def setup_logging():
	logging.basicConfig(level=logging.DEBUG,  # Root logger level
					format='%(asctime)s - %(levelname)s - %(message)s',
					handlers=[
						logging.StreamHandler(),  # Console handler
						logging.FileHandler('debug.log')  # File handler
					])

	# Set levels for each handler
	logging.getLogger().handlers[0].setLevel(logging.INFO)  # Console handler: INFO and above
	logging.getLogger().handlers[1].setLevel(logging.DEBUG)  # File handler: DEBUG and above

def initialize_data_from_json(): 
	output_json = 'data/novice_peeps_output.json'

	json_data = load_json(output_json)
	response_data = json_data['responses'] # don't really need this but could help debugging 
	event_data = json_data['events']
	peeps_data = json_data['peeps']
		
	events = [Event.from_dict(e) for e in event_data] if event_data else []
	peeps = [Peep(**p) for p in peeps_data] if peeps_data else []

	# sort peeps by their current index from the csv, which represents 
	# their current order in the priority queue 
	# TODO: if we want to be super crazy we can check that the priorities are in descending order 
	sorted_peeps = sorted(peeps, key=lambda peep: peep.index)
	return sorted_peeps, events

import logging
import logging

def find_most_overlapping_events(events, peeps):
    """
    Identify the event with the highest participant overlap.
    
    Overlap is calculated by counting the number of shared participants between each pair of events. 
    If a peep is available for both event A and event B, they contribute to the overlap score for both events.
    """
    overlap_scores = {event.id: 0 for event in events}
    
    logging.debug("Computing event overlap...")
    
    # Create a lookup for peep availability
    peep_event_map = {peep.id: set(peep.availability) for peep in peeps}
    
    # Compute event overlap
    for i, event_a in enumerate(events):
        for j, event_b in enumerate(events):
            if i >= j:
                continue  # Avoid redundant checks
            
            # Count shared peeps who are available for both events
            shared_peeps = sum(1 for peep in peeps if 
                               event_a.id in peep_event_map[peep.id] and event_b.id in peep_event_map[peep.id])
            
            overlap_scores[event_a.id] += shared_peeps
            overlap_scores[event_b.id] += shared_peeps
    
    logging.debug(f"Overlap scores: {overlap_scores}")
    return overlap_scores

def find_event_to_remove(events, peeps):
    """
    Find the event with the highest overlap. If there's a tie, remove the event with the lowest weight.
    """
    overlap_scores = find_most_overlapping_events(events, peeps)
    max_overlap = max(overlap_scores.values())
    candidates = [event for event in events if overlap_scores[event.id] == max_overlap]
    
    logging.info(f"Events with max overlap ({max_overlap}): {[event.id for event in candidates]}")
    
    if len(candidates) == 1:
        return candidates[0]
    
    # Use weight as a tiebreaker
    event_weights = {event: sum(peep.priority for peep in peeps if event.id in peep.availability) for event in candidates}
    event_to_remove = min(event_weights, key=event_weights.get)
    
    logging.info(f"Tie on overlap. Removing event based on lowest weight")
    return event_to_remove

def remove_high_overlap_events(events, peeps, max_events):
    """
    Remove events with the highest participant overlap. If overlap is the same, remove the lowest-weighted event.
    """
    logging.info(f"Initial event count: {len(events)}. Target event count: {max_events}.")
    
    while len(events) > max_events:
        event_to_remove = find_event_to_remove(events, peeps)
        logging.info(f"Removing event: Event({event_to_remove.id}) Date: {event_to_remove.date}. Remaining events: {len(events) - 1}.")
        events = [event for event in events if event.id != event_to_remove.id]
    
    logging.info(f"Final event count: {len(events)}.")
    return events


def main():
	setup_logging()
	
	# should we generate new lists? otherwise read from file 
	generate_events = False 
	generate_peeps = False 
	
	peeps, events = initialize_data(generate_events, generate_peeps)
	# peeps, events = initialize_data_from_json()

	# Sort peeps by priority (descending)
	# TODO: the list should already come in from the file correctly sorted, 
	# need to check for this once we finalize how the import works 
	peeps = sorted(peeps, reverse=True, key=lambda peep: peep.priority)

	logging.debug("Initial Peeps")
	logging.debug(Peep.peeps_str(peeps))

	sanitized_events = Event.sanitize_events(events, peeps)
	logging.info(f"Sanitized Events: {len(sanitized_events)}/{len(events)}")

	if len(sanitized_events) > Globals.max_events: 
		logging.warning(
			f"Found {len(sanitized_events)} events with enough available peeps; this results in too many permutations to compute. " 
			f"Removing events with high overlap and low priority."
			)
		sanitized_events = remove_high_overlap_events(sanitized_events, peeps, Globals.max_events)

	# process all event sequences, assigning peeps in order and determining valid events
	event_sequences = evaluate_all_event_sequences(peeps, sanitized_events)
	
	# remove duplicates:sequences with the same valid event ids in the same order, and the same leader/followers assigned
	unique_sequences = EventSequence.get_unique_sequences(event_sequences)
	logging.info(f"Found {len(unique_sequences)} unique sequences")

	# sort by unique attendees (desc) and system weight (desc)
	sorted_unique = sorted(unique_sequences, key=lambda sequence: (-sequence.num_unique_attendees, -sequence.system_weight))
	
	# remove any sequences where any two event dates conflict (based on Globals.days_between_events)
	days_between_events = Globals.days_between_events
	removed = EventSequence.pop_until_no_conflict(sorted_unique, days_between_events)
	logging.info(f"Removed {len(removed)} sequences with conflicts; days between events = {days_between_events}")
	logging.debug(f"Removed sequences: {removed}")

	best_sequence = sorted_unique[0] if sorted_unique else None
	if best_sequence:
		logging.info(f"Best {best_sequence}")
			
		logging.debug("Final Peeps:")
		logging.debug(peep for peep in best_sequence.peeps)
	else:
		logging.info(f"No sequence found; couldn't fill any events.")

if __name__ == "__main__":
	for i in range(1):
		main()
