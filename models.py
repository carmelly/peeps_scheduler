import datetime
import random
import logging
from enum import Enum

from globals import Globals

class Role(Enum):
	LEADER = "Leader"
	FOLLOWER = "Follower"

class Peep:
	def __init__(self, **kwargs):
		self.id = str(kwargs.get("id", "")).strip()  # Ensure ID is a string and trimmed
		self.name = str(kwargs.get("name", "")).strip()
		role = kwargs.get("role", "") 
		self.role = role if isinstance(role, Role) else Role(role)
		self.index = int(kwargs.get("index", 0) or 0)  # Handles empty or missing values
		self.priority = int(kwargs.get("priority", 0) or 0)
		self.total_attended = int(kwargs.get("total_attended", 0) or 0)
		self.availability = list(kwargs.get("availability", []))  # Ensure list format
		self.event_limit = int(kwargs.get("event_limit", 0) or 0)
		self.num_events = 0 # always start at 0, gets incremented during the run
		self.min_interval_days = int(kwargs.get("min_interval_days", 0) or 0)
		self.assigned_event_dates = []

	def to_dict(self):
		return {
			**self.__dict__,
			"role": self.role.value if hasattr(self.role, "value") else self.role
		}
	
	def can_attend(self, event):
		"""Checks if a peep can attend an event based on peep availability, role limit, and personal event limit."""
		# meets the person's availability
		if event.id not in self.availability:
			return False

		# space for the role
		if len(event.get_attendees_by_role(self.role)) >= event.max_role:
			return False

		# personal limit for the month
		if self.num_events >= self.event_limit:
			return False

		for assigned_date in self.assigned_event_dates:
		# Calculate days difference based on calendar days
			days_gap = abs((event.date.date() - assigned_date.date()).days)
			if days_gap < self.min_interval_days:
				return False

		return True

	@staticmethod
	def update_event_attendees(peeps, winners, event):
		"""For all successful attendees, reset priority and send to the back of the line."""
		for peep in winners:
			peep.num_events += 1
			peep.priority = 0  # Reset priority after successful attendance
			peep.assigned_event_dates.append(event.date)

			# Move successful peeps to the end of the list
			peeps.remove(peep)
			peeps.append(peep) 

	@classmethod
	def generate_test_peep(cls, id, index, event_ids):

		"""Generate a test Peep with random values"""
		data = {
			"id": id,
			"index": index,
			"name": f"Person{id}",
			"email": f"person{id}@example.com", 
			"priority": random.randint(0, 3),# Priority between 0 and 3
			"event_limit": random.randint(1, 3),
			"role": random.choice([Role.LEADER.value, Role.FOLLOWER.value]), 
			"min_interval_days": random.choice([0, 1, 2, 3]),
			"total_attended": random.randint(0, 5)
		}

		# Generate random event availability
		availability = sorted(random.sample(event_ids, random.randint(1, len(event_ids))))
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
				f"availability={self.availability}, event_limit={self.event_limit}, role={self.role.name}, "
				f"total_attended={self.total_attended}, "
				f"num_events={self.num_events}, index={self.index})")

	# Simplified tostring for easier testing
	def __str__(self):
		role_str = "L" if self.role == Role.LEADER else "F"
		return (f"Peep({self.id:>3}): p: {self.priority}, limit: {self.event_limit}, "
				f"role: {role_str}, a: {self.availability}")

class Event:
	def __init__(self, **kwargs):
		self.id = kwargs.get("id", 0)
		self.date = kwargs.get("date", None)
		self.min_role = kwargs.get("min_role", 4)
		self.max_role = kwargs.get("max_role", 8)
		self.attendees = []

	def to_dict(self):
		return {
			**self.__dict__,
		}
	
	@property
	def leaders(self):
		return self.get_attendees_by_role(Role.LEADER)

	@property
	def followers(self):
		return self.get_attendees_by_role(Role.FOLLOWER)

	def get_attendees_by_role(self, role): 
		return [p for p, r in self.attendees if r == role]

	def add_attendee(self, peep):
		self.attendees.append((peep, peep.role))

	def is_valid(self):
		""" Event is valid if we have enough leaders and enough followers to fill the minimum per role """
		return( len( self.leaders) >= self.min_role and len(self.followers) >= self.min_role)
	
	@classmethod
	def from_dict(cls, data):
		"""Convert dictionary data back into an Event object."""
		data["date"] = datetime.datetime.strptime(data["date"], Globals.date_format)
		return cls(**data)

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

	@staticmethod
	def sanitize_events(events, peeps):
		"""Sanitize events to ensure there are enough leaders and followers to fill roles."""
		valid_events = []
		removed_events = []
		for event in events:
			num_leaders = sum(1 for peep in peeps if event.id in peep.availability and peep.role == Role.LEADER)
			num_followers = sum(1 for peep in peeps if event.id in peep.availability and peep.role == Role.FOLLOWER)

			if num_leaders >= event.min_role and num_followers >= event.min_role:
				valid_events.append(event)
			else:
				removed_events.append(event)

		return valid_events

	@staticmethod
	def remove_high_overlap_events(events, peeps, max_events):
		"""
		Remove events that have the highest participant overlap with all other events in the list,
		until we have no more than max_events in the list. If overlap is the same, remove the lowest-weighted event.
		Returns a new list.
		"""

		def find_overlapping_events(events, peeps):
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
			overlap_scores = find_overlapping_events(events, peeps)
			max_overlap = max(overlap_scores.values())
			candidates = [event for event in events if overlap_scores[event.id] == max_overlap]

			logging.debug(f"Events with max overlap ({max_overlap}): {[event.id for event in candidates]}")

			if len(candidates) == 1:
				return candidates[0]

			# Use weight as a tiebreaker
			event_weights = {event: sum(peep.priority for peep in peeps if event.id in peep.availability) for event in candidates}
			event_to_remove = min(event_weights, key=event_weights.get)

			logging.debug(f"Tie on overlap. Removing event based on lowest weight")
			return event_to_remove

		logging.debug(f"Initial event count: {len(events)}. Target event count: {max_events}.")
		while len(events) > max_events:
			event_to_remove = find_event_to_remove(events, peeps)
			logging.debug(f"Removing event: Event({event_to_remove.id}) Date: {event_to_remove.date}. Remaining events: {len(events) - 1}.")
			events = [event for event in events if event.id != event_to_remove.id]

		logging.info(f"Final event count: {len(events)}.")
		return events

	def formatted_date(self):
		dt = self.date
		formatted = dt.strftime(Globals.datestr_format if hasattr(dt, 'strftime') else "")
		# For Unix systems, remove leading zeros manually (since %#I doesn't work)
		formatted = formatted.replace(" 0", " ")
		formatted = formatted[:-2] + formatted[-2:].lower()  # Lowercase am/pm
		return formatted
	
	def __repr__(self):
		""" Used for logging at DEBUG level - detailed format """
		return (f"Event(event_id={self.id}, date={self.date}, "
				f"min_role={self.min_role}, max_role={self.max_role}, "
				f"attendees={[peep.name for peep, _ in self.attendees]})")

	def __str__(self):
		""" Used for logging at INFO level - concise format """
		return f"Event {self.id} on {self.formatted_date()}"

	def get_leaders_str(self):
		leaders = [peep.name.split()[0] for peep, role in self.attendees if role == Role.LEADER]
		return f"Leaders({len(leaders)}): " + ", ".join(leaders)

	def get_followers_str(self):
		followers = [peep.name.split()[0] for peep, role in self.attendees if role == Role.FOLLOWER]
		return  f"Followers({len(followers)}): " + ", ".join(followers)
	
class EventSequence:
	def __init__(self, events, peeps):
		self.events = events
		self.peeps = peeps
		self.num_unique_attendees = 0
		self.total_attendees = 0
		self.system_weight = 0
		self.valid_events = []
		

	def finalize(self):
		"""Finalizes a sequence by increasing priority for unsuccessful peeps and tracking metrics."""
		for peep in self.peeps:
			if peep.num_events == 0:
				peep.priority += 1  # Increase priority if not assigned to any event
			else:
				peep.total_attended += peep.num_events 	# keep track of peep total in csv
				self.total_attendees += peep.num_events # count people who went more than once as a backup metric
				self.num_unique_attendees += 1
				

			self.system_weight += peep.priority  # Track total system priority weight

		# Sort peeps by priority descending
		self.peeps.sort(key=lambda p: p.priority, reverse=True)

		# Reassign index based on sorted order
		for i, peep in enumerate(self.peeps):
			peep.index = i

	@staticmethod
	def get_unique_sequences(sequences):
		"""
		Returns a list of unique EventSequences based on their valid events.
		"""
		return list({sequence: sequence for sequence in sequences}.values())

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
			(
			event.id, 
			tuple(sorted(peep.id for peep in event.leaders)), 
			tuple(sorted(peep.id for peep in event.followers))
			)
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
