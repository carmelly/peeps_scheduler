import datetime
import random
import logging
from enum import Enum
from constants import DATE_FORMAT, DATESTR_FORMAT

class Role(Enum):
	LEADER = "Leader"
	FOLLOWER = "Follower"

	@classmethod
	def from_string(cls, value):
		#TODO: fix response form using "lead" and "follow" instead of "leader" and "follower"
		value = value.strip().lower()
		if value in ["lead", "leader"]:
			return cls.LEADER
		elif value in ["follow", "follower"]:
			return cls.FOLLOWER
		else:
			raise ValueError(f"Unknown role: {value}")

class Peep:
	def __init__(self, **kwargs):
		self.id = int(kwargs.get("id"))
		self.full_name = str(kwargs.get("name", "")).strip()
		self.display_name = str(kwargs.get("display_name", "")).strip()
		self.email = str(kwargs.get("email", "")).strip()
		self.role = Role.from_string(kwargs.get("role", ""))
		self.index = int(kwargs.get("index", 0) or 0)  # Handles empty or missing values
		self.priority = int(kwargs.get("priority", 0) or 0)
		self.total_attended = int(kwargs.get("total_attended", 0) or 0)
		self.availability = list(kwargs.get("availability", []))  # Ensure list format
		self.event_limit = int(kwargs.get("event_limit", 0) or 0)
		self.num_events = 0 # always start at 0, gets incremented during the run
		self.min_interval_days = int(kwargs.get("min_interval_days", 0) or 0)
		self.assigned_event_dates = [] 
		# keep these as strings, just to print back to updated members csv
		self.active =  kwargs.get('active')
		self.date_joined = kwargs.get('date_joined')

	@property
	def name(self): 
		return self.display_name 
	
	def to_dict(self):
		return {
			**self.__dict__,
			"role": self.role.value if hasattr(self.role, "value") else self.role
		}
	
	def can_attend(self, event):
		"""Checks if a peep can attend an event based on peep availability, event limit, and interval. 
		   Does not take into account role limit, so that we can add this peep as an alternate if needed """
		# meets the person's availability
		if event.id not in self.availability:
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
	def find_matching_peep(peeps, name, email): 
		matched_peeps = []
		if email:
			matched_peeps = [peep for peep in peeps.values() if peep.get("email", "").lower() == email.lower()]
			
		if not matched_peeps: 
			logging.error(f"No matching peeps for {name} ({email}). Please check input data.")
			return None 
		elif len(matched_peeps) > 1: 
			logging.error(f"More than one matching peep for {name} ({email}). Please check input data.")
			return None 
		
		return matched_peeps[0]

	@staticmethod
	def update_event_attendees(peeps, event):
		"""For all successful attendees, reset priority and send to the back of the line."""
		for peep in event.attendees:
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
		self.min_role = kwargs.get("min_role", 5)
		self.max_role = kwargs.get("max_role", 6)
		self.attendees = []
		self.alternates = []

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
	
	@property
	def alt_leaders(self):
		return self.get_alternates_by_role(Role.LEADER)

	@property
	def alt_followers(self):
		return self.get_alternates_by_role(Role.FOLLOWER)

	def get_attendees_by_role(self, role): 
		return [p for p in self.attendees if p.role == role]
	
	def get_alternates_by_role(self, role): 
		return [p for p in self.alternates if p.role == role]

	def add_attendee(self, peep):
		self.attendees.append(peep)

	def add_alternate(self, peep): 
		self.alternates.append(peep)

	def is_valid(self):
		""" Event is valid if we have enough leaders and enough followers to fill the minimum per role """
		return( len( self.leaders) >= self.min_role and len(self.followers) >= self.min_role)
	
	def balance_roles(self):
		"""
		Ensures leaders and followers are balanced within an event.
		If one group is larger, remove from attendees and add to alternates until balanced. 
		"""
		leaders = self.leaders
		followers = self.followers
		if len(leaders) != len(followers):
			larger_group = leaders if len(leaders) > len(followers) else followers
			while len(leaders) != len(followers):
				if not larger_group:
					logging.warning(f"Unable to balance roles for event {self.id}.")
					break
				alt_peep = larger_group.pop()
				self.attendees.remove(alt_peep)
				self.add_alternate(alt_peep)
		assert len(self.leaders) == len(self.followers) >= self.min_role
		assert len(self.leaders) <= self.max_role

	@classmethod
	def from_dict(cls, data):
		"""Convert dictionary data back into an Event object."""
		data["date"] = datetime.datetime.strptime(data["date"], DATE_FORMAT)
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

	def formatted_date(self):
		dt = self.date
		formatted = dt.strftime(DATESTR_FORMAT if hasattr(dt, 'strftime') else "")
		# For Unix systems, remove leading zeros manually (since %#I doesn't work)
		formatted = formatted.replace(" 0", " ")
		formatted = formatted[:-2] + formatted[-2:].lower()  # Lowercase am/pm
		return formatted
	
	def __repr__(self):
		""" Used for logging at DEBUG level - detailed format """
		return (f"Event(event_id={self.id}, date={self.date}, "
				f"min_role={self.min_role}, max_role={self.max_role}, "
				f"attendees={[peep.name for peep in self.attendees]})")

	def __str__(self):
		""" Used for logging at INFO level - concise format """
		return f"Event {self.id} on {self.formatted_date()}"

	def get_leaders_str(self):
		names = ', '.join([peep.name for peep in sorted(self.leaders, key=lambda p: p.name )])
		alt_names = ', '.join([peep.name for peep in self.alt_leaders])
		return f"Leaders({len(self.leaders)}): {names}" + (f" [alt: {alt_names}]" if alt_names else "")

	def get_followers_str(self):
		names = ', '.join([peep.name for peep in sorted(self.followers, key=lambda p: p.name )])
		alt_names = ', '.join([peep.name for peep in self.alt_followers])
		return f"Followers({len(self.followers)}): {names}" + (f" [alt: {alt_names}]" if alt_names else "")
	
class EventSequence:
	def __init__(self, events, peeps):
		self.events = events
		self.peeps = peeps
		self.num_unique_attendees = 0
		self.total_attendees = 0
		self.system_weight = 0
		self.valid_events = []
		
	def validate_alternates(self): 
		""" Removes alternates that can no longer attend an event due to personal limits """
		for event in self.valid_events: 
			valid_alternates = [] 
			for peep in event.alternates: 
				if peep.can_attend(event): 
					valid_alternates.append(peep)
			event.alternates = valid_alternates 

	def finalize(self):
		"""Finalizes a sequence by increasing priority for unsuccessful peeps and tracking metrics."""
		for peep in self.peeps:
			if peep.num_events == 0:
				peep.priority += 1  # Increase priority if not assigned to any event
				# TODO: priority should only be increased if the peep submitted a response for this schedule period
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
		sorted_events = sorted(self.valid_events, key=lambda e: (e.id))
		result = (f"EventSequence: "
			f"valid events: {{ {', '.join(str(event.id) for event in sorted_events)} }}, "
			f"unique_peeps {self.num_unique_attendees}/{len(self.peeps)}, " 
			f"total_attendance {self.total_attendees}, system_weight {self.system_weight}"
		)
		result += f"\t"
		for event in sorted_events:
			result += f"\n\t{event}"
			result += f"\n\t  {event.get_leaders_str()}"
			result += f"\n\t  {event.get_followers_str()}"

		return result	
