import datetime
import random
import logging
from enum import Enum
from constants import DATE_FORMAT, DATESTR_FORMAT
import constants

class Role(Enum):
	LEADER = "leader"
	FOLLOWER = "follower"

	def opposite(self):
		if self == Role.LEADER:
			return Role.FOLLOWER
		elif self == Role.FOLLOWER:
			return Role.LEADER
		raise ValueError(f"No opposite defined for Role: {self}")
	
	@classmethod
	def from_string(cls, value):
		value = value.strip().lower()
		if value in ["lead", "leader"]:
			return cls.LEADER
		elif value in ["follow", "follower"]:
			return cls.FOLLOWER
		else:
			raise ValueError(f"Unknown role: {value}")

class SwitchPreference(Enum): 
	PRIMARY_ONLY = 1       # "I only want to be scheduled in my primary role"
	SWITCH_IF_PRIMARY_FULL = 2  # "Happy to dance secondary if primary is full"
	SWITCH_IF_NEEDED = 3  # "Only if needed to fill a session"

	@classmethod
	def from_string(cls, value):
		value = value.strip()
		if value == "I only want to be scheduled in my primary role": 
			return cls.PRIMARY_ONLY
		elif value == "I'm happy to dance my secondary role if it lets me attend when my primary is full": 
			return cls.SWITCH_IF_PRIMARY_FULL
		elif value == "I'm willing to dance my secondary role only if it's needed to enable filling a session": 
			return cls.SWITCH_IF_NEEDED
		else:
			raise ValueError(f"Unknown role: {value}")

class Peep:
	def __init__(self, **kwargs):
		# Validate required fields first
		if not kwargs.get("id"):
			raise ValueError("Peep requires an 'id' field")
		
		if not kwargs.get("role"):
			raise ValueError("Peep requires a 'role' field")
		
		self.id = int(kwargs.get("id"))
		self.full_name = str(kwargs.get("full_name", "")).strip()
		self.display_name = str(kwargs.get("display_name", "")).strip()
		self.email = str(kwargs.get("email", "")).strip()

		role_input = kwargs.get("role", "")
		try:
			self.role = role_input if isinstance(role_input, Role) else Role.from_string(role_input)
		except ValueError as e:
			raise ValueError(f"Invalid role '{role_input}': {str(e)}") from e

		switch_input = kwargs.get("switch_pref", SwitchPreference.PRIMARY_ONLY)
		self.switch_pref = switch_input if isinstance(switch_input, SwitchPreference) else SwitchPreference(switch_input)
		
		self.index = int(kwargs.get("index", 0) or 0)  # Handles empty or missing values
		self.priority = int(kwargs.get("priority", 0) or 0)
		self.original_priority = self.priority
		self.total_attended = int(kwargs.get("total_attended", 0) or 0)
		self.availability = list(kwargs.get("availability", []))  # Ensure list format
		self.event_limit = int(kwargs.get("event_limit", 0) or 0)
		self.num_events = 0 # always start at 0, gets incremented during the run
		self.min_interval_days = int(kwargs.get("min_interval_days", 0) or 0)
		self.assigned_event_dates = [] 
		# keep these as strings, just to print back to updated members csv
		self.active =  kwargs.get('active')
		self.date_joined = kwargs.get('date_joined')
		self.responded = kwargs.get('responded', False)

	@staticmethod
	def is_peeps_list_sorted_by_priority(peeps: list["Peep"]): 
		return all(peeps[i].priority >= peeps[i + 1].priority for i in range(len(peeps)-1))
	
	@staticmethod
	def from_csv(row: dict) -> "Peep":
		return Peep(
			id=int(row["id"]),
			full_name=row["Name"].strip(),
			display_name=row["Display Name"].strip(),
			email=row["Email Address"].strip(),
			role=row["Role"].strip(),
			index=int(row["Index"]),
			priority=int(row["Priority"]),
			total_attended=int(row["Total Attended"]),
			active=row["Active"].strip().upper() == "TRUE",
			date_joined = row["Date Joined"]
		)
	
	def to_csv(self) -> dict: 
		return {
			'id': self.id,
			'Name': self.full_name,
			'Display Name': self.display_name,
			'Email Address': self.email,
			'Role': self.role.value,
			'Index': self.index,
			'Priority': self.priority,
			'Total Attended': self.total_attended,
			'Active': str(self.active).upper(),
			'Date Joined': self.date_joined
		}
	
	@property
	def name(self): 
		return self.display_name 
	
	def to_dict(self):
		peep_dict = {
			"id": self.id,
			"name": self.full_name,
			"display_name": self.display_name,
			'email': self.email,
			"role": self.role.value,
			"index": self.index,
			"priority": self.priority,
			"total_attended": self.total_attended,
			"active": self.active,
			"date_joined": self.date_joined,
		}
		if self.responded: 
			peep_dict.update({
				"availability": self.availability, 
				"switch_pref": self.switch_pref.value,
				"responded": self.responded,
				"event_limit": self.event_limit,
				"min_interval_days": self.min_interval_days
			})
		return peep_dict
	
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
		self.date = kwargs.get("date", None) #TODO: validate that this is a datetime
		
		self.duration_minutes = kwargs.get("duration_minutes")
		if self.duration_minutes not in constants.CLASS_CONFIG:
			raise ValueError(f"Unknown event duration: {self.duration_minutes}")

		# Attendee lists are role-specific and managed via internal assignment methods.
		self._leaders = [] 
		self._followers = [] 
		self._alt_leaders = [] 
		self._alt_followers = [] 
		self._attendee_order = [] #keep track of assignment order 
	
	@property
	def config(self):
		return constants.CLASS_CONFIG[self.duration_minutes]

	@property
	def min_role(self):
		return self.config["min_role"]

	@property
	def max_role(self):
		return self.config["max_role"]

	@property
	def price(self):
		return self.config["price"]
	
	@property
	def price_per_person(self):
		num_people = self.num_attendees()
		# Round to nearest dollar 
		return round(self.price / num_people, 0) if num_people else None

	@property
	def leaders(self) -> tuple[Peep, ...]:
		return tuple(self._leaders)

	@property
	def followers(self) -> tuple[Peep, ...]:
		return tuple(self._followers)
	
	@property
	def alt_leaders(self) -> tuple[Peep, ...]:
		return tuple(self._alt_leaders)

	@property
	def alt_followers(self) -> tuple[Peep, ...]:
		return tuple(self._alt_followers)
	
	@property
	def attendees(self) -> tuple[Peep, ...]:
		return tuple(self._attendee_order)

	def clear_participants(self): 
		"""
		Clear all attendees and alternates from the event.
		"""
		self._leaders.clear() 
		self._followers.clear() 
		self._alt_leaders.clear() 
		self._alt_followers.clear()
		self._attendee_order.clear()

	def get_attendees(self, role: Role = None) -> list[Peep]:
		"""
		Return the list of assigned attendees.
		If a role is provided, returns attendees for that role only.
		"""
		if role is None:
			return self._leaders + self._followers
		return self._leaders if role == Role.LEADER else self._followers
		
	def get_alternates(self, role: Role = None) -> list[Peep]:
		"""
		Return the list of alternates.
		If a role is provided, returns alternates for that role only.
		"""
		if role is None:
			return self._alt_leaders + self._alt_followers
		return self._alt_leaders if role == Role.LEADER else self._alt_followers

	# def set_alternates_by_role(self, role: Role, peeps: list[Peep]):
	# 	if role == Role.LEADER:
	# 		self._alt_leaders = peeps
	# 	else:
	# 		self._alt_followers = peeps

	def add_attendee(self, peep: Peep, role: Role):
		"""
		Adds peep to the appropriate role list and tracks assignment order.
		"""
		if self.is_full(role):
			logging.error(f"Attempted to overfill {role.value} for Event {self.id}")
			raise RuntimeError(f"Too many attendees in role {role.value} for Event {self.id}")
		
		if peep in self._attendee_order:
			logging.error(f"Peep {peep.id} is already assigned to Event {self.id}")
			raise RuntimeError("Cannot add attendee twice")

		if role == Role.LEADER: 
			self._leaders.append(peep)
		else: 
			self._followers.append(peep)
		self._attendee_order.append(peep)  # preserve assignment order

	def add_alternate(self, peep: Peep, role: Role): 
		"""
		Add a peep to the alternate list for the given role.
		"""
		#TODO: sanity check that peep is not already an alternate on either list 
		if role == Role.LEADER: 
			self._alt_leaders.append(peep)
		else: 
			self._alt_followers.append(peep)

	def remove_alternate(self, peep: Peep, role: Role):
		"""
		Remove a peep from the alternate list for the given role.
		Raises RuntimeError if peep is not found.
		"""
		alt_list = self._alt_leaders if role == Role.LEADER else self._alt_followers

		if peep not in alt_list:
			logging.error(f"Peep {peep.id} not in alt list for role {role.value} in Event {self.id}")
			raise RuntimeError(f"Peep {peep.id} not in {role.value} alternates")

		alt_list.remove(peep)

	def num_attendees(self, role: Role = None) -> int:
		"""
		Return the number of assigned attendees.
		If a role is provided, returns the count for that role only.
		"""
		return len(self.get_attendees(role))
	
	def meets_min(self, role: Role = None) -> bool:
		"""
		Check if the event meets minimum attendance requirements.
		If a role is provided, checks only that role. Otherwise, checks both roles.
		"""
		if role is None:
			return all(self.meets_min(role) for role in (Role.LEADER, Role.FOLLOWER))
		return self.num_attendees(role) >= self.min_role

	def meets_absolute_min(self) -> bool:
		"""
		Return True if the event meets the fixed absolute minimum number of attendees
		per role, regardless of duration or class configuration.
		"""
		return (
			len(self.leaders) >= constants.ABS_MIN_ROLE and
			len(self.followers) >= constants.ABS_MIN_ROLE
		)

	def is_full(self, role: Role = None) -> bool:
		"""
		Check if the event is at maximum capacity.
		If a role is provided, checks only that role. Otherwise, checks both roles.
		"""
		if role is None:
			return all(self.is_full(role) for role in (Role.LEADER, Role.FOLLOWER))
		count = self.num_attendees(role)
		if count > self.max_role:
			logging.error(f"Too many {role.value}s in Event {self.id}: {count} > max of {self.max_role}")
			raise RuntimeError(f"Too many {role.value}s assigned")
		return count == self.max_role
	
	def has_space(self, role: Role): 
		"""
		Return True if the event is not full for the given role.
		Equivalent to: not is_full(role)
		"""
		return not self.is_full(role)
	
	def promote_alt(self, peep: Peep, role: Role):
		"""
		Promote an alternate to full attendee for the specified role.
		"""
		if role == Role.LEADER:
			if peep not in self._alt_leaders:
				logging.error(f"Peep {peep.id} not found in alt_leaders before promotion")
				raise RuntimeError(f"Peep {peep.id} not in alt_leaders")
			self._leaders.append(peep)
			self._alt_leaders.remove(peep)
		elif role == Role.FOLLOWER:
			if peep not in self._alt_followers:
				logging.error(f"Peep {peep.id} not found in alt_followers before promotion")
				raise RuntimeError(f"Peep {peep.id} not in alt_followers")
			self._followers.append(peep)
			self._alt_followers.remove(peep)

		self._attendee_order.append(peep)

	def demote_attendee_to_alt(self, peep: Peep, role: Role):
		"""
		Demote a full attendee to the front of the alternates list.
		"""
		if role == Role.LEADER:
			if peep not in self._leaders:
				logging.error(f"Peep {peep.id} not found in leaders before demotion")
				raise RuntimeError(f"Peep {peep.id} not in leaders")
			self._leaders.remove(peep)
			self._alt_leaders.insert(0, peep)
		elif role == Role.FOLLOWER:
			if peep not in self._followers:
				logging.error(f"Peep {peep.id} not found in followers before demotion")
				raise RuntimeError(f"Peep {peep.id} not in followers")
			self._followers.remove(peep)
			self._alt_followers.insert(0, peep)

		if peep not in self._attendee_order:
			logging.error(f"Peep {peep.id} not in attendee_order before demotion")
			raise RuntimeError(f"Peep {peep.id} not in attendee_order")

		self._attendee_order.remove(peep)

	def balance_roles(self):
		"""
		Ensures leaders and followers are balanced within an event.
		If one group is larger, demote extras to alternates until balanced. 
		"""
		# Copy of current role lists for balance tracking
		leaders = list(self._leaders)
		followers = list(self._followers)

		if len(leaders) != len(followers):
			larger_role = Role.LEADER if len(leaders) > len(followers) else Role.FOLLOWER
			larger_list = leaders if larger_role == Role.LEADER else followers
			
			while len(leaders) != len(followers):
				if not larger_list:
					logging.warning(f"Unable to balance roles for event {self.id}.")
					break
				alt_peep = larger_list.pop()
				self.demote_attendee_to_alt(alt_peep, larger_role)
		
		# Post-check: roles must now be balanced and meet minimums
		if len(self.leaders) != len(self.followers):
			logging.error(f"Role imbalance after balancing for Event {self.id}: {len(self.leaders)}L / {len(self.followers)}F")
			raise RuntimeError(f"Event {self.id} still unbalanced after balance_roles()")
		
	def downgrade_duration(self) -> bool:
		"""
		Downgrade the event duration if current attendance no longer supports it.

		Returns True if downgraded, False if no valid shorter duration is available.
		Raises RuntimeError if roles are unbalanced or the event is not underfilled.
		"""
		# Roles must be balanced before attempting downgrade
		if len(self.leaders) != len(self.followers):
			logging.error(f"Cannot downgrade Event {self.id}: roles are unbalanced ({len(self.leaders)}L / {len(self.followers)}F)")
			raise RuntimeError("Cannot downgrade unbalanced event")

		# Event must be underfilled for its current duration
		if self.meets_min():
			logging.error(f"Event {self.id} meets minimums; cannot downgrade")
			raise RuntimeError("Cannot downgrade: event is not underfilled")

		count_per_role = len(self.leaders)
		logging.debug(f"Attempting to downgrade Event {self.id} due to underfill ({count_per_role}/role)")

		# Search for a valid downgrade option in CLASS_CONFIG
		for duration in sorted(constants.CLASS_CONFIG.keys()):
			new_config = constants.CLASS_CONFIG[duration]
			if (
				new_config["allow_downgrade"] and
				new_config["min_role"] <= count_per_role <= new_config["max_role"]
			):
				logging.debug(f"Downgrading Event {self.id} to {duration} minutes (was {self.duration_minutes})")
				self.duration_minutes = duration

				# Sanity check: after downgrade, current count must meet new min_role
				if len(self.leaders) < self.min_role:
					logging.error(f"Too few attendees after balancing for Event {self.id}: {len(self.leaders)} per role, minimum required is {self.min_role}")
					raise RuntimeError(f"Event {self.id} has too few attendees after balance_roles()")
				
				return True

		logging.warning(f"No valid downgrade found for Event {self.id} with {count_per_role} per role")
		return False

	def validate_alternates(self):
		"""
		Validate all alternates for this event.
		Remove any peeps who are no longer eligible to attend based on constraints.
		"""
		for role in (Role.LEADER, Role.FOLLOWER):
			for peep in self.get_alternates(role)[:]:  # copy list to avoid mutation during iteration
				if not peep.can_attend(self):
					logging.debug(f"Removing ineligible alternate {peep.name} from Event {self.id} ({role.value})")
					self.remove_alternate(peep, role)

	def to_dict(self):
		return {
			"id": self.id,
            "date": self.date.strftime(DATE_FORMAT),
            "duration_minutes": self.duration_minutes,
		}
	
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
		formatted = dt.strftime(DATESTR_FORMAT) 
		# For Unix systems, remove leading zeros manually (since %#I doesn't work)
		formatted = formatted.replace(" 0", " ")
		formatted = formatted[:-2] + formatted[-2:].lower()  # Lowercase am/pm
		return formatted
	
	def __repr__(self):
		""" Used for logging at DEBUG level - detailed format """
		return (f"Event(event_id={self.id}, date={self.date}, "
				f"min_role={self.min_role}, max_role={self.max_role}, "
				f"attendees={[peep.name for peep in self.attendees]})")

	def get_participants_str(self, role: Role = None) -> str:
		"""
		Return a formatted string listing assigned attendees and alternates.
		Attendees are listed in alphabetical order, while alternates are listed 
		in order of assignment. 

		If a role is provided, returns participants for that role only.
		If no role is given, returns both leaders and followers sections.

		Peeps dancing in their non-primary role are marked with an asterisk.
		"""
		def format_group(role: Role) -> str:
			attendees = sorted(self.get_attendees(role), key=lambda p: p.name)
			alternates = self.get_alternates(role)  # Preserve assignment order

			name_str = ', '.join(
				f"*{peep.name}" if peep.role != role else peep.name
				for peep in attendees
			)
			alt_str = ', '.join(
				f"*{peep.name}" if peep.role != role else peep.name
				for peep in alternates
			)

			label = "Leaders" if role == Role.LEADER else "Followers"
			base = f"{label}({len(attendees)}): {name_str}"
			return base + (f" [alt: {alt_str}]" if alt_str else "")

		if role is None:
			return format_group(Role.LEADER) + "\n" + format_group(Role.FOLLOWER)
		return format_group(role)
	
	def __str__(self):
		""" Used for logging at INFO level - concise format """
		return f"Event {self.id} on {self.formatted_date()}"

class EventSequence:
	def __init__(self, events: list[Event], peeps: list[Peep]):
		# Needed for evaluation
		self.events = events
		self.peeps = peeps
		self.valid_events: list[Event] = []

		# Efficiency metrics 
		self.num_unique_attendees = 0
		self.total_attendees = 0
		self.system_weight = 0
		self.priority_fulfilled = 0 
		self.normalized_utilization = 0 
				
	
	def to_dict(self): 
		return {
			"valid_events": [
				{
					"id": event.id,
					"date": event.date.strftime(DATE_FORMAT),
					"duration_minutes": event.duration_minutes, 
					"attendees": [
						{
							"id": peep.id,
							"name": peep.name,
							"role": peep.role.value
						}
						for peep in event.attendees
					],
					"alternates": [
						{
							"id": peep.id,
							"name": peep.name,
							"role": peep.role.value
						}
						for peep in event.alt_leaders + event.alt_followers
					],
					"leaders_string": event.get_participants_str(Role.LEADER), 
					"followers_string": event.get_participants_str(Role.FOLLOWER), 

				}
				for event in self.valid_events
			],
			"peeps": [peep.to_dict() for peep in self.peeps],
			"num_unique_attendees": self.num_unique_attendees,
			"priority_fulfilled": self.priority_fulfilled,
			"system_weight": self.system_weight
		}
	
	def finalize(self):
		"""Finalizes a sequence by increasing priority for unsuccessful peeps and tracking metrics."""
		for peep in self.peeps:
			# Update peep stats 
			if peep.num_events == 0: 
				# increase priority if peep responded but was not scheduled this period
				if peep.responded: 
					peep.priority += 1  # Increase priority if not assigned to any event
			else: # peep was scheduled to at least one event 
				peep.total_attended += peep.num_events 	
				
			# Track sequence efficiency metrics 
			self.num_unique_attendees += 1 if peep.num_events > 0 else 0 
			self.priority_fulfilled += peep.original_priority if peep.num_events > 0 else 0
			self.normalized_utilization += peep.num_events / peep.event_limit if peep.event_limit > 0 else 0
			self.total_attendees += peep.num_events 
			self.system_weight += peep.priority  

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
			for event in sorted(self.valid_events, key=lambda e: e.id)
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

	def  __str__(self):
		sorted_events:list[Event] = sorted(self.valid_events, key=lambda e: (e.id))
		result = (f"EventSequence: "
			f"valid events: {{ {', '.join(str(event.id) for event in sorted_events)} }}, "
			f"unique attendees {self.num_unique_attendees}/{len(self.peeps)}, " 
			f"priority fulfilled {self.priority_fulfilled}, "
			f"normalized utilization {self.normalized_utilization:.2f}, "
			f"total attendees {self.total_attendees}, "
			f"system_weight {self.system_weight}"
		)
		result += f"\t"
		for event in sorted_events:
			result += f"\n\t{event}, {event.duration_minutes} mins, ${event.price_per_person:.0f}/person"
			result += f"\n\t  {event.get_participants_str(Role.LEADER)}"
			result += f"\n\t  {event.get_participants_str(Role.FOLLOWER)}"

		unassigned = [p.name for p in self.peeps if p.responded and p.availability and p.num_events == 0]
		result += f"\n\tUnscheduled Peeps ({len(unassigned)}): {', '.join(sorted(unassigned)) if unassigned else 'None'}"

		return result	
