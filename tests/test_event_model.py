import pytest
import constants
from models import Event, Role, EventSequence, Peep
from scheduler import Scheduler

def test_promote_alternate(event_factory, peep_factory):
	event = event_factory(id=1)
	peep = peep_factory(id=1, role=Role.LEADER)
	
	event.add_alternate(peep, Role.LEADER)
	assert peep in event.get_alternates()
	
	event.promote_alt(peep, Role.LEADER)
	assert peep in event.get_attendees()
	assert peep not in event.get_alternates()

def test_demote_attendee(event_factory, peep_factory):
	event = event_factory(id=2)
	peep = peep_factory(id=2, role=Role.FOLLOWER)

	event.add_attendee(peep, Role.FOLLOWER)
	assert peep in event.get_attendees(Role.FOLLOWER)

	event.demote_attendee_to_alt(peep, Role.FOLLOWER)
	assert peep in event.get_alternates(Role.FOLLOWER)
	assert peep not in event.get_attendees(Role.FOLLOWER)

def test_balance_roles_with_extra_leader(event_factory, peep_factory):
	event = event_factory(id=3)
	lead1 = peep_factory(id=3, role=Role.LEADER)
	lead2 = peep_factory(id=4, role=Role.LEADER)
	follow = peep_factory(id=5, role=Role.FOLLOWER)

	event.add_attendee(lead1, Role.LEADER)
	event.add_attendee(lead2, Role.LEADER)
	event.add_attendee(follow, Role.FOLLOWER)

	event.balance_roles()
	assert len(event.leaders) == len(event.followers)
	assert lead1 in event.leaders 
	assert lead2 not in event.leaders
	assert lead2 in event.alt_leaders

def test_downgrade_duration(event_factory, peep_factory):
	event = event_factory(id=4, duration_minutes=3)
	lead1 = peep_factory(id=1, role=Role.LEADER)
	lead2 = peep_factory(id=2, role=Role.LEADER)
	follow1 = peep_factory(id=3, role=Role.FOLLOWER)
	follow2 = peep_factory(id=4, role=Role.FOLLOWER)

	event.add_attendee(lead1, Role.LEADER)
	event.add_attendee(lead2, Role.LEADER)
	event.add_attendee(follow1, Role.FOLLOWER)
	event.add_attendee(follow2, Role.FOLLOWER)

	event.balance_roles()
	success = event.downgrade_duration()

	assert success
	assert event.duration_minutes == 2

def test_assignment_meets_minimums(events, peeps):
	"""
	Test that evaluating a sequence correctly fills a small event
	with enough balanced attendees.
	"""
	event = events[3]# event with 2 leaders and 2 followers available

	sequence = EventSequence([event], peeps)
	scheduler = Scheduler(data_folder="test", max_events=1)
	scheduler.evaluate_sequence(sequence)

	assert len(sequence.valid_events) == 1
	valid_event = sequence.valid_events[0]
	assert len(valid_event.leaders) == len(valid_event.followers)
	assert len(valid_event.leaders) >= constants.ABS_MIN_ROLE
	assert all(p in valid_event.attendees for p in valid_event.leaders + valid_event.followers)
