import copy
import pytest
import datetime
from models import Peep, Event, EventSequence, Role

@pytest.fixture(scope="module")
def events():
	# Define 8 specific events, varying times and days, including same day and early times
	return [
		Event(id=0, date=datetime.datetime(2025, 3, 21, 16), min_role=3, max_role=5),  # Friday 4 PM
		Event(id=1, date=datetime.datetime(2025, 3, 21, 19), min_role=3, max_role=5),  # Friday 7 PM same day
		Event(id=2, date=datetime.datetime(2025, 3, 22, 11), min_role=3, max_role=5),  # Saturday 11 AM
		Event(id=3, date=datetime.datetime(2025, 3, 23, 16), min_role=3, max_role=5),  # Sunday 4 PM
		Event(id=4, date=datetime.datetime(2025, 3, 24, 19), min_role=3, max_role=5),  # Monday 7 PM
		Event(id=5, date=datetime.datetime(2025, 3, 25, 11), min_role=3, max_role=5),  # Tuesday 11 AM
		Event(id=6, date=datetime.datetime(2025, 3, 26, 16), min_role=3, max_role=5),  # Wednesday 4 PM
		Event(id=7, date=datetime.datetime(2025, 3, 27, 16), min_role=3, max_role=5),  # Thursday 4 PM
	]

@pytest.fixture(scope="module")
def peeps():
	leaders = [
		Peep(id="1", name="Leader1", role=Role.LEADER.value, index=0, priority=1, total_attended=0, availability=[0,1,2], event_limit=2, min_interval_days=0),
		Peep(id="2", name="Leader2", role=Role.LEADER.value, index=1, priority=2, total_attended=0, availability=[1,3,4], event_limit=3, min_interval_days=1),
		Peep(id="3", name="Leader3", role=Role.LEADER.value, index=2, priority=3, total_attended=0, availability=[0,2,5], event_limit=1, min_interval_days=2),
		Peep(id="4", name="Leader4", role=Role.LEADER.value, index=3, priority=0, total_attended=0, availability=[2,3,6], event_limit=2, min_interval_days=0),
		Peep(id="5", name="Leader5", role=Role.LEADER.value, index=4, priority=1, total_attended=0, availability=[0,4,7], event_limit=3, min_interval_days=3),
		Peep(id="6", name="Leader6", role=Role.LEADER.value, index=5, priority=2, total_attended=0, availability=[1,5,6], event_limit=1, min_interval_days=1),
		Peep(id="7", name="Leader7", role=Role.LEADER.value, index=6, priority=3, total_attended=0, availability=[2,4,7], event_limit=2, min_interval_days=2),
		Peep(id="8", name="Leader8", role=Role.LEADER.value, index=7, priority=4, total_attended=0, availability=[0,3,6], event_limit=3, min_interval_days=0),
		Peep(id="9", name="Leader9", role=Role.LEADER.value, index=8, priority=0, total_attended=0, availability=[1,2,5], event_limit=1, min_interval_days=1),
		Peep(id="10", name="Leader10", role=Role.LEADER.value, index=9, priority=1, total_attended=0, availability=[0,4,7], event_limit=2, min_interval_days=2),
	]

	followers = [
		Peep(id="11", name="Follower1", role=Role.FOLLOWER.value, index=10, priority=1, total_attended=0, availability=[0,1,3], event_limit=2, min_interval_days=1),
		Peep(id="12", name="Follower2", role=Role.FOLLOWER.value, index=11, priority=2, total_attended=0, availability=[2,4,5], event_limit=4, min_interval_days=2),
		Peep(id="13", name="Follower3", role=Role.FOLLOWER.value, index=12, priority=3, total_attended=0, availability=[0,2,3], event_limit=1, min_interval_days=3),
		Peep(id="14", name="Follower4", role=Role.FOLLOWER.value, index=13, priority=4, total_attended=0, availability=[1,4,6], event_limit=3, min_interval_days=0),
		Peep(id="15", name="Follower5", role=Role.FOLLOWER.value, index=14, priority=0, total_attended=0, availability=[0,3,7], event_limit=2, min_interval_days=1),
		Peep(id="16", name="Follower6", role=Role.FOLLOWER.value, index=15, priority=1, total_attended=0, availability=[2,5,6], event_limit=1, min_interval_days=4),
		Peep(id="17", name="Follower7", role=Role.FOLLOWER.value, index=16, priority=2, total_attended=0, availability=[1,3,7], event_limit=3, min_interval_days=2),
		Peep(id="18", name="Follower8", role=Role.FOLLOWER.value, index=17, priority=3, total_attended=0, availability=[0,2,6], event_limit=4, min_interval_days=3),
		Peep(id="19", name="Follower9", role=Role.FOLLOWER.value, index=18, priority=4, total_attended=0, availability=[1,4,5], event_limit=2, min_interval_days=0),
		Peep(id="20", name="Follower10", role=Role.FOLLOWER.value, index=19, priority=0, total_attended=0, availability=[0,3,7], event_limit=1, min_interval_days=1),
	]

	return leaders + followers

def test_event_is_valid(events, peeps):
	# Case 1: Valid event with sufficient leaders and followers
	event = events[0]
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders: event.attendees.append((leader, leader.role))
	for follower in followers: event.attendees.append((follower, follower.role))
	assert event.is_valid()

	# Case 2: Invalid event - not enough followers
	event2 = events[1]
	leaders2 = peeps[:3]
	for leader in leaders2: event2.attendees.append((leader, leader.role))
	assert not event2.is_valid()

	# Case 3: Invalid event - not enough leaders
	event3 = events[2]
	followers3 = peeps[10:13]
	for follower in followers3: event3.attendees.append((follower, follower.role))
	assert not event3.is_valid()

def test_add_peeps_to_event(events, peeps):
	# Case 1: Add exact required leaders and followers
	event = events[3]
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders: event.attendees.append((leader, leader.role))
	for follower in followers: event.attendees.append((follower, follower.role))
	assert len(event.leaders) == 3
	assert len(event.followers) == 3

	# Case 2: Add more than max role (overflow test)
	event2 = events[4]
	leaders2 = peeps[:6]
	followers2 = peeps[10:16]
	for leader in leaders2: event2.attendees.append((leader, leader.role))
	for follower in followers2: event2.attendees.append((follower, follower.role))
	assert len(event2.leaders) > event2.max_role or len(event2.followers) > event2.max_role

def test_peep_can_attend(events, peeps):
	# Case 1: Peep eligible (event in availability, limit ok, no conflict)
	event = events[5]
	peep = [p for p in peeps if event.id in p.availability and p.role == Role.LEADER][0]
	assert peep.can_attend(event)

	# Case 2: Peep exceeds event limit
	over_limit_peep = copy.deepcopy(peep)
	over_limit_peep.num_events = over_limit_peep.event_limit
	assert not over_limit_peep.can_attend(event)

	# Case 3: Peep availability conflict (event not in availability)
	unavailable_peep = [p for p in peeps if event.id not in p.availability and p.role == Role.LEADER][0]
	assert not unavailable_peep.can_attend(event)
	
def test_update_event_attendees(events, peeps):
	event = events[6]
	leaders = peeps[:3]
	followers = peeps[10:13]
	winners = leaders + followers
	peep_subset = leaders + followers

	Peep.update_event_attendees(peep_subset, winners, event)

	for peep in winners:
		assert peep.num_events == 1
		assert peep.assigned_event_dates[0] == event.date
		assert peep.priority == 0

def test_eventsequence_equality(events, peeps):
	event = events[7]
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders:
		event.attendees.append((leader, leader.role))
	for follower in followers:
		event.attendees.append((follower, follower.role))

	sequence1 = EventSequence([event], leaders + followers)
	sequence2 = EventSequence([event], leaders + followers)
	sequence1.valid_events.append(event)
	sequence2.valid_events.append(event)

	# Case 1: Sequences with same attendees & order
	assert sequence1 == sequence2
	assert hash(sequence1) == hash(sequence2)

	# Case 2: Sequences with different attendee lists
	modified_peep = peeps[4]
	event2 = Event(id=99, date=datetime.datetime(2025, 3, 30, 16), min_role=3, max_role=5)
	event2.attendees.append((modified_peep, modified_peep.role))
	sequence3 = EventSequence([event2], [modified_peep])
	sequence3.valid_events.append(event2)
	assert sequence1 != sequence3
