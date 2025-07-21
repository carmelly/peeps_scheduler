import copy
import pytest
import datetime
from models import Peep, Event, EventSequence, Role
import constants

@pytest.fixture(autouse=True)
def patch_constants(monkeypatch):
	monkeypatch.setattr("constants.ABS_MIN_ROLE", 2)


@pytest.fixture(scope="module")
def events():
	
	constants.CLASS_CONFIG[999] = {
		"price": 0,
		"min_role": 3,
		"max_role": 5,
		"allow_downgrade": False
	}

	# Define 8 specific events, varying times and days, including same day and early times
	return [
		Event(id=0, date=datetime.datetime(2025, 3, 21, 16), duration_minutes=999),  # Friday 4 PM
		Event(id=1, date=datetime.datetime(2025, 3, 21, 19), duration_minutes=999),  # Friday 7 PM same day
		Event(id=2, date=datetime.datetime(2025, 3, 22, 11), duration_minutes=999),  # Saturday 11 AM
		Event(id=3, date=datetime.datetime(2025, 3, 23, 16), duration_minutes=999),  # Sunday 4 PM
		Event(id=4, date=datetime.datetime(2025, 3, 24, 19), duration_minutes=999),  # Monday 7 PM
		Event(id=5, date=datetime.datetime(2025, 3, 25, 11), duration_minutes=999),  # Tuesday 11 AM
		Event(id=6, date=datetime.datetime(2025, 3, 26, 16), duration_minutes=999),  # Wednesday 4 PM
		Event(id=7, date=datetime.datetime(2025, 3, 27, 16), duration_minutes=999),  # Thursday 4 PM
	]

@pytest.fixture(scope="module")
def peeps():
	
	leaders = [
		Peep(id=1, name="Leader1", role=Role.LEADER.value, index=0, priority=1, total_attended=0, availability=[0,1,2], event_limit=2, min_interval_days=0),
		Peep(id=2, name="Leader2", role=Role.LEADER.value, index=1, priority=2, total_attended=0, availability=[1,3,4], event_limit=3, min_interval_days=1),
		Peep(id=3, name="Leader3", role=Role.LEADER.value, index=2, priority=3, total_attended=0, availability=[0,2,5], event_limit=1, min_interval_days=2),
		Peep(id=4, name="Leader4", role=Role.LEADER.value, index=3, priority=0, total_attended=0, availability=[2,3,6], event_limit=2, min_interval_days=0),
		Peep(id=5, name="Leader5", role=Role.LEADER.value, index=4, priority=1, total_attended=0, availability=[0,4,7], event_limit=3, min_interval_days=3),
		Peep(id=6, name="Leader6", role=Role.LEADER.value, index=5, priority=2, total_attended=0, availability=[1,5,6], event_limit=1, min_interval_days=1),
		Peep(id=7, name="Leader7", role=Role.LEADER.value, index=6, priority=3, total_attended=0, availability=[2,4,7], event_limit=2, min_interval_days=2),
		Peep(id=8, name="Leader8", role=Role.LEADER.value, index=7, priority=4, total_attended=0, availability=[0,3,6], event_limit=3, min_interval_days=0),
		Peep(id=9, name="Leader9", role=Role.LEADER.value, index=8, priority=0, total_attended=0, availability=[1,2,5], event_limit=1, min_interval_days=1),
		Peep(id=10, name="Leader10", role=Role.LEADER.value, index=9, priority=1, total_attended=0, availability=[0,4,7], event_limit=2, min_interval_days=2),
	]

	followers = [
		Peep(id=11, name="Follower1", role=Role.FOLLOWER.value, index=10, priority=1, total_attended=0, availability=[0,1,3], event_limit=2, min_interval_days=1),
		Peep(id=12, name="Follower2", role=Role.FOLLOWER.value, index=11, priority=2, total_attended=0, availability=[2,4,5], event_limit=4, min_interval_days=2),
		Peep(id=13, name="Follower3", role=Role.FOLLOWER.value, index=12, priority=3, total_attended=0, availability=[0,2,3], event_limit=1, min_interval_days=3),
		Peep(id=14, name="Follower4", role=Role.FOLLOWER.value, index=13, priority=4, total_attended=0, availability=[1,4,6], event_limit=3, min_interval_days=0),
		Peep(id=15, name="Follower5", role=Role.FOLLOWER.value, index=14, priority=0, total_attended=0, availability=[0,3,7], event_limit=2, min_interval_days=1),
		Peep(id=16, name="Follower6", role=Role.FOLLOWER.value, index=15, priority=1, total_attended=0, availability=[2,5,6], event_limit=1, min_interval_days=4),
		Peep(id=17, name="Follower7", role=Role.FOLLOWER.value, index=16, priority=2, total_attended=0, availability=[1,3,7], event_limit=3, min_interval_days=2),
		Peep(id=18, name="Follower8", role=Role.FOLLOWER.value, index=17, priority=3, total_attended=0, availability=[0,2,6], event_limit=4, min_interval_days=3),
		Peep(id=19, name="Follower9", role=Role.FOLLOWER.value, index=18, priority=4, total_attended=0, availability=[1,4,5], event_limit=2, min_interval_days=0),
		Peep(id=20, name="Follower10", role=Role.FOLLOWER.value, index=19, priority=0, total_attended=0, availability=[0,3,7], event_limit=1, min_interval_days=1),
	]

	# TODO: this fixture is bad because it is not sorted in order of priority
	# we should be returning peeps in the correct sort order in order to properly test sequence 
	# evaluation 
	return leaders + followers

def test_event_is_valid(events, peeps):
	events = copy.deepcopy(events)

	# Case 1: Valid event with sufficient leaders and followers
	event = events[0]
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders: event.add_attendee(leader, Role.LEADER)
	for follower in followers: event.add_attendee(follower, Role.FOLLOWER)
	assert event.meets_absolute_min()

	# Case 2: Invalid event - not enough followers
	event2 = events[1]
	leaders2 = peeps[:3]
	for leader in leaders2: event2.add_attendee(leader, Role.LEADER)
	assert not event2.meets_absolute_min()

	# Case 3: Invalid event - not enough leaders
	event3 = events[2]
	followers3 = peeps[10:13]
	for follower in followers3: event3.add_attendee(follower, Role.FOLLOWER)
	assert not event3.meets_absolute_min()

def test_add_peeps_to_event(events, peeps):
	events = copy.deepcopy(events)

	# Case 1: Add exact required leaders and followers
	event = events[3]
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders: event.add_attendee(leader, Role.LEADER)
	for follower in followers: event.add_attendee(follower, Role.FOLLOWER)
	assert len(event.leaders) == 3
	assert len(event.followers) == 3

	# Case 2: Add more than max role (overflow test)
	event2 = events[4]
	leaders2 = peeps[:6]
	followers2 = peeps[10:16]

	with pytest.raises(RuntimeError, match="Too many attendees in role Leader"):
		for leader in leaders2: event2.add_attendee(leader, Role.LEADER)
	with pytest.raises(RuntimeError, match="Too many attendees in role Follower"):
		for follower in followers2: event2.add_attendee(follower, Role.FOLLOWER)

	# we should still only have the max allowed per role 
	assert len(event2.leaders) == event2.max_role and len(event2.followers) == event2.max_role

def test_peep_can_attend(events, peeps):
	events = copy.deepcopy(events)

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
	events = copy.deepcopy(events)

	event = events[6]
	leaders = peeps[:3]
	followers = peeps[10:13]
	winners = leaders + followers
	for peep in winners: 
		event.add_attendee(peep, peep.role)

	Peep.update_event_attendees(peeps, event)

	for peep in winners:
		assert peep.num_events == 1
		assert peep.assigned_event_dates[0] == event.date
		assert peep.priority == 0

	for peep in peeps: 
		# check that peeps that attended were updated, and peeps that didn't attend were not 
		if peep in winners: 
			assert peep.num_events == 1
			assert peep.assigned_event_dates[0] == event.date
			assert peep.priority == 0
		else: 
			assert peep.num_events == 0 
			assert peep.assigned_event_dates == []

	# First 3 leaders and first 3 followers should have moved to the bottom of the list. 
	# Check that the list is in the correct order and that priority is unchanged for non-attendees. 
	assert peeps[0].id == 4 and peeps[0].priority == 0 
	assert peeps[1].id == 5 and peeps[1].priority == 1 
	assert peeps[2].id == 6 and peeps[2].priority == 2 
	assert peeps[3].id == 7 and peeps[3].priority == 3 
	assert peeps[4].id == 8 and peeps[4].priority == 4
	assert peeps[5].id == 9 and peeps[5].priority == 0
	assert peeps[6].id == 10 and peeps[6].priority == 1 
	assert peeps[7].id == 14 and peeps[7].priority == 4
	assert peeps[8].id == 15 and peeps[8].priority == 0 
	assert peeps[9].id == 16 and peeps[9].priority == 1 
	assert peeps[10].id == 17 and peeps[10].priority == 2
	assert peeps[11].id == 18 and peeps[11].priority == 3
	assert peeps[12].id == 19 and peeps[12].priority == 4
	assert peeps[13].id == 20 and peeps[13].priority == 0
	assert peeps[14].id == 1 and peeps[14].priority == 0
	assert peeps[15].id == 2 and peeps[17].priority == 0
	assert peeps[16].id == 3 and peeps[16].priority == 0
	assert peeps[17].id == 11 and peeps[17].priority == 0
	assert peeps[18].id == 12 and peeps[18].priority == 0
	assert peeps[19].id == 13 and peeps[19].priority == 0

def test_eventsequence_equality(events, peeps):
	events = copy.deepcopy(events)

	# Case 1: Sequences with same event & attendee order
	event = copy.deepcopy(events[7])
	leaders = peeps[:3]
	followers = peeps[10:13]
	for leader in leaders:
		event.add_attendee(leader, Role.LEADER)
	for follower in followers:
		event.add_attendee(follower, Role.FOLLOWER)

	sequence1 = EventSequence([event], peeps)
	sequence2 = EventSequence([event], peeps)
	sequence1.valid_events.append(event)
	sequence2.valid_events.append(event)
	assert sequence1 == sequence2
	assert hash(sequence1) == hash(sequence2)

	# Case 2: Sequences with same event but attendees added in a different order 
	event2 = copy.deepcopy(events[7])
	for follower in followers:
		event2.add_attendee(follower, Role.FOLLOWER)
	for leader in leaders:
		event2.add_attendee(leader, Role.LEADER)
	sequence3 = EventSequence([event2], peeps)
	sequence3.valid_events.append(event2)
	assert sequence1 == sequence3

	# Case 3: Same event, different attendees 
	event3 = copy.deepcopy(events[7])
	for leader in peeps[4:7]:
		event3.add_attendee(leader, Role.LEADER)
	for follower in peeps[14:17]:
		event3.add_attendee(follower, Role.FOLLOWER)
	
	sequence4 = EventSequence([event3], peeps)
	sequence4.valid_events.append(event3)
	assert sequence1 != sequence4

	# Case 4: Different events, same attendees 
	event4 = events[4]
	for leader in leaders:
		event4.add_attendee(leader, Role.LEADER)
	for follower in followers:
		event4.add_attendee(follower, Role.FOLLOWER)
	sequence5 = EventSequence([event4], peeps)
	sequence5.valid_events.append(event4)
	assert sequence1 != sequence5
