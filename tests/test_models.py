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

# Removed test_event_is_valid - now covered by TestEventValidation in test_event.py

# Removed test_add_peeps_to_event - now covered by TestEventAttendeeManagement in test_event.py
	
	
# Removed test_update_event_attendees - now covered by TestEventPeepIntegration in test_event.py

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
