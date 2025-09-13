import pytest
import constants
from models import Event, Role, EventSequence, Peep
from scheduler import Scheduler

# Removed test_promote_alternate - now covered by TestEventAlternateManagement in test_event.py

# Removed test_demote_attendee - now covered by TestEventAlternateManagement in test_event.py

# Removed test_balance_roles_with_extra_leader - now covered by TestEventRoleBalancing in test_event.py

# Removed test_downgrade_duration - now covered by TestEventDurationManagement in test_event.py

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
