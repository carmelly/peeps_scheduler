
import datetime
import pytest
from constants import DATE_FORMAT
from models import EventSequence, Peep, Event, Role, SwitchPreference
from scheduler import Scheduler
import constants


@pytest.fixture(autouse=True)
def patch_constants(monkeypatch):
	monkeypatch.setattr("constants.ABS_MIN_ROLE", 2)
	constants.CLASS_CONFIG[999] = {
		"price": 0,
		"min_role": 2,
		"max_role": 2,
		"allow_downgrade": False
	}

def make_event(id=1):
	return Event(
		id=id,
		date=datetime.datetime.strptime("2025-08-05 14:00", DATE_FORMAT),
		duration_minutes=999
	)

def test_switch_if_primary_full_peep_gets_scheduled_in_secondary():
	event = make_event()
	peeps:list[Peep] = []

	# Fill primary role (leaders)
	for i in range(3):
		peeps.append(Peep(
			id=i, name=f"L{i+1}", display_name=f"L{i}", email=f"l{i}@x.com",
			role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY, 
			availability = [1], event_limit = 1, index=i, priority=0
		))
	
	# Peep with LEADER primary but willing to switch
	switch_peep = peeps[2]
	switch_peep.display_name = 'L-Switch'
	switch_peep.switch_pref=SwitchPreference.SWITCH_IF_PRIMARY_FULL

	for peep in peeps: 
		peep.availability = [1]

	# Scheduler setup
	sched = Scheduler(data_folder="tests/data", max_events=1)
	sequence = EventSequence([event], peeps)
	sched.evaluate_sequence(sequence, keep_invalid=True)

	# Flexy should be assigned as a FOLLOWER (their opposite role)
	assert switch_peep in event.followers
	assert switch_peep.role == Role.LEADER  # original role unchanged


def test_primary_only_peep_does_not_switch():
	event = make_event()
	peeps:list[Peep] = []

	# Fill primary role (leaders)
	for i in range(3):
		peeps.append(Peep(
			id=i, name=f"L{i+1}", display_name=f"L{i}", email=f"l{i}@x.com",
			role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY, 
			availability = [1], event_limit = 1, index=i, priority=0
		))
	
	peep = peeps[2]
	peep.display_name = "L-Rigid"
	

	sched = Scheduler(data_folder="tests/data", max_events=1)
	sequence = EventSequence([event], peeps)
	sched.evaluate_sequence(sequence, keep_invalid=True)

	assert peep not in event.followers
	assert peep not in event.leaders  # Because leaders were full
	assert peep in event.alt_leaders


def test_switch_if_needed_is_ignored_in_initial_pass():
	event = make_event()
	peeps:list[Peep] = []

	# Fill primary role (leaders)
	for i in range(3):
		peeps.append(Peep(
			id=i, name=f"L{i+1}", display_name=f"L{i}", email=f"l{i}@x.com",
			role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY, 
			availability = [1], event_limit = 1, index=i, priority=0
		))

	# Peep with LEADER primary but willing to switch
	peep = peeps[2]
	peep.display_name = 'L-LastResort'
	peep.switch_pref=SwitchPreference.SWITCH_IF_NEEDED

	sched = Scheduler(data_folder="tests/data", max_events=1)
	sequence = EventSequence([event], peeps)
	sched.evaluate_sequence(sequence, keep_invalid=True)

	# Should only be added as alternate in primary role
	assert peep not in event.leaders
	assert peep not in event.followers
	assert peep in event.alt_leaders
