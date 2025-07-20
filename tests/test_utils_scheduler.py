import datetime
import math
import pytest

import utils
from scheduler import Scheduler
from models import Event, Peep, Role

@pytest.fixture
def basic_data():
	events = [
		Event(id=1, date=datetime.datetime(2025,1,1,12), duration_minutes=60),
		Event(id=2, date=datetime.datetime(2025,1,2,12), duration_minutes=60),
		Event(id=3, date=datetime.datetime(2025,1,3,12), duration_minutes=60),
	]
	peeps = [
		Peep(id=1, name="L1", role=Role.LEADER.value, availability=[1,2], event_limit=2, priority=0),
		Peep(id=2, name="F1", role=Role.FOLLOWER.value, availability=[1,2,3], event_limit=2, priority=1),
		Peep(id=3, name="L2", role=Role.LEADER.value, availability=[3], event_limit=2, priority=0),
	]
	return events, peeps


def test_parse_event_date():
	year = datetime.datetime.now().year
	result = utils.parse_event_date("Wednesday March 05 - 4PM (ignore)")
	assert result == f"{year}-03-05 16:00"


def test_generate_event_permutations(basic_data):
	events, _ = basic_data
	perms = utils.generate_event_permutations(events)
	assert len(perms) == math.factorial(len(events))
	assert sorted(perms[0]) == sorted([e.id for e in events])


def test_is_sorted_by_priority(basic_data):
	_, peeps = basic_data
	sorted_peeps = sorted(peeps, key=lambda p: p.priority, reverse=True)
	assert utils.is_sorted_by_priority(sorted_peeps)
	sorted_peeps[0].priority = -1
	assert not utils.is_sorted_by_priority(sorted_peeps)


def test_sanitize_events(monkeypatch, basic_data):
	events, peeps = basic_data
	monkeypatch.setattr("models.ABS_MIN_ROLE", 1)
	monkeypatch.setattr("scheduler.ABS_MIN_ROLE", 1)
	sched = Scheduler(data_folder="tests/data", max_events=3)
	valid = sched.sanitize_events(events, peeps)
	assert len(valid) == 3
	# make event 3 invalid by removing follower availability
	peeps[1].availability.remove(3)
	valid2 = sched.sanitize_events(events, peeps)
	assert {e.id for e in valid2} == {1,2}


def test_remove_high_overlap_events(monkeypatch, basic_data):
	events, peeps = basic_data
	monkeypatch.setattr("models.ABS_MIN_ROLE", 1)
	monkeypatch.setattr("scheduler.ABS_MIN_ROLE", 1)
	sched = Scheduler(data_folder="tests/data", max_events=2)
	trimmed = sched.remove_high_overlap_events(events, peeps, 2)
	assert len(trimmed) == 2
	assert 1 not in [e.id for e in trimmed]