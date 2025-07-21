from typing import Callable
import pytest
import datetime
from models import Peep, Event, Role, SwitchPreference
import constants

@pytest.fixture(autouse=True)
def patch_abs_min(monkeypatch):
	abs_min = min(config["min_role"] for config in constants.CLASS_CONFIG.values() if config["allow_downgrade"])
	monkeypatch.setattr(constants, "ABS_MIN_ROLE", abs_min)

@pytest.fixture(autouse=True, scope="session")
def patch_class_config():
	constants.CLASS_CONFIG[1] = {
		"price": 50,
		"min_role": 1,
		"max_role": 2,
		"allow_downgrade": False,
	}
	constants.CLASS_CONFIG[2] = {
		"price": 100,
		"min_role": 2,
		"max_role": 3,
		"allow_downgrade": True,
	}
	constants.CLASS_CONFIG[3] = {
		"price": 150,
		"min_role": 3,
		"max_role": 5,
		"allow_downgrade": True,
	}

@pytest.fixture
def peep_factory() -> Callable[..., Peep]:
	def _create(
		id,
		name=None,
		role=Role.LEADER,
		switch_pref=SwitchPreference.PRIMARY_ONLY,
		index=0,
		priority=0,
		total_attended=0,
		availability=None,
		event_limit=1,
		min_interval_days=0,
		responded=True
	) -> Peep:
		return Peep(
			id=id,
			name=name or f"{role.value}{id}",
			display_name=name or f"{role.value}{id}",
			email=f"peep{id}@test.com",
			role=role, 
			switch_pref=switch_pref,
			index=index,
			priority=priority,
			total_attended=total_attended,
			availability=availability or [0],
			event_limit=event_limit,
			min_interval_days=min_interval_days,
			responded=responded
		)
	return _create

@pytest.fixture
def event_factory() -> Callable[..., Event]:
	def _create(id: int, duration_minutes=2, date: datetime.datetime = None) -> Event:
		if not date:
			date = datetime.datetime(2025, 3, 21, 18)
		return Event(id=id, date=date, duration_minutes=duration_minutes)
	return _create

@pytest.fixture
def events(event_factory) -> list[Event]:

	return [
		event_factory(id=1, date=datetime.datetime(2025, 3, 21, 16)),
		event_factory(id=2, date=datetime.datetime(2025, 3, 21, 19)),
		event_factory(id=3, date=datetime.datetime(2025, 3, 22, 11)),
		event_factory(id=4, date=datetime.datetime(2025, 3, 23, 16)),
		event_factory(id=5, date=datetime.datetime(2025, 3, 24, 19)),
		event_factory(id=6, date=datetime.datetime(2025, 3, 25, 11)),
		event_factory(id=7, date=datetime.datetime(2025, 3, 26, 16)),
		event_factory(id=8, date=datetime.datetime(2025, 3, 27, 16)),
	]

@pytest.fixture
def peeps(peep_factory) -> list[Peep]:
	"""
	Return a list of peeps (mixed roles) with fixed availability across events 0–7.
	Sorted by descending priority and ascending index.
	"""

	# Availability matrix by event index (0–7):
	#
	# | Index | Event ID | Leaders Available | Followers Available |
	# | ----- | -------- | ----------------- | ------------------- |
	# | 0     | 1        | 1, 9              | 7                   |
	# | 1     | 2        | 1                 | 2, 8                |
	# | 2     | 3        | 1, 3, 9           | 2                   |
	# | 3     | 4        | 3, 4              | 2, 10               |
	# | 4     | 5        | 3, 5, 9           |                     |
	# | 5     | 6        | 4, 5              | 6, 8                |
	# | 6     | 7        | 5                 | 6, 7, 10            |
	# | 7     | 8        |                   | 6, 7, 8, 10         |
	#
	# events[3] and events[5] have at least 2 leaders and 2 followers available.
	# Use these for tests requiring minimum valid attendance.

	return [
	# Priority 3
	peep_factory(id=1, role=Role.LEADER, priority=3, index=0, availability=[1, 2, 3]),

	# Priority 2
	peep_factory(id=2, role=Role.FOLLOWER, priority=2, index=1, availability=[2, 3, 4]),
	peep_factory(id=3, role=Role.LEADER, priority=2, index=2, availability=[3, 4, 5]),

	# Priority 1
	peep_factory(id=4, role=Role.LEADER, priority=1, index=3, availability=[4, 5, 6]),
	peep_factory(id=5, role=Role.LEADER, priority=1, index=4, availability=[5, 6, 7]),
	peep_factory(id=6, role=Role.FOLLOWER, priority=1, index=5, availability=[6, 7, 8]),

	# Priority 0
	peep_factory(id=7, role=Role.FOLLOWER, priority=0, index=6, availability=[1, 7, 8]),
	peep_factory(id=8, role=Role.FOLLOWER, priority=0, index=7, availability=[2, 6, 8]),
	peep_factory(id=9, role=Role.LEADER, priority=0, index=8, availability=[1, 3, 5]),
	peep_factory(id=10, role=Role.FOLLOWER, priority=0, index=9, availability=[4, 7, 8]),
]

def leaders(peeps) -> list[Peep]:
	return [p for p in peeps if p.role == Role.LEADER]

def followers(peeps) -> list[Peep]:
	return [p for p in peeps if p.role == Role.FOLLOWER]
