import datetime
import pytest
from models import Peep, Event, Role

@pytest.fixture
def common_dates():
	return {
		"date1": datetime.datetime(2025, 3, 1, 17, 0),  # March 1, 5PM
		"date2": datetime.datetime(2025, 3, 1, 19, 0),  # March 1, 7PM
		"date3": datetime.datetime(2025, 3, 2, 19, 0),  # March 2, 7PM
		"date4": datetime.datetime(2025, 3, 8, 17, 0),  # March 8, 5PM
		"date_near": datetime.datetime(2025, 3, 6, 17, 0),  # March 6
		"date4_early": datetime.datetime(2025, 3, 8, 13, 0)  # March 8, 1PM
	}

def test_zero_interval_days(common_dates):
	peep = Peep(id="1", name="P1", role=Role.LEADER, availability=[1, 2], event_limit=5, priority=1, min_interval_days=0)
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)
	event2 = Event(id=2, date=common_dates["date2"],duration_minutes=120)

	assert peep.can_attend(event1)
	peep.assigned_event_dates.append(event1.date)
	assert peep.can_attend(event2)  # Same day OK

def test_one_interval_day(common_dates):
	peep = Peep(id="2", name="P2", role=Role.FOLLOWER, availability=[1, 2, 3], event_limit=5, priority=1, min_interval_days=1)
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)
	event2 = Event(id=2, date=common_dates["date2"],duration_minutes=120)
	event3 = Event(id=3, date=common_dates["date3"],duration_minutes=120)

	assert peep.can_attend(event1)
	peep.assigned_event_dates.append(event1.date)
	assert not peep.can_attend(event2)  # Same day blocked
	assert peep.can_attend(event3)      # Next day allowed

def test_seven_interval_days(common_dates):
	peep = Peep(id="3", name="P3", role=Role.LEADER, availability=[1, 4], event_limit=5, priority=1, min_interval_days=7)
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)
	event4 = Event(id=4, date=common_dates["date4"],duration_minutes=120)
	event_near = Event(id=5, date=common_dates["date_near"],duration_minutes=120)

	assert peep.can_attend(event1)
	peep.assigned_event_dates.append(event1.date)
	assert not peep.can_attend(event_near)  # 6 days apart blocked
	assert peep.can_attend(event4)          # Exactly 7 days apart allowed

def test_seven_days_apart_earlier_time(common_dates):
	peep = Peep(id="4", name="P4", role=Role.FOLLOWER, availability=[1, 6], event_limit=5, priority=1, min_interval_days=7)
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)
	event4_early = Event(id=6, date=common_dates["date4_early"],duration_minutes=120)

	assert peep.can_attend(event1)
	peep.assigned_event_dates.append(event1.date)
	assert peep.can_attend(event4_early)  # 7 days apart, earlier time allowed

def test_assign_out_of_order_pass(common_dates):
	peep = Peep(id="5", name="P5", role=Role.LEADER, availability=[1, 4], event_limit=5, priority=1, min_interval_days=7)
	event4 = Event(id=4, date=common_dates["date4"],duration_minutes=120)  # March 8
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)  # March 1

	assert peep.can_attend(event4)
	peep.assigned_event_dates.append(event4.date)
	assert peep.can_attend(event1)  # March 1 is before March 8, 7 days apart, should pass

def test_assign_out_of_order_fail(common_dates):
	peep = Peep(id="6", name="P6", role=Role.FOLLOWER, availability=[1, 5], event_limit=5, priority=1, min_interval_days=7)
	event_near = Event(id=5, date=common_dates["date_near"],duration_minutes=120)  # March 6
	event1 = Event(id=1, date=common_dates["date1"],duration_minutes=120)  # March 1

	assert peep.can_attend(event_near)
	peep.assigned_event_dates.append(event_near.date)
	assert not peep.can_attend(event1)  # March 1 is 5 days before March 6, violates 7 days
