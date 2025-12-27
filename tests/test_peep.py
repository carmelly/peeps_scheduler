"""
Test Peep class functionality with focus on constraint checking and data handling.

Following testing philosophy:
- Test what could actually break
- Use inline creation for simple constraint tests
- Use factories for complex multi-field scenarios
- Focus on individual Peep behavior, not scheduling logic
"""

import pytest
import datetime
from peeps_scheduler.models import Peep, Event, Role, SwitchPreference


@pytest.mark.unit
class TestPeepConstraints:
    """Test core constraint checking logic - the most critical functionality."""

    @pytest.mark.parametrize("availability,event_id,can_attend_expected", [
        ([1, 2, 3], 2, True),   # available
        ([1, 3], 2, False),     # unavailable
    ])
    def test_can_attend_availability_constraint(self, availability, event_id, can_attend_expected):
        """Test that peep can only attend events in their availability."""
        peep = Peep(id=1, role="leader", availability=availability, event_limit=2)
        event = Event(id=event_id, duration_minutes=120)

        assert peep.can_attend(event) == can_attend_expected

    @pytest.mark.parametrize("num_events,event_limit,can_attend_expected", [
        (1, 1, False),  # at or over limit
        (1, 2, True),   # under limit
    ])
    def test_can_attend_event_limit_constraint(self, num_events, event_limit, can_attend_expected):
        """Test that peep respects event limit."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=event_limit)
        peep.num_events = num_events
        event = Event(id=1, duration_minutes=120)

        assert peep.can_attend(event) == can_attend_expected

    @pytest.mark.parametrize("min_interval,previous_days_ago,can_attend_expected,description", [
        (3, 2, False, "within_interval"),
        (3, 3, True, "exactly_at_interval"),
        (0, 0, True, "zero_interval_same_day"),
        (2, 1, False, "bidirectional_blocks_future"),
    ])
    def test_can_attend_interval_constraint(self, min_interval, previous_days_ago, can_attend_expected, description):
        """Test that peep respects minimum interval between events."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2, min_interval_days=min_interval)

        if min_interval == 0:
            # Same day test - use time offset instead of date offset
            event = Event(id=1, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)
            same_day_earlier = datetime.datetime(2025, 1, 10, 10)
            peep.assigned_event_dates.append(same_day_earlier)
        elif description == "bidirectional_blocks_future":
            # Event before, previous event after (2025-01-11)
            event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)
            future_date = datetime.datetime(2025, 1, 11)
            peep.assigned_event_dates.append(future_date)
        else:
            # Normal case: event on 2025-01-10, previous event N days ago
            event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)
            previous_date = datetime.datetime(2025, 1, 10) - datetime.timedelta(days=previous_days_ago)
            peep.assigned_event_dates.append(previous_date)

        assert peep.can_attend(event) == can_attend_expected


@pytest.mark.unit
class TestSwitchPreferences:
    """Test switch preference data handling."""
    
    def test_switch_preference_from_string_primary_only(self):
        """Test PRIMARY_ONLY preference parsing."""
        pref = SwitchPreference.from_string("I only want to be scheduled in my primary role")
        assert pref == SwitchPreference.PRIMARY_ONLY
    
    def test_switch_preference_from_string_switch_if_full(self):
        """Test SWITCH_IF_PRIMARY_FULL preference parsing."""
        pref = SwitchPreference.from_string(
            "I'm happy to dance my secondary role if it lets me attend when my primary is full"
        )
        assert pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
    
    def test_switch_preference_from_string_switch_if_needed(self):
        """Test SWITCH_IF_NEEDED preference parsing."""
        pref = SwitchPreference.from_string(
            "I'm willing to dance my secondary role only if it's needed to enable filling a session"
        )
        assert pref == SwitchPreference.SWITCH_IF_NEEDED
    
    def test_switch_preference_from_string_invalid_raises(self):
        """Test that invalid switch preference strings raise ValueError."""
        with pytest.raises(ValueError, match="unknown role"):
            SwitchPreference.from_string("Invalid preference string")


@pytest.mark.unit
class TestRoleHandling:
    """Test role string/enum conversion."""
    
    @pytest.mark.parametrize("role_string,expected_role", [
        ("Leader", Role.LEADER),
        ("leader", Role.LEADER),
        ("LEADER", Role.LEADER),
        ("lead", Role.LEADER),
        ("Lead", Role.LEADER),
        ("Follower", Role.FOLLOWER),
        ("follower", Role.FOLLOWER),
        ("FOLLOWER", Role.FOLLOWER),
        ("follow", Role.FOLLOWER),
        ("Follow", Role.FOLLOWER),
    ])
    def test_role_from_string_variations(self, role_string, expected_role):
        """Test that various role strings parse correctly."""
        assert Role.from_string(role_string) == expected_role
    
    def test_role_from_string_invalid_raises(self):
        """Test that invalid role strings raise ValueError."""
        with pytest.raises(ValueError, match="unknown role"):
            Role.from_string("invalid")
    
    def test_role_opposite_behavior(self):
        """Test that role opposite() method works correctly."""
        assert Role.LEADER.opposite() == Role.FOLLOWER
        assert Role.FOLLOWER.opposite() == Role.LEADER


@pytest.mark.unit
class TestDataConversion:
    """Test CSV/dict conversion for data pipeline."""
    
    def test_from_csv_with_typical_data(self):
        """Test that from_csv creates correct Peep from typical CSV row."""
        csv_row = {
            "id": "42",
            "Name": "Alice Alpha",
            "Display Name": "Alice",
            "Email Address": "alice@test.com",
            "Role": "Leader",
            "Index": "5",
            "Priority": "3",
            "Total Attended": "7",
            "Active": "TRUE",
            "Date Joined": "2022-01-01"
        }
        
        peep = Peep.from_csv(csv_row)
        
        assert peep.id == 42
        assert peep.full_name == "Alice Alpha"
        assert peep.display_name == "Alice"
        assert peep.email == "alice@test.com"
        assert peep.role == Role.LEADER
        assert peep.index == 5
        assert peep.priority == 3
        assert peep.total_attended == 7
        assert peep.active is True
        assert peep.date_joined == "2022-01-01"
    
    def test_to_csv_roundtrip_integrity(self):
        """Test that to_csv produces data that can recreate the peep."""
        original = Peep(
            id=123,
            full_name="Bob Beta", 
            display_name="Bob",
            email="bob@test.com",
            role=Role.FOLLOWER,
            index=2,
            priority=1,
            total_attended=4,
            active=False,
            date_joined="2023-05-15"
        )
        
        csv_data = original.to_csv()
        recreated = Peep.from_csv(csv_data)
        
        assert recreated.id == original.id
        assert recreated.full_name == original.full_name
        assert recreated.display_name == original.display_name
        assert recreated.email == original.email
        assert recreated.role == original.role
        assert recreated.index == original.index
        assert recreated.priority == original.priority
        assert recreated.total_attended == original.total_attended
        assert recreated.active == original.active
        assert recreated.date_joined == original.date_joined
    
    def test_constructor_handles_missing_optional_fields(self):
        """Test that constructor gracefully handles missing optional fields."""
        # Test with minimal required fields
        peep = Peep(id=1, role="leader")
        
        assert peep.id == 1
        assert peep.role == Role.LEADER
        assert peep.index == 0
        assert peep.priority == 0
        assert peep.total_attended == 0
        assert peep.availability == []
        assert peep.event_limit == 0
        assert peep.min_interval_days == 0
    
    def test_constructor_requires_id(self):
        """Test that constructor raises clear error for missing ID."""
        with pytest.raises(ValueError, match="peep requires an 'id' field"):
            Peep(role="leader")
    
    def test_constructor_requires_role(self):
        """Test that constructor raises clear error for missing role."""
        with pytest.raises(ValueError, match="peep requires a 'role' field"):
            Peep(id=1)


