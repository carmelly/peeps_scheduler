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
from models import Peep, Event, Role, SwitchPreference


class TestPeepConstraints:
    """Test core constraint checking logic - the most critical functionality."""
    
    def test_can_attend_when_available(self):
        """Test that peep can attend event in their availability."""
        peep = Peep(id=1, role="leader", availability=[1, 2, 3], event_limit=2)
        event = Event(id=2, duration_minutes=120)
        
        assert peep.can_attend(event)
    
    def test_cannot_attend_when_unavailable(self):
        """Test that peep cannot attend event not in availability."""
        peep = Peep(id=1, role="leader", availability=[1, 3], event_limit=2)  # No event 2
        event = Event(id=2, duration_minutes=120)
        
        assert not peep.can_attend(event)
    
    def test_cannot_attend_when_over_event_limit(self):
        """Test that peep cannot attend when at event limit."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=1)
        peep.num_events = 1  # Already at limit
        event = Event(id=1, duration_minutes=120)
        
        assert not peep.can_attend(event)
    
    def test_can_attend_when_under_event_limit(self):
        """Test that peep can attend when under event limit."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2)
        peep.num_events = 1  # Under limit
        event = Event(id=1, duration_minutes=120)
        
        assert peep.can_attend(event)
    
    def test_cannot_attend_within_interval_days(self):
        """Test that peep cannot attend event within minimum interval."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2, min_interval_days=3)
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)
        
        # Add a previous event 2 days ago (within 3-day interval)
        previous_date = datetime.datetime(2025, 1, 8)
        peep.assigned_event_dates.append(previous_date)
        
        assert not peep.can_attend(event)
    
    def test_can_attend_exactly_at_interval_days(self):
        """Test that peep can attend event exactly at minimum interval."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2, min_interval_days=3)
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)
        
        # Add a previous event exactly 3 days ago (meets interval requirement)
        previous_date = datetime.datetime(2025, 1, 7)
        peep.assigned_event_dates.append(previous_date)
        
        assert peep.can_attend(event)
    
    def test_can_attend_with_zero_interval_days(self):
        """Test that peep with zero interval can attend multiple events same day."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2, min_interval_days=0)
        event = Event(id=1, date=datetime.datetime(2025, 1, 10, 14), duration_minutes=120)
        
        # Add a previous event same day
        same_day_earlier = datetime.datetime(2025, 1, 10, 10)
        peep.assigned_event_dates.append(same_day_earlier)
        
        assert peep.can_attend(event)
    
    def test_interval_calculation_works_both_directions(self):
        """Test that interval checking works for events before or after previous events."""
        peep = Peep(id=1, role="leader", availability=[1], event_limit=2, min_interval_days=2)
        
        # Event is 2025-01-10
        event = Event(id=1, date=datetime.datetime(2025, 1, 10), duration_minutes=120)
        
        # Previous event 1 day after (2025-01-11) - should block
        future_date = datetime.datetime(2025, 1, 11)
        peep.assigned_event_dates.append(future_date)
        
        assert not peep.can_attend(event)


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


class TestRoleHandling:
    """Test role string/enum conversion."""
    
    def test_role_from_string_leader_variations(self):
        """Test that various leader strings parse correctly."""
        assert Role.from_string("Leader") == Role.LEADER
        assert Role.from_string("leader") == Role.LEADER
        assert Role.from_string("LEADER") == Role.LEADER
        assert Role.from_string("lead") == Role.LEADER
        assert Role.from_string("Lead") == Role.LEADER
    
    def test_role_from_string_follower_variations(self):
        """Test that various follower strings parse correctly."""
        assert Role.from_string("Follower") == Role.FOLLOWER
        assert Role.from_string("follower") == Role.FOLLOWER
        assert Role.from_string("FOLLOWER") == Role.FOLLOWER
        assert Role.from_string("follow") == Role.FOLLOWER
        assert Role.from_string("Follow") == Role.FOLLOWER
    
    def test_role_from_string_invalid_raises(self):
        """Test that invalid role strings raise ValueError."""
        with pytest.raises(ValueError, match="unknown role"):
            Role.from_string("invalid")
    
    def test_role_opposite_behavior(self):
        """Test that role opposite() method works correctly."""
        assert Role.LEADER.opposite() == Role.FOLLOWER
        assert Role.FOLLOWER.opposite() == Role.LEADER


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


