"""
Test Event class functionality with focus on attendee management and validation.

Following testing philosophy:
- Test what could actually break
- Use inline creation for simple event scenarios
- Use factories for complex multi-attendee setups
- Focus on individual Event behavior, not scheduling logic
"""

import pytest
import datetime
from models import Event, Peep, Role
import constants




class TestEventAttendeeManagement:
    """Test core attendee addition, removal, and capacity management."""
    
    def test_add_attendee_success(self, event_factory, peep_factory):
        """Test that attendees can be added to event."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_attendee(peep, Role.LEADER)
        
        assert peep in event.leaders
        assert peep in event.attendees
        assert len(event.leaders) == 1
    
    def test_add_attendee_tracks_assignment_order(self, event_factory, peep_factory):
        """Test that attendee assignment order is preserved."""
        event = event_factory()
        peep1 = peep_factory(id=1, role=Role.LEADER)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER)
        
        event.add_attendee(peep1, Role.LEADER)
        event.add_attendee(peep2, Role.FOLLOWER)
        
        assert event.attendees == (peep1, peep2)
    
    def test_add_attendee_respects_capacity_limits(self, event_factory, peep_factory):
        """Test that events reject attendees when at capacity."""
        event = event_factory(duration_minutes=60)  # 60min = max 3 per role
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(3)]
        
        # Fill to capacity
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        
        # Should reject the 4th
        overflow_peep = peep_factory(id=4, role=Role.LEADER)
        with pytest.raises(RuntimeError, match="Too many attendees in role Leader"):
            event.add_attendee(overflow_peep, Role.LEADER)
    
    def test_add_attendee_prevents_duplicates(self, event_factory, peep_factory):
        """Test that same peep cannot be added twice."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_attendee(peep, Role.LEADER)
        
        with pytest.raises(RuntimeError, match="Cannot add attendee twice"):
            event.add_attendee(peep, Role.LEADER)
    
    def test_num_attendees_counts_correctly(self, event_factory, peep_factory):
        """Test that attendee counts are accurate."""
        event = event_factory()
        leader = peep_factory(id=1, role=Role.LEADER)
        follower = peep_factory(id=2, role=Role.FOLLOWER)
        
        assert event.num_attendees() == 0
        assert event.num_attendees(Role.LEADER) == 0
        
        event.add_attendee(leader, Role.LEADER)
        assert event.num_attendees() == 1
        assert event.num_attendees(Role.LEADER) == 1
        assert event.num_attendees(Role.FOLLOWER) == 0
        
        event.add_attendee(follower, Role.FOLLOWER)
        assert event.num_attendees() == 2
        assert event.num_attendees(Role.LEADER) == 1
        assert event.num_attendees(Role.FOLLOWER) == 1
    
    def test_clear_participants_removes_all(self, event_factory, peep_factory):
        """Test that clear_participants removes all attendees and alternates."""
        event = event_factory()
        leader = peep_factory(id=1, role=Role.LEADER)
        follower = peep_factory(id=2, role=Role.FOLLOWER)
        alt = peep_factory(id=3, role=Role.LEADER)
        
        event.add_attendee(leader, Role.LEADER)
        event.add_attendee(follower, Role.FOLLOWER)
        event.add_alternate(alt, Role.LEADER)
        
        event.clear_participants()
        
        assert len(event.attendees) == 0
        assert len(event.leaders) == 0
        assert len(event.followers) == 0
        assert len(event.alt_leaders) == 0
        assert len(event.alt_followers) == 0


class TestEventAlternateManagement:
    """Test alternate tracking and promotion/demotion."""
    
    def test_add_alternate_success(self, event_factory, peep_factory):
        """Test that alternates can be added to event."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_alternate(peep, Role.LEADER)
        
        assert peep in event.alt_leaders
        assert peep in event.get_alternates()
        assert peep not in event.attendees
    
    def test_promote_alt_to_attendee(self, event_factory, peep_factory):
        """Test that alternates can be promoted to full attendees."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_alternate(peep, Role.LEADER)
        event.promote_alt(peep, Role.LEADER)
        
        assert peep in event.leaders
        assert peep in event.attendees
        assert peep not in event.alt_leaders
    
    def test_demote_attendee_to_alt(self, event_factory, peep_factory):
        """Test that attendees can be demoted to alternates."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_attendee(peep, Role.LEADER)
        event.demote_attendee_to_alt(peep, Role.LEADER)
        
        assert peep in event.alt_leaders
        assert peep not in event.leaders
        assert peep not in event.attendees
    
    def test_demote_adds_to_front_of_alternates(self, event_factory, peep_factory):
        """Test that demoted attendees go to front of alternate list."""
        event = event_factory()
        existing_alt = peep_factory(id=1, role=Role.LEADER)
        attendee = peep_factory(id=2, role=Role.LEADER)
        
        event.add_alternate(existing_alt, Role.LEADER)
        event.add_attendee(attendee, Role.LEADER)
        event.demote_attendee_to_alt(attendee, Role.LEADER)
        
        assert event.alt_leaders == (attendee, existing_alt)
    
    def test_remove_alternate_success(self, event_factory, peep_factory):
        """Test that alternates can be removed."""
        event = event_factory()
        peep = peep_factory(id=1, role=Role.LEADER)
        
        event.add_alternate(peep, Role.LEADER)
        event.remove_alternate(peep, Role.LEADER)
        
        assert peep not in event.alt_leaders


class TestEventValidation:
    """Test event validation methods for capacity and minimums."""
    
    def test_meets_min_when_sufficient_attendees(self, event_factory, peep_factory):
        """Test that events with sufficient attendees meet minimums."""
        event = event_factory(duration_minutes=120)  # min_role = 6
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(6)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER) for i in range(6)]
        
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        for follower in followers:
            event.add_attendee(follower, Role.FOLLOWER)
        
        assert event.meets_min()
        assert event.meets_min(Role.LEADER)
        assert event.meets_min(Role.FOLLOWER)
    
    def test_does_not_meet_min_when_insufficient(self, event_factory, peep_factory):
        """Test that events with insufficient attendees don't meet minimums."""
        event = event_factory(duration_minutes=120)  # min_role = 6
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(3)]  # Only 3
        
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        
        assert not event.meets_min()
        assert not event.meets_min(Role.LEADER)
    
    def test_meets_absolute_min_with_basic_attendees(self, event_factory, peep_factory):
        """Test absolute minimum validation with ABS_MIN_ROLE attendees."""
        event = event_factory()
        # Add exactly ABS_MIN_ROLE attendees per role
        for i in range(constants.ABS_MIN_ROLE):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        assert event.meets_absolute_min()
    
    def test_does_not_meet_absolute_min_when_insufficient(self, event_factory, peep_factory):
        """Test that events below absolute minimum fail validation."""
        event = event_factory()
        # Add one less than ABS_MIN_ROLE
        for i in range(constants.ABS_MIN_ROLE - 1):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        assert not event.meets_absolute_min()
    
    def test_is_full_when_at_capacity(self, event_factory, peep_factory):
        """Test that events at capacity report as full."""
        event = event_factory(duration_minutes=60)  # max_role = 3
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(3)]
        
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        
        assert event.is_full(Role.LEADER)
        assert not event.is_full(Role.FOLLOWER)
        assert not event.is_full()  # Not full overall
    
    def test_is_full_overall_when_both_roles_full(self, event_factory, peep_factory):
        """Test that events report full when both roles are at capacity."""
        event = event_factory(duration_minutes=60)  # max_role = 3
        
        for i in range(3):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        assert event.is_full()
        assert event.is_full(Role.LEADER)
        assert event.is_full(Role.FOLLOWER)


class TestEventRoleBalancing:
    """Test role balancing functionality."""
    
    def test_balance_roles_with_equal_counts(self, event_factory, peep_factory):
        """Test that balanced events remain unchanged."""
        event = event_factory()
        
        for i in range(3):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        event.balance_roles()
        
        assert len(event.leaders) == 3
        assert len(event.followers) == 3
        assert len(event.alt_leaders) == 0
        assert len(event.alt_followers) == 0
    
    def test_balance_roles_demotes_excess_leaders(self, event_factory, peep_factory):
        """Test that excess leaders are demoted to alternates."""
        event = event_factory()
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(4)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER) for i in range(2)]
        
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        for follower in followers:
            event.add_attendee(follower, Role.FOLLOWER)
        
        event.balance_roles()
        
        assert len(event.leaders) == 2  # Balanced to match followers
        assert len(event.followers) == 2
        assert len(event.alt_leaders) == 2  # Excess leaders become alternates
    
    def test_balance_roles_demotes_excess_followers(self, event_factory, peep_factory):
        """Test that excess followers are demoted to alternates."""
        event = event_factory()
        leaders = [peep_factory(id=i+1, role=Role.LEADER) for i in range(2)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER) for i in range(5)]
        
        for leader in leaders:
            event.add_attendee(leader, Role.LEADER)
        for follower in followers:
            event.add_attendee(follower, Role.FOLLOWER)
        
        event.balance_roles()
        
        assert len(event.leaders) == 2
        assert len(event.followers) == 2  # Balanced to match leaders
        assert len(event.alt_followers) == 3  # Excess followers become alternates
    



class TestEventDurationManagement:
    """Test duration downgrade functionality."""
    
    def test_downgrade_duration_when_underfilled(self, event_factory, peep_factory):
        """Test that underfilled events can downgrade duration."""
        event = event_factory(duration_minutes=120)  # min_role = 6, max_role = 7, allow_downgrade = True
        
        # Add only 4 per role (below 120min minimum, exactly meets 90min requirement)
        for i in range(4):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        result = event.downgrade_duration()
        
        # Should succeed: 120min -> 90min (4 per role meets 90min requirements)
        assert result is True
        assert event.duration_minutes == 90
    
    def test_downgrade_duration_fails_when_no_downgrade_allowed(self, event_factory, peep_factory):
        """Test that downgrade fails when target duration doesn't allow downgrade."""
        event = event_factory(duration_minutes=90)  # min_role = 4, max_role = 5
        
        # Add only 2 per role (below 90min minimum, meets 60min requirement)
        # But 60min has allow_downgrade=False, so this should fail
        for i in range(2):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        result = event.downgrade_duration()
        
        # Should fail because 60min doesn't allow downgrade
        assert result is False
        assert event.duration_minutes == 90  # Unchanged
    
    def test_downgrade_duration_fails_when_no_valid_option(self, event_factory, peep_factory):
        """Test that downgrade fails when no valid duration exists."""
        event = event_factory(duration_minutes=120)
        
        # Add only 1 per role (below any valid minimum)
        event.add_attendee(peep_factory(id=1, role=Role.LEADER), Role.LEADER)
        event.add_attendee(peep_factory(id=2, role=Role.FOLLOWER), Role.FOLLOWER)
        
        result = event.downgrade_duration()
        
        assert result is False
        assert event.duration_minutes == 120  # Unchanged
    
    def test_downgrade_duration_raises_if_not_underfilled(self, event_factory, peep_factory):
        """Test that downgrade raises error if event meets current minimums."""
        event = event_factory(duration_minutes=120)  # min_role = 6
        
        # Add exactly the minimum required
        for i in range(6):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        with pytest.raises(RuntimeError, match="Cannot downgrade: event is not underfilled"):
            event.downgrade_duration()
    
    def test_downgrade_duration_raises_if_unbalanced(self, event_factory, peep_factory):
        """Test that downgrade raises error if roles are unbalanced."""
        event = event_factory(duration_minutes=120)
        
        # Add unbalanced attendees (4 leaders, 2 followers)
        for i in range(4):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
        for i in range(2):
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        # Don't balance - leave them unbalanced to trigger the error
        with pytest.raises(RuntimeError, match="Cannot downgrade unbalanced event"):
            event.downgrade_duration()


class TestEventConfiguration:
    """Test event configuration properties and class config integration."""
    
    def test_price_per_person_calculation(self, event_factory, peep_factory):
        """Test that price per person is calculated correctly."""
        event = event_factory(duration_minutes=120)  # $260 total
        
        # Add 6 attendees (3 leaders + 3 followers)
        for i in range(3):
            event.add_attendee(peep_factory(id=i+1, role=Role.LEADER), Role.LEADER)
            event.add_attendee(peep_factory(id=i+11, role=Role.FOLLOWER), Role.FOLLOWER)
        
        assert event.price_per_person == 43.0  # $260 / 6 people = $43.33, rounded to $43
    
    def test_invalid_duration_raises_error(self, event_factory, peep_factory):
        """Test that invalid durations raise ValueError."""
        with pytest.raises(ValueError, match="Unknown event duration"):
            Event(id=1, duration_minutes=45)  # Not in CLASS_CONFIG


class TestEventDataConversion:
    """Test event data conversion and serialization."""
    
    def test_to_dict_includes_core_fields(self, event_factory, peep_factory):
        """Test that to_dict includes essential event data."""
        date = datetime.datetime(2025, 3, 15, 19, 0)
        event = Event(id=42, date=date, duration_minutes=90)
        
        data = event.to_dict()
        
        assert data['id'] == 42
        assert data['date'] == date
        assert data['duration_minutes'] == 90
    
    def test_from_dict_recreates_event(self, event_factory, peep_factory):
        """Test that from_dict can recreate an event from dict data."""
        original_data = {
            'id': 123,
            'date': '2025-07-20 18:00',
            'duration_minutes': 120
        }
        
        event = Event.from_dict(original_data)
        
        assert event.id == 123
        assert event.date == datetime.datetime(2025, 7, 20, 18, 0)
        assert event.duration_minutes == 120


class TestEventAlternateValidation:
    """Test alternate validation and cleanup."""
    
    def test_validate_alternates_removes_ineligible(self, event_factory, peep_factory):
        """Test that validate_alternates removes alternates who can't attend."""
        event = event_factory(id=1)
        
        # Create peep who is available for this event
        eligible_peep = peep_factory(id=1, role=Role.LEADER, availability=[1])
        
        # Create peep who is NOT available for this event  
        ineligible_peep = peep_factory(id=2, role=Role.LEADER, availability=[2, 3])  # No event 1
        
        event.add_alternate(eligible_peep, Role.LEADER)
        event.add_alternate(ineligible_peep, Role.LEADER)
        
        event.validate_alternates()
        
        assert eligible_peep in event.alt_leaders
        assert ineligible_peep not in event.alt_leaders
    
    def test_validate_alternates_removes_over_limit_peeps(self, event_factory, peep_factory):
        """Test that alternates who exceed event limits are removed."""
        event = event_factory(id=1)
        
        # Create peep who has reached their event limit
        over_limit_peep = peep_factory(id=1, role=Role.LEADER, availability=[1], event_limit=2)
        over_limit_peep.num_events = 2  # At limit
        
        # Create peep who is under limit
        under_limit_peep = peep_factory(id=2, role=Role.LEADER, availability=[1], event_limit=2)
        under_limit_peep.num_events = 1  # Under limit
        
        event.add_alternate(over_limit_peep, Role.LEADER)
        event.add_alternate(under_limit_peep, Role.LEADER)
        
        event.validate_alternates()
        
        assert over_limit_peep not in event.alt_leaders
        assert under_limit_peep in event.alt_leaders


class TestEventPeepIntegration:
    """Test Event integration with Peep model, particularly post-event updates."""
    
    def test_update_event_attendees_updates_stats(self, event_factory, peep_factory):
        """Test that update_event_attendees correctly updates attendee statistics."""
        event = event_factory(id=1)
        
        # Create attendees with initial state
        leader = peep_factory(id=1, role=Role.LEADER, priority=3)
        follower = peep_factory(id=2, role=Role.FOLLOWER, priority=2)
        non_attendee = peep_factory(id=3, role=Role.LEADER, priority=1)
        
        # Set up initial state
        leader.num_events = 0
        follower.num_events = 0
        non_attendee.num_events = 0
        
        # Add attendees to event
        event.add_attendee(leader, Role.LEADER)
        event.add_attendee(follower, Role.FOLLOWER)
        
        # Create peeps list
        peeps = [leader, follower, non_attendee]
        
        # Update attendees
        Peep.update_event_attendees(peeps, event)
        
        # Verify attendee stats were updated
        assert leader.num_events == 1
        assert leader.assigned_event_dates == [event.date]
        assert leader.priority == 0  # Reset to 0
        
        assert follower.num_events == 1
        assert follower.assigned_event_dates == [event.date]
        assert follower.priority == 0  # Reset to 0
        
        # Verify non-attendee was not affected
        assert non_attendee.num_events == 0
        assert non_attendee.assigned_event_dates == []
        assert non_attendee.priority == 1  # Unchanged
    
    def test_update_event_attendees_reorders_list(self, event_factory, peep_factory):
        """Test that update_event_attendees moves attendees to back of list."""
        event = event_factory(id=1)
        
        # Create peeps in specific order
        peep1 = peep_factory(id=1, role=Role.LEADER, priority=3)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER, priority=2) 
        peep3 = peep_factory(id=3, role=Role.LEADER, priority=1)
        peep4 = peep_factory(id=4, role=Role.FOLLOWER, priority=0)
        
        peeps = [peep1, peep2, peep3, peep4]
        original_order = [p.id for p in peeps]
        
        # Add some attendees (not all)
        event.add_attendee(peep1, Role.LEADER)  # First peep
        event.add_attendee(peep3, Role.LEADER)  # Third peep
        
        # Update attendees
        Peep.update_event_attendees(peeps, event)
        
        # Verify list reordering: non-attendees first, then attendees
        new_order = [p.id for p in peeps]
        assert new_order == [2, 4, 1, 3]  # peep2, peep4, then peep1, peep3
        
        # Verify attendees are at the end
        assert peeps[-2:] == [peep1, peep3]
        
        # Verify non-attendees are at the beginning
        assert peeps[:2] == [peep2, peep4]
    
    def test_update_event_attendees_complex_scenario(self, event_factory, peep_factory):
        """Test update_event_attendees with complex multi-role scenario."""
        event = event_factory(id=1)
        
        # Create mixed leaders and followers
        leaders = [peep_factory(id=i+1, role=Role.LEADER, priority=i) for i in range(3)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, priority=i) for i in range(3)]
        
        # Mix them in the list
        peeps = [leaders[0], followers[0], leaders[1], followers[1], leaders[2], followers[2]]
        
        # Add some (but not all) to event
        event.add_attendee(leaders[0], Role.LEADER)    # id=1
        event.add_attendee(leaders[2], Role.LEADER)    # id=3  
        event.add_attendee(followers[1], Role.FOLLOWER) # id=12
        
        winners = [leaders[0], leaders[2], followers[1]]
        non_winners = [followers[0], leaders[1], followers[2]]
        
        # Update attendees
        Peep.update_event_attendees(peeps, event)
        
        # Verify all winners have updated stats
        for peep in winners:
            assert peep.num_events == 1
            assert peep.assigned_event_dates == [event.date] 
            assert peep.priority == 0
        
        # Verify non-winners unchanged
        for i, peep in enumerate(non_winners):
            assert peep.num_events == 0
            assert peep.assigned_event_dates == []
            # Priority should be unchanged from original
        
        # Verify list reordering: non-winners first, then winners
        final_ids = [p.id for p in peeps]
        non_winner_ids = [p.id for p in non_winners]
        winner_ids = [p.id for p in winners]
        
        # All non-winners should come before all winners
        assert final_ids[:len(non_winners)] == non_winner_ids
        assert final_ids[len(non_winners):] == winner_ids


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
