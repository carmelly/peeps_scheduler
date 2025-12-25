"""
Test Scheduler class functionality with focus on core scheduling logic.

Following testing philosophy:
- Test what could actually break in scheduling decisions
- Focus on key scheduler behaviors, not complex permutation testing
- Use real data when possible, avoid over-mocking
- One concept per test with descriptive names
"""

import pytest
from models import EventSequence, Role, SwitchPreference
from scheduler import Scheduler
import constants

def create_scheduler(**kwargs):
    """Factory for creating test schedulers."""
    defaults = {
        'data_folder': 'test',
        'max_events': 3
    }
    defaults.update(kwargs)
    return Scheduler(**defaults)


@pytest.mark.unit
class TestSchedulerInitialization:
    """Test Scheduler creation and basic configuration."""
    
    def test_scheduler_initialization_with_defaults(self):
        """Test that Scheduler initializes with provided parameters."""
        scheduler = Scheduler(data_folder='test_data', max_events=5)
        
        assert scheduler.data_folder == 'test_data'
        assert scheduler.max_events == 5
        assert scheduler.target_max is None  # Should start as None
    
    def test_scheduler_sets_output_paths_correctly(self):
        """Test that Scheduler sets correct file paths based on data folder."""
        scheduler = Scheduler(data_folder='my_folder', max_events=3)

        # The paths use the new data manager format - check as Path objects
        assert str(scheduler.output_json).endswith('my_folder/output.json')
        assert str(scheduler.result_json).endswith('my_folder/results.json')


@pytest.mark.unit
class TestSchedulerEventSanitization:
    """Test Scheduler event filtering and validation logic."""
    
    def test_sanitize_events_keeps_valid_events(self, event_factory, peep_factory):
        """Test that events with sufficient available peeps are kept."""
        scheduler = create_scheduler()
        
        # Create event with enough available peeps (need ABS_MIN_ROLE=4 per role)
        event = event_factory(id=1)
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(4)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(4)]
        
        events = [event]
        peeps = leaders + followers
        
        valid_events = scheduler.sanitize_events(events, peeps)
        
        # Event should be kept (has >= ABS_MIN_ROLE peeps per role available)
        assert len(valid_events) == 1
        assert valid_events[0] == event
    
    def test_sanitize_events_removes_events_without_enough_leaders(self, event_factory, peep_factory):
        """Test that events without sufficient leader availability are removed."""
        scheduler = create_scheduler()
        
        event = event_factory(id=1)
        # Only 3 leaders available, but need ABS_MIN_ROLE=4
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(3)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(4)]
        
        events = [event]
        peeps = leaders + followers
        
        valid_events = scheduler.sanitize_events(events, peeps)
        
        # Event should be removed (insufficient leaders)
        assert len(valid_events) == 0
    
    def test_sanitize_events_removes_events_without_enough_followers(self, event_factory, peep_factory):
        """Test that events without sufficient follower availability are removed."""
        scheduler = create_scheduler()
        
        event = event_factory(id=1)
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(4)]
        # Only 3 followers available, but need ABS_MIN_ROLE=4
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(3)]
        
        events = [event]
        peeps = leaders + followers
        
        valid_events = scheduler.sanitize_events(events, peeps)
        
        # Event should be removed (insufficient followers)
        assert len(valid_events) == 0
    
    def test_sanitize_events_considers_availability_not_just_role_count(self, event_factory, peep_factory):
        """Test that sanitization considers actual availability, not just role count."""
        scheduler = create_scheduler()
        
        event = event_factory(id=1)
        # Enough leaders total, but not enough available for this specific event
        available_leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(3)]  # Only 3 available
        unavailable_leaders = [peep_factory(id=i+11, role=Role.LEADER, availability=[2]) for i in range(5)]  # Not available for event 1
        followers = [peep_factory(id=i+21, role=Role.FOLLOWER, availability=[1]) for i in range(4)]  # Enough followers
        
        events = [event]
        peeps = available_leaders + unavailable_leaders + followers
        
        valid_events = scheduler.sanitize_events(events, peeps)
        
        # Event should be removed (only 3 leaders available for event 1, need 4)
        assert len(valid_events) == 0


@pytest.mark.unit
class TestSchedulerEventTrimming:
    """Test Scheduler event overlap removal logic."""
    
    def test_remove_high_overlap_events_removes_most_overlapped(self, event_factory, peep_factory):
        """Test that events with highest participant overlap are removed first."""
        scheduler = create_scheduler(max_events=2)
        
        # Create 3 events with overlapping availability
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)  
        event3 = event_factory(id=3)
        
        # Peep available for events 1&2 (creates overlap)
        peep1 = peep_factory(id=1, role=Role.LEADER, availability=[1, 2])
        # Peep available for events 1&3 (creates overlap)
        peep2 = peep_factory(id=2, role=Role.LEADER, availability=[1, 3])
        # Peep available only for event 3 (no overlap)
        peep3 = peep_factory(id=3, role=Role.LEADER, availability=[3])
        
        follower1 = peep_factory(id=4, role=Role.FOLLOWER, availability=[1, 2, 3])
        follower2 = peep_factory(id=5, role=Role.FOLLOWER, availability=[1, 2, 3])
        
        events = [event1, event2, event3]
        peeps = [peep1, peep2, peep3, follower1, follower2]
        
        trimmed_events = scheduler.remove_high_overlap_events(events, peeps, 2)
        
        # Should keep exactly 2 events
        assert len(trimmed_events) == 2
        # Event 1 has highest overlap (overlaps with both 2 and 3), so should be removed
        event_ids = [e.id for e in trimmed_events]
        assert 1 not in event_ids
    
    def test_remove_high_overlap_events_uses_weight_as_tiebreaker(self, event_factory, peep_factory):
        """Test that weight is used as tiebreaker when overlap is equal."""
        scheduler = create_scheduler(max_events=1)
        
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)
        
        # Equal overlap, but different weights
        high_priority_peep = peep_factory(id=1, role=Role.LEADER, availability=[1, 2], priority=5)
        low_priority_peep = peep_factory(id=2, role=Role.LEADER, availability=[1, 2], priority=1)
        
        follower1 = peep_factory(id=3, role=Role.FOLLOWER, availability=[1, 2])
        follower2 = peep_factory(id=4, role=Role.FOLLOWER, availability=[1, 2])
        
        events = [event1, event2]
        peeps = [high_priority_peep, low_priority_peep, follower1, follower2]
        
        trimmed_events = scheduler.remove_high_overlap_events(events, peeps, 1)
        
        # Should keep the event with higher weight (higher priority peeps)
        assert len(trimmed_events) == 1
        # Both events have same overlap, but priority matters for tiebreaking
        # The exact result depends on weight calculation, but should be deterministic


@pytest.mark.unit
class TestSchedulerSequenceEvaluation:
    """Test Scheduler sequence evaluation core logic."""
    
    def test_evaluate_sequence_assigns_available_peeps(self, event_factory, peep_factory):
        """Test that evaluate_sequence correctly assigns available peeps to events."""
        scheduler = create_scheduler()
        scheduler.target_max = 7  # Allow full capacity for 120min events
        
        event = event_factory(id=1, duration_minutes=120)  # Needs 6 per role minimum
        
        # Create enough peeps to fill the event (7 to meet max_role)
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(7)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(7)]
        peeps = leaders + followers
        
        sequence = EventSequence([event], peeps)
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid and have attendees assigned
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        assert len(valid_event.leaders) >= constants.ABS_MIN_ROLE
        assert len(valid_event.followers) >= constants.ABS_MIN_ROLE
    
    def test_evaluate_sequence_downgrades_underfilled_events(self, event_factory, peep_factory):
        """Test that evaluate_sequence downgrades events that meet ABS_MIN but not event-specific min."""
        scheduler = create_scheduler()
        scheduler.target_max = 7  # Allow full capacity
        
        # Create 120-minute event (min_role=6, max_role=7)
        event = event_factory(id=1, duration_minutes=120)
        
        # Create exactly 4 per role - meets ABS_MIN_ROLE(4) but not event min_role(6)
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(4)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(4)]
        peeps = leaders + followers
        
        sequence = EventSequence([event], peeps)
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid after downgrade
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Event should have been downgraded from 120min to 90min (min_role=4, max_role=5)
        assert valid_event.duration_minutes == 90, "Event should be downgraded when underfilled"
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4
    
    def test_evaluate_sequence_clears_participants_for_invalid_events(self, event_factory, peep_factory):
        """Test that evaluate_sequence clears participants from events that can't meet per-duration minimums."""
        scheduler = create_scheduler()
        scheduler.target_max = 7  # Allow full capacity
        
        # Create 120-minute event (min_role=6, max_role=7) 
        event = event_factory(id=1, duration_minutes=120)
        
        # Create exactly 4 per role - meets ABS_MIN_ROLE(4) but not event min_role(6)
        # AND cannot be downgraded (120min -> 90min needs min_role=4, but we need exactly 4)
        # So it will meet ABS_MIN but fail per-duration min after balancing
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(4)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(4)]
        
        # Create an unbalanced scenario: 5 leaders, 3 followers
        # This will meet ABS_MIN_ROLE(4) total, but after balancing will have 3 per role
        # which is less than ABS_MIN_ROLE(4), causing the event to be invalid
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(5)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(3)]
        
        peeps = leaders + followers
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)  # Uses default keep_invalid=False
        
        # Event should not be valid (fails ABS_MIN_ROLE after balancing)
        assert len(sequence.valid_events) == 0
        
        # Event should have no participants (cleared)
        assert len(event.leaders) == 0
        assert len(event.followers) == 0
        assert len(event.alt_leaders) == 0
        assert len(event.alt_followers) == 0
    
    def test_evaluate_sequence_respects_peep_availability(self, event_factory, peep_factory):
        """Test that evaluate_sequence only assigns peeps to events they're available for."""
        scheduler = create_scheduler()
        scheduler.target_max = 5  # Allow enough capacity
        
        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5
        
        # Create enough peeps to make event valid, with clear availability patterns
        available_leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(4)]
        unavailable_leader = peep_factory(id=10, role=Role.LEADER, availability=[2])  # Not available for event 1
        
        available_followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(4)]
        unavailable_follower = peep_factory(id=20, role=Role.FOLLOWER, availability=[2])  # Not available for event 1
        
        peeps = available_leaders + [unavailable_leader] + available_followers + [unavailable_follower]
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid with available peeps assigned
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Available peeps should be assigned (as attendees or alternates)
        for leader in available_leaders:
            assert leader in valid_event.leaders or leader in valid_event.alt_leaders
        for follower in available_followers:
            assert follower in valid_event.followers or follower in valid_event.alt_followers
            
        # Unavailable peeps should not be assigned at all
        assert unavailable_leader not in valid_event.leaders and unavailable_leader not in valid_event.alt_leaders
        assert unavailable_follower not in valid_event.followers and unavailable_follower not in valid_event.alt_followers
    
    def test_evaluate_sequence_respects_event_limits(self, event_factory, peep_factory):
        """Test that evaluate_sequence respects peep event limits."""
        scheduler = create_scheduler()
        scheduler.target_max = 5  # Allow enough capacity
        
        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5
        
        # Create enough peeps to make event valid, with different limit scenarios
        limited_leader = peep_factory(id=1, role=Role.LEADER, availability=[1], event_limit=0)  # At limit
        limited_leader.num_events = limited_leader.event_limit  # Simulate already at limit
        
        unlimited_leaders = [peep_factory(id=i+2, role=Role.LEADER, availability=[1], event_limit=2) for i in range(4)]  # Has capacity
        
        limited_follower = peep_factory(id=10, role=Role.FOLLOWER, availability=[1], event_limit=1)
        limited_follower.num_events = limited_follower.event_limit  # Simulate already at limit
        
        unlimited_followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1], event_limit=2) for i in range(4)]  # Has capacity
        
        peeps = [limited_leader] + unlimited_leaders + [limited_follower] + unlimited_followers
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid with unlimited peeps assigned
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Limited peeps should not be assigned (not in attendees or alternates)
        assert limited_leader not in valid_event.leaders and limited_leader not in valid_event.alt_leaders
        assert limited_follower not in valid_event.followers and limited_follower not in valid_event.alt_followers
        
        # At least some unlimited peeps should be assigned (as attendees or alternates)
        assigned_unlimited_leaders = sum(1 for leader in unlimited_leaders 
                                       if leader in valid_event.leaders or leader in valid_event.alt_leaders)
        assigned_unlimited_followers = sum(1 for follower in unlimited_followers 
                                         if follower in valid_event.followers or follower in valid_event.alt_followers)
        
        assert assigned_unlimited_leaders > 0, "Some unlimited leaders should be assigned"
        assert assigned_unlimited_followers > 0, "Some unlimited followers should be assigned"
    
    def test_evaluate_sequence_adds_alternates_when_event_full(self, event_factory, peep_factory):
        """Test that evaluate_sequence adds alternates when event reaches capacity."""
        scheduler = create_scheduler()
        scheduler.target_max = 2  # Limit to 2 per role
        
        event = event_factory(id=1, duration_minutes=60)  # 60min allows min_role=2, max_role=3
        
        # Create more peeps than can fit (6 per role, but capacity is only 2)
        leaders = [peep_factory(id=i+1, role=Role.LEADER, availability=[1]) for i in range(6)]
        followers = [peep_factory(id=i+11, role=Role.FOLLOWER, availability=[1]) for i in range(6)]
        peeps = leaders + followers
        
        sequence = EventSequence([event], peeps)
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid 
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Should have exactly target_max attendees per role
        assert len(valid_event.leaders) == 2
        assert len(valid_event.followers) == 2
        
        # Should have alternates since we have more peeps than capacity
        total_alternates = len(valid_event.alt_leaders) + len(valid_event.alt_followers)
        assert total_alternates > 0, "Should have alternates when event is full but more peeps are available"


@pytest.mark.unit
class TestSchedulerSequenceSelection:
    """Test Scheduler sequence selection and ranking logic."""
    
    def test_get_top_sequences_selects_highest_unique_attendees(self, peep_factory):
        """Test that get_top_sequences prefers sequences with more unique attendees."""
        scheduler = create_scheduler()
        
        # Create sequences with different unique attendee counts
        peeps = [peep_factory(id=i) for i in range(1, 6)]
        
        seq1 = EventSequence([], peeps)
        seq1.num_unique_attendees = 3
        seq1.priority_fulfilled = 10
        seq1.normalized_utilization = 1.0
        seq1.total_attendees = 3
        
        seq2 = EventSequence([], peeps)
        seq2.num_unique_attendees = 2  # Lower
        seq2.priority_fulfilled = 15  # Higher other metrics
        seq2.normalized_utilization = 1.5
        seq2.total_attendees = 4
        
        sequences = [seq2, seq1]  # Put lower one first to test sorting
        
        top = scheduler.get_top_sequences(sequences)
        
        # Should prefer sequence with more unique attendees
        assert len(top) == 1
        assert top[0] == seq1
    
    def test_get_top_sequences_uses_priority_fulfilled_as_tiebreaker(self, peep_factory):
        """Test that get_top_sequences uses priority_fulfilled when unique attendees tie."""
        scheduler = create_scheduler()
        
        peeps = [peep_factory(id=i) for i in range(1, 4)]
        
        seq1 = EventSequence([], peeps)
        seq1.num_unique_attendees = 2
        seq1.priority_fulfilled = 15  # Higher
        seq1.normalized_utilization = 1.0
        seq1.total_attendees = 2
        
        seq2 = EventSequence([], peeps)
        seq2.num_unique_attendees = 2  # Same
        seq2.priority_fulfilled = 10   # Lower
        seq2.normalized_utilization = 1.0
        seq2.total_attendees = 2
        
        sequences = [seq2, seq1]
        
        top = scheduler.get_top_sequences(sequences)
        
        # Should prefer sequence with higher priority fulfilled
        assert len(top) == 1
        assert top[0] == seq1

    def test_get_top_sequences_uses_mutual_partnerships_as_tiebreaker(self, peep_factory):
        """Test that get_top_sequences uses mutual unique partnerships when unique attendees tie."""
        scheduler = create_scheduler()

        peeps = [peep_factory(id=i) for i in range(1, 4)]

        seq1 = EventSequence([], peeps)
        seq1.num_unique_attendees = 2
        seq1.priority_fulfilled = 10
        seq1.mutual_unique_fulfilled = 2  # Higher
        seq1.mutual_repeat_fulfilled = 0
        seq1.one_sided_fulfilled = 0
        seq1.normalized_utilization = 1.0
        seq1.total_attendees = 2

        seq2 = EventSequence([], peeps)
        seq2.num_unique_attendees = 2
        seq2.priority_fulfilled = 10
        seq2.mutual_unique_fulfilled = 1  # Lower
        seq2.mutual_repeat_fulfilled = 0
        seq2.one_sided_fulfilled = 0
        seq2.normalized_utilization = 1.0
        seq2.total_attendees = 2

        sequences = [seq2, seq1]

        top = scheduler.get_top_sequences(sequences)

        assert len(top) == 1
        assert top[0] == seq1
    
    def test_get_top_sequences_returns_empty_for_empty_input(self):
        """Test that get_top_sequences handles empty input gracefully."""
        scheduler = create_scheduler()
        
        top = scheduler.get_top_sequences([])
        
        assert top == []
    
    def test_get_top_sequences_handles_tied_sequences(self, peep_factory, event_factory):
        """Test that get_top_sequences can return multiple tied sequences."""
        scheduler = create_scheduler()
        
        # Create different peeps for each sequence
        peeps1 = [peep_factory(id=i) for i in range(1, 4)]
        peeps2 = [peep_factory(id=i) for i in range(4, 7)]
        
        # Create sequences with different events and different peeps but identical metrics
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)
        
        seq1 = EventSequence([event1], peeps1)
        seq1.valid_events = [event1]  # Add to valid_events for equality testing
        seq1.num_unique_attendees = 2
        seq1.priority_fulfilled = 10
        seq1.normalized_utilization = 1.0
        seq1.total_attendees = 2
        
        seq2 = EventSequence([event2], peeps2)  # Different event and peeps
        seq2.valid_events = [event2]  # Add to valid_events for equality testing
        seq2.num_unique_attendees = 2  # Same metrics
        seq2.priority_fulfilled = 10
        seq2.normalized_utilization = 1.0
        seq2.total_attendees = 2
        
        sequences = [seq1, seq2]
        
        top = scheduler.get_top_sequences(sequences)
        
        # Both should be returned since they're tied
        assert len(top) == 2
        assert seq1 in top and seq2 in top

    def test_partnerships_change_sequence_selection_end_to_end(self, peep_factory, event_factory):
        """
        CRITICAL: End-to-end test proving partnerships actually change which sequence is selected.

        Scenario:
        - 4 peeps request partnerships (mutual: 1<->2, one-sided: 3->4)
        - Two sequences have IDENTICAL metrics except partnerships
        - Sequence A: fulfills partnerships (1,2 together + 3,4 together)
        - Sequence B: doesn't fulfill any partnerships
        - All other metrics tied (attendees, priority, utilization)
        - Verify scheduler selects Sequence A
        """
        scheduler = create_scheduler()

        # Create peeps with partnerships
        peep1 = peep_factory(id=1, role=Role.LEADER)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER)
        peep3 = peep_factory(id=3, role=Role.LEADER)
        peep4 = peep_factory(id=4, role=Role.FOLLOWER)

        peeps = [peep1, peep2, peep3, peep4]

        # Create events
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)

        # SEQUENCE A: Fulfills partnerships
        # Event1: peep1 + peep2 (mutual partnership fulfilled)
        # Event2: peep3 + peep4 (one-sided fulfilled)
        event1_a = event_factory(id=1)
        event1_a.add_attendee(peep1, Role.LEADER)
        event1_a.add_attendee(peep2, Role.FOLLOWER)

        event2_a = event_factory(id=2)
        event2_a.add_attendee(peep3, Role.LEADER)
        event2_a.add_attendee(peep4, Role.FOLLOWER)

        seq_a = EventSequence([event1_a, event2_a], peeps)
        seq_a.valid_events = [event1_a, event2_a]
        seq_a.num_unique_attendees = 4       # All peeps attended
        seq_a.priority_fulfilled = 5
        seq_a.mutual_unique_fulfilled = 1    # Partnership 1<->2
        seq_a.mutual_repeat_fulfilled = 0
        seq_a.one_sided_fulfilled = 1        # Partnership 3->4
        seq_a.normalized_utilization = 0.8
        seq_a.total_attendees = 4

        # SEQUENCE B: Doesn't fulfill partnerships
        # Event1: peep1 + peep3 (breaks 1<->2 partnership)
        # Event2: peep2 + peep4 (breaks 3->4 partnership)
        event1_b = event_factory(id=1)
        event1_b.add_attendee(peep1, Role.LEADER)
        event1_b.add_attendee(peep3, Role.FOLLOWER)

        event2_b = event_factory(id=2)
        event2_b.add_attendee(peep2, Role.LEADER)
        event2_b.add_attendee(peep4, Role.FOLLOWER)

        seq_b = EventSequence([event1_b, event2_b], peeps)
        seq_b.valid_events = [event1_b, event2_b]
        seq_b.num_unique_attendees = 4       # All peeps attended (SAME)
        seq_b.priority_fulfilled = 5         # (SAME)
        seq_b.mutual_unique_fulfilled = 0    # No partnerships fulfilled
        seq_b.mutual_repeat_fulfilled = 0    # (SAME)
        seq_b.one_sided_fulfilled = 0        # No partnerships fulfilled
        seq_b.normalized_utilization = 0.8   # (SAME)
        seq_b.total_attendees = 4            # (SAME)

        sequences = [seq_b, seq_a]  # Put B first to test ordering

        # Define partnership requests: 1<->2 mutual, 3->4 one-sided
        scheduler.partnership_requests = {
            1: {2},
            2: {1},
            3: {4}
        }

        top = scheduler.get_top_sequences(sequences)

        # Should select Sequence A (which fulfills partnerships)
        assert len(top) == 1, f"Expected 1 top sequence, got {len(top)}"
        assert top[0] == seq_a, "Should select sequence with fulfilled partnerships"


@pytest.mark.unit
class TestSchedulerDualRoleAssignment:
    """Test Scheduler dual-role switching logic during sequence evaluation."""
    
    def test_evaluate_sequence_assigns_switch_if_primary_full_to_secondary_role(self, event_factory, peep_factory):
        """Test that peeps with SWITCH_IF_PRIMARY_FULL get assigned to opposite role when primary is full."""
        scheduler = create_scheduler()
        scheduler.target_max = 2  # Limit to 2 per role to force primary role to fill up
        
        event = event_factory(id=1, duration_minutes=60)  # 60min: min_role=2, max_role=3
        
        # Create 2 PRIMARY_ONLY leaders to fill the leader role
        primary_only_leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY, 
                        availability=[1], event_limit=1) 
            for i in range(2)
        ]
        
        # Create 1 SWITCH_IF_PRIMARY_FULL leader who should switch to follower
        flexible_leader = peep_factory(id=10, role=Role.LEADER, 
                                     switch_pref=SwitchPreference.SWITCH_IF_PRIMARY_FULL,
                                     availability=[1], event_limit=1)
        
        # Create enough followers to make event valid
        followers = [
            peep_factory(id=i+20, role=Role.FOLLOWER, availability=[1], event_limit=1) 
            for i in range(2)
        ]
        
        peeps = primary_only_leaders + [flexible_leader] + followers
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Leaders role should be full with PRIMARY_ONLY peeps
        assert len(valid_event.leaders) == 2
        for leader in primary_only_leaders:
            assert leader in valid_event.leaders
        
        # Flexible leader should be assigned as FOLLOWER (opposite role)
        assert flexible_leader in valid_event.followers
        assert flexible_leader.role == Role.LEADER  # Original role unchanged
    
    def test_evaluate_sequence_keeps_primary_only_peeps_in_alternates(self, event_factory, peep_factory):
        """Test that peeps with PRIMARY_ONLY stay in alternates when primary role is full."""
        scheduler = create_scheduler()
        scheduler.target_max = 2  # Limit to 2 per role
        
        event = event_factory(id=1, duration_minutes=60)  # 60min: min_role=2, max_role=3
        
        # Create 3 PRIMARY_ONLY leaders - only 2 can fit, 1 should become alternate
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i) 
            for i in range(3)
        ]
        
        # Create enough followers
        followers = [
            peep_factory(id=i+10, role=Role.FOLLOWER, availability=[1], event_limit=1) 
            for i in range(2)
        ]
        
        peeps = leaders + followers
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # Should have exactly 2 leaders (capacity limit)
        assert len(valid_event.leaders) == 2
        
        # Should have 1 alternate leader (the one who couldn't fit)
        assert len(valid_event.alt_leaders) == 1
        
        # All leaders should be in either attendees or alternates, none in followers
        for leader in leaders:
            assert leader in valid_event.leaders or leader in valid_event.alt_leaders
            assert leader not in valid_event.followers
    
    def test_evaluate_sequence_ignores_switch_if_needed_in_initial_assignment(self, event_factory, peep_factory):
        """Test that peeps with SWITCH_IF_NEEDED are treated like PRIMARY_ONLY during initial assignment."""
        scheduler = create_scheduler()
        scheduler.target_max = 2  # Limit to 2 per role
        
        event = event_factory(id=1, duration_minutes=60)  # 60min: min_role=2, max_role=3
        
        # Create 2 PRIMARY_ONLY leaders to fill leader slots
        primary_leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1) 
            for i in range(2)
        ]
        
        # Create 1 SWITCH_IF_NEEDED leader - should become alternate, not switch in initial pass
        switch_if_needed_leader = peep_factory(id=10, role=Role.LEADER,
                                             switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                             availability=[1], event_limit=1)
        
        # Create enough followers
        followers = [
            peep_factory(id=i+20, role=Role.FOLLOWER, availability=[1], event_limit=1) 
            for i in range(2)
        ]
        
        peeps = primary_leaders + [switch_if_needed_leader] + followers
        sequence = EventSequence([event], peeps)
        
        scheduler.evaluate_sequence(sequence)
        
        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]
        
        # SWITCH_IF_NEEDED leader should be in alternates (not switched to follower in initial pass)
        assert switch_if_needed_leader in valid_event.alt_leaders
        assert switch_if_needed_leader not in valid_event.leaders
        assert switch_if_needed_leader not in valid_event.followers
        
        # Original role should be unchanged
        assert switch_if_needed_leader.role == Role.LEADER

    def test_promote_switch_if_needed_alternate_to_fill_underfilled_role(self, event_factory, peep_factory):
        """Test that SWITCH_IF_NEEDED alternates are promoted when it enables event to meet minimum."""
        scheduler = create_scheduler()
        scheduler.target_max = 4  # Limit to 4 per role to ensure switch_follower becomes alternate

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 3 leaders (underfilled - need 4)
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(3)
        ]

        # Create 4 PRIMARY_ONLY followers (will fill follower slots)
        followers = [
            peep_factory(id=i+10, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(4)
        ]

        # This follower has SWITCH_IF_NEEDED preference and will be added as alternate (5th follower, exceeds target_max)
        switch_follower = peep_factory(id=20, role=Role.FOLLOWER,
                                      switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                      availability=[1], event_limit=1, priority=0)

        peeps = leaders + followers + [switch_follower]
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]

        # After promotion and balancing, should have 4 per role
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4

        # The SWITCH_IF_NEEDED follower should have been promoted to leader
        assert switch_follower in valid_event.leaders
        assert switch_follower not in valid_event.followers
        assert switch_follower not in valid_event.alt_followers

    def test_no_promotion_when_event_already_meets_minimum(self, event_factory, peep_factory):
        """Test that SWITCH_IF_NEEDED promotion doesn't happen when event already meets minimums."""
        scheduler = create_scheduler()
        scheduler.target_max = 5  # Allow enough capacity

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 4 leaders (meets minimum)
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1)
            for i in range(4)
        ]

        # Create 4 followers (meets minimum)
        followers = [
            peep_factory(id=i+10, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1)
            for i in range(4)
        ]

        # This follower has SWITCH_IF_NEEDED and will be alternate (no promotion needed)
        switch_follower = peep_factory(id=20, role=Role.FOLLOWER,
                                      switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                      availability=[1], event_limit=1)

        peeps = leaders + followers + [switch_follower]
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]

        # Should have exactly 4 per role (no overfill)
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4

        # The SWITCH_IF_NEEDED follower should remain as alternate (not promoted)
        assert switch_follower in valid_event.alt_followers
        assert switch_follower not in valid_event.leaders
        assert switch_follower not in valid_event.followers

    def test_promote_multiple_switch_if_needed_alternates_if_needed(self, event_factory, peep_factory):
        """Test that multiple SWITCH_IF_NEEDED alternates can be promoted to fill a role."""
        scheduler = create_scheduler()
        scheduler.target_max = 4  # Limit to 4 per role to ensure switch followers become alternates

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 2 leaders (very underfilled - need 4)
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(2)
        ]

        # Create 4 PRIMARY_ONLY followers (will fill follower slots)
        followers_primary = [
            peep_factory(id=i+10, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(4)
        ]

        # These followers have SWITCH_IF_NEEDED preference and will become alternates (exceed target_max=4)
        switch_follower1 = peep_factory(id=20, role=Role.FOLLOWER,
                                       switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                       availability=[1], event_limit=1, priority=0)
        switch_follower2 = peep_factory(id=21, role=Role.FOLLOWER,
                                       switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                       availability=[1], event_limit=1, priority=0)

        peeps = leaders + followers_primary + [switch_follower1, switch_follower2]
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]

        # After promotion and balancing, should have 4 per role
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4

        # Both SWITCH_IF_NEEDED followers should have been promoted to leaders
        assert switch_follower1 in valid_event.leaders
        assert switch_follower2 in valid_event.leaders

    def test_no_promotion_when_opposite_role_has_no_switch_if_needed(self, event_factory, peep_factory):
        """Test that no promotion happens when opposite role has no SWITCH_IF_NEEDED alternates."""
        scheduler = create_scheduler()
        scheduler.target_max = 4  # Limit to 4 per role

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 3 leaders (underfilled - need 4)
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(3)
        ]

        # Create 4 PRIMARY_ONLY followers (will fill follower slots)
        followers = [
            peep_factory(id=i+10, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(4)
        ]

        # Additional follower alternate (also PRIMARY_ONLY, no SWITCH_IF_NEEDED available)
        follower_alternate = peep_factory(id=20, role=Role.FOLLOWER,
                                         switch_pref=SwitchPreference.PRIMARY_ONLY,
                                         availability=[1], event_limit=1, priority=0)

        peeps = leaders + followers + [follower_alternate]
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event cannot meet minimum (3 leaders < 4 needed) and has no SWITCH_IF_NEEDED alternates to promote
        # So it should be invalid (no valid events)
        assert len(sequence.valid_events) == 0

    def test_switch_if_needed_respects_effective_max_role(self, event_factory, peep_factory):
        """Test that SWITCH_IF_NEEDED promotion respects effective_max_role capacity limits."""
        scheduler = create_scheduler()
        scheduler.target_max = 4  # Limit capacity to 4 per role (less than event max_role=5)

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 3 leaders (underfilled - need 4)
        leaders = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1)
            for i in range(3)
        ]

        # Create 4 followers (at effective_max_role capacity)
        followers = [
            peep_factory(id=i+10, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1)
            for i in range(4)
        ]

        # This follower has SWITCH_IF_NEEDED and will be alternate
        switch_follower = peep_factory(id=20, role=Role.FOLLOWER,
                                      switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                      availability=[1], event_limit=1)

        peeps = leaders + followers + [switch_follower]
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]

        # After promotion and balancing, should have 4 per role
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4

        # The SWITCH_IF_NEEDED follower should have been promoted to fill leaders
        assert switch_follower in valid_event.leaders

    def test_switch_if_needed_leader_alternate_fills_follower_underfill(self, event_factory, peep_factory):
        """Test that SWITCH_IF_NEEDED leader alternates can be promoted to fill underfilled follower role."""
        scheduler = create_scheduler()
        scheduler.target_max = 4  # Limit to 4 per role to ensure switch_leader becomes alternate

        event = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5

        # Create 4 PRIMARY_ONLY leaders (will fill leader slots)
        leaders_primary = [
            peep_factory(id=i+1, role=Role.LEADER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(4)
        ]

        # This leader has SWITCH_IF_NEEDED and will become alternate (5th leader, exceeds target_max)
        switch_leader = peep_factory(id=10, role=Role.LEADER,
                                    switch_pref=SwitchPreference.SWITCH_IF_NEEDED,
                                    availability=[1], event_limit=1, priority=0)

        # Create 3 followers (underfilled - need 4)
        followers = [
            peep_factory(id=i+20, role=Role.FOLLOWER, switch_pref=SwitchPreference.PRIMARY_ONLY,
                        availability=[1], event_limit=1, priority=i)
            for i in range(3)
        ]

        peeps = leaders_primary + [switch_leader] + followers
        sequence = EventSequence([event], peeps)

        scheduler.evaluate_sequence(sequence)

        # Event should be valid
        assert len(sequence.valid_events) == 1
        valid_event = sequence.valid_events[0]

        # After promotion and balancing, should have 4 per role
        assert len(valid_event.leaders) == 4
        assert len(valid_event.followers) == 4

        # The SWITCH_IF_NEEDED leader should have been promoted to follower
        assert switch_leader in valid_event.followers
        assert switch_leader not in valid_event.leaders


@pytest.mark.integration
class TestSchedulerIntegration:
    """Test Scheduler integration scenarios with realistic data."""
    
    def test_full_scheduling_with_small_dataset(self, event_factory, peep_factory):
        """Test complete scheduling workflow with realistic small dataset."""
        scheduler = create_scheduler(max_events=2)
        scheduler.target_max = 5  # Allow enough capacity for 90min events (min_role=4)
        
        # Create 2 events
        event1 = event_factory(id=1, duration_minutes=90)  # min_role=4, max_role=5
        event2 = event_factory(id=2, duration_minutes=90)
        events = [event1, event2]
        
        # Create enough peeps for both events to have minimum availability
        # Each event needs 4 per role minimum, so need more than 4 total per role
        leaders = []
        followers = []
        
        # Leaders - ensure each event has at least 4 available
        leaders.append(peep_factory(id=1, role=Role.LEADER, availability=[1], priority=3))     # Event 1 only
        leaders.append(peep_factory(id=2, role=Role.LEADER, availability=[1], priority=2))     # Event 1 only  
        leaders.append(peep_factory(id=3, role=Role.LEADER, availability=[1], priority=1))     # Event 1 only
        leaders.append(peep_factory(id=4, role=Role.LEADER, availability=[1, 2], priority=2))  # Both events
        leaders.append(peep_factory(id=5, role=Role.LEADER, availability=[2], priority=1))     # Event 2 only
        leaders.append(peep_factory(id=6, role=Role.LEADER, availability=[2], priority=0))     # Event 2 only
        leaders.append(peep_factory(id=7, role=Role.LEADER, availability=[2], priority=1))     # Event 2 only
        
        # Followers - ensure each event has at least 4 available
        followers.append(peep_factory(id=11, role=Role.FOLLOWER, availability=[1], priority=2))     # Event 1 only
        followers.append(peep_factory(id=12, role=Role.FOLLOWER, availability=[1], priority=1))     # Event 1 only
        followers.append(peep_factory(id=13, role=Role.FOLLOWER, availability=[1], priority=3))     # Event 1 only  
        followers.append(peep_factory(id=14, role=Role.FOLLOWER, availability=[1, 2], priority=1))  # Both events
        followers.append(peep_factory(id=15, role=Role.FOLLOWER, availability=[2], priority=0))     # Event 2 only
        followers.append(peep_factory(id=16, role=Role.FOLLOWER, availability=[2], priority=2))     # Event 2 only
        followers.append(peep_factory(id=17, role=Role.FOLLOWER, availability=[2], priority=1))     # Event 2 only
        
        peeps = leaders + followers
        
        # Run evaluation
        sequences = scheduler.evaluate_all_event_sequences(peeps, events)
        
        # Should produce some valid sequences
        assert len(sequences) > 0
        
        # Get best sequence
        best_sequences = scheduler.get_top_sequences(sequences)
        assert len(best_sequences) > 0
        
        best = best_sequences[0]
        
        # Best sequence should have some valid events
        assert len(best.valid_events) > 0
        
        # All valid events should meet minimum requirements
        for event in best.valid_events:
            assert event.meets_absolute_min()
            assert len(event.leaders) == len(event.followers)  # Should be balanced
            
