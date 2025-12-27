"""
Test EventSequence class functionality with focus on core behavior.

Following testing philosophy:
- Test what could actually break
- One concept per test with descriptive names
- Focus on scheduler-critical functionality (equality, finalization, metrics)
- Skip complex scheduling integration scenarios
"""

import pytest
import datetime
from peeps_scheduler.models import EventSequence, Event, Peep, Role



@pytest.mark.unit
class TestEventSequenceInitialization:
    """Test EventSequence creation and initial state."""
    
    def test_initialization_with_empty_lists(self):
        """Test that EventSequence can be created with empty lists."""
        sequence = EventSequence([], [])
        
        assert sequence.events == []
        assert sequence.peeps == []
        assert sequence.valid_events == []
        assert sequence.num_unique_attendees == 0
        assert sequence.total_attendees == 0
        assert sequence.priority_fulfilled == 0
        assert sequence.partnerships_fulfilled == 0
        assert sequence.mutual_unique_fulfilled == 0
        assert sequence.mutual_repeat_fulfilled == 0
        assert sequence.one_sided_fulfilled == 0
    
    def test_initialization_with_events_and_peeps(self, event_factory, peep_factory):
        """Test that EventSequence stores provided events and peeps."""
        events = [event_factory(id=1), event_factory(id=2)]
        peeps = [peep_factory(id=1), peep_factory(id=2)]
        
        sequence = EventSequence(events, peeps)
        
        assert sequence.events == events
        assert sequence.peeps == peeps
        assert sequence.valid_events == []  # Should start empty
    
    def test_initialization_metrics_start_at_zero(self, event_factory, peep_factory):
        """Test that all metrics start at zero regardless of input."""
        events = [event_factory(id=1)]
        peeps = [peep_factory(id=1, priority=5, total_attended=3)]
        
        sequence = EventSequence(events, peeps)
        
        # All metrics should start at zero
        assert sequence.num_unique_attendees == 0
        assert sequence.total_attendees == 0
        assert sequence.system_weight == 0
        assert sequence.priority_fulfilled == 0
        assert sequence.partnerships_fulfilled == 0
        assert sequence.mutual_unique_fulfilled == 0
        assert sequence.mutual_repeat_fulfilled == 0
        assert sequence.one_sided_fulfilled == 0
        assert sequence.normalized_utilization == 0


@pytest.mark.unit
class TestEventSequenceEquality:
    """Test EventSequence equality and hashing for scheduler deduplication."""
    
    def test_empty_sequences_are_equal(self):
        """Test that empty sequences are equal and have same hash."""
        seq1 = EventSequence([], [])
        seq2 = EventSequence([], [])
        
        assert seq1 == seq2
        assert hash(seq1) == hash(seq2)
    
    def test_sequences_with_same_valid_events_are_equal(self, event_factory, peep_factory):
        """Test that sequences with identical valid events are equal."""
        events = [event_factory(id=1)]
        peeps = [peep_factory(id=1), peep_factory(id=2)]
        
        # Create two sequences
        seq1 = EventSequence(events, peeps)
        seq2 = EventSequence(events, peeps)
        
        # Add same event with same attendees to both
        event1 = event_factory(id=1)
        event2 = event_factory(id=1) 
        
        peep1 = peep_factory(id=1)
        peep2 = peep_factory(id=2)
        
        event1.add_attendee(peep1, Role.LEADER)
        event1.add_attendee(peep2, Role.FOLLOWER)
        
        event2.add_attendee(peep1, Role.LEADER)
        event2.add_attendee(peep2, Role.FOLLOWER)
        
        seq1.valid_events = [event1]
        seq2.valid_events = [event2]
        
        assert seq1 == seq2
        assert hash(seq1) == hash(seq2)
    
    def test_sequences_with_different_attendees_are_not_equal(self, event_factory, peep_factory):
        """Test that sequences with different attendee assignments are not equal."""
        events = [event_factory(id=1)]
        peeps = [peep_factory(id=1), peep_factory(id=2), peep_factory(id=3)]
        
        seq1 = EventSequence(events, peeps)
        seq2 = EventSequence(events, peeps)
        
        # Same event, different attendees
        event1 = event_factory(id=1)
        event2 = event_factory(id=1)
        
        event1.add_attendee(peeps[0], Role.LEADER)  # peep 1
        event2.add_attendee(peeps[1], Role.LEADER)  # peep 2 (different)
        
        seq1.valid_events = [event1]
        seq2.valid_events = [event2]
        
        assert seq1 != seq2
        assert hash(seq1) != hash(seq2)
    
    def test_sequences_with_different_events_are_not_equal(self, event_factory, peep_factory):
        """Test that sequences with different events are not equal."""
        peeps = [peep_factory(id=1), peep_factory(id=2)]
        
        seq1 = EventSequence([event_factory(id=1)], peeps)
        seq2 = EventSequence([event_factory(id=2)], peeps)
        
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)  # Different event
        
        event1.add_attendee(peeps[0], Role.LEADER)
        event2.add_attendee(peeps[0], Role.LEADER)  # Same attendee, different event
        
        seq1.valid_events = [event1]
        seq2.valid_events = [event2]
        
        assert seq1 != seq2
        assert hash(seq1) != hash(seq2)
    
    def test_attendee_order_does_not_affect_equality(self, event_factory, peep_factory):
        """Test that different attendee assignment order doesn't affect equality."""
        events = [event_factory(id=1)]
        peep1 = peep_factory(id=1, role=Role.LEADER)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER)
        peeps = [peep1, peep2]
        
        seq1 = EventSequence(events, peeps)
        seq2 = EventSequence(events, peeps)
        
        # Add attendees in different order
        event1 = event_factory(id=1)
        event2 = event_factory(id=1)
        
        # Sequence 1: Leader first, then Follower
        event1.add_attendee(peep1, Role.LEADER)
        event1.add_attendee(peep2, Role.FOLLOWER)
        
        # Sequence 2: Follower first, then Leader
        event2.add_attendee(peep2, Role.FOLLOWER)
        event2.add_attendee(peep1, Role.LEADER)
        
        seq1.valid_events = [event1]
        seq2.valid_events = [event2]
        
        # Should still be equal (hashing sorts by ID)
        assert seq1 == seq2
        assert hash(seq1) == hash(seq2)

    def test_equality_failure_scenarios(self, event_factory, peep_factory):
        """Test various scenarios where EventSequence equality should fail."""
        peep1 = peep_factory(id=1, role=Role.LEADER)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER)
        peep3 = peep_factory(id=3, role=Role.LEADER)
        peeps = [peep1, peep2, peep3]

        # Test 1: Different number of valid events
        event1 = event_factory(id=1)
        event2 = event_factory(id=2)

        seq1 = EventSequence([event1, event2], peeps)
        seq2 = EventSequence([event1], peeps)

        event1_copy1 = event_factory(id=1)
        event1_copy2 = event_factory(id=1)
        event2_copy = event_factory(id=2)

        event1_copy1.add_attendee(peep1, Role.LEADER)
        event1_copy2.add_attendee(peep1, Role.LEADER)
        event2_copy.add_attendee(peep2, Role.FOLLOWER)

        seq1.valid_events = [event1_copy1, event2_copy]  # 2 events
        seq2.valid_events = [event1_copy2]               # 1 event

        assert seq1 != seq2
        assert hash(seq1) != hash(seq2)

        # Test 2: Same events but different roles for same peep
        event_a = event_factory(id=1)
        event_b = event_factory(id=1)

        seq3 = EventSequence([event_a], peeps)
        seq4 = EventSequence([event_b], peeps)

        event_a.add_attendee(peep1, Role.LEADER)    # peep1 as leader
        event_b.add_attendee(peep1, Role.FOLLOWER)  # peep1 as follower

        seq3.valid_events = [event_a]
        seq4.valid_events = [event_b]

        assert seq3 != seq4
        assert hash(seq3) != hash(seq4)

        # Test 3: Comparing with non-EventSequence object
        seq5 = EventSequence([], [])
        assert seq5 != "not an EventSequence"
        assert seq5 != None
        assert seq5 != 42

        # Test 4: Empty vs non-empty sequences
        empty_seq = EventSequence([], [])
        non_empty_seq = EventSequence([event_factory(id=1)], [peep1])
        event = event_factory(id=1)
        event.add_attendee(peep1, Role.LEADER)
        non_empty_seq.valid_events = [event]

        assert empty_seq != non_empty_seq
        assert hash(empty_seq) != hash(non_empty_seq)


@pytest.mark.unit
class TestEventSequenceFinalizationPriorities:
    """Test EventSequence finalization priority update logic."""
    
    
    def test_finalize_updates_total_attended_for_successful_peeps(self, event_factory, peep_factory):
        """Test that successful attendees get total_attended incremented."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, total_attended=5)
        peep2 = peep_factory(id=2, total_attended=2)
        peeps = [peep1, peep2]

        # peep1 attends, gets num_events = 1 during event processing
        event.add_attendee(peep1, Role.LEADER)
        peep1.num_events = 1  # Simulate event processing

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        sequence.finalize()

        # Successful attendee: total_attended increased by num_events
        assert peep1.total_attended == 6  # 5 + 1
        # Non-attendee: total_attended unchanged
        assert peep2.total_attended == 2

    def test_finalize_increases_priority_for_unscheduled_responded_peeps(self, event_factory, peep_factory):
        """Test that priority increases for peeps who responded but didn't get into events."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, priority=2, responded=True)   # Responded but won't attend
        peep2 = peep_factory(id=2, priority=3, responded=True)   # Responded and will attend
        peep3 = peep_factory(id=3, priority=1, responded=False)  # Didn't respond, won't attend
        peeps = [peep1, peep2, peep3]

        # Only peep2 gets into the event
        event.add_attendee(peep2, Role.LEADER)
        peep1.num_events = 0  # Didn't get into any event
        peep2.num_events = 1  # Got into one event
        peep3.num_events = 0  # Didn't get into any event

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        sequence.finalize()

        # peep1: responded but unscheduled -> priority increased
        assert peep1.priority == 3  # 2 + 1
        # peep2: attended event -> priority unchanged (no increase for successful attendees)
        assert peep2.priority == 3  # unchanged
        # peep3: didn't respond -> priority unchanged
        assert peep3.priority == 1  # unchanged


@pytest.mark.unit
class TestEventSequenceFinalizationMetrics:
    """Test EventSequence finalization metrics calculation."""
    
    def test_finalize_calculates_unique_attendees_correctly(self, event_factory, peep_factory):
        """Test that num_unique_attendees counts peeps who attended any event."""
        events = [event_factory(id=1), event_factory(id=2)]
        peep1 = peep_factory(id=1)
        peep2 = peep_factory(id=2) 
        peep3 = peep_factory(id=3)
        peeps = [peep1, peep2, peep3]
        
        # peep1 attends both events, peep2 attends one, peep3 attends none
        events[0].add_attendee(peep1, Role.LEADER)
        events[0].add_attendee(peep2, Role.FOLLOWER)
        events[1].add_attendee(peep1, Role.LEADER)
        
        # Simulate event processing
        peep1.num_events = 2
        peep2.num_events = 1
        peep3.num_events = 0
        
        sequence = EventSequence(events, peeps)
        sequence.valid_events = events
        
        sequence.finalize()
        
        # Should count 2 unique attendees (peep1 and peep2)
        assert sequence.num_unique_attendees == 2
    
    def test_finalize_calculates_priority_fulfilled_correctly(self, event_factory, peep_factory):
        """Test that priority_fulfilled sums original priority of successful attendees."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, priority=3)
        peep2 = peep_factory(id=2, priority=5)
        peep3 = peep_factory(id=3, priority=2)
        peeps = [peep1, peep2, peep3]
        
        # Store original priorities
        peep1.original_priority = peep1.priority
        peep2.original_priority = peep2.priority
        peep3.original_priority = peep3.priority
        
        # peep1 and peep2 attend
        event.add_attendee(peep1, Role.LEADER)
        event.add_attendee(peep2, Role.FOLLOWER)
        
        peep1.num_events = 1
        peep2.num_events = 1
        peep3.num_events = 0
        
        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]
        
        sequence.finalize()
        
        # Should sum original priorities of successful attendees
        assert sequence.priority_fulfilled == 8  # 3 + 5
    
    def test_finalize_calculates_normalized_utilization_correctly(self, event_factory, peep_factory):
        """Test that normalized_utilization averages utilization rates as a percent with clamping."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, event_limit=4, availability=[1, 2])  # clamp 4 -> 2
        peep2 = peep_factory(id=2, event_limit=1, availability=[1])
        peep3 = peep_factory(id=3, event_limit=3, availability=[1, 2, 3])
        peeps = [peep1, peep2, peep3]

        event.add_attendee(peep1, Role.LEADER)
        event.add_attendee(peep2, Role.FOLLOWER)

        peep1.num_events = 1
        peep2.num_events = 1
        peep3.num_events = 0

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        sequence.finalize()

        # (1/2 + 1/1 + 0/3) / 3 * 100 = 50%
        assert sequence.normalized_utilization == 50.0
    
    def test_finalize_calculates_total_attendees_correctly(self, event_factory, peep_factory):
        """Test that total_attendees sums all num_events across peeps."""
        events = [event_factory(id=1), event_factory(id=2)]
        peep1 = peep_factory(id=1)
        peep2 = peep_factory(id=2)
        peep3 = peep_factory(id=3)
        peeps = [peep1, peep2, peep3]

        # Simulate various attendance patterns
        peep1.num_events = 2  # Attends both events
        peep2.num_events = 1  # Attends one event
        peep3.num_events = 0  # Attends no events

        sequence = EventSequence(events, peeps)
        sequence.valid_events = events

        sequence.finalize()

        # Should sum all num_events: 2 + 1 + 0 = 3
        assert sequence.total_attendees == 3

    def test_normalized_utilization_excludes_non_responded_peeps(self, event_factory, peep_factory):
        """Test that non-responded peeps are excluded from normalized_utilization denominator."""
        # Create 5 total peeps: 3 responded, 2 did not
        peep1 = peep_factory(id=1, responded=True, event_limit=2, availability=[1])
        peep2 = peep_factory(id=2, responded=True, event_limit=2, availability=[1])
        peep3 = peep_factory(id=3, responded=True, event_limit=2, availability=[1])
        peep4 = peep_factory(id=4, responded=False, event_limit=2, availability=[1])  # Did not respond
        peep5 = peep_factory(id=5, responded=False, event_limit=2, availability=[1])  # Did not respond

        peeps = [peep1, peep2, peep3, peep4, peep5]

        event = event_factory(id=1)
        event.add_attendee(peep1, Role.LEADER)
        event.add_attendee(peep2, Role.FOLLOWER)

        peep1.num_events = 1
        peep2.num_events = 1
        peep3.num_events = 0  # Responded but unscheduled
        peep4.num_events = 0  # Did not respond
        peep5.num_events = 0  # Did not respond

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        sequence.finalize()

        # Utilization should only count responding peeps (3, not 5):
        # (1/1 + 1/1 + 0/2) / 3 * 100 = 66.67%
        # NOT (1/1 + 1/1 + 0/2 + 0/2 + 0/2) / 5 * 100 = 40%
        assert sequence.normalized_utilization == pytest.approx(66.67, abs=0.01)

    def test_normalized_utilization_handles_empty_availability(self, event_factory, peep_factory):
        """Test that peeps with empty availability lists don't break utilization calculation."""
        peep1 = peep_factory(id=1, responded=True, event_limit=2, availability=[1])
        peep2 = peep_factory(id=2, responded=True, event_limit=2, availability=[])  # Empty availability
        peep3 = peep_factory(id=3, responded=True, event_limit=2, availability=[])  # Empty availability

        peeps = [peep1, peep2, peep3]

        event = event_factory(id=1)
        event.add_attendee(peep1, Role.LEADER)

        peep1.num_events = 1
        peep2.num_events = 0
        peep3.num_events = 0

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        # Should not raise division by zero or crash
        sequence.finalize()

        # Utilization should only count peep1 (has availability and responded):
        # (1/1) / 1 * 100 = 100%
        # peep2 and peep3 should be excluded due to empty availability
        assert sequence.normalized_utilization == 100.0

    def test_normalized_utilization_handles_zero_eligible_peeps(self, event_factory, peep_factory):
        """Test that zero eligible peeps after filtering doesn't cause division by zero."""
        # Create peeps but none are eligible for utilization calculation
        peep1 = peep_factory(id=1, responded=False, event_limit=2, availability=[1])  # Did not respond
        peep2 = peep_factory(id=2, responded=True, event_limit=0, availability=[1])   # event_limit is 0
        peep3 = peep_factory(id=3, responded=True, event_limit=2, availability=[])   # Empty availability

        peeps = [peep1, peep2, peep3]

        event = event_factory(id=1)
        peep1.num_events = 0
        peep2.num_events = 0
        peep3.num_events = 0

        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]

        # Should not raise division by zero
        sequence.finalize()

        # Should return sensible default (0) when no eligible peeps
        assert sequence.normalized_utilization == 0


@pytest.mark.unit
class TestEventSequenceFinalizationSorting:
    """Test EventSequence finalization sorting and index updates."""
    
    def test_finalize_sorts_peeps_by_priority_descending(self, event_factory, peep_factory):
        """Test that finalize sorts peeps by priority (highest first)."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, priority=1, index=0)
        peep2 = peep_factory(id=2, priority=5, index=1) 
        peep3 = peep_factory(id=3, priority=3, index=2)
        peeps = [peep1, peep2, peep3]  # Not sorted by priority initially
        
        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]
        
        sequence.finalize()
        
        # Should be sorted by priority descending: peep2(5), peep3(3), peep1(1)
        assert sequence.peeps[0] == peep2
        assert sequence.peeps[1] == peep3  
        assert sequence.peeps[2] == peep1
    
    def test_finalize_updates_peep_indices_after_sorting(self, event_factory, peep_factory):
        """Test that finalize updates peep indices to match new sorted order."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, priority=1, index=0)
        peep2 = peep_factory(id=2, priority=5, index=1)
        peep3 = peep_factory(id=3, priority=3, index=2)
        peeps = [peep1, peep2, peep3]
        
        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]
        
        sequence.finalize()
        
        # After sorting by priority: peep2(index=0), peep3(index=1), peep1(index=2)
        assert peep2.index == 0
        assert peep3.index == 1
        assert peep1.index == 2
    
    def test_finalize_preserves_order_for_tied_priorities(self, event_factory, peep_factory):
        """Test that finalize preserves original order when priorities are tied (stable sort)."""
        event = event_factory(id=1)
        peep1 = peep_factory(id=1, priority=2, index=0)
        peep2 = peep_factory(id=2, priority=2, index=1)  # Same priority, should stay after peep1
        peep3 = peep_factory(id=3, priority=3, index=2)
        peeps = [peep1, peep2, peep3]  # Original order: 1, 2, 3
        
        sequence = EventSequence([event], peeps)
        sequence.valid_events = [event]
        
        sequence.finalize()
        
        # After sort: peep3 first (highest), then peep1, peep2 (stable sort preserves original order)
        assert sequence.peeps == [peep3, peep1, peep2]
        assert peep3.index == 0
        assert peep1.index == 1
        assert peep2.index == 2


@pytest.mark.unit
class TestEventSequenceDataConversion:
    """Test EventSequence data conversion for serialization."""
    
    def test_to_dict_includes_essential_fields(self, event_factory, peep_factory):
        """Test that to_dict includes all fields needed for serialization."""
        events = [event_factory(id=1)]
        peeps = [peep_factory(id=1), peep_factory(id=2)]
        
        sequence = EventSequence(events, peeps)
        sequence.num_unique_attendees = 2
        sequence.system_weight = 10
        sequence.partnerships_fulfilled = 3
        sequence.mutual_unique_fulfilled = 2
        sequence.mutual_repeat_fulfilled = 1
        sequence.one_sided_fulfilled = 1
        
        data = sequence.to_dict()
        
        # Should include key serialization fields
        assert 'valid_events' in data
        assert 'peeps' in data
        assert 'num_unique_attendees' in data
        assert 'system_weight' in data
        assert 'partnerships_fulfilled' in data
        assert 'mutual_unique_fulfilled' in data
        assert 'mutual_repeat_fulfilled' in data
        assert 'one_sided_fulfilled' in data
        
        assert data['num_unique_attendees'] == 2
        assert data['system_weight'] == 10
        assert data['partnerships_fulfilled'] == 3
        assert data['mutual_unique_fulfilled'] == 2
        assert data['mutual_repeat_fulfilled'] == 1
        assert data['one_sided_fulfilled'] == 1
    
    def test_to_dict_serializes_valid_events_with_attendees(self, event_factory, peep_factory):
        """Test that to_dict properly serializes valid events with attendee info."""
        event = event_factory(id=42)
        peep = peep_factory(id=1, display_name='TestPeep')
        
        event.add_attendee(peep, Role.LEADER)
        
        sequence = EventSequence([event], [peep])
        sequence.valid_events = [event]
        
        data = sequence.to_dict()
        
        # Should have valid_events with attendee information
        assert len(data['valid_events']) == 1
        event_data = data['valid_events'][0]
        
        assert event_data['id'] == 42
        assert 'attendees' in event_data
        assert len(event_data['attendees']) == 1
        assert event_data['attendees'][0]['name'] == 'TestPeep'
    
    def test_to_dict_handles_empty_sequence(self):
        """Test that to_dict works correctly with empty sequence."""
        sequence = EventSequence([], [])
        
        data = sequence.to_dict()
        
        assert data['valid_events'] == []
        assert data['peeps'] == []
        assert data['num_unique_attendees'] == 0
        assert data['system_weight'] == 0
        assert data['partnerships_fulfilled'] == 0
        assert data['mutual_unique_fulfilled'] == 0
        assert data['mutual_repeat_fulfilled'] == 0
        assert data['one_sided_fulfilled'] == 0


@pytest.mark.unit
class TestEventSequencePartnerships:
    """Test partnership fulfillment scoring for EventSequence."""

    def test_calculate_partnerships_fulfilled_counts_mutuals_and_one_sided(self, event_factory, peep_factory):
        """Test that partnership fulfillment correctly counts mutual and one-sided partnerships."""
        peep1 = peep_factory(id=1, role=Role.LEADER)
        peep2 = peep_factory(id=2, role=Role.FOLLOWER)
        peep3 = peep_factory(id=3, role=Role.FOLLOWER)

        event1 = event_factory(id=1)
        event1.add_attendee(peep1, Role.LEADER)
        event1.add_attendee(peep2, Role.FOLLOWER)

        event2 = event_factory(id=2)
        event2.add_attendee(peep1, Role.LEADER)
        event2.add_attendee(peep2, Role.FOLLOWER)

        event3 = event_factory(id=3)
        event3.add_attendee(peep1, Role.LEADER)
        event3.add_attendee(peep3, Role.FOLLOWER)

        sequence = EventSequence([event1, event2, event3], [peep1, peep2, peep3])
        sequence.valid_events = [event1, event2, event3]

        partnership_requests = {
            1: {2, 3},
            2: {1}
        }

        sequence.calculate_partnerships_fulfilled(partnership_requests)

        assert sequence.mutual_unique_fulfilled == 1
        assert sequence.mutual_repeat_fulfilled == 1
        assert sequence.one_sided_fulfilled == 1
        assert sequence.partnerships_fulfilled == 2
