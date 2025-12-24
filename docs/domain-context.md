# Domain Context: West Coast Swing Dance Scheduling

## Overview

The Peeps Scheduler is designed for scheduling West Coast Swing dance events within a community.

## Scheduling Domain

**Monthly periods containing 4-7 events:**

- Events are grouped into monthly periods
- Each period typically contains 4-7 individual dance events
- Scheduler optimizes assignments across the entire period

**Members have primary roles (Leader/Follower) with switch preferences:**

- Each member has a primary role: Leader or Follower
- Members can indicate willingness to switch roles
- Scheduler respects role preferences while optimizing assignments

**Priority system ensures fair distribution over time:**

- Members accumulate priority when available but not assigned
- Priority resets to 0 when assigned to an event
- Ensures fair rotation over multiple periods

**Scheduler uses exhaustive permutation evaluation to optimize assignments:**

- Evaluates multiple possible assignment combinations
- Optimizes for fairness, role balance, and member preferences
- Constraint-based algorithm ensures valid schedules

**Constraints: event limits, interval rules, role balancing:**

- Event capacity limits (minimum/maximum attendees)
- Interval rules (spacing between assigned events)
- Role balancing (maintain Leader/Follower ratios)

## Key Concepts

**Priority:**

- Increments when available but not assigned
- Resets to 0 when assigned to an event
- Tracks over time for fairness

**Snapshots:**

- Two types of snapshots track member status:
  - **SCHEDULING:** Expected state for planning
  - **PERMANENT:** Actual attendance record
- Used for historical tracking and future planning

**Downgrade:**

- Events can reduce duration if unable to meet minimum attendance
- Allows flexibility while maintaining event viability
- Preserves member experience with adjusted expectations
