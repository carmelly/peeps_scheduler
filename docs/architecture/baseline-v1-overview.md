# Peeps Scheduler - Baseline Architecture (v1.0)

**Document Version:** 1.0
**Code Base:** Main branch (as of Task 0.1 review)
**Purpose:** Comprehensive architecture documentation of the current main branch codebase

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Core Components](#3-core-components)
4. [Data Models](#4-data-models)
5. [Scheduling Algorithm](#5-scheduling-algorithm)
6. [Key Features & Mechanisms](#6-key-features--mechanisms)
7. [Data Flow](#7-data-flow)
8. [CLI Workflow](#8-cli-workflow)
9. [File Organization](#9-file-organization)
10. [Configuration](#10-configuration)

---

## 1. System Overview

Peeps Scheduler is a **constraint-based scheduling system** designed for West Coast Swing dance classes. It assigns participants (peeps) to limited-capacity events while:

- **Maximizing unique attendance** across all events
- **Balancing role distribution** (leaders vs. followers)
- **Respecting participant constraints** (availability, event limits, minimum intervals)
- **Prioritizing fairness** based on historical attendance patterns

### Primary Use Case

Schedule dance practice sessions where:

- Each session requires balanced pairs (leaders and followers)
- Participants have varying availability and preferences
- Fair rotation ensures everyone gets opportunities over time

---

## 2. High-Level Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                      CLI Interface                          │
│                       (main.py)                             │
└────────────┬────────────────────────────────────────────────┘
             │
             ├── run           (Schedule events)
             ├── apply-results (Update member data post-event)
             └── availability-report (Generate availability report)
             │
┌────────────▼────────────────────────────────────────────────┐
│                   Data Layer                                │
│                   (file_io.py)                              │
│  ┌──────────────┐    ┌──────────────┐   ┌──────────────┐   │
│  │ members.csv  │◄───┤ responses.csv│   │ output.json  │   │
│  │  (static)    │    │  (period)    │   │  (working)   │   │
│  └──────────────┘    └──────────────┘   └──────────────┘   │
└────────────┬────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────────────┐
│                  Core Domain Models                         │
│                    (models.py)                              │
│   Peep  │  Event  │  EventSequence  │  Role  │ SwitchPref  │
└────────────┬────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────────────┐
│              Scheduling Engine                              │
│                (scheduler.py)                               │
│  ┌─────────────────────────────────────────────────┐        │
│  │ 1. Sanitize events (filter infeasible)         │        │
│  │ 2. Remove overlapping events (if > max_events) │        │
│  │ 3. Generate all event permutations              │        │
│  │ 4. Evaluate each permutation (all target_max)  │        │
│  │ 5. Score & rank sequences                      │        │
│  │ 6. Select best sequence                         │        │
│  └─────────────────────────────────────────────────┘        │
└────────────┬────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────────────────────────────────┐
│                    Output Layer                             │
│  results.json  │  members_updated.csv  │  debug.log         │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components

### 3.1 models.py

**Purpose:** Defines all domain entities and their relationships

**Core Domain Models (entities with state and behavior):**

#### Peep (Class)

Represents a participant with:

**Identity Attributes:**

- `id`, `full_name`, `display_name`, `email`
- `role` - Primary dance role (leader/follower)

**Scheduling Attributes:**

- `availability` - List of event IDs they can attend
- `event_limit` - Max events per period
- `min_interval_days` - Minimum days between assigned events
- `switch_pref` - Role flexibility preference

**State Tracking:**

- `priority` - Current scheduling priority (0-N, higher = more urgent)
- `original_priority` - Priority at start of scheduling run
- `num_events` - Events assigned during current scheduling run
- `assigned_event_dates` - Dates of assigned events (for interval checking)
- `total_attended` - Historical attendance count

**Key Methods:**

- `can_attend(event)` - Checks availability, event limit, and interval constraints _(models.py:143)_
- `update_event_attendees(peeps, event)` - Resets priority and moves successful attendees to end of queue _(models.py:178)_
- `from_csv(row)` / `to_csv()` - CSV serialization _(models.py:88, 102)_

#### Event (Class)

Represents a scheduled class session with:

**Core Attributes:**

- `id` - Unique identifier
- `date` - Event datetime
- `duration_minutes` - Event length (60, 90, or 120)
- Config-derived: `min_role`, `max_role`, `price` (from CLASS*CONFIG)*(models.py:247-261)\_

**Participant Tracking (Internal Lists):**

- `_leaders` / `_followers` - Confirmed attendees by role
- `_alt_leaders` / `_alt_followers` - Alternates by role
- `_attendee_order` - Preserves assignment order

**Key Methods:**

- `add_attendee(peep, role)` - Assigns peep to event in specified role _(models.py:323)_
- `add_alternate(peep, role)` - Adds peep to alternate list _(models.py:341)_
- `promote_alt(peep, role)` - Promotes alternate to full attendee _(models.py:410)_
- `demote_attendee_to_alt(peep, role)` - Demotes attendee to front of alternate list _(models.py:429)_
- `balance_roles()` - Ensures equal leaders and followers, demoting extras _(models.py:452)_
- `downgrade_duration()` - Reduces event duration if underfilled _(models.py:477)_
- `validate_alternates()` - Removes ineligible alternates _(models.py:517)_
- `meets_min(role)` / `is_full(role)` - Capacity checks _(models.py:371, 390)_

#### EventSequence (Class)

Represents a complete schedule (one possible permutation):

**Attributes:**

- `events` - All events being considered
- `peeps` - All participants
- `valid_events` - Events that met minimum requirements after evaluation

**Efficiency Metrics:**

- `num_unique_attendees` - Count of distinct people scheduled
- `priority_fulfilled` - Sum of original priorities of scheduled peeps
- `normalized_utilization` - Average utilization rate per person
- `total_attendees` - Total attendance slots filled
- `system_weight` - Sum of final priorities (lower = better fairness)

**Key Methods:**

- `finalize()` - Updates peep priorities, sorts by priority, reassigns indices _(models.py:665)_
- `to_dict()` - Serializes for JSON output _(models.py:630)_
- `__key__()` / `__hash__()` / `__eq__()` - Enables deduplication of identical schedules _(models.py:697-730)_

**Supporting Types (enums and value objects):**

#### Role (Enum)

- `LEADER` / `FOLLOWER` - Dance role types
- `opposite()` - Returns the complementary role
- `from_string()` - Parses role from CSV input

#### SwitchPreference (Enum)

Controls flexibility in role assignment:

1. **PRIMARY_ONLY** - Only schedule in primary role
2. **SWITCH_IF_PRIMARY_FULL** - Willing to switch if primary role is full
3. **SWITCH_IF_NEEDED** - Only switch if needed to fill a session (not yet fully implemented)

---

### 3.2 scheduler.py

**Purpose:** Orchestrates the scheduling algorithm

Class: **Scheduler**

**Initialization:**

- `data_folder` - Path to period data
- `max_events` - Maximum number of events to schedule
- `interactive` - Whether to prompt user for tied sequences
- `sequence_choice` - Auto-select index for non-interactive mode

**Core Methods:**

#### `sanitize_events(events, peeps)` _(scheduler.py:24)_

Filters out events that cannot meet absolute minimum role requirements:

- Counts available leaders and followers per event
- Removes events with fewer than `ABS_MIN_ROLE` (4) available per role
- Returns only viable events

#### `remove_high_overlap_events(events, peeps, max_events)` _(scheduler.py:120)_

Reduces event count to `max_events` by removing high-overlap events:

1. Calculates overlap scores (shared available participants between event pairs)
2. Removes event with highest overlap
3. Uses priority weight as tiebreaker (removes lowest-weight event)
4. Repeats until count ≤ `max_events`

#### `evaluate_sequence(sequence, keep_invalid)` _(scheduler.py:39)_

**The core assignment algorithm** - evaluates a single event permutation:

1. **For each event in order:**
   - For each peep (in priority order):
     - Skip if `can_attend()` returns False
     - Try assigning in **primary role** if space available
     - Else if `SWITCH_IF_PRIMARY_FULL` and **secondary role** has space: assign there
     - Else: add as **alternate** in primary role

2. **Post-assignment processing:**
   - If event meets `ABS_MIN_ROLE`: call `balance_roles()`
   - If underfilled for its duration: call `downgrade_duration()`
   - If still doesn't meet `min_role`: discard event
   - If valid: call `update_event_attendees()` to reset priorities and move assigned peeps to end of queue

3. **Cleanup:**
   - Call `validate_alternates()` on all valid events
   - Call `finalize()` on sequence to update peep stats and compute metrics

#### `evaluate_all_event_sequences(og_peeps, og_events)` _(scheduler.py:102)_

Generates and evaluates all possible orderings:

1. Generate all permutations of event IDs
2. For each permutation:
   - Deep copy events and peeps
   - Create EventSequence
   - Call `evaluate_sequence()`
   - Add to results if any valid events
3. Return all sequences with ≥1 valid event

#### `get_top_sequences(sequences)` _(scheduler.py:186)_

Ranks sequences by priority:

1. Deduplicate using `EventSequence.get_unique_sequences()`
2. Sort by (in order):
   - `-num_unique_attendees` (maximize)
   - `-priority_fulfilled` (maximize)
   - `-normalized_utilization` (maximize)
   - `-total_attendees` (maximize)
3. Return all sequences tied on top 3 metrics

#### `run(generate_test_data, load_from_csv)` _(scheduler.py:215)_

**Main orchestration method:**

1. Load/generate data → `output.json`
2. Load peeps and events from JSON
3. Sanitize events
4. If too many events: remove high-overlap events
5. **For each target_max from ABS_MIN_ROLE to ABS_MAX_ROLE:**
   - Evaluate all permutations with that max
   - Collect all sequences
6. Get top sequences
7. If single best: auto-select
8. If tied: prompt user (interactive) or auto-select (non-interactive)
9. Save to `results.json`

---

### 3.3 file_io.py

**Purpose:** Handles all data I/O and format conversions

**Key Functions:**

#### `load_csv(filename, required_columns)` _(file_io.py:41)_

- Trims whitespace from headers and values
- Validates required columns exist
- Normalizes smart quotes and multiple spaces
- Returns list of cleaned dicts

#### `load_peeps(peeps_csv_path)` _(file_io.py:77)_

- Loads members.csv and creates Peep instances
- Validates unique emails (with Gmail dot-normalization)
- Checks active peeps have emails

#### `load_responses(response_csv_path)` _(file_io.py:98)_

- Loads responses.csv (period-specific availability submissions)

#### `save_peeps_csv(peeps, filename)` _(file_io.py:102)_

- Writes updated peeps to `members_updated.csv`
- Used after `apply-results` command to persist priority/attendance changes

#### `convert_to_json(response_csv, peeps_csv, output_json, year)` _(file_io.py:159)_

**Primary data conversion pipeline:**

1. Load peeps from members.csv
2. Load responses from responses.csv
3. Extract events (auto-derive from availability or from Event rows)
4. Process responses to update peep availability
5. Write combined output.json with peeps, events, responses

#### `extract_events(rows, year)` _(file_io.py:173)_

Supports two modes:

1. **Event rows** (backward compatibility): Rows with Name starting with "Event:"
2. **Auto-derive**: Scans availability strings to extract unique event dates/times

Calls `parse_event_date()` to parse date strings into event IDs.

#### `parse_event_date(date_str, year)` _(file_io.py:387)_

Handles two formats:

1. **New format with time range:** "Friday January 9th - 5:30pm to 7pm"
   - Parses date and time range
   - Calculates duration from start/end times
   - Returns `(event_id, duration, display_name)`
2. **Old format:** "Friday October 17 - 5pm"
   - Returns `(event_id, None, display_name)` (duration from Event Duration column)

#### `parse_time_range(time_str)` _(file_io.py:329)_

- Parses "5:30pm to 7pm" → `("17:30", "19:00", 90)`
- Calculates duration in minutes

#### `process_responses(rows, peeps, event_map, year)` _(file_io.py:282)_

- Matches responses to peeps by email
- Updates peep attributes: role, event_limit, min_interval_days, switch_pref
- Marks peeps as `responded = True`
- Populates `availability` lists with event IDs
- Returns updated peeps and response summaries

#### `load_data_from_json(filename)` _(file_io.py:136)_

- Deserializes output.json into Peep and Event objects
- Sorts peeps by index (which reflects priority order from previous period's finalization)
- Validates that index order matches priority order (highest to lowest)

#### `save_event_sequence(sequence, filename)` _(file_io.py:152)_

- Serializes EventSequence to results.json

---

### 3.4 main.py

**Purpose:** CLI interface and command routing

**Commands:**

#### `run` _(main.py:48)_

**Arguments:**

- `--generate-tests` - Generate synthetic test data
- `--load-from-csv` - Load from members.csv + responses.csv
- `--data-folder` - Path to period folder (required)
- `--max-events` - Maximum events to schedule (default: 7)

**Flow:**

1. Create Scheduler instance
2. If `--load-from-csv`: call `convert_to_json()` to create output.json
3. Call `scheduler.run()`
4. Output results.json

#### `apply-results` _(main.py:55)_

**Arguments:**

- `--period-folder` - Path to period folder (required)
- `--results-file` - Filename of results JSON (default: actual_attendance.json)

**Purpose:** Update member data after events complete

- Loads actual_attendance.json (manually created from real attendance)
- Loads members.csv
- Applies attendance to update priorities and total_attended
- Writes members_updated.csv for Google Sheets re-upload

#### `availability-report` _(main.py:60)_

**Arguments:**

- `--data-folder` - Path to period folder

**Purpose:** Generate availability summary report from responses

---

### 3.5 utils.py

**Purpose:** Helper utilities

**Key Functions:**

#### `generate_event_permutations(events)` _(utils.py:10)_

- Uses `itertools.permutations()` to generate all event orderings
- Returns list of event ID tuples

#### `setup_logging(verbose)` _(utils.py:21)_

- Configures dual logging: console (INFO/DEBUG) + debug.log (DEBUG)

#### `apply_event_results(result_json, members_csv, responses_csv)` _(utils.py:38)_

Called by `apply-results` command:

1. Load fresh peeps from members.csv
2. Mark who responded (from responses.csv)
3. Load actual attendance from result_json
4. Reconstruct EventSequence
5. Call `update_event_attendees()` and `finalize()`
6. Return updated peeps

---

### 3.6 constants.py

**Purpose:** Global configuration

**Key Constants:**

#### Date Formats

- `DATE_FORMAT = "%Y-%m-%d %H:%M"` - ISO format for event IDs
- `DATESTR_FORMAT = "%A %B %d - %I%p"` - Display format

#### CLASS*CONFIG*(constants.py:6)\_

Defines event duration tiers:

```python
{
    60: {"price": 120, "min_role": 2, "max_role": 3, "allow_downgrade": False},
    90: {"price": 195, "min_role": 4, "max_role": 5, "allow_downgrade": True},
   120: {"price": 260, "min_role": 6, "max_role": 7, "allow_downgrade": True}
}
```

#### Derived Constants

- `ABS_MIN_ROLE = 4` - Minimum per role across all downgradeable durations
- `ABS_MAX_ROLE = 7` - Maximum per role across all durations

#### Data Management

- `PRIVATE_DATA_ROOT` - Submodule path (default: "peeps_data", overrideable via env)

---

## 4. Data Models

### Peep Lifecycle

```text
┌──────────────────────────────────────────────────────────┐
│                    Initial State                         │
│  (loaded from members.csv + responses.csv)               │
│  - priority: carried over from previous period           │
│  - num_events: 0                                         │
│  - assigned_event_dates: []                              │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              Scheduling Evaluation                       │
│  For each event permutation:                             │
│    - Peeps ordered by index (reflects previous period's  │
│      finalized priority order, highest to lowest)        │
│    - can_attend() checks availability, limit, interval   │
│    - Assigned to primary role OR secondary (if switch)   │
│    - OR added as alternate                               │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│           After Assignment to Event                      │
│  update_event_attendees() called:                        │
│    - priority ← 0 (reset)                                │
│    - num_events += 1                                     │
│    - assigned_event_dates.append(date)                   │
│    - Peep moved to END of priority queue                 │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│                  Finalization                            │
│  sequence.finalize() called:                             │
│    - If num_events == 0 AND responded:                   │
│        priority += 1  (increase for next period)         │
│    - If num_events > 0:                                  │
│        total_attended += num_events                      │
│    - Re-sort peeps by priority                           │
│    - Reassign index based on sorted order                │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│                 Saved to Output                          │
│  results.json → members_updated.csv                      │
│  (ready for next period)                                 │
└──────────────────────────────────────────────────────────┘
```

### Event State Transitions

```text
┌──────────────────────────────────────────────────────────┐
│                   Event Created                          │
│  (from responses.csv or output.json)                     │
│  - Empty attendee/alternate lists                        │
│  - duration_minutes from CLASS_CONFIG                    │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              Assignment Phase                            │
│  For each peep in priority order:                        │
│    - add_attendee(peep, role)                            │
│      OR                                                  │
│    - add_alternate(peep, role)                           │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              Balance Phase                               │
│  If meets_absolute_min() (≥4 per role):                  │
│    - balance_roles()                                     │
│      → Demote extras to alternates until equal           │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│             Downgrade Phase                              │
│  If NOT meets_min() for current duration:                │
│    - downgrade_duration()                                │
│      → Find shorter duration in CLASS_CONFIG             │
│      → Update duration_minutes                           │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              Validation                                  │
│  If meets_min():                                         │
│    - Mark as VALID                                       │
│    - validate_alternates() to remove ineligible          │
│  Else:                                                   │
│    - Discard event (clear_participants())                │
└──────────────────────────────────────────────────────────┘
```

---

## 5. Scheduling Algorithm

### Overview

The scheduler uses **exhaustive permutation evaluation** to find the optimal event ordering that maximizes unique attendance while respecting all constraints.

### Algorithm Steps

#### Step 1: Data Loading & Preprocessing

```text
1. Load members.csv → Peep objects
2. Load responses.csv → Update peep availability
3. Create output.json (combined data)
4. Load peeps, events from output.json
5. Sort peeps by priority (descending)
```

#### Step 2: Event Sanitization

```text
For each event:
  Count available leaders
  Count available followers
  If either < ABS_MIN_ROLE (4):
    Remove event (cannot fill to minimum)
```

#### Step 3: Overlap Reduction (if needed)

```text
If event_count > max_events:
  While event_count > max_events:
    Calculate overlap scores for all event pairs
    Find event with highest overlap
    If tied: remove event with lowest priority weight
    Remove event from list
```

#### Step 4: Permutation Generation & Evaluation

```text
For target_max in [ABS_MIN_ROLE...ABS_MAX_ROLE]:  # Try 4, 5, 6, 7
  all_permutations = generate_permutations(events)

  For each permutation:
    Deep copy peeps and events
    Create EventSequence

    For each event in order:
      For each peep in priority order:
        If NOT can_attend(event):
          Skip

        If space in primary_role AND count < target_max:
          add_attendee(peep, primary_role)

        Else if SWITCH_IF_PRIMARY_FULL AND space in secondary_role:
          add_attendee(peep, secondary_role)

        Else:
          add_alternate(peep, primary_role)

      # Post-assignment processing
      If meets_absolute_min():  # ≥4 per role
        balance_roles()  # Demote extras until equal

        If NOT meets_min():  # Underfilled for duration
          downgrade_duration()  # Try shorter duration

      If meets_min():
        Mark event as VALID
        update_event_attendees()  # Reset priorities, move to end
      Else:
        Discard event

    # End of event loop
    validate_alternates()  # Remove ineligible alternates
    finalize()  # Update peep stats, compute metrics

    If valid_events > 0:
      Add sequence to results
```

#### Step 5: Ranking & Selection

```text
Deduplicate sequences (identical valid events)

Sort by:
  1. -num_unique_attendees  (most people scheduled)
  2. -priority_fulfilled    (favor overdue peeps)
  3. -normalized_utilization (maximize capacity usage)
  4. -total_attendees       (overall throughput)

Filter to top-tied sequences (same on metrics 1-3)

If single best:
  Auto-select
Else:
  If interactive: prompt user to choose
  Else: auto-select sequence_choice index
```

#### Step 6: Output

```text
Save selected EventSequence to results.json
Write members_updated.csv with updated priorities/attendance
```

---

### Constraint Enforcement

**Peep-Level Constraints** (checked in `can_attend()`):

1. **Availability:** Event ID must be in peep's availability list
2. **Event Limit:** `num_events < event_limit` for the period
3. **Interval:** All assigned event dates must be ≥ `min_interval_days` apart

**Event-Level Constraints:**

1. **Role Capacity:** Cannot exceed `max_role` per role (enforced in `add_attendee()`)
2. **Role Balance:** Leaders and followers must be equal after `balance_roles()`
3. **Minimum Attendance:** Must meet `min_role` for its duration (or downgrade)

**Global Constraints:**

1. **Max Events:** Total scheduled events ≤ `max_events` (via overlap removal)
2. **Absolute Minimums:** Events must have ≥ `ABS_MIN_ROLE` (4) available per role to be considered

---

## 6. Key Features & Mechanisms

### 6.1 Role Balancing

**Purpose:** Ensure equal number of leaders and followers in each event

**Mechanism:** `Event.balance_roles()` _(models.py:452)_

```python
def balance_roles(self):
    leaders = list(self._leaders)
    followers = list(self._followers)

    if len(leaders) != len(followers):
        larger_role = LEADER if len(leaders) > len(followers) else FOLLOWER
        larger_list = leaders if larger_role == LEADER else followers

        while len(leaders) != len(followers):
            alt_peep = larger_list.pop()  # Remove last assigned
            demote_attendee_to_alt(alt_peep, larger_role)
```

**Key Points:**

- Demotes excess attendees to **front** of alternate list (preserves priority)
- Always demotes from the role with more attendees
- Raises RuntimeError if roles still unbalanced after completion

---

### 6.2 Priority System

**Purpose:** Ensure fair rotation - people who haven't attended recently get higher priority

**Components:**

1. **Priority Initialization:** Loaded from members.csv (carried from previous period)

2. **Priority During Scheduling:**
   - Peeps ordered by index (set during previous period's finalization based on priority)
   - Index order reflects descending priority from previous period
   - Higher priority → lower index → earlier in assignment queue → better chance of getting in

3. **Priority Reset on Success:**
   - When assigned to event: `priority ← 0` _(models.py:182)_
   - Peep moved to **end** of queue for subsequent events in same permutation

4. **Priority Increment on Failure:**
   - If `num_events == 0` AND `responded == True`: `priority += 1` _(models.py:672)_
   - Ensures they have higher priority next period

5. **Priority Persistence:**
   - Saved to members_updated.csv
   - Becomes starting priority for next period

**Example Scenario:**

```text
Period N:
  Alice: priority=3, responded, NOT assigned → priority becomes 4
  Bob:   priority=2, responded, assigned → priority becomes 0
  Carol: priority=1, responded, assigned → priority becomes 0

Period N+1 starts with:
  Alice: priority=4  (highest - gets first pick)
  Bob:   priority=0
  Carol: priority=0
```

---

### 6.3 Permutation Evaluation

**Purpose:** Find the optimal event ordering that maximizes unique attendance

**Why Order Matters:**

- Peeps have event limits and interval constraints
- Assigning to early events affects eligibility for later events
- Different orderings produce different outcomes

**Approach:**

1. Generate all permutations of event IDs _(utils.py:10)_
2. Evaluate each permutation independently
3. For each permutation, peeps start with same initial state
4. Assignment is greedy within each permutation (priority order)

**Deduplication:**

- Multiple permutations may produce identical results
- Uses `EventSequence.__key__()` to identify duplicates _(models.py:697)_
- Key based on: (event ID, sorted leader IDs, sorted follower IDs) for each valid event

**Complexity:**

- N events → N! permutations
- Current max_events = 7 → max 5,040 permutations
- Each permutation evaluated across 4 target_max values (4-7) → ~20k evaluations typical

---

### 6.4 Downgrade Handling

**Purpose:** Salvage underfilled events by reducing duration instead of canceling

**Mechanism:** `Event.downgrade_duration()` _(models.py:477)_

**Trigger:**

- Event meets `ABS_MIN_ROLE` (≥4 per role, balanced)
- But does NOT meet `min_role` for its current duration

**Process:**

```python
count_per_role = len(self.leaders)  # Equal to len(followers) after balance

# Search CLASS_CONFIG for valid downgrade
for duration in sorted(CLASS_CONFIG.keys()):
    config = CLASS_CONFIG[duration]
    if (config["allow_downgrade"] and
        config["min_role"] <= count_per_role <= config["max_role"]):

        self.duration_minutes = duration
        return True  # Downgrade successful

return False  # No valid downgrade found
```

**Example:**

```text
Event initially set to 120 min (requires 6-7 per role)
After assignment: 5 leaders, 5 followers
  → Does NOT meet 120-min minimum (6)
  → Downgrades to 90 min (requires 4-5 per role)
  → Now meets minimum ✓
  → Event is VALID with 90-min duration
```

**Configuration:**

- Only durations with `"allow_downgrade": True` are eligible targets
- 60-min duration cannot be downgraded (no shorter option)

---

### 6.5 Switch Preferences

**Purpose:** Allow flexible participants to dance in their non-primary role when beneficial

**Enum Values:**

1. **PRIMARY_ONLY** - Never switch roles
2. **SWITCH_IF_PRIMARY_FULL** - Switch if primary role is full
3. **SWITCH_IF_NEEDED** - Switch only if needed to enable a session (TODO: not yet fully implemented)

**Implementation in Scheduling:**

```python
# In evaluate_sequence() (scheduler.py:39)

primary_role = peep.role
secondary_role = primary_role.opposite()

# Try primary role first
if event.num_attendees(primary_role) < effective_max_role:
    event.add_attendee(peep, primary_role)

# Try secondary if allowed and primary is full
elif (peep.switch_pref == SWITCH_IF_PRIMARY_FULL and
      event.num_attendees(secondary_role) < effective_max_role):
    event.add_attendee(peep, secondary_role)

# Otherwise, add as alternate in primary role
else:
    event.add_alternate(peep, primary_role)
```

**Display:**

- Peeps dancing in non-primary role marked with asterisk in output
- Example: `Leaders(5): Alice, *Bob, Carol, Dave, Eve`
  - Bob is a follower dancing as leader

**Future Enhancement:**

- `SWITCH_IF_NEEDED` intended to promote alternates of opposite role when it would enable filling a session
- Not yet implemented in current codebase

---

### 6.6 Alternates Management

**Purpose:** Track waitlist in priority order for each role

**Key Mechanisms:**

1. **Adding Alternates:**
   - Peeps who can attend but event is full → added as alternates
   - Always added in **primary role** (even if event is full)
   - Preserves assignment order within alternate list

2. **Promotion:**
   - `promote_alt(peep, role)` moves alternate to full attendee _(models.py:410)_
   - Not currently used in main algorithm (reserved for manual adjustments)

3. **Demotion:**
   - `demote_attendee_to_alt(peep, role)` during `balance_roles()` _(models.py:429)_
   - Demoted peep inserted at **front** of alternate list (preserves priority)

4. **Validation:**
   - `validate_alternates()` called after all events evaluated _(models.py:517)_
   - Removes alternates who are no longer eligible (e.g., assigned to another event, interval violated)

5. **Output:**
   - Displayed in bracket notation: `Leaders(5): Alice, Bob [alt: Carol, Dave]`
   - Preserved in results.json for manual reference

---

### 6.7 Event Overlap Removal

**Purpose:** Reduce event count to max_events by removing events with highest participant overlap

**Mechanism:** `Scheduler.remove_high_overlap_events()` _(scheduler.py:120)_

**Algorithm:**

```text
1. Create peep_event_map: {peep_id: set of available event_ids}

2. For each event pair (A, B):
     shared_peeps = count peeps available for BOTH A and B
     overlap_score[A] += shared_peeps
     overlap_score[B] += shared_peeps

3. Find event with highest overlap_score

4. If tied:
     Calculate priority_weight = sum of priorities of available peeps
     Remove event with LOWEST weight

5. Repeat until event_count <= max_events
```

**Rationale:**

- High-overlap events compete for the same peeps
- Removing high-overlap events increases diversity of available peeps across events
- Weight tiebreaker favors keeping events with higher-priority peeps available

**Example:**

```text
Event A: 15 peeps available (overlap_score = 45)
Event B: 12 peeps available (overlap_score = 45)  [tied with A]
  Event A: sum priorities = 18
  Event B: sum priorities = 12
→ Remove Event B (lower weight)
```

---

## 7. Data Flow

### End-to-End Flow: CSV → Schedule → Updated CSV

```text
┌─────────────────────────────────────────────────────────────┐
│                    Period Setup                             │
│  Input: members.csv (static roster)                         │
│         responses.csv (period-specific availability)        │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Conversion                                │
│  file_io.convert_to_json()                                  │
│    1. load_peeps(members.csv)                               │
│    2. load_responses(responses.csv)                         │
│    3. extract_events(responses)                             │
│    4. process_responses() → update peep availability        │
│    5. save_json(output.json)                                │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│              Scheduling                                     │
│  Scheduler.run()                                            │
│    1. Load from output.json                                 │
│    2. Sanitize events                                       │
│    3. Remove high-overlap events (if needed)                │
│    4. For each target_max (4-7):                            │
│         - Generate all permutations                         │
│         - Evaluate each permutation                         │
│    5. Rank sequences                                        │
│    6. Select best (or prompt user if tied)                  │
│    7. save_event_sequence(results.json)                     │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│              Results Review & Execution                     │
│  Manual: Review results.json                                │
│          Execute events                                     │
│          Create actual_attendance.json (manual edit)        │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│              Apply Results                                  │
│  main.py apply-results                                      │
│    1. utils.apply_event_results()                           │
│         - Load fresh peeps from members.csv                 │
│         - Load actual_attendance.json                       │
│         - Reconstruct EventSequence                         │
│         - update_event_attendees()                          │
│         - finalize()                                        │
│    2. file_io.save_peeps_csv(members_updated.csv)           │
└────────────┬────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Persistence                               │
│  Manual: Upload members_updated.csv to Google Sheets        │
│          → Becomes members.csv for next period              │
└─────────────────────────────────────────────────────────────┘
```

### File Formats

#### members.csv (Static Roster)

```csv
id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,John Doe,John,john@example.com,leader,0,2,15,TRUE,2023-01-15
2,Jane Smith,Jane,jane@example.com,follower,1,0,18,TRUE,2023-02-20
```

**Key Fields:**

- `Priority` - Scheduling priority (higher = more urgent)
- `Total Attended` - Historical attendance count
- `Active` - Whether peep is currently active
- `Index` - Sort order (reassigned each period based on priority)

#### responses.csv (Period-Specific Availability)

```csv
Timestamp,Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days
2026-01-01 10:30,John Doe,john@example.com,leader,I only want...,2,"Friday January 9th - 5:30pm to 7pm, Saturday January 10th - 11am to 12:30pm",3
```

**Key Fields:**

- `Primary Role` - Preferred role (leader/follower)
- `Secondary Role` - Switch preference (full text from SwitchPreference enum)
- `Max Sessions` - Event limit for this period
- `Availability` - Comma-separated list of event date strings
- `Min Interval Days` - Minimum days between events

#### output.json (Combined Working Data)

```json
{
  "responses": [
    /* response summaries */
  ],
  "events": [
    {
      "id": 0,
      "date": "2026-01-09 17:30",
      "duration_minutes": 90
    }
  ],
  "peeps": [
    {
      "id": 1,
      "name": "John Doe",
      "display_name": "John",
      "email": "john@example.com",
      "role": "leader",
      "priority": 2,
      "availability": [0, 1, 3],
      "event_limit": 2,
      "min_interval_days": 3,
      "switch_pref": 1,
      "responded": true,
      "total_attended": 15
    }
  ]
}
```

#### results.json (Final Schedule)

```json
{
  "valid_events": [
    {
      "id": 0,
      "date": "2026-01-09 17:30",
      "duration_minutes": 90,
      "attendees": [
        { "id": 1, "name": "John", "role": "leader" },
        { "id": 2, "name": "Jane", "role": "follower" }
      ],
      "alternates": [{ "id": 3, "name": "Bob", "role": "leader" }],
      "leaders_string": "Leaders(4): Alice, John, Mike, Tom [alt: Bob]",
      "followers_string": "Followers(4): Jane, Lisa, Mary, Sue"
    }
  ],
  "peeps": [
    /* updated peep data with new priorities */
  ],
  "num_unique_attendees": 24,
  "priority_fulfilled": 38,
  "system_weight": 12
}
```

---

## 8. CLI Workflow

### Typical Period Workflow

#### 1. Collect Responses

```bash
# Manual: Export Google Forms responses to CSV
# → peeps_data/2026-01/responses.csv
```

#### 2. Run Scheduler

```bash
python main.py run \
  --load-from-csv \
  --data-folder peeps_data/2026-01 \
  --max-events 7
```

**Output:**

```text
📋 Mini Availability Report: 42 responses / 38 available / 45 active / 50 total
  🚫  No availability: Alice, Bob
  ❌  Did not respond: Carol, Dave

Found 3 tied top sequences with 24 unique attendees:
[0] EventSequence: valid events: { 0, 2, 4, 5, 7 }, unique attendees 24/50, ...
[1] EventSequence: valid events: { 0, 1, 4, 5, 7 }, unique attendees 24/50, ...
[2] EventSequence: valid events: { 2, 3, 4, 5, 6 }, unique attendees 24/50, ...

Enter the index of the sequence to save (0-2):
```

User selects option → results.json created

#### 3. Review Schedule

```bash
# Manual: Review peeps_data/2026-01/results.json
# Verify assignments look reasonable
```

#### 4. Execute Events

```bash
# Manual: Run scheduled events over the period
# Track actual attendance
```

#### 5. Record Actual Attendance

```bash
# Manual: Create actual_attendance.json (copy results.json, edit attendees)
# → peeps_data/2026-01/actual_attendance.json
```

#### 6. Apply Results

```bash
python main.py apply-results \
  --period-folder peeps_data/2026-01 \
  --results-file actual_attendance.json
```

**Output:**

```text
✓ Updated peeps saved to peeps_data/2026-01/members_updated.csv
```

#### 7. Update Master Roster

```bash
# Manual: Upload members_updated.csv to Google Sheets
# → Becomes new members.csv for next period
```

---

### Non-Interactive Mode

For automated testing or scripted runs:

```bash
python main.py run \
  --load-from-csv \
  --data-folder peeps_data/2026-01 \
  --max-events 7 \
  --non-interactive \
  --sequence-choice 0
```

- No user prompts
- Auto-selects sequence at index 0 if tied

---

## 9. File Organization

### Project Structure

```text
peeps_scheduler/
├── .apm/                    # APM session management (Task 0.1+)
├── .claude/                 # Claude Code configuration
├── .venv/                   # Python virtual environment
├── db/                      # Database utilities
│   ├── migrate.py           # Migration scripts
│   ├── period_report.py     # Period reporting
│   └── sqlite3.exe          # SQLite CLI (Windows)
├── peeps_data/              # Git submodule (private data)
│   ├── 2025-12/
│   │   ├── members.csv
│   │   ├── responses.csv
│   │   ├── output.json
│   │   ├── results.json
│   │   └── actual_attendance.json
│   └── peeps_scheduler.db   # SQLite database (future)
├── scripts/                 # Utility scripts
├── tests/                   # Unit tests (pytest)
├── webapp/                  # Future web interface
├── docs/                    # Documentation (NEW)
│   └── architecture/
│       └── baseline-v1-overview.md  (THIS FILE)
├── models.py                # Domain models (Peep, Event, EventSequence)
├── scheduler.py             # Scheduling algorithm
├── file_io.py               # CSV/JSON I/O
├── main.py                  # CLI interface
├── constants.py             # Configuration
├── utils.py                 # Helper functions
├── data_manager.py          # Data path management
├── availability_report.py   # Availability reporting
├── CLAUDE.md                # User instructions for Claude Code
├── README.md                # Project overview
├── changelog.md             # Version history
└── debug.log                # Debug output (gitignored)
```

### Key Directories

#### peeps_data/ (Git Submodule)

- **Purpose:** Private data storage (not in main repo)
- **Structure:** One folder per period (e.g., 2026-01/)
- **Contents:** members.csv, responses.csv, output.json, results.json, actual_attendance.json
- **Database:** peeps_scheduler.db (future feature, not yet active)

#### tests/

- **Purpose:** Unit tests for models, scheduler, file_io
- **Framework:** pytest
- **Coverage:** ~80% (as of last run)

#### db/

- **Purpose:** Database migration and reporting scripts
- **Key Files:**
  - `migrate.py` - Schema migrations (not yet active)
  - `period_report.py` - Generate period summary reports
  - `sqlite3.exe` - SQLite CLI for Windows

#### .apm/

- **Purpose:** Agentic Project Management session tracking
- **Created:** Task 0.1 (this documentation task)
- **Contents:** Memory logs, guides, task prompts

---

## 10. Configuration

### CLASS_CONFIG (constants.py)

Defines three event duration tiers:

| Duration | Price | Min/Role | Max/Role | Allow Downgrade |
| -------- | ----- | -------- | -------- | --------------- |
| 60 min   | $120  | 2        | 3        | No              |
| 90 min   | $195  | 4        | 5        | Yes             |
| 120 min  | $260  | 6        | 7        | Yes             |

**Derived Constants:**

- `ABS_MIN_ROLE = 4` (minimum across downgradeable tiers)
- `ABS_MAX_ROLE = 7` (maximum across all tiers)

### Scheduling Parameters

**Hardcoded in Scheduler:**

- `max_events` - CLI argument (default: 7)
- `target_max` - Iterates 4-7 during evaluation
- `interactive` - CLI flag (default: True)
- `sequence_choice` - CLI argument (default: 0)

**Event Sanitization:**

- Minimum available per role: `ABS_MIN_ROLE` (4)
- Events below this threshold are removed before scheduling

**Overlap Removal:**

- Triggered when: `len(events) > max_events`
- Strategy: Remove highest overlap, tiebreak by lowest weight

### Date/Time Formats

**Internal Storage (ISO):**

- Event ID: `%Y-%m-%d %H:%M` (e.g., "2026-01-09 17:30")

**Display Format:**

- `%A %B %d - %I%p` (e.g., "Friday January 9 - 5PM")
- Post-processed to remove leading zeros and lowercase am/pm

**Parsing:**

- Old format: "Friday January 9 - 5pm"
- New format: "Friday January 9th - 5:30pm to 7pm" (with duration calculation)

### Environment Variables

**PEEPS_DATA_PATH:**

- Default: "peeps_data"
- Override: `export PEEPS_DATA_PATH=/path/to/data`

**DATA_FOLDER:**

- Default: None (must specify via CLI)
- Override: `export DATA_FOLDER=peeps_data/2026-01`

### Logging

**Console:**

- INFO level (default)
- DEBUG level (with `--verbose` flag)

**File (debug.log):**

- Always DEBUG level
- Appended to (not overwritten)
- Gitignored

---

## Summary

This architecture represents a **constraint-based scheduling system** that:

1. **Loads** participant data and availability from CSV/JSON
2. **Sanitizes** events to remove infeasible options
3. **Evaluates** all possible event orderings (permutations)
4. **Assigns** peeps to events respecting capacity, availability, and fairness constraints
5. **Balances** roles and downgrades durations as needed
6. **Ranks** schedules by unique attendance, priority fulfillment, and utilization
7. **Outputs** the optimal schedule with attendees and alternates
8. **Updates** participant data for the next scheduling period

The system is designed for **manual oversight** (interactive mode, user review of results) while automating the complex combinatorial optimization of event assignments.

**Future Enhancements** (planned in sqlite-refactor branch):

- Database-backed persistence
- Web UI for schedule review and editing
- Historical attendance snapshots
- Enhanced reporting and analytics

---
