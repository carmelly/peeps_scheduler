# Known Issues - Baseline Release

**Document Version:** 1.0
**Code Base:** Main branch (as of baseline documentation)
**Date:** 2024-12-14
**Purpose:** Comprehensive catalog of known bugs, limitations, and technical debt

---

## Overview

This document catalogs all known issues, incomplete features, system limitations, and technical debt in the baseline release of Peeps Scheduler. Issues are categorized by severity and type to guide future development priorities.

---

## Table of Contents

1. [Active Bugs](#1-active-bugs)
2. [Incomplete Features](#2-incomplete-features)
3. [System Limitations](#3-system-limitations)
4. [Technical Debt](#4-technical-debt)
5. [Configuration Uncertainties](#5-configuration-uncertainties)
6. [Test Status](#6-test-status)
7. [Deferred Features](#7-deferred-features)

---

## 1. Active Bugs

### 1.1 Snapshot Generation Bug - Missing Members (Database Branch Only)

**Status:** ACTIVE BUG - Affects `db-migration` branch, not main branch
**Severity:** HIGH (for database branch)
**Discovered:** 2024-10-01
**Impact:** Some members missing from period snapshots despite being in raw_members table

**Symptom:**
- A specific member (ID 30) exists in `raw_members` and `peeps` tables for periods 2025-04 through 2025-09
- This member is completely missing from ALL `peep_order_snapshots` for those periods
- Other inactive members are correctly included in snapshots
- This creates inconsistent member inclusion in historical snapshots

**Root Cause:**
- Bug in snapshot generation logic (in `db/snapshot_generator.py` on db-migration branch)
- Members who weren't present in early baseline periods may not get properly added to later snapshots
- The regeneration process that was run previously had a flaw

**Required Fix:**
1. Identify and fix the bug in snapshot generation logic
2. Regenerate ALL period snapshots from scratch using corrected logic
3. Validate that all members in `raw_members` appear in their corresponding period snapshots
4. Update period_snapshots_report.md with corrected data

**Temporary Workaround:**
- Manual insertion of missing members for immediate scheduling needs
- Use CSV workflow (main branch) for October/November scheduling while bug is being fixed

**Files Affected:**
- `db/snapshot_generator.py` (db-migration branch)
- Any scripts that call snapshot generation for bulk regeneration

**Validation Query (for database branch):**
```sql
-- Find members in raw_members but missing from snapshots
SELECT DISTINCT rm.csv_id, rm.period_name, p.display_name
FROM raw_members rm
JOIN peeps p ON rm.csv_id = p.id
LEFT JOIN peep_order_snapshots pos ON p.id = pos.peep_id
    AND pos.period_id = (SELECT id FROM schedule_periods WHERE period_name = rm.period_name)
WHERE pos.id IS NULL
ORDER BY rm.period_name, rm.csv_id;
```

**Note:** This bug does **NOT** affect the main branch CSV workflow, which is the baseline for this release.

**Source:** CLAUDE.md.backup lines 137-178

---

## 2. Incomplete Features

### 2.1 SWITCH_IF_NEEDED Alternate Promotion ✅

**Status:** RESOLVED (v1.0.0-baseline)
**Original Severity:** MEDIUM

**Resolution:**
Implemented alternate promotion logic for `SWITCH_IF_NEEDED` preference. Members with this preference are now promoted from alternates to attendees when it enables underfilled events to meet minimum attendance requirements.

**Implementation:**
- Promotion algorithm: `scheduler.py:77-102`
- Activates after initial assignment, before minimum validation
- Bidirectional (Leader ↔ Follower) with capacity enforcement
- Test coverage: `tests/test_scheduler.py:588-844` (6 comprehensive tests)

**Historical Validation:**
Validated against real periods (2025-09-real, 2025-10, 2025-11). Feature successfully prevented event downgrades/cancellations and improved fill rates while correctly acting as safety net (only activates when necessary).

---

### 2.2 Advanced Dual-Role Promotion ✅

**Status:** RESOLVED (v1.0.0-baseline)
**Original Severity:** MEDIUM

**Resolution:**
Implemented via SWITCH_IF_NEEDED alternate promotion (Issue 2.1). This provides the viable approach for dual-role promotion by allowing alternates to fill underfilled events in their opposite role.

**Architectural Finding:**
The alternative approach using SWITCH_IF_PRIMARY_FULL members was determined to be infeasible: these members are assigned to their secondary role as attendees during initial assignment (not as alternates), so they cannot be used in a subsequent promotion phase. SWITCH_IF_NEEDED is the correct and only mechanism for alternate-to-attendee promotion.

**Implementation:**
See Issue 2.1 for complete details on the promotion algorithm, test coverage, and historical validation.

**Example Scenario:**
```
Event A: 3 leaders, 5 followers (needs 4 leaders to meet min)
Alternates:
  - Leaders: [Carol (leader, priority 5)]
  - Followers: [Dan (follower, SWITCH_IF_PRIMARY_FULL)]

Current behavior: Event gets 1 more follower from elsewhere, or downgrades duration

Desired behavior: Dan switches to leader role, Carol promoted to leader
Result: 4 leaders, 4 followers (event proceeds at full duration)
```

**Workaround:**
Currently relies on manual selection of tied sequences or duration downgrades to handle underfilled events.

---

### 2.3 Partnership Score Heuristic Not Implemented

**Status:** PLANNED, NOT IMPLEMENTED
**Severity:** LOW
**Impact:** No optimization for partnership diversity

**Description:**
The changelog mentions a planned "partnership score heuristic" to evaluate event sequences based on partnership diversity (avoiding same pairs dancing together repeatedly).

**Code Location:**
- Mentioned in: `changelog.md:27`

**Current Workaround:**
- Users can manually review tied sequences and select based on partnership preferences
- No automated scoring for partnership diversity

---

### 2.4 Manual Results Editing Support

**Status:** DEFERRED TO DATABASE BRANCH
**Severity:** LOW
**Impact:** No automated way to apply manually edited schedules

**Description:**
The system generates `results.json` with scheduled attendees and alternates. If users manually edit this file (e.g., swap attendees, change assignments), there's no dedicated command to apply those edits back to member data.

**Code Location:**
- Mentioned in: `changelog.md:30`

**Current Workaround:**
- Create `actual_attendance.json` by manually copying and editing `results.json`
- Use `apply-results` command with the edited file

**Note:** This is planned for the db-migration branch with web UI support.

---

## 3. System Limitations

### 3.1 CSV-Only Workflow

**Status:** BY DESIGN (main branch)
**Severity:** MEDIUM
**Impact:** Manual file management, no web interface

**Description:**
The main branch operates entirely on CSV and JSON files. All data input and output requires manual file handling.

**Limitations:**
- No web UI for data entry
- Requires Google Sheets or CSV editor for member/response management
- Manual file uploads/downloads for each scheduling period
- No real-time collaboration on schedules

**Workflow:**
1. Export Google Forms responses to CSV
2. Download members.csv from Google Sheets
3. Run CLI command to generate schedule
4. Manually review results.json
5. Execute events
6. Manually create actual_attendance.json
7. Run apply-results CLI command
8. Upload updated members.csv back to Google Sheets

**Mitigation:**
- Database-backed workflow with web UI planned for `db-migration` branch
- CSV workflow is well-tested and functional for current use case

**Code Locations:**
- Data loading: `file_io.py:77-100`
- CLI workflow: `main.py:36-78`

---

### 3.2 Single-User Operation

**Status:** BY DESIGN
**Severity:** LOW
**Impact:** No concurrent access, no access control

**Description:**
The system has no multi-user support or access control. All operations are single-user, file-based.

**Limitations:**
- No user authentication
- No concurrent access protection
- No audit trail of who made changes
- No role-based permissions (e.g., admin vs viewer)

**Workaround:**
- Google Sheets provides access control for member roster
- Manual coordination for scheduling operations

**Future:** Multi-user support planned for web UI in db-migration branch

---

### 3.3 No Historical Data Verification Tools

**Status:** LIMITATION
**Severity:** LOW
**Impact:** No automated way to verify historical attendance accuracy

**Description:**
The system has no tools to verify that historical attendance data (in members.csv `Total Attended` field) matches the sum of actual attendance across all periods.

**Limitations:**
- No automated reconciliation of attendance totals
- No audit trail of period-by-period attendance
- No validation that priorities are correctly calculated from attendance history

**Workaround:**
- Manual verification via spreadsheet formulas
- Database branch has snapshot tables that track historical data

**Future:** Database branch provides historical snapshot verification

---

### 3.4 Interactive Mode Required for Tied Sequences

**Status:** BY DESIGN
**Severity:** LOW
**Impact:** Cannot fully automate scheduling runs

**Description:**
When multiple event sequences are tied on optimization metrics, the system prompts the user to select which one to use. This prevents fully automated scheduling.

**Current Options:**
- Interactive mode: Prompts user to choose (default)
- Non-interactive mode: Auto-selects sequence at specified index (via `--sequence-choice` flag)

**Limitations:**
- Non-interactive mode requires knowing how many tied sequences exist
- No automated selection criteria beyond the optimization metrics
- User must review and select manually for best results

**Code Location:**
- Interactive selection: `scheduler.py:282-299`
- Non-interactive selection: `scheduler.py:300-312`

**Workaround:**
Use `--sequence-choice 0` for automated testing/CI, but manually review for production runs.

---

### 3.5 No Undo/Rollback Capability

**Status:** LIMITATION
**Severity:** LOW
**Impact:** Cannot easily revert scheduling mistakes

**Description:**
Once `apply-results` is run and members_updated.csv is uploaded to Google Sheets, there's no built-in undo functionality.

**Limitations:**
- No version control for member data (unless Google Sheets history is used)
- No way to revert priority/attendance changes automatically
- Must manually restore from backup CSV

**Mitigation:**
- Keep backup copies of members.csv before running apply-results
- Use Google Sheets version history for rollback
- Database branch will have better snapshot/version support

---

## 4. Technical Debt

### 4.1 Event Date Validation Missing

**Status:** TECHNICAL DEBT
**Severity:** LOW
**Impact:** Potential runtime errors if invalid date passed to Event constructor

**Description:**
The `Event` class accepts a `date` parameter but does not validate that it's a `datetime` object. Invalid input could cause runtime errors later when date operations are performed.

**Code Location:**
- TODO comment: `models.py:234`

**Current Code:**
```python
def __init__(self, **kwargs):
    self.id = kwargs.get("id", 0)
    self.date = kwargs.get("date", None)  # TODO: validate that this is a datetime
```

**Risk:**
- Low (all current code paths create Events with proper datetime objects)
- Could cause issues if Event is constructed manually with invalid data

**Recommended Fix:**
```python
if not isinstance(self.date, datetime.datetime):
    raise ValueError(f"Event date must be a datetime object, got {type(self.date)}")
```

---

### 4.2 Alternate Duplicate Check Missing

**Status:** TECHNICAL DEBT
**Severity:** LOW
**Impact:** Potential duplicate alternates in lists, though unlikely given current usage

**Description:**
The `Event.add_alternate()` method does not check if a peep is already in an alternate list before adding them.

**Code Location:**
- TODO comment: `models.py:345`

**Current Code:**
```python
def add_alternate(self, peep: Peep, role: Role):
    """
    Add a peep to the alternate list for the given role.
    """
    # TODO: sanity check that peep is not already an alternate on either list
    if role == Role.LEADER:
        self._alt_leaders.append(peep)
    else:
        self._alt_followers.append(peep)
```

**Risk:**
- Low (current scheduler code doesn't call add_alternate multiple times for same peep/event)
- Could cause confusion if alternate lists have duplicates

**Recommended Fix:**
```python
# Check if already an alternate in either role
if peep in self._alt_leaders or peep in self._alt_followers:
    raise RuntimeError(f"Peep {peep.id} is already an alternate for Event {self.id}")
```

---

### 4.3 No Input Validation for SwitchPreference

**Status:** TECHNICAL DEBT
**Severity:** LOW
**Impact:** Enum conversion could fail with unclear error message

**Description:**
The `SwitchPreference.from_string()` method raises a generic `ValueError` if the input string doesn't match any known preference text. This could make debugging difficult for users.

**Code Location:**
- `models.py:34-44`

**Current Behavior:**
```python
@classmethod
def from_string(cls, value):
    value = value.strip()
    if value == "I only want to be scheduled in my primary role":
        return cls.PRIMARY_ONLY
    elif value == "I'm happy to dance my secondary role if it lets me attend when my primary is full":
        return cls.SWITCH_IF_PRIMARY_FULL
    elif value == "I'm willing to dance my secondary role only if it's needed to enable filling a session":
        return cls.SWITCH_IF_NEEDED
    else:
        raise ValueError(f"Unknown role: {value}")
```

**Issue:** Error message says "Unknown role" but should say "Unknown switch preference"

**Recommended Fix:**
```python
raise ValueError(f"Unknown switch preference: '{value}'. Expected one of: {[...valid options...]}")
```

---

### 4.4 Hardcoded Date Formats

**Status:** TECHNICAL DEBT
**Severity:** LOW
**Impact:** Difficult to support alternative date formats or locales

**Description:**
Date formats are hardcoded in `constants.py` and used throughout the codebase. No support for alternative formats or localization.

**Code Locations:**
- Format definitions: `constants.py:3-4`
- Date parsing: `file_io.py:387-460`
- Date formatting: `models.py:565-571`

**Current Formats:**
- `DATE_FORMAT = "%Y-%m-%d %H:%M"` (ISO format for event IDs)
- `DATESTR_FORMAT = "%A %B %d - %I%p"` (display format)

**Limitations:**
- No support for non-English day/month names
- No support for alternative time formats (e.g., 24-hour)
- Parsing logic manually strips leading zeros and lowercases am/pm

**Impact:**
- Works well for current use case (English, 12-hour format)
- Would require refactor to support internationalization

---

### 4.5 Magic Numbers in Scheduler

**Status:** TECHNICAL DEBT
**Severity:** LOW
**Impact:** Reduced code maintainability

**Description:**
Some constants used in the scheduler are not clearly documented or defined in a central location.

**Examples:**
- `target_max` range (4-7) hardcoded in loop: `scheduler.py:264`
- Could be made more explicit with named constants

**Current Code:**
```python
for target_max in range(constants.ABS_MIN_ROLE, constants.ABS_MAX_ROLE + 1):
    self.target_max = target_max
    sequences = self.evaluate_all_event_sequences(peeps, sanitized_events)
```

**Recommendation:**
- Add comments explaining why we iterate through all possible max values
- Consider extracting to named constant like `TARGET_MAX_RANGE`

---

## 5. Configuration Uncertainties

### 5.1 60-Minute Class Pricing

**Status:** UNCERTAIN
**Severity:** LOW
**Impact:** Incorrect pricing displayed for 60-minute events (if they occur)

**Description:**
The `CLASS_CONFIG` has a TODO note indicating uncertainty about the pricing and role limits for 60-minute events.

**Code Location:**
- `constants.py:8`

**Current Configuration:**
```python
60: {
    "price": 120.0,  # TODO: Not sure if this is correct, or what the role limits should be
    "min_role": 2,
    "max_role": 3,
    "allow_downgrade": False
}
```

**Impact:**
- 60-minute events are rare or non-existent in current scheduling
- If they occur, pricing may be incorrect
- Role limits (2-3) may not match actual venue/pricing requirements

**Recommended Action:**
- Verify with venue/organizer what the correct pricing and limits should be
- Update constants.py with confirmed values
- Remove TODO comment once verified

---

### 5.2 No Configuration File Support

**Status:** LIMITATION
**Severity:** LOW
**Impact:** All configuration changes require code modification

**Description:**
All configuration (CLASS_CONFIG, date formats, etc.) is hardcoded in `constants.py`. No support for external configuration files.

**Limitations:**
- Cannot change pricing, role limits, etc. without editing Python code
- No per-period or per-venue configuration overrides
- No environment-specific configuration (dev vs production)

**Current Workaround:**
- Edit constants.py and commit to version control
- Different configurations require different branches/forks

**Future Enhancement:**
- Support for YAML/JSON configuration file
- Per-period configuration overrides
- Environment variable overrides

---

## 6. Test Status

### 6.1 Test Coverage Summary

**Status:** ✅ ALL TESTS PASSING
**Test Count:** 177 tests
**Execution Time:** ~1.07 seconds
**Last Run:** 2024-12-14

**Test Breakdown:**
- `test_data_manager.py`: 9 tests - Data path resolution and directory creation
- `test_event.py`: 44 tests - Event attendee/alternate management, role balancing, downgrade logic
- `test_event_sequence.py`: 18 tests - Sequence equality, finalization, metrics calculation
- `test_file_io.py`: 49 tests - CSV/JSON loading, date parsing, event extraction
- `test_peep.py`: 29 tests - Peep validation, availability, priority management
- `test_scheduler.py`: 19 tests - Event sanitization, sequence evaluation, dual-role assignment
- `test_utils.py`: 9 tests - Apply results, priority updates, attendance tracking

**Coverage Highlights:**
- ✅ Core domain models (Peep, Event, EventSequence) well-tested
- ✅ Scheduling algorithm thoroughly tested
- ✅ File I/O and data conversion tested
- ✅ Edge cases and error handling covered

**Test Execution:**
```bash
$ pytest -v
============================= test session starts =============================
...
============================= 177 passed in 1.07s =============================
```

---

### 6.2 Known Test Gaps

**Status:** INFORMATIONAL
**Severity:** LOW

While test coverage is good, some areas could use additional testing:

1. **Integration Testing:**
   - End-to-end workflow tests (CSV → schedule → apply-results)
   - Multi-period sequence testing
   - Large dataset performance testing (100+ peeps, 20+ events)

2. **Error Path Testing:**
   - Malformed CSV input handling
   - Invalid JSON structure handling
   - Corrupted data recovery

3. **Concurrency:**
   - No tests for concurrent file access (though system is single-user)

4. **CLI Testing:**
   - Limited testing of main.py CLI argument parsing
   - No tests for interactive mode user prompts

**Recommendation:**
- Add integration tests for full workflows
- Add performance/stress tests for large datasets
- Consider property-based testing for scheduling algorithm

---

## 7. Deferred Features

The following features are explicitly deferred to the `db-migration` branch and are not part of the baseline release scope:

### 7.1 Web UI

**Status:** DEFERRED
**Branch:** `db-migration`
**Impact:** No browser-based interface

**Description:**
A web UI for scheduling input, results browsing, and historical data review.

**Planned Features:**
- Web-based data entry for member/response information
- Interactive schedule review and editing
- Historical attendance visualization
- Real-time collaboration on schedules

**Current Workaround:**
- Use CSV workflow with Google Sheets
- Manual file management via CLI

**Source:** `changelog.md:31`

---

### 7.2 Database-Backed Persistence

**Status:** IN PROGRESS (separate branch)
**Branch:** `db-migration`
**Impact:** No relational data storage on main branch

**Description:**
Normalized database schema with SQLite for persistent data storage, historical snapshots, and relational queries.

**Completed (on db-migration branch):**
- ✅ Normalized schema (8 core tables)
- ✅ Data transformation from CSV to database
- ✅ Historical snapshot tables
- ✅ Migration infrastructure (10 migrations applied)

**Remaining Work:**
- Application integration with database
- Fix snapshot generation bug (Issue 1.1)
- Performance optimization
- Query tuning

**Current Workaround:**
- Main branch uses CSV/JSON file-based workflow
- Historical data managed via members.csv Total Attended field

**Source:** `CLAUDE.md.backup:23-40`

---

### 7.3 Partnership Request Support

**Status:** PLANNED
**Impact:** No automated handling of partnership requests

**Description:**
Support for participants requesting to be scheduled with specific partners, with heuristic scoring to evaluate how well sequences honor partnership requests.

**Planned Features:**
- Partnership request input (e.g., "I want to dance with X")
- Partnership score heuristic for sequence evaluation
- Optimization to honor partnership requests when possible

**Current Workaround:**
- Users manually review tied sequences to check partnership compatibility

**Source:** `changelog.md:27`, Implementation Plan

---

## Summary

### Issue Counts by Category

| Category | Count | High Severity | Medium Severity | Low Severity |
|----------|-------|---------------|-----------------|--------------|
| Active Bugs | 1 | 1 (db branch only) | 0 | 0 |
| Incomplete Features | 2 | 0 | 0 | 2 |
| System Limitations | 5 | 0 | 1 | 4 |
| Technical Debt | 5 | 0 | 0 | 5 |
| Configuration Uncertainties | 2 | 0 | 0 | 2 |
| **Total** | **15** | **1** | **1** | **13** |

### Priority Recommendations

**Immediate (Next Release):**
1. Verify and update 60-min pricing (Issue 5.1)

**Short-Term:**
1. Add input validation (Issues 4.1, 4.2, 4.3)
2. Add integration tests (Issue 6.2)

**Long-Term (db-migration branch):**
1. Fix snapshot generation bug (Issue 1.1)
2. Implement web UI (Issue 7.1)
3. Complete database integration (Issue 7.2)
4. Add partnership tracking (Issue 7.3)

---

**End of Known Issues Documentation**
