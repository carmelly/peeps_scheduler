# Database Import Process

## Overview

The `db/import_period_data.py` script imports historical CSV/JSON data into the normalized database. Import runs in two phases: member collection, then sequential period processing.

---

## Import Phases

### Phase 1: Member Collection

**Purpose:** Build complete member roster before importing any period data.

**Process:**
1. Scan all period directories chronologically
2. Extract member identity from each `members.csv`
3. Track first appearance to determine `date_joined`
4. Insert all members into `peeps` table
5. Build `peep_id_mapping` (csv_id → database_id)

**Why Sequential Scan:**
Ensures all members are registered before snapshot calculation begins, preventing incomplete tracking.

---

### Phase 2: Period Processing

**Purpose:** Import period data in chronological order to build accurate historical snapshots.

**Requirements:** Periods MUST be imported chronologically. Each period's snapshot depends on the prior period's final state.

**Per-Period Steps:**
1. **Schedule Period** - Create `schedule_periods` record
2. **Responses** - Import from `responses.csv` → `responses` table
3. **Events** - Parse availability strings → `events` table
4. **Availability** - Link responses to events → `event_availability` table
5. **Assignments** - Import from `results.json` → `event_assignments` table
6. **Attendance** - Import from `actual_attendance.json` → `event_attendance` table
7. **Changes** - Derive changes (scheduled vs actual) → `event_assignment_changes` table
8. **Snapshots** - Calculate period-end state → `peep_order_snapshots` table

---

## Snapshot Calculation

Snapshots capture member state at period boundaries for priority tracking and fairness enforcement.

### Snapshot Types

**PERMANENT Snapshots (Historical)**
- Based on actual attendance only
- Saved to database as historical record
- Used as starting point for next period

**SCHEDULING Snapshots (Active Period)**
- Based on both actual + expected attendance
- NOT saved to database
- Used for active scheduling decisions

### Priority Rules

**Priority Logic:**
1. **Attended ≥1 Event:** Priority reset to 0, moved to back of queue, `total_attended` incremented
2. **Responded but Not Attended:** Priority += 1 (fairness bump)
3. **Didn't Respond:** Priority unchanged

**Key Points:**
- Priority reset happens when attendance is applied
- Priority increment happens at period finalization
- `total_attended` only increments for actual attendance

---

## Assignment Change Tracking

Changes derive from comparing scheduled assignments to actual attendance.

### Change Types

**cancel**
- Trigger: Assigned but didn't attend
- Logic: Assignment exists, no attendance record

**promote_alternate**
- Trigger: Alternate who attended
- Logic: `assignment_type='alternate'` + attendance exists

**add**
- Trigger: Volunteer fill (no assignment)
- Logic: Attendance exists, no assignment record

---

## Usage

### First-Time Import

```bash
# Validate schema first
python db/import_period_data.py --validate-schema

# Import all periods
python db/import_period_data.py --all
```

### Single Period

```bash
# Requires prior periods already imported
python db/import_period_data.py --period 2025-03
```

### Testing

```bash
# Dry run (no database changes)
python db/import_period_data.py --all --dry-run

# Skip snapshot calculation (faster testing)
python db/import_period_data.py --period 2025-02 --skip-snapshots

# Verbose logging
python db/import_period_data.py --period 2025-02 --verbose
```

---

## Common Issues

### Sequential Order Required

**Error:** `Cannot calculate snapshots for 2025-03: Prior period 2025-02 has no snapshots`

**Fix:** Import periods chronologically starting from earliest period

### Email Mismatches

**Warning:** `No peep found for email user@example.com`

**Cause:** Email differs between `members.csv` and `responses.csv`

**Impact:** Response skipped, but attendance still imports (uses CSV IDs)

### Missing Schema

**Error:** `Missing required tables`

**Fix:** Run migrations first: `python db/migrate.py`

---

## CLI Flags

- `--all` - Import all available periods
- `--period YYYY-MM` - Import specific period
- `--dry-run` - Test without committing changes
- `--verbose` - Enable debug logging
- `--validate-schema` - Check database schema before import
- `--skip-snapshots` - Skip snapshot calculation (testing only)
- `--force-phase1` - Re-run member collection even if members exist

---

## References

- Import script: `db/import_period_data.py`
- Snapshot logic: `db/snapshot_generator.py`
- Schema: See `docs/architecture/database-schema.md` (future)
