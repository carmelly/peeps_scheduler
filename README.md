# Peeps Event Scheduler

## Overview

Peeps Event Scheduler is a tool for assigning participants to limited-capacity events based on availability, role balance, and participation history. Designed specifically for role-based dance class scheduling.

The system aims to maximize overall attendance, ensure fair distribution across roles (e.g., leader/follower), and prioritize participants who have historically had fewer opportunities.

## Key Features

- **Maximizes unique participation:** Evaluates all event permutations to schedule as many distinct attendees as possible.
- **Fairness-aware priority:** Participants with lower recent attendance are given higher scheduling priority.
- **Role balancing:** Ensures even or target-balanced assignment across roles (e.g., leaders/followers).
- **Constraint enforcement:** Respects user-defined limits on number of sessions and spacing between events (`min_interval_days`).
- **Automatic pruning:** Filters out infeasible events and removes redundant or conflicting event sequences.
- **Flexible input formats:** Supports data input from CSV (e.g., Google Sheets exports) or pre-built JSON files.

## How It Works

1. **Data Input**
   - Participant data and event availability are loaded from CSV or JSON files.
2. **Sanitization**
   - Events that can’t meet minimum role or attendance requirements are removed.
3. **Permutation & Evaluation**
   - All valid event orderings are generated and scored based on constraints and priorities.
4. **Optimization**
   - Redundant sequences are eliminated; the best scoring option is selected.
5. **Output**
   - A finalized schedule is exported with assigned participants and alternates.

## Current Status

**Version:** v1.0.0-baseline (December 2024)

This is the **production-ready baseline release** of the file/CLI-based scheduler:

- ✅ All 183 unit tests passing
- ✅ Complete CSV-to-JSON workflow
- ✅ Comprehensive architecture documentation in `docs/architecture/`
- ✅ Known issues catalogued in `docs/known-issues-baseline.md`

The `main` branch provides a stable, single-user CLI workflow using CSV and JSON files. Database integration and web UI features are under development on separate branches.

## Usage

The scheduler provides three main commands:

### 1. Run Scheduler

Generate an optimal event schedule from availability data:

```bash
python main.py run --load-from-csv --data-folder peeps-data/2025-01 --max-events 7
```

### 2. Apply Results

Update member priorities after events conclude:

```bash
python main.py apply-results --period-folder peeps-data/2025-01
```

### 3. Generate Availability Report

Create a summary of member availability:

```bash
python main.py availability-report --data-folder peeps-data/2025-01
```

For detailed workflow documentation, see `docs/architecture/baseline-v1-overview.md`.

## Roadmap

### Phase 1: Database Integration

Database infrastructure is complete on the `db-migration` branch. Remaining work includes application layer integration and snapshot generation refinements.

### Phase 2+: Web UI and Multi-User Support

Future phases will add web-based interfaces, real-time collaboration, and multi-user workflows.

For detailed implementation plans, see `.apm/Implementation_Plan.md`.

## Documentation

- **Architecture Overview:** `docs/architecture/baseline-v1-overview.md`
- **Known Issues:** `docs/known-issues-baseline.md`
- **Changelog:** `changelog.md`
