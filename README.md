# Peeps Event Scheduler

## Overview

Peeps Event Scheduler is a tool for assigning participants to limited-capacity events based on availability, role balance, and participation history. Designed specifically for role-based dance class scheduling, with potential to be adapted to other constrained group assignment problems in the future.

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

- The `main` branch uses CSV and JSON files for input/output.
- A database-backed version with attendance snapshots and a web UI is under development in the [`sqlite-refactor`](https://github.com/carmelly/peeps_scheduler/tree/sqlite-refactor) branch.

## Usage

Run the scheduler from the command line:

```bash
python main.py --members members.csv --responses responses.csv --output results.json

See main.py and scripts/ for entry points, configuration, and result-application tools.

## Changelog
See CHANGELOG.md for version history.