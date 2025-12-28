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

## Quick Start

The scheduler provides three main commands (run from project root with venv activated):

```bash
# Generate optimal schedule
python -m peeps_scheduler.main run --load-from-csv --data-folder <data-folder> --max-events 7

# Apply results after events conclude
python -m peeps_scheduler.main apply-results --period-folder <data-folder>

# Generate availability report
python -m peeps_scheduler.main availability-report --data-folder <data-folder>
```

Replace `<data-folder>` with the path to your CSV data files.

For detailed workflow documentation, see `docs/architecture/baseline-v1-overview.md`.

## Repository Structure

This project uses a src/ layout with the Python package at `src/peeps_scheduler/`:

```text
peeps-scheduler/
  src/
    peeps_scheduler/     # Python package
      __init__.py
      main.py
      models.py
      scheduler.py
      db/                # Database modules
      scripts/           # CLI scripts
  tests/                 # Test suite
  scripts/               # Developer utilities
  docs/                  # Architecture and domain docs
```

**Note:** This repository uses git submodules for development workflows and example data, but the scheduler can run independently with your own data.

## Development Workflow

### Initial Setup

```bash
# Clone repository
git clone <url> peeps-scheduler
cd peeps-scheduler

# Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install package in editable mode
pip install -e .

# Install dependencies
pip install -r requirements.txt
```

**Optional:** If you have access to the private submodules (example data and development configs):

```bash
# Initialize submodules after cloning
git submodule update --init
```

### Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run only unit tests
pytest -m unit

# Run with coverage
pytest --cov=peeps_scheduler
```

## Documentation

- **Architecture Overview:** `docs/architecture/baseline-v1-overview.md`
- **Known Issues:** `docs/known-issues-baseline.md`
- **Changelog:** `changelog.md`
