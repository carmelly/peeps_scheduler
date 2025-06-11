# Changelog

All notable changes to the Peeps Scheduler project will be documented in this file.  
This project uses semantic versioning. For planned future changes, see the "Planned" and "Deferred" sections under [Unreleased].

## [Unreleased]

### Added
- Support for different event durations
- Optional display of top tied event sequences with manual selection
- Tiebreaker heuristics to rank top event sequences
- Improved scheduler output: includes display names, peeps with no availability, and sorted display of events and attendees

### Changed
- Only increases priority for peeps who responded during this period
- Uses `display_name` consistently throughout reports and updated members.csv
- `Role` enum now accepts both `"Lead"` and `"Leader"` for compatibility
- Warns on duplicate email addresses during input
- Refactored role balancing to fix alternate ordering and improve event validation

### Fixed
- Bug in `Event.is_valid` when validating role counts
- Bug in sorting of alternates after role balancing

### Planned for next release
- Dual-role support via `can_switch_roles`
- Partnership score heuristic to evaluate event sequences

### Deferred (SQLite refactor branch)
- Apply manually edited `results.json` (event + attendees only) to generate final attendance snapshot
- Web UI for scheduling input and results browsing

## [v0.3.0](https://github.com/carmelly/peeps_scheduler/tree/v0.3.0) – 2025-05-13

### Added
- Scheduler loops through possible min/max role configurations to find the best sequence
- `Peep` model now includes `active` and `date_joined` fields for spreadsheet syncing
- Availability reporting tools:
  - Script to show who is available on each date
  - Summary report now includes non-respondents

### Changed
- CSV loader checks for required columns and fails cleanly with helpful errors

### Fixed
- Logging output cleanup
- Miscellaneous test fixes

## [v0.2.0](https://github.com/carmelly/peeps_scheduler/tree/v0.2.0) – 2025-04-09

### Added
- `Scheduler` class to encapsulate sequence evaluation logic
- CLI interface with arguments for file paths and runtime options
- Alternate tracking system: events now support and store alternates
- Email-based peep matching for better identity resolution

### Changed
- Replaced `globals.py` with explicit config (`constants.py`)
- Stream log level can now be set via command-line flag
- Tests updated to use `Role` enum instead of string literals

### Fixed
- Gitignore improvements for development across branches and Google Drive sync

## [v0.1.0](https://github.com/carmelly/peeps_scheduler/tree/v0.1.0) – 2025-03-21

### Added
- CSV-to-JSON pipeline for loading member and response data
- Event sequence simulator and scheduling engine
- Unified attendee lists with role support
- Utilities for generating test data and saving/loading peep/event JSON
- Snapshot saving and result application to update attendance across runs

### Changed
- Major refactor: modularized into `models`, `utils`, `globals`
- Replaced role strings with a `Role` enum
- Introduced `min_interval_days` (cooldown) per peep for schedule spacing
- Reformatted event timestamps to match Google Forms inputs
- Rewrote sequence evaluation logic to improve pruning and deduplication

### Fixed
- Handling of peeps with missing availability
- Crash bugs related to peep serialization and event conflict checking
- Bugs in sequence evaluation, including tiebreakers and attendee ordering

### Removed
- Deprecated AppsScript code in favor of full CSV-based workflow
- Removed broken or redundant event sequences during scheduling
