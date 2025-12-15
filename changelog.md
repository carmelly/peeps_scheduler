# Changelog

All notable changes to the Peeps Scheduler project will be documented in this file.  
This project uses semantic versioning. For planned future changes, see the "Planned" and "Deferred" sections under [Unreleased].

## [Unreleased]

### Planned for next release
- Partnership request support with scoring heuristic

### Deferred (db-migration branch)
- Database-backed persistence with normalized schema
- Web UI for scheduling input and results browsing
- Historical data snapshots and verification tools

---

## [v1.0.0-baseline] – 2024-12-14

**Baseline Release:** First documented stable release of CSV/file-based scheduler.

### Added
- Comprehensive architecture documentation (`docs/architecture/baseline-v1-overview.md`)
- Known issues catalog (`docs/known-issues-baseline.md`)
- Support for different event durations (60, 90, 120 minutes) with automatic downgrade
- Optional display of top tied event sequences with manual selection
- Tiebreaker heuristics to rank sequences by unique attendance and priority
- Improved scheduler output with display names and availability reporting
- SWITCH_IF_PRIMARY_FULL role flexibility (switch when primary role full)
- SWITCH_IF_NEEDED alternate promotion (promotes alternates to fill underfilled events)
- Alternate tracking with priority-preserved ordering

### Changed
- Priority increases only for peeps who responded during this period
- Uses `display_name` consistently throughout reports and updated members.csv
- `Role` enum accepts both `"Lead"` and `"Leader"` for compatibility
- Refactored role balancing to fix alternate ordering and improve event validation
- Index-based peep ordering preserves priority from previous periods

### Fixed
- Bug in `Event.is_valid` when validating role counts
- Bug in sorting of alternates after role balancing

### Known Limitations
- CSV-only workflow (no web interface)
- Single-user operation (no multi-user support)
- Advanced dual-role promotion not implemented
- 60-minute class pricing uncertain

See `docs/known-issues-baseline.md` for complete issue list with severity ratings.

### Notes
- All 183 tests passing
- Database integration in progress on `db-migration` branch
- No breaking changes to CSV workflow planned

---

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
