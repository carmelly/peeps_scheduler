import pytest
import tempfile
import os
import json
import datetime
import logging
from file_io import (
	load_data_from_json,
	convert_to_json,
	parse_event_date,
	parse_time_range,
	load_csv,
	load_peeps,
	extract_events,
	process_responses,
	save_event_sequence,
	save_peeps_csv,
	load_json,
	save_json,
	normalize_email,
	load_cancellations,
	load_partnerships
)
from models import Event, EventSequence, Role, SwitchPreference, Peep

# ============================================================================
# SHARED FIXTURES
# ============================================================================

@pytest.fixture
def valid_peeps_rows():
	return [
		{"id": "1", "Name": "Alice Alpha", "Display Name": "Alice", "Email Address": "alice@test.com", "Role": "Leader",
		 "Index": "0", "Priority": "1", "Total Attended": "3", "Active": "TRUE", "Date Joined": "2022-01-01"},
		{"id": "2", "Name": "Bob Beta", "Display Name": "Bob", "Email Address": "bob@test.com", "Role": "Follower",
		 "Index": "1", "Priority": "2", "Total Attended": "5", "Active": "TRUE", "Date Joined": "2022-01-01"},
		{"id": "3", "Name": "Inactive Gamma", "Display Name": "Gamma", "Email Address": "gamma@test.com", "Role": "Leader",
		 "Index": "2", "Priority": "3", "Total Attended": "2", "Active": "FALSE", "Date Joined": "2022-01-01"},
	]

@pytest.fixture
def peeps_csv_path():
	"""Valid peeps.csv with one inactive and one active peep with blank email."""
	content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Alpha,Alice,alice@test.com,Leader,0,2,3,TRUE,2022-01-01
2,Bob Beta,Bob,bob@test.com,Follower,1,1,5,TRUE,2022-01-01
3,Charlie Gamma,Charlie,,Follower,2,0,2,FALSE,2022-01-01
"""
	with tempfile.NamedTemporaryFile(mode="w", delete=False, newline='') as f:
		f.write(content)
		return f.name

@pytest.fixture
def responses_csv_path():
	"""responses.csv with 3 event rows and 2 responder rows, matching responses_csv_rows() content."""
	content = """Name,Email Address,Primary Role,Secondary Role,Max Sessions,Availability,Min Interval Days,Event Duration,Timestamp
Event: Saturday July 5 - 1pm,,,,,,,120,
Event: Sunday July 6 - 2pm (extra info),,,,,,,90,
Event: Monday July 7 - 11am,,,,,,,60,
Alice Alpha,alice@test.com,Leader,I'm happy to dance my secondary role if it lets me attend when my primary is full,2,"Saturday July 5 - 1pm, Monday July 7 - 11am",0,,"2025-07-01 12:00"
Bob Beta,bob@test.com,Follower,I only want to be scheduled in my primary role,1,"Sunday July 6 - 2pm (extra info)",6,,"2025-07-01 12:01"
"""
	with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as f:
		f.write(content)
		return f.name

@pytest.fixture
def responses_csv_rows():
	"""Three event rows followed by two response rows with different availabilities."""
	return [
		{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "120"},
		{"Name": "Event: Sunday July 6 - 2pm (extra info)", "Event Duration": "90"},
		{"Name": "Event: Monday July 7 - 11am", "Event Duration": "60"},
		{
			"Name": "Alice Alpha",
			"Email Address": "alice@test.com",
			"Primary Role": "Leader",
			"Secondary Role": "I'm happy to dance my secondary role if it lets me attend when my primary is full",
			"Max Sessions": "2",
			"Availability": "Saturday July 5 - 1pm, Monday July 7 - 11am",
			"Min Interval Days": "0",
			"Timestamp": "2025-07-01 12:00"
		},
		{
			"Name": "Bob Beta",
			"Email Address": "bob@test.com",
			"Primary Role": "Follower",
			"Secondary Role": "I only want to be scheduled in my primary role",
			"Max Sessions": "1",
			"Availability": "Sunday July 6 - 2pm (extra info)",
			"Min Interval Days": "6",
			"Timestamp": "2025-07-01 12:01"
		}
	]

@pytest.fixture
def sample_peeps():
	"""Two sample peeps with all fields filled out."""
	return [
		Peep(
			id=1,
			full_name="Alice Alpha",
			display_name="Alice",
			email="alice@test.com",
			role=Role.LEADER,
			index=0,
			priority=1,
			total_attended=3,
			active=True,
			date_joined="2022-01-01"
		),
		Peep(
			id=2,
			full_name="Bob Beta",
			display_name="Bob",
			email="bob@test.com",
			role=Role.FOLLOWER,
			index=1,
			priority=2,
			total_attended=5,
			active=True,
			date_joined="2022-01-01"
		)
	]

# ============================================================================
# TEST CLASSES
# ============================================================================

class TestCSVLoading:
	"""Tests for CSV file loading and validation."""

	def test_load_csv_success_with_required_columns(self):
		content = "col1,col2,col3\na,b,c\n"
		with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
			tmp.write(content)
			tmp_path = tmp.name

		result = load_csv(tmp_path, required_columns=["col1", "col2"])
		assert isinstance(result, list)
		assert result[0]["col1"] == "a"
		assert result[0]["col3"] == "c"
		os.remove(tmp_path)

	def test_load_csv_raises_on_missing_required_columns(self):
		content = "col1,col2\na,b\n"
		with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
			tmp.write(content)
			tmp_path = tmp.name

		with pytest.raises(ValueError):
			load_csv(tmp_path, required_columns=["col1", "col3"])
		os.remove(tmp_path)

	def test_load_csv_strips_whitespace_from_fields(self, tmp_path):
		path = tmp_path / "trim.csv"
		path.write_text(" Name , Role \n Alice , Follow \n Bob , Lead \n")
		rows = load_csv(path)
		assert rows[0]["Name"] == "Alice"
		assert rows[0]["Role"] == "Follow"
		assert rows[1]["Name"] == "Bob"
		assert rows[1]["Role"] == "Lead"

	def test_load_csv_sanitizes_curly_quotes(self, tmp_path):
		"""Test that curly quotes are converted to straight quotes."""
		path = tmp_path / "quotes.csv"
		# Using curly quotes: \u2018 \u2019 (single) \u201c \u201d (double)
		content = "Name,Description\nAlice,It\u2019s a test\nBob,He said \u201chello\u201d\n"
		path.write_text(content, encoding='utf-8')
		rows = load_csv(path)
		assert rows[0]["Description"] == "It's a test"  # Curly ' → straight '
		assert rows[1]["Description"] == 'He said "hello"'  # Curly " " → straight "

	def test_load_csv_normalizes_multiple_spaces(self, tmp_path):
		"""Test that multiple spaces are normalized to single space."""
		path = tmp_path / "spaces.csv"
		# Double spaces in data
		path.write_text("Name,Location\nAlice,New  York\nBob,Los   Angeles\n")
		rows = load_csv(path)
		assert rows[0]["Location"] == "New York"  # Double space → single
		assert rows[1]["Location"] == "Los Angeles"  # Triple space → single

	def test_load_csv_sanitizes_mixed_formatting(self, tmp_path):
		"""Test that curly quotes and multiple spaces are both sanitized."""
		path = tmp_path / "mixed.csv"
		# Combination of curly quotes and multiple spaces
		content = "Name,Event\nAlice,Friday January  9th - 5:30pm to 7pm\nBob,It\u2019s  available\n"
		path.write_text(content, encoding='utf-8')
		rows = load_csv(path)
		assert rows[0]["Event"] == "Friday January 9th - 5:30pm to 7pm"  # Double space → single
		assert rows[1]["Event"] == "It's available"  # Curly ' → straight ', double space → single


class TestJSONOperations:
	"""Tests for JSON loading, saving, and serialization."""

	def test_load_json_file_not_found(self, tmp_path):
		"""load_json should return None if file doesn't exist."""
		nonexistent_file = tmp_path / "missing.json"
		assert load_json(nonexistent_file) is None

	def test_save_json_serializes_dates(self, tmp_path):
		"""Test save_json handles datetime.date, datetime.datetime, and fallback types."""
		data = {
			"today": datetime.date(2025, 7, 21),
			"now": datetime.datetime(2025, 7, 21, 15, 0),
			"fallback": {"custom": "data"}
		}
		out_path = tmp_path / "dates.json"
		save_json(data, out_path)

		loaded = json.loads(out_path.read_text())
		assert loaded["today"] == "2025-07-21"
		assert "2025" in loaded["now"]
		assert isinstance(loaded["fallback"], dict)

	def test_save_json_serializes_enum(self, tmp_path):
		"""Ensure save_json uses .value for Enums."""
		data = {"role": Role.LEADER}
		out_path = tmp_path / "enum.json"
		save_json(data, out_path)

		result = json.loads(out_path.read_text())
		assert result["role"] == "leader"

	def test_save_json_fallback_str_for_unknown_type(self, tmp_path):
		"""Ensure save_json falls back to str(obj) for unknown types like set."""
		path = tmp_path / "fallback.json"
		data = {"example": {1, 2, 3}}  # sets are not JSON serializable by default
		save_json(data, path)

		with open(path, "r") as f:
			contents = json.load(f)

		# The set should have been stringified like "{1, 2, 3}"
		assert contents["example"].startswith("{") and "1" in contents["example"]


class TestTimeDateParsing:
	"""Tests for time range and event date parsing functions."""

	def test_parse_event_date_strips_parentheticals_and_formats(self):
		"""Test parsing old format with parenthetical content."""
		raw_date = "Saturday July 5 - 1pm (extra info)"
		event_id, duration, display_name = parse_event_date(raw_date)
		assert event_id == f"{datetime.datetime.now().year}-07-05 13:00"
		assert duration is None  # Old format doesn't have duration
		assert display_name == "Saturday July 5 - 1pm"

	def test_parse_event_date_invalid_format_raises(self):
		"""Test that invalid date format raises ValueError."""
		with pytest.raises(ValueError):
			parse_event_date("not a real date")

	def test_parse_time_range_basic(self):
		"""Test parsing basic time ranges."""
		start, end, duration = parse_time_range("5pm to 6:30pm")
		assert start == "17:00"
		assert end == "18:30"
		assert duration == 90

	def test_parse_time_range_with_minutes(self):
		"""Test parsing time range with minutes in start time."""
		start, end, duration = parse_time_range("5:30pm to 7pm")
		assert start == "17:30"
		assert end == "19:00"
		assert duration == 90

	def test_parse_time_range_afternoon(self):
		"""Test parsing afternoon time range."""
		start, end, duration = parse_time_range("4pm to 5:30pm")
		assert start == "16:00"
		assert end == "17:30"
		assert duration == 90

	def test_parse_time_range_two_hours(self):
		"""Test parsing 2-hour time range."""
		start, end, duration = parse_time_range("4pm to 6pm")
		assert start == "16:00"
		assert end == "18:00"
		assert duration == 120

	def test_parse_time_range_invalid_format(self):
		"""Test that invalid format raises ValueError."""
		with pytest.raises(ValueError, match="Invalid time range format"):
			parse_time_range("5pm - 6pm")  # Wrong separator

	def test_parse_time_range_invalid_time(self):
		"""Test that invalid time format raises ValueError."""
		with pytest.raises(ValueError, match="Time out of range"):
			parse_time_range("25pm to 6pm")  # Invalid hour

	def test_parse_time_range_end_before_start(self):
		"""Test that end time before start time raises ValueError."""
		with pytest.raises(ValueError, match="End time must be after start time"):
			parse_time_range("6pm to 5pm")

	def test_parse_event_date_with_time_range(self):
		"""Test parsing new format with time range."""
		date_str = "Friday January 9th - 5:30pm to 7pm"
		event_id, duration, display_name = parse_event_date(date_str)

		assert event_id == f"{datetime.datetime.now().year}-01-09 17:30"
		assert duration == 90
		assert display_name == date_str

	def test_parse_event_date_with_time_range_saturday(self):
		"""Test parsing Saturday event with time range."""
		date_str = "Saturday January 10th - 4pm to 5:30pm"
		event_id, duration, display_name = parse_event_date(date_str)

		assert event_id == f"{datetime.datetime.now().year}-01-10 16:00"
		assert duration == 90
		assert display_name == date_str

	def test_parse_event_date_with_time_range_two_hours(self):
		"""Test parsing event with 2-hour duration."""
		date_str = "Saturday January 17th - 4pm to 6pm"
		event_id, duration, display_name = parse_event_date(date_str)

		assert event_id == f"{datetime.datetime.now().year}-01-17 16:00"
		assert duration == 120
		assert display_name == date_str

	def test_parse_event_date_ordinal_suffixes(self):
		"""Test that ordinal suffixes (st, nd, rd, th) are handled correctly."""
		test_cases = [
			("Friday January 1st - 5pm to 6:30pm", "01-01"),
			("Saturday January 2nd - 4pm to 6pm", "01-02"),
			("Sunday January 3rd - 3pm to 5pm", "01-03"),
			("Monday January 9th - 5pm to 6:30pm", "01-09"),
			("Tuesday January 10th - 5pm to 6:30pm", "01-10"),
			("Wednesday January 17th - 5pm to 6:30pm", "01-17"),
			("Thursday January 24th - 4pm to 5:30pm", "01-24"),
		]

		year = datetime.datetime.now().year
		for date_str, expected_date in test_cases:
			event_id, duration, display_name = parse_event_date(date_str)
			assert expected_date in event_id, f"Failed for {date_str}"
			assert duration is not None, f"Duration missing for {date_str}"

	def test_parse_event_date_old_format_backward_compatibility(self):
		"""Test that old format without time range still works."""
		date_str = "Saturday July 5 - 1pm"
		event_id, duration, display_name = parse_event_date(date_str)

		assert event_id == f"{datetime.datetime.now().year}-07-05 13:00"
		assert duration is None
		assert display_name == "Saturday July 5 - 1pm"

	def test_parse_event_date_old_format_with_ordinal(self):
		"""Test old format with ordinal suffix."""
		date_str = "Saturday July 5th - 1pm"
		event_id, duration, display_name = parse_event_date(date_str)

		assert event_id == f"{datetime.datetime.now().year}-07-05 13:00"
		assert duration is None

	def test_parse_event_date_with_custom_year_new_format(self):
		"""Test that custom year parameter works with new time range format."""
		date_str = "Friday January 9th - 5:30pm to 7pm"
		event_id, duration, display_name = parse_event_date(date_str, year=2026)

		assert event_id == "2026-01-09 17:30"
		assert duration == 90
		assert display_name == date_str

	def test_parse_event_date_with_custom_year_old_format(self):
		"""Test that custom year parameter works with old backward compatibility format."""
		date_str = "Saturday July 5 - 1pm"
		event_id, duration, display_name = parse_event_date(date_str, year=2026)

		assert event_id == "2026-07-05 13:00"
		assert duration is None
		assert display_name == "Saturday July 5 - 1pm"

	def test_parse_event_date_year_defaults_to_current_year(self):
		"""Test that when year parameter is None, defaults to current year."""
		date_str = "Monday January 12th - 5pm to 6:30pm"
		event_id, duration, display_name = parse_event_date(date_str, year=None)

		current_year = datetime.datetime.now().year
		assert event_id == f"{current_year}-01-12 17:00"
		assert duration == 90


class TestEventExtraction:
	"""Tests for event extraction from responses (old Event rows + new auto-derive)."""

	def test_extract_events(self, responses_csv_rows):
		"""Ensure event rows are parsed and assigned correct durations and IDs."""
		events = extract_events(responses_csv_rows)
		assert len(events) == 3
		assert sorted(e.duration_minutes for e in events.values()) == [60, 90, 120]

	def test_extract_events_auto_derive_from_availability(self):
		"""Test auto-deriving events from availability strings when no Event rows exist."""
		rows = [
			{
				"Name": "Alice",
				"Email Address": "alice@test.com",
				"Availability": "Friday January 9th - 5:30pm to 7pm, Saturday January 10th - 4pm to 5:30pm"
			},
			{
				"Name": "Bob",
				"Email Address": "bob@test.com",
				"Availability": "Friday January 9th - 5:30pm to 7pm, Saturday January 17th - 4pm to 6pm"
			}
		]

		events = extract_events(rows)
		assert len(events) == 3  # Three unique events

		# Check that events have correct durations
		durations = sorted([e.duration_minutes for e in events.values()])
		assert durations == [90, 90, 120]

	def test_extract_events_auto_derive_validates_consistency(self):
		"""Test that inconsistent durations for same event logs warning and skips."""
		rows = [
			{
				"Name": "Alice",
				"Availability": "Friday January 9th - 5:30pm to 7pm"  # 90 min
			},
			{
				"Name": "Bob",
				"Availability": "Friday January 9th - 5:30pm to 6:30pm"  # 60 min (different but valid!)
			}
		]

		# Should log warning for inconsistency and skip Bob's event
		events = extract_events(rows)

		# Only one event should be created (Alice's), Bob's is skipped due to inconsistency
		assert len(events) == 1
		assert list(events.values())[0].duration_minutes == 90

	def test_extract_events_auto_derive_requires_time_range(self):
		"""Test that auto-derive mode requires time ranges in availability."""
		rows = [
			{
				"Name": "Alice",
				"Availability": "Friday January 9th - 5pm"  # No time range!
			}
		]

		# Should log warning and skip, resulting in empty event map
		events = extract_events(rows)
		assert len(events) == 0

	def test_extract_events_auto_derive_validates_duration_in_config(self):
		"""Test that auto-derived durations must be in CLASS_CONFIG."""
		rows = [
			{
				"Name": "Alice",
				"Availability": "Friday January 9th - 5pm to 10pm"  # 300 minutes, not in CLASS_CONFIG
			}
		]

		# Should log warning and skip invalid duration
		events = extract_events(rows)
		assert len(events) == 0

	def test_extract_events_missing_name_ignored(self):
		"""Blank or missing 'Name' fields in rows should be ignored silently."""
		rows = [{"Event Duration": "120"}, {"Name": "", "Event Duration": "90"}]
		assert extract_events(rows) == {}

	def test_extract_events_malformed_name_raises(self):
		"""Malformed event name without date should raise ValueError."""
		rows = [{"Name": "Event:", "Event Duration": "120"}]
		with pytest.raises(ValueError, match="missing date"):
			extract_events(rows)

	def test_extract_events_invalid_duration_raises(self):
		"""Non-integer Event Duration should raise."""
		rows = [{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "abc"}]
		with pytest.raises(ValueError, match="Invalid Event Duration"):
			extract_events(rows)

	def test_extract_events_missing_duration_raises(self):
		"""Missing Event Duration field should raise."""
		rows = [{"Name": "Event: Saturday July 5 - 1pm"}]
		with pytest.raises(ValueError, match="Missing Event Duration"):
			extract_events(rows)

	def test_extract_events_duration_not_in_config_raises(self):
		"""Duration not listed in CLASS_CONFIG should raise ValueError."""
		rows = [{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "666"}]
		with pytest.raises(ValueError, match="Duration 666 not in CLASS_CONFIG"):
			extract_events(rows)


class TestPeepLoading:
	"""Tests for peep loading and validation."""

	def test_load_peeps(self, peeps_csv_path):
		"""Check that peeps load correctly and inactive peep with blank email is allowed."""
		peeps = load_peeps(peeps_csv_path)
		assert len(peeps) == 3
		assert peeps[0].full_name == "Alice Alpha"
		assert peeps[0].email == "alice@test.com"
		assert peeps[2].active is False
		assert peeps[2].email == ""

	def test_missing_email_for_active_peep_raises(self):
		"""Active peep with blank email should raise a ValueError."""
		content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Alpha,Alice,,Leader,0,1,3,TRUE,2022-01-01
"""
		with tempfile.NamedTemporaryFile(mode="w", delete=False, newline='') as f:
			f.write(content)
			f.flush()
			with pytest.raises(ValueError, match="missing an email"):
				load_peeps(f.name)

	def test_duplicate_email_raises(self):
		"""Duplicate email among active peeps should raise a ValueError."""
		content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice,Alice,duplicate@test.com,Leader,0,1,1,TRUE,2022-01-01
2,Bob,Bob,duplicate@test.com,Follower,1,2,2,TRUE,2022-01-01
"""
		with tempfile.NamedTemporaryFile(mode="w", delete=False, newline='') as f:
			f.write(content)
			f.flush()
			with pytest.raises(ValueError, match="Duplicate email"):
				load_peeps(f.name)

	def test_load_peeps_allows_inactive_peep_with_email(self, tmp_path):
		"""Inactive peeps with non-blank emails should be allowed without error."""
		csv_path = tmp_path / "peeps.csv"
		csv_path.write_text(
			"id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined\n"
			"1,Alice,Alice,alice@test.com,Leader,0,1,3,TRUE,2022-01-01\n"
			"2,Inactive,Inactive,inactive@test.com,Follower,1,2,2,FALSE,2022-01-01\n"
		)
		peeps = load_peeps(csv_path)
		assert len(peeps) == 2
		assert peeps[1].active is False
		assert peeps[1].email == "inactive@test.com"


class TestResponseProcessing:
	"""Tests for processing response data and updating peeps."""

	def test_process_responses(self, peeps_csv_path, responses_csv_rows):
		"""Ensure peeps are updated with correct role, preferences, and availability."""
		peeps = load_peeps(peeps_csv_path)
		event_map = extract_events(responses_csv_rows)
		updated_peeps, responses = process_responses(responses_csv_rows, peeps, event_map)

		assert len(responses) == 2
		peep_map = {p.email: p for p in updated_peeps if p.email}

		alice = peep_map["alice@test.com"]
		assert alice.role == Role.LEADER
		assert alice.switch_pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
		assert alice.availability == [0, 2]

		bob = peep_map["bob@test.com"]
		assert bob.role == Role.FOLLOWER
		assert bob.availability == [1]

	def test_response_from_inactive_peep_raises(self, valid_peeps_rows, responses_csv_rows):
		"""Test that response from inactive peep raises error."""
		peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
		responses_csv_rows = responses_csv_rows.copy()
		responses_csv_rows.append({
			"Name": "Inactive Gamma",
			"Email Address": "gamma@test.com",
			"Primary Role": "Leader",
			"Secondary Role": "I only want to be scheduled in my primary role",
			"Max Sessions": "2",
			"Availability": "Sunday July 6 - 2pm (extra info)",
			"Min Interval Days": "0",
			"Timestamp": "2025-07-01 12:01"
		})
		event_map = extract_events(responses_csv_rows)
		with pytest.raises(ValueError, match="Response from inactive peep: Inactive Gamma"):
			updated_peeps, responses = process_responses(responses_csv_rows, peeps, event_map)

	def test_unknown_event_in_availability_logs_warning(self, peeps_csv_path):
		"""Availability listing a date that wasn't defined in Event rows should log warning and skip."""
		rows = [
			{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "120"},
			{
				"Name": "Alice Alpha",
				"Email Address": "alice@test.com",
				"Primary Role": "Leader",
				"Secondary Role": "I'm happy to dance my secondary role if it lets me attend when my primary is full",
				"Max Sessions": "2",
				"Availability": "Saturday July 5 - 1pm, Monday July 7 - 11am",  # Monday not in events
				"Min Interval Days": "0",
				"Timestamp": "2025-07-01 12:00"
			}
		]
		peeps = load_peeps(peeps_csv_path)
		event_map = extract_events(rows)

		# Should not raise, just log warning and skip unknown event
		updated_peeps, responses = process_responses(rows, peeps, event_map)

		# Alice should only have availability for the known event (Saturday July 5)
		alice = next(p for p in updated_peeps if p.email == "alice@test.com")
		assert len(alice.availability) == 1  # Only Saturday event, Monday skipped

	def test_process_responses_missing_name_skipped(self, valid_peeps_rows):
		"""Row missing name should be skipped without error."""
		peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
		rows = [{"Name": "", "Email Address": "x", "Primary Role": "Leader", "Secondary Role": "NONE",
				 "Max Sessions": "1", "Availability": "July 5 - 1pm", "Timestamp": "2025-07-01 12:00"}]
		assert process_responses(rows, peeps, {})[1] == []

	def test_process_responses_missing_email_raises(self, valid_peeps_rows):
		"""Blank email in active response row should raise ValueError."""
		peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
		rows = [{"Name": "Alice", "Email Address": "", "Primary Role": "Leader", "Secondary Role": "NONE",
				 "Max Sessions": "1", "Availability": "July 5 - 1pm", "Timestamp": "2025-07-01 12:00"}]
		with pytest.raises(ValueError, match="Missing email"):
			process_responses(rows, peeps, {})

	def test_process_responses_unknown_email_raises(self, valid_peeps_rows):
		"""Email not matching any peep should raise."""
		peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
		rows = [{"Name": "Unknown", "Email Address": "notfound@test.com", "Primary Role": "Leader",
				 "Secondary Role": "NONE", "Max Sessions": "1", "Availability": "July 5 - 1pm",
				 "Timestamp": "2025-07-01 12:00"}]
		with pytest.raises(ValueError, match="No matching peep found for email"):
			process_responses(rows, peeps, {})


class TestDataSaving:
	"""Tests for saving peeps, sequences, and events."""

	def test_save_peeps_csv(self, sample_peeps):
		"""Ensure save_peeps_csv writes correct rows and creates file."""
		with tempfile.TemporaryDirectory() as tmpdir:
			filename = os.path.join(tmpdir, "members.csv")
			save_peeps_csv(sample_peeps, filename)

			output_path = os.path.join(tmpdir, "members_updated.csv")
			assert os.path.exists(output_path)

			with open(output_path) as f:
				lines = f.readlines()
				assert lines[0].startswith("id,Name,Display Name")  # Header
				assert "Alice Alpha" in lines[1]
				assert "Bob Beta" in lines[2]

	def test_save_event_sequence(self, tmp_path):
		"""Test saving an EventSequence to JSON."""
		# Step 1: Create peeps
		peep1 = Peep(id=1, full_name="Alice", display_name="Alice", email="alice@example.com", role=Role.LEADER, index=0, priority=1)
		peep2 = Peep(id=2, full_name="Bob", display_name="Bob", email="bob@example.com", role=Role.FOLLOWER, index=1, priority=2)

		# Step 2: Create events
		event1 = Event(id=0, date=datetime.datetime(2025, 7, 5, 13), duration_minutes=120)
		event2 = Event(id=1, date=datetime.datetime(2025, 7, 6, 14), duration_minutes=90)

		# Step 3: Add attendees
		event1.add_attendee(peep1, Role.LEADER)
		event1.add_attendee(peep2, Role.FOLLOWER)
		event2.add_attendee(peep1, Role.LEADER)

		# Step 4: Build EventSequence
		peeps = [peep1, peep2]
		events = [event1, event2]
		sequence = EventSequence(events, peeps)
		sequence.valid_events = events

		# Step 5: Save to temp file
		output_path = tmp_path / "sequence.json"
		save_event_sequence(sequence, output_path)

		# Step 6: Verify file contents
		with open(output_path) as f:
			data = json.load(f)

		assert "valid_events" in data
		assert "peeps" in data
		valid_events = data["valid_events"]
		assert len(valid_events) == 2
		assert any(e["id"] == 0 for e in valid_events)
		assert any("attendees" in e for e in valid_events)
		assert len(data["peeps"]) == 2


class TestIntegration:
	"""End-to-end integration tests."""

	def test_convert_to_json_and_load_data_from_json_roundtrip(self, peeps_csv_path, responses_csv_path):
		"""Test full roundtrip from CSVs -> output.json -> object loading."""
		with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
			output_json_path = f.name

		convert_to_json(responses_csv_path, peeps_csv_path, output_json_path)
		loaded_peeps, loaded_events = load_data_from_json(output_json_path)

		assert len(loaded_peeps) == 3  # includes inactive peep
		assert len(loaded_events) == 3
		assert loaded_events[0].duration_minutes == 120

		# Alice
		alice = next(p for p in loaded_peeps if p.email == "alice@test.com")
		assert alice.role == Role.LEADER
		assert alice.switch_pref == SwitchPreference.SWITCH_IF_PRIMARY_FULL
		assert alice.availability == [0,2]
		assert alice.responded is True

		# Bob
		bob = next(p for p in loaded_peeps if p.email == "bob@test.com")
		assert bob.role == Role.FOLLOWER
		assert bob.switch_pref == SwitchPreference.PRIMARY_ONLY
		assert bob.availability == [1]
		assert bob.responded is True


class TestEmailNormalization:
	"""Tests for email normalization (Gmail dot handling)."""

	def test_normalize_email_gmail_removes_dots(self):
		"""Gmail addresses should have dots removed from local part."""
		assert normalize_email("john.smith@gmail.com") == "johnsmith@gmail.com"
		assert normalize_email("John.Smith@Gmail.COM") == "johnsmith@gmail.com"

	def test_normalize_email_non_gmail_preserves_dots(self):
		"""Non-Gmail addresses should keep dots in local part."""
		assert normalize_email("john.smith@outlook.com") == "john.smith@outlook.com"
		assert normalize_email("user.name@company.com") == "user.name@company.com"

	def test_normalize_email_empty_and_none(self):
		"""Empty string and None should return empty string."""
		assert normalize_email("") == ""
		assert normalize_email(None) == ""

	def test_gmail_dot_matching_integration(self):
		"""End-to-end: Gmail response with dots should match peep without dots."""
		peeps_content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,John Smith,John,johnsmith@gmail.com,Leader,0,1,1,TRUE,2022-01-01
"""
		responses_rows = [{
			"Name": "John Smith",
			"Email Address": "john.smith@gmail.com",  # Has dots
			"Primary Role": "Leader",
			"Secondary Role": "I only want to be scheduled in my primary role",
			"Max Sessions": "1",
			"Availability": "",
			"Min Interval Days": "0",
			"Timestamp": "2025-07-01 12:00"
		}]

		with tempfile.NamedTemporaryFile(mode="w", delete=False, newline='') as f:
			f.write(peeps_content)
			f.flush()
			tmp_name = f.name

		peeps = load_peeps(tmp_name)
		os.remove(tmp_name)

		# Should match despite different dots
		updated_peeps, responses = process_responses(responses_rows, peeps, {})
		assert updated_peeps[0].responded is True


class TestCancellations:
	"""Tests for loading cancellations.json (events + availability)."""

	def test_load_cancellations_returns_parsed_event_ids(self, tmp_path):
		"""Test loading valid cancellations.json returns parsed event_id strings.

		Event strings are parsed immediately and returned as event_ids in format:
		"YYYY-MM-DD HH:MM"
		"""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [
				"Saturday March 1 - 5pm to 6pm",
				"Friday March 7 - 5:30pm to 7pm"
			],
			"cancelled_availability": [
				{
					"email": "alex@example.com",
					"events": [
						"Saturday March 1 - 5pm to 6pm"
					]
				}
			],
			"notes": "Instructor unavailable"
		}))

		cancelled_event_ids, cancelled_availability = load_cancellations(str(cancelled_file), year=2025)

		assert isinstance(cancelled_event_ids, set)
		assert len(cancelled_event_ids) == 2
		# Event IDs should be parsed to format "YYYY-MM-DD HH:MM"
		assert "2025-03-01 17:00" in cancelled_event_ids
		assert "2025-03-07 17:30" in cancelled_event_ids
		assert cancelled_availability["alex@example.com"] == {"2025-03-01 17:00"}

	def test_load_cancellations_invalid_event_string(self, tmp_path):
		"""Test that unparseable event string raises exception with meaningful message.

		Configuration error - user specified event string that cannot be parsed.
		"""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [
				"Invalid Event String"
			],
			"cancelled_availability": [],
			"notes": "Bad format"
		}))

		with pytest.raises(ValueError, match="invalid event format|Cannot parse|unparseable"):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_requires_year_parameter(self, tmp_path):
		"""Test that year parameter is required for proper event_id parsing."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [
				"Saturday March 1 - 5pm to 6pm"
			],
			"cancelled_availability": [
				{
					"email": "alex@example.com",
					"events": ["Friday March 7 - 5:30pm to 7pm"]
				}
			]
		}))

		cancelled_event_ids, cancelled_availability = load_cancellations(str(cancelled_file), year=2026)

		# Event ID should use provided year
		assert "2026-03-01 17:00" in cancelled_event_ids
		assert "2026-03-07 17:30" in cancelled_availability["alex@example.com"]
		assert "2025-03-01 17:00" not in cancelled_event_ids

	def test_load_cancellations_file_not_found(self, tmp_path):
		"""Test file doesn't exist returns empty set (backward compatible)."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_event_ids, cancelled_availability = load_cancellations(str(cancelled_file), year=2025)

		assert isinstance(cancelled_event_ids, set)
		assert len(cancelled_event_ids) == 0
		assert cancelled_availability == {}

	def test_load_cancellations_empty_array(self, tmp_path):
		"""Test empty cancellation arrays return empty structures."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [],
			"cancelled_availability": [],
			"notes": "None cancelled"
		}))

		cancelled_event_ids, cancelled_availability = load_cancellations(str(cancelled_file), year=2025)

		assert isinstance(cancelled_event_ids, set)
		assert len(cancelled_event_ids) == 0
		assert cancelled_availability == {}

	def test_load_cancellations_malformed_json(self, tmp_path):
		"""Test malformed JSON raises JSONDecodeError (configuration error)."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text("{invalid json")

		# Malformed JSON is a configuration error - should raise
		with pytest.raises(Exception, match="invalid cancellations file"):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_missing_key_raises(self, tmp_path):
		"""Test missing required keys raises error."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": []
		}))

		with pytest.raises(ValueError, match="cancelled_availability"):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_null_value_raises(self, tmp_path):
		"""Test null cancelled_events value raises error."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": None,
			"cancelled_availability": []
		}))

		with pytest.raises(ValueError, match="cancelled_events"):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_mixed_valid_and_invalid_strings(self, tmp_path):
		"""Test that even one invalid event string raises error (fail-fast)."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [
				"Saturday March 1 - 5pm to 6pm",  # Valid
				"Not A Valid Event",               # Invalid
				"Friday March 7 - 5:30pm to 7pm"  # Valid but not processed
			],
			"cancelled_availability": []
		}))

		with pytest.raises(ValueError):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_missing_email_raises(self, tmp_path):
		"""Test cancelled_availability entries require email."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [],
			"cancelled_availability": [
				{"events": ["Saturday March 1 - 5pm to 6pm"]}
			]
		}))

		with pytest.raises(ValueError, match="email"):
			load_cancellations(str(cancelled_file), year=2025)

	def test_load_cancellations_invalid_events_list_raises(self, tmp_path):
		"""Test cancelled_availability entries require events list."""
		cancelled_file = tmp_path / "cancellations.json"
		cancelled_file.write_text(json.dumps({
			"cancelled_events": [],
			"cancelled_availability": [
				{"email": "alex@example.com", "events": "Saturday March 1 - 5pm to 6pm"}
			]
		}))

		with pytest.raises(ValueError, match="events"):
			load_cancellations(str(cancelled_file), year=2025)

class TestPartnershipRequests:
	"""Tests for loading partnership requests from JSON."""

	def test_load_partnerships_file_not_found(self, tmp_path):
		result = load_partnerships(str(tmp_path))
		assert result == {}

	def test_load_partnerships_with_wrapper(self, tmp_path):
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"partnerships": {
				"1": [2, "3"],
				2: [4]
			}
		}))

		result = load_partnerships(str(tmp_path))
		assert result == {1: {2, 3}, 2: {4}}

	def test_load_partnerships_all_ids_valid(self, tmp_path):
		"""Test loading partnerships when all IDs are valid (strict validation)."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": [2, 3],
			"2": [1]
		}))

		result = load_partnerships(str(tmp_path), valid_peep_ids={1, 2, 3})
		assert result == {1: {2, 3}, 2: {1}}

	def test_load_partnerships_requires_mapping(self, tmp_path):
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps(["1", "2"]))

		with pytest.raises(ValueError, match="partnerships.json must map"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_malformed_json_raises(self, tmp_path):
		"""Test that invalid JSON syntax raises an error."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text("{invalid json")

		with pytest.raises(Exception, match="invalid partnerships.json"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_non_list_partners_raises(self, tmp_path):
		"""Test that partner value that is not a list raises an error (strict validation)."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": "2"  # String instead of list
		}))

		with pytest.raises(ValueError, match="must be a list"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_self_partnership_raises(self, tmp_path):
		"""Test that self-partnership requests raise an error (strict validation)."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": [1, 2]  # Includes self (1)
		}))

		with pytest.raises(ValueError, match="cannot partner with themselves"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_unknown_requester_id_raises(self, tmp_path):
		"""Test that unknown requester ID raises an error (strict validation)."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"99": [1, 2]  # 99 is not in valid peeps
		}))

		with pytest.raises(ValueError, match="requester.*not found|unknown requester"):
			load_partnerships(str(tmp_path), valid_peep_ids={1, 2, 3})

	def test_load_partnerships_unknown_partner_id_raises(self, tmp_path):
		"""Test that unknown partner ID raises an error (strict validation)."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": [2, 99]  # 99 is not in valid peeps
		}))

		with pytest.raises(ValueError, match="partner.*not found|unknown.*partner"):
			load_partnerships(str(tmp_path), valid_peep_ids={1, 2, 3})

	def test_load_partnerships_invalid_id_string_raises(self, tmp_path):
		"""Test that non-integer IDs raise an error."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"abc": [1, 2]  # Invalid requester id
		}))

		with pytest.raises(ValueError, match="invalid.*id|must be.*integer"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_invalid_partner_id_string_raises(self, tmp_path):
		"""Test that non-integer partner IDs raise an error."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": ["xyz", 2]  # Invalid partner id
		}))

		with pytest.raises(ValueError, match="invalid.*id|must be.*integer"):
			load_partnerships(str(tmp_path))

	def test_load_partnerships_null_partners_list_raises(self, tmp_path):
		"""Test that null partners list raises an error."""
		import constants

		requests_file = tmp_path / constants.PARTNERSHIPS_FILE
		requests_file.write_text(json.dumps({
			"1": None  # Null
		}))

		with pytest.raises(ValueError, match="partners.*required|cannot be null"):
			load_partnerships(str(tmp_path))

