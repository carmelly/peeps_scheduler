import pytest
import tempfile
import os
import json
import datetime
from file_io import (
	load_data_from_json,
	convert_to_json,
	normalize_role,
	parse_event_date,
	load_csv,
	load_peeps,
	extract_events,
	process_responses,
	save_event_sequence,
	save_peeps_csv,
	load_json, 
	save_json
)
from models import Event, EventSequence, Role, SwitchPreference, Peep

# --- Fixtures ---

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

@pytest.fixture
def sample_events():
	"""Two simple events with different durations and timestamps."""
	return [
		Event(id=0, date="2025-07-05 13:00", duration_minutes=90),
		Event(id=1, date="2025-07-06 14:00", duration_minutes=60)
	]

# --- Peep and Event Loading ---

def test_load_peeps(peeps_csv_path):
	"""Check that peeps load correctly and inactive peep with blank email is allowed."""
	peeps = load_peeps(peeps_csv_path)
	assert len(peeps) == 3
	assert peeps[0].full_name == "Alice Alpha"
	assert peeps[0].email == "alice@test.com"
	assert peeps[2].active is False
	assert peeps[2].email == None

def test_extract_events(responses_csv_rows):
	"""Ensure event rows are parsed and assigned correct durations and IDs."""
	events = extract_events(responses_csv_rows)
	assert len(events) == 3
	assert sorted(e.duration_minutes for e in events.values()) == [60, 90, 120]

def test_normalize_role():
	# canonical values
	assert normalize_role("leader") == "leader"
	assert normalize_role("follower") == "follower"

	# aliases
	assert normalize_role("lead") == "leader"
	assert normalize_role("follow") == "follower"

	# case and whitespace handling
	assert normalize_role(" Leader ") == "leader"
	assert normalize_role("FOLLOW") == "follower"

	# invalid values
	with pytest.raises(ValueError):
		normalize_role("coach")
	with pytest.raises(ValueError):
		normalize_role("")
	with pytest.raises(ValueError):
		normalize_role("leaders")

def test_parse_event_date_strips_parentheticals_and_formats():
	# Includes parenthetical content which should be ignored
	raw_date = "Saturday July 5 - 1pm (extra info)"
	result = parse_event_date(raw_date)
	assert result == f"{datetime.datetime.now().year}-07-05 13:00"

def test_parse_event_date_invalid_format_raises():
	with pytest.raises(ValueError):
		parse_event_date("not a real date")

# --- CSV Loading ---

def test_load_csv_success_with_required_columns():
	content = "col1,col2,col3\na,b,c\n"
	with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
		tmp.write(content)
		tmp_path = tmp.name

	result = load_csv(tmp_path, required_columns=["col1", "col2"])
	assert isinstance(result, list)
	assert result[0]["col1"] == "a"
	assert result[0]["col3"] == "c"
	os.remove(tmp_path)

def test_load_csv_raises_on_missing_required_columns():
	content = "col1,col2\na,b\n"
	with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
		tmp.write(content)
		tmp_path = tmp.name

	with pytest.raises(ValueError):
		load_csv(tmp_path, required_columns=["col1", "col3"])
	os.remove(tmp_path)

def test_load_csv_strips_whitespace_from_fields(tmp_path):
	path = tmp_path / "trim.csv"
	path.write_text(" Name , Role \n Alice , Follow \n Bob , Lead \n")
	rows = load_csv(path)
	assert rows[0]["Name"] == "Alice"
	assert rows[0]["Role"] == "Follow"
	assert rows[1]["Name"] == "Bob"
	assert rows[1]["Role"] == "Lead"

# --- JSON Loading / Saving ---

def test_load_json_file_not_found(tmp_path):
	"""load_json should return None if file doesn't exist."""
	nonexistent_file = tmp_path / "missing.json"
	assert load_json(nonexistent_file) is None

def test_save_json_serializes_dates(tmp_path):
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

def test_save_json_serializes_enum(tmp_path):
	"""Ensure save_json uses .value for Enums."""
	data = {"role": Role.LEADER}
	out_path = tmp_path / "enum.json"
	save_json(data, out_path)

	result = json.loads(out_path.read_text())
	assert result["role"] == "leader"

def test_save_json_fallback_str_for_unknown_type(tmp_path):
	"""Ensure save_json falls back to str(obj) for unknown types like set."""
	path = tmp_path / "fallback.json"
	data = {"example": {1, 2, 3}}  # sets are not JSON serializable by default
	save_json(data, path)

	with open(path, "r") as f:
		contents = json.load(f)
	
	# The set should have been stringified like "{1, 2, 3}"
	assert contents["example"].startswith("{") and "1" in contents["example"]

# ---  Response Processing Logic ---

def test_process_responses(peeps_csv_path, responses_csv_rows):
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

def test_response_from_inactive_peep_raises(valid_peeps_rows, responses_csv_rows): 
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

# ---  Roundtrip + End-to-End Integration Tests ---

def test_convert_to_json_and_load_data_from_json_roundtrip(peeps_csv_path, responses_csv_path):
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

def test_save_peeps_csv(sample_peeps):
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

def test_save_event_sequence(tmp_path):
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

# ---  Edge Case and Error Handling ---

def test_missing_email_for_active_peep_raises():
	"""Active peep with blank email should raise a ValueError."""
	content = """id,Name,Display Name,Email Address,Role,Index,Priority,Total Attended,Active,Date Joined
1,Alice Alpha,Alice,,Leader,0,1,3,TRUE,2022-01-01
"""
	with tempfile.NamedTemporaryFile(mode="w", delete=False, newline='') as f:
		f.write(content)
		f.flush()
		with pytest.raises(ValueError, match="missing an email"):
			load_peeps(f.name)

def test_duplicate_email_raises():
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

def test_load_peeps_allows_inactive_peep_with_email(tmp_path):
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

def test_unknown_event_in_availability_raises(peeps_csv_path):
	"""Availability listing a date that wasn't defined in Event rows should raise."""
	rows = [
		{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "120"},
		{
			"Name": "Alice Alpha",
			"Email Address": "alice@test.com",
			"Primary Role": "Leader",
			"Secondary Role": "I'm happy to dance my secondary role if it lets me attend when my primary is full",
			"Max Sessions": "2",
			"Availability": "Monday July 7 - 11am",
			"Timestamp": "2025-07-01 12:00"
		}
	]
	peeps = load_peeps(peeps_csv_path)
	event_map = extract_events(rows)

	with pytest.raises(ValueError, match="unknown event"):
		process_responses(rows, peeps, event_map)

def test_extract_events_missing_name_ignored():
	"""Blank or missing 'Name' fields in rows should be ignored silently."""
	rows = [{"Event Duration": "120"}, {"Name": "", "Event Duration": "90"}]
	assert extract_events(rows) == {}

def test_extract_events_malformed_name_raises():
	"""Malformed event name without date should raise ValueError."""
	rows = [{"Name": "Event:", "Event Duration": "120"}]
	with pytest.raises(ValueError, match="missing date"):
		extract_events(rows)

def test_extract_events_invalid_duration_raises():
	"""Non-integer Event Duration should raise."""
	rows = [{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "abc"}]
	with pytest.raises(ValueError, match="Invalid Event Duration"):
		extract_events(rows)

def test_extract_events_missing_duration_raises():
	"""Missing Event Duration field should raise."""
	rows = [{"Name": "Event: Saturday July 5 - 1pm"}]
	with pytest.raises(ValueError, match="Missing Event Duration"):
		extract_events(rows)

def test_extract_events_duration_not_in_config_raises():
	"""Duration not listed in CLASS_CONFIG should raise ValueError."""
	rows = [{"Name": "Event: Saturday July 5 - 1pm", "Event Duration": "666"}]  # 999 not in CLASS_CONFIG
	with pytest.raises(ValueError, match="Duration 666 not in CLASS_CONFIG"):
		extract_events(rows)

def test_process_responses_missing_name_skipped(valid_peeps_rows):
	"""Row missing name should be skipped without error."""
	peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
	rows = [{"Name": "", "Email Address": "x", "Primary Role": "Leader", "Secondary Role": "NONE",
			 "Max Sessions": "1", "Availability": "July 5 - 1pm", "Timestamp": "2025-07-01 12:00"}]
	assert process_responses(rows, peeps, {})[1] == []

def test_process_responses_missing_email_raises(valid_peeps_rows):
	"""Blank email in active response row should raise ValueError."""
	peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
	rows = [{"Name": "Alice", "Email Address": "", "Primary Role": "Leader", "Secondary Role": "NONE",
			 "Max Sessions": "1", "Availability": "July 5 - 1pm", "Timestamp": "2025-07-01 12:00"}]
	with pytest.raises(ValueError, match="Missing email"):
		process_responses(rows, peeps, {})

def test_process_responses_unknown_email_raises(valid_peeps_rows):
	"""Email not matching any peep should raise."""
	peeps = [Peep.from_csv(p) for p in valid_peeps_rows]
	rows = [{"Name": "Unknown", "Email Address": "notfound@test.com", "Primary Role": "Leader",
			 "Secondary Role": "NONE", "Max Sessions": "1", "Availability": "July 5 - 1pm",
			 "Timestamp": "2025-07-01 12:00"}]
	with pytest.raises(ValueError, match="No matching peep found for email"):
		process_responses(rows, peeps, {})



