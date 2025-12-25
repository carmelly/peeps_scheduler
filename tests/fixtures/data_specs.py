"""Shared data specification classes for building test data.

These dataclasses specify test data in a human-readable format that can be
converted to the production file formats (CSV/JSON) used by both CLI tests
and database import tests.

Usage:
    # Specify test data
    events = [
        EventSpec(date="2025-02-07 17:00", attendees=[(1, "Alice", "leader")]),
        EventSpec(date="2025-02-14 17:00", attendees=[(2, "Bob", "follower")])
    ]

    # Convert to production format using file builder fixtures
    results_json_builder(events)  # Creates results.json with production format
"""

from dataclasses import dataclass, field


@dataclass
class EventSpec:
    """Specification for building test event data.

    Attributes:
        date: Event date/time in format "YYYY-MM-DD HH:MM" (e.g., "2025-02-07 17:00")
        duration_minutes: Event duration in minutes (default: 120)
        attendees: List of (id, name, role) tuples for assigned attendees
        alternates: List of (id, name, role) tuples for alternate attendees
        leaders_string: Optional custom leaders string (auto-generated if None)
        followers_string: Optional custom followers string (auto-generated if None)
    """
    date: str
    duration_minutes: int = 120
    attendees: list[tuple[int, str, str]] = field(default_factory=list)
    alternates: list[tuple[int, str, str]] = field(default_factory=list)
    leaders_string: str | None = None
    followers_string: str | None = None


@dataclass
class ResponseSpec:
    """Specification for building test response data (responses.csv format).

    Attributes:
        email: Respondent email address
        name: Respondent name
        role: Primary role ("leader" or "follower")
        availability: List of date strings respondent is available for
        max_sessions: Maximum sessions to schedule (default: 2)
        min_interval_days: Minimum days between sessions (default: 0)
        timestamp: Response timestamp in format "M/D/YYYY HH:MM:SS" (default: "2/1/2025 10:00:00")
        secondary_role: Secondary role preference (default: "I only want to be scheduled in my primary role")
        partnership_preference: Partnership preferences (default: empty string)
        questions_comments: Free-form questions/comments (default: empty string)
    """
    email: str
    name: str
    role: str = "leader"
    availability: list[str] = field(default_factory=list)
    max_sessions: int = 2
    min_interval_days: int = 0
    timestamp: str = "2/1/2025 10:00:00"
    secondary_role: str = "I only want to be scheduled in my primary role"
    partnership_preference: str = ""
    questions_comments: str = ""


@dataclass
class MemberSpec:
    """Specification for building test member data (members.csv format).

    Attributes:
        csv_id: Member ID from CSV (integer)
        name: Full name
        email: Email address (auto-generated from name if None)
        display_name: Display name (auto-generated from name if None)
        role: Member role ("leader" or "follower")
        active: Whether member is active (default: True)
        priority: Priority value (default: 0)
        total_attended: Total sessions attended historically (default: 0)
        index: Scheduling index (default: 0)
        date_joined: Date joined in format "M/D/YYYY" (default: "1/1/2025")
    """
    csv_id: int
    name: str
    email: str | None = None
    display_name: str | None = None
    role: str = "leader"
    active: bool = True
    priority: int = 0
    total_attended: int = 0
    index: int = 0
    date_joined: str = "1/1/2025"

    def __post_init__(self):
        """Auto-generate email and display_name if not provided."""
        if self.email is None:
            # Generate email from name: "Test Member" -> "test.member@test.com"
            email_part = self.name.lower().replace(" ", ".")
            self.email = f"{email_part}@test.com"

        if self.display_name is None:
            # Use name without spaces: "Test Member" -> "TestMember"
            self.display_name = self.name.replace(" ", "")


@dataclass
class AttendanceSpec:
    """Specification for building test attendance data (actual_attendance.json format).

    Similar to EventSpec but represents actual attendance (no alternates,
    only confirmed attendees).

    Attributes:
        date: Event date/time in format "YYYY-MM-DD HH:MM"
        duration_minutes: Event duration in minutes (default: 120)
        attendees: List of (id, name, role) tuples for attendees who showed up
        leaders_string: Optional custom leaders string (auto-generated if None)
        followers_string: Optional custom followers string (auto-generated if None)
    """
    date: str
    duration_minutes: int = 120
    attendees: list[tuple[int, str, str]] = field(default_factory=list)
    leaders_string: str | None = None
    followers_string: str | None = None
