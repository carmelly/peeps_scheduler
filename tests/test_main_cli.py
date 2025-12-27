"""
Comprehensive test suite for main.py CLI entry point.

Tests cover:
- apply_results(): Attendance result application with file validation
  - Required files validation (actual_attendance.json, members.csv)
  - Optional file handling (responses.csv)
  - Success/failure returns
  - Logging behavior

- main(): CLI argument parsing and command routing
  - Command parsing (run, apply-results, availability-report)
  - Help text generation
  - --verbose flag handling
  - DATA_FOLDER environment variable fallback
  - Subcommand routing
  - Error handling for missing arguments
"""

import csv
import json
import logging
import os
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch
import pytest
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.main import apply_results, main


@pytest.fixture
def temp_period_dir(tmp_path):
    """Create a temporary period directory with test files."""
    period_dir = tmp_path / "2025-02"
    period_dir.mkdir()
    return period_dir


@pytest.fixture
def test_attendance_file(temp_period_dir):
    """Create a test actual_attendance.json file."""
    attendance_data = {
        "valid_events": [
            {
                "id": 1,
                "date": "2025-02-07 17:00",
                "duration_minutes": 120,
                "attendees": [
                    {"id": 1, "name": "Member1", "role": "leader"},
                    {"id": 2, "name": "Member2", "role": "follower"}
                ],
                "alternates": []
            }
        ]
    }
    filepath = temp_period_dir / "actual_attendance.json"
    with open(filepath, 'w') as f:
        json.dump(attendance_data, f)
    return filepath


@pytest.fixture
def test_members_file(temp_period_dir):
    """Create a test members.csv file."""
    members_data = [
        {
            'id': '1',
            'Name': 'Member1',
            'Display Name': 'Member1',
            'Email Address': 'member1@test.com',
            'Role': 'leader',
            'Index': '0',
            'Priority': '1',
            'Total Attended': '0',
            'Active': 'TRUE',
            'Date Joined': '1/1/2025'
        },
        {
            'id': '2',
            'Name': 'Member2',
            'Display Name': 'Member2',
            'Email Address': 'member2@test.com',
            'Role': 'follower',
            'Index': '1',
            'Priority': '2',
            'Total Attended': '0',
            'Active': 'TRUE',
            'Date Joined': '1/1/2025'
        }
    ]
    filepath = temp_period_dir / "members.csv"
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=members_data[0].keys())
        writer.writeheader()
        writer.writerows(members_data)
    return filepath


@pytest.fixture
def test_responses_file(temp_period_dir):
    """Create a test responses.csv file."""
    responses_data = [
        {
            'Timestamp': '2/1/2025 10:00:00',
            'Email Address': 'member1@test.com',
            'Name': 'Member1',
            'Primary Role': 'leader',
            'Secondary Role': 'I only want to be scheduled in my primary role',
            'Max Sessions': '2',
            'Availability': 'Friday February 7th - 5pm to 7pm',
            'Event Duration': '',
            'Session Spacing Preference': '',
            'Min Interval Days': '0',
            'Partnership Preference': '',
            'Questions or Comments': ''
        }
    ]
    filepath = temp_period_dir / "responses.csv"
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=responses_data[0].keys())
        writer.writeheader()
        writer.writerows(responses_data)
    return filepath


@pytest.fixture
def mock_logger():
    """Create a mock logger for testing."""
    return Mock(spec=logging.Logger)


@pytest.mark.integration
class TestApplyResults:
    """Test suite for apply_results() function."""

    def test_apply_results_success_with_all_files(
        self, temp_period_dir, test_attendance_file,
        test_members_file, test_responses_file, mock_logger
    ):
        """Test successful apply_results when all files exist."""
        # Mock data_manager to return our test directory
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    mock_apply.return_value = []

                    result = apply_results(
                        "2025-02", results_filename="actual_attendance.json", logger=mock_logger
                    )

        assert result is True
        # Verify logging
        assert mock_logger.info.called
        mock_logger.error.assert_not_called()

    def test_apply_results_missing_attendance_file(
        self, temp_period_dir, test_members_file, mock_logger
    ):
        """Test apply_results returns False when actual_attendance.json is missing."""
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            result = apply_results(
                "2025-02", results_filename="actual_attendance.json", logger=mock_logger
            )

        assert result is False
        mock_logger.error.assert_called_once()
        assert "Actual attendance file not found" in mock_logger.error.call_args[0][0]

    def test_apply_results_missing_members_file(
        self, temp_period_dir, test_attendance_file, mock_logger
    ):
        """Test apply_results returns False when members.csv is missing."""
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            result = apply_results(
                "2025-02",
                results_filename="actual_attendance.json",
                logger=mock_logger
            )

        assert result is False
        mock_logger.error.assert_called_once()
        assert "Members file not found" in mock_logger.error.call_args[0][0]

    def test_apply_results_missing_responses_file_is_optional(
        self, temp_period_dir, test_attendance_file,
        test_members_file, mock_logger
    ):
        """Test apply_results succeeds when responses.csv is missing (optional file)."""
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    mock_apply.return_value = []

                    result = apply_results(
                        "2025-02",
                        results_filename="actual_attendance.json",
                        logger=mock_logger
                    )

        assert result is True
        # Verify warning about missing responses file
        warning_calls = [call for call in mock_logger.warning.call_args_list]
        assert len(warning_calls) > 0
        assert any(
            "Responses file not found" in str(call)
            for call in warning_calls
        )

    def test_apply_results_custom_results_filename(
        self, temp_period_dir, test_members_file, test_attendance_file, mock_logger
    ):
        """Test apply_results with custom results filename."""
        # Create custom results file
        custom_results = temp_period_dir / "custom_results.json"
        custom_results.write_text(json.dumps({"valid_events": []}))

        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    mock_apply.return_value = []

                    result = apply_results(
                        "2025-02",
                        results_filename="custom_results.json",
                        logger=mock_logger
                    )

        assert result is True  # Should succeed with custom results file

    def test_apply_results_uses_provided_logger(
        self, temp_period_dir, test_attendance_file, test_members_file
    ):
        """Test apply_results uses provided logger instance."""
        custom_logger = Mock(spec=logging.Logger)
        dm = get_data_manager()

        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    mock_apply.return_value = []

                    apply_results(
                        "2025-02",
                        logger=custom_logger
                    )

        assert custom_logger.info.called

    def test_apply_results_creates_default_logger_if_none_provided(
        self, temp_period_dir, test_attendance_file, test_members_file
    ):
        """Test apply_results creates a logger when none provided."""
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    with patch('logging.getLogger') as mock_get_logger:
                        mock_logger = Mock()
                        mock_get_logger.return_value = mock_logger
                        mock_apply.return_value = []

                        apply_results("2025-02", logger=None)

        # Should have created a logger with name 'cli'
        mock_get_logger.assert_called_with('cli')

    def test_apply_results_calls_apply_event_results(
        self, temp_period_dir, test_attendance_file,
        test_members_file, test_responses_file, mock_logger
    ):
        """Test apply_results calls utils.apply_event_results with correct files."""
        dm = get_data_manager()
        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results") as mock_apply:
                with patch("peeps_scheduler.file_io.save_peeps_csv"):
                    mock_apply.return_value = []

                    apply_results(
                        "2025-02",
                        results_filename="actual_attendance.json",
                        logger=mock_logger
                    )

        # Verify apply_event_results was called with correct arguments
        assert mock_apply.called
        call_args = mock_apply.call_args
        assert str(temp_period_dir / "actual_attendance.json") in call_args[0]
        assert str(temp_period_dir / "members.csv") in call_args[0]

    def test_apply_results_saves_updated_peeps(
        self, temp_period_dir, test_attendance_file,
        test_members_file, test_responses_file, mock_logger
    ):
        """Test apply_results saves updated peeps to members.csv."""
        dm = get_data_manager()
        updated_peeps = [{"id": 1, "name": "Updated"}]

        with patch.object(dm, 'get_period_path', return_value=str(temp_period_dir)):
            with patch("peeps_scheduler.utils.apply_event_results", return_value=updated_peeps):
                with patch("peeps_scheduler.file_io.save_peeps_csv") as mock_save:
                    apply_results(
                        "2025-02",
                        logger=mock_logger
                    )

        # Verify save_peeps_csv was called with updated peeps
        mock_save.assert_called_once()
        call_args = mock_save.call_args
        assert call_args[0][0] == updated_peeps


@pytest.mark.integration
class TestMainCLI:
    """Test suite for main() CLI entry point."""

    def test_main_run_command_basic(self):
        """Test main() routes 'run' command correctly."""
        test_args = ['main.py', 'run', '--data-folder', '/tmp/test']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify Scheduler was instantiated
        mock_scheduler_class.assert_called_once()
        # Verify run was called
        mock_instance.run.assert_called_once()

    def test_main_run_command_with_verbose_flag(self):
        """Test main() passes --verbose flag to logging setup."""
        test_args = ['main.py', '--verbose', 'run', '--data-folder', '/tmp/test']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler"):
                with patch("peeps_scheduler.utils.setup_logging") as mock_setup_logging:
                    main()

        # Verify setup_logging was called with verbose=True
        mock_setup_logging.assert_called_once_with(verbose=True)

    def test_main_run_command_with_generate_tests_flag(self):
        """Test main() passes --generate-tests flag to scheduler.run()."""
        test_args = ['main.py', 'run', '--data-folder', '/tmp/test', '--generate-tests']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify run was called with generate_test_data=True
        mock_instance.run.assert_called_once()
        assert mock_instance.run.call_args.kwargs['generate_test_data'] is True

    def test_main_run_command_with_load_from_csv_flag(self):
        """Test main() passes --load-from-csv flag to scheduler.run()."""
        test_args = ['main.py', 'run', '--data-folder', '/tmp/test', '--load-from-csv']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify run was called with load_from_csv=True
        mock_instance.run.assert_called_once()
        assert mock_instance.run.call_args.kwargs['load_from_csv'] is True

    def test_main_run_command_with_max_events(self):
        """Test main() passes --max-events to Scheduler."""
        test_args = ['main.py', 'run', '--data-folder', '/tmp/test', '--max-events', '10']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify Scheduler was created with max_events=10
        call_kwargs = mock_scheduler_class.call_args.kwargs
        assert call_kwargs['max_events'] == 10

    def test_main_run_command_with_custom_files(self):
        """Test main() passes custom cancellations and partnerships files."""
        test_args = [
            'main.py', 'run', '--data-folder', '/tmp/test',
            '--cancellations-file', 'custom_cancellations.json',
            '--partnerships-file', 'custom_partnerships.json'
        ]
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify Scheduler was created with custom files
        call_kwargs = mock_scheduler_class.call_args.kwargs
        assert call_kwargs['cancellations_file'] == 'custom_cancellations.json'
        assert call_kwargs['partnerships_file'] == 'custom_partnerships.json'

    def test_main_run_command_requires_data_folder_if_env_not_set(self):
        """Test main() run command requires --data-folder if DATA_FOLDER env var not set."""
        test_args = ['main.py', 'run']
        with patch.object(sys, 'argv', test_args):
            with patch.dict(os.environ, {}, clear=False):
                # Remove DATA_FOLDER if it exists
                if 'DATA_FOLDER' in os.environ:
                    del os.environ['DATA_FOLDER']

                with patch("peeps_scheduler.utils.setup_logging"):
                    # Should fail with SystemExit due to missing required argument
                    with pytest.raises(SystemExit):
                        main()

    def test_main_run_command_uses_data_folder_env_var(self):
        """Test main() uses DATA_FOLDER environment variable."""
        test_args = ['main.py', 'run']
        with patch.object(sys, 'argv', test_args):
            with patch.dict(os.environ, {'DATA_FOLDER': '/env/data'}):
                with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                    mock_instance = Mock()
                    mock_scheduler_class.return_value = mock_instance
                    with patch("peeps_scheduler.utils.setup_logging"):
                        main()

        # Verify Scheduler was created with env var data folder
        call_kwargs = mock_scheduler_class.call_args.kwargs
        assert call_kwargs['data_folder'] == '/env/data'

    def test_main_apply_results_command(self, temp_period_dir):
        """Test main() routes 'apply-results' command correctly."""
        test_args = ['main.py', 'apply-results', '--period-folder', str(temp_period_dir)]
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch("peeps_scheduler.main.apply_results") as mock_apply:
                    mock_apply.return_value = True
                    main()

        # Verify apply_results was called
        mock_apply.assert_called_once()

    def test_main_apply_results_command_with_custom_results_file(self, temp_period_dir):
        """Test main() passes custom results filename to apply_results."""
        test_args = [
            'main.py', 'apply-results',
            '--period-folder', str(temp_period_dir),
            '--results-file', 'custom_results.json'
        ]
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch("peeps_scheduler.main.apply_results") as mock_apply:
                    mock_apply.return_value = True
                    main()

        # Verify apply_results was called with custom filename (second positional arg)
        call_args = mock_apply.call_args
        assert call_args[0][1] == 'custom_results.json'

    def test_main_apply_results_requires_period_folder(self):
        """Test main() apply-results requires --period-folder argument."""
        test_args = ['main.py', 'apply-results']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                # Should fail with SystemExit due to missing required argument
                with pytest.raises(SystemExit):
                    main()

    def test_main_availability_report_command(self):
        """Test main() routes 'availability-report' command correctly."""
        test_args = ['main.py', 'availability-report', '--data-folder', '/tmp/test']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch(
                    "peeps_scheduler.availability_report.run_availability_report"
                ) as mock_report:
                    main()

        # Verify run_availability_report was called
        mock_report.assert_called_once()

    def test_main_availability_report_with_custom_cancellations_file(self):
        """Test main() passes custom cancellations file to availability_report."""
        test_args = [
            'main.py', 'availability-report',
            '--data-folder', '/tmp/test',
            '--cancellations-file', 'custom_cancellations.json'
        ]
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch(
                    "peeps_scheduler.availability_report.run_availability_report"
                ) as mock_report:
                    main()

        # Verify cancellations_file was passed
        call_kwargs = mock_report.call_args.kwargs
        assert call_kwargs['cancellations_file'] == 'custom_cancellations.json'

    def test_main_availability_report_requires_data_folder_if_env_not_set(self):
        """Test main() availability-report requires --data-folder if DATA_FOLDER env not set."""
        test_args = ['main.py', 'availability-report']
        with patch.object(sys, 'argv', test_args):
            with patch.dict(os.environ, {}, clear=False):
                # Remove DATA_FOLDER if it exists
                if 'DATA_FOLDER' in os.environ:
                    del os.environ['DATA_FOLDER']

                with patch("peeps_scheduler.utils.setup_logging"):
                    # Should fail with SystemExit
                    with pytest.raises(SystemExit):
                        main()

    def test_main_availability_report_uses_data_folder_env_var(self):
        """Test main() availability-report uses DATA_FOLDER environment variable."""
        test_args = ['main.py', 'availability-report']
        with patch.object(sys, 'argv', test_args):
            with patch.dict(os.environ, {'DATA_FOLDER': '/env/data'}):
                with patch("peeps_scheduler.utils.setup_logging"):
                    with patch(
                        "peeps_scheduler.availability_report.run_availability_report"
                    ) as mock_report:
                        main()

        # Verify data_folder argument was passed
        call_args = mock_report.call_args[0]
        assert call_args[0] == '/env/data'

    def test_main_prints_help_when_no_command_given(self, capsys):
        """Test main() prints help text when no command is provided."""
        test_args = ['main.py']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                main()

        captured = capsys.readouterr()
        # Help text should contain command descriptions
        assert 'run' in captured.out or 'run' in captured.err
        assert 'apply-results' in captured.out or 'apply-results' in captured.err

    def test_main_prints_help_with_help_flag(self, capsys):
        """Test main() prints help text with --help flag."""
        test_args = ['main.py', '--help']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                # ArgumentParser exits with 0 on --help
                with pytest.raises(SystemExit) as exc_info:
                    main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        # Help should contain usage information
        assert 'usage' in captured.out.lower() or 'Peeps Event Scheduler CLI' in captured.out

    def test_main_verbose_flag_without_command(self):
        """Test main() --verbose flag works without specific command."""
        test_args = ['main.py', '--verbose']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging") as mock_setup:
                main()

        # Should still set up logging with verbose=True
        mock_setup.assert_called_once_with(verbose=True)

    def test_main_passes_logger_to_apply_results(self, temp_period_dir):
        """Test main() passes configured logger to apply_results."""
        test_args = ['main.py', 'apply-results', '--period-folder', str(temp_period_dir)]
        mock_logger = Mock(spec=logging.Logger)

        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging", return_value=mock_logger):
                with patch("peeps_scheduler.main.apply_results") as mock_apply:
                    mock_apply.return_value = True
                    main()

        # Verify logger was passed to apply_results
        call_kwargs = mock_apply.call_args[1]
        assert call_kwargs['logger'] == mock_logger

    def test_main_handles_invalid_command(self):
        """Test main() handles invalid command gracefully."""
        test_args = ['main.py', 'invalid-command']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                # Invalid command should exit with error
                with pytest.raises(SystemExit):
                    main()

    def test_main_run_all_default_values(self):
        """Test main() run command with all default argument values."""
        test_args = ['main.py', 'run', '--data-folder', '/tmp/test']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging"):
                    main()

        # Verify defaults were used
        call_kwargs = mock_scheduler_class.call_args.kwargs
        assert call_kwargs['max_events'] == 7  # default
        assert call_kwargs['cancellations_file'] == 'cancellations.json'  # default
        assert call_kwargs['partnerships_file'] == 'partnerships.json'  # default

    def test_main_apply_results_default_results_filename(self, temp_period_dir):
        """Test main() apply-results uses default results filename."""
        test_args = ['main.py', 'apply-results', '--period-folder', str(temp_period_dir)]
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch("peeps_scheduler.main.apply_results") as mock_apply:
                    mock_apply.return_value = True
                    main()

        # Verify default filename was used (second positional arg)
        call_args = mock_apply.call_args
        assert call_args[0][1] == 'actual_attendance.json'  # default

    def test_main_availability_report_default_cancellations_file(self):
        """Test main() availability-report uses default cancellations filename."""
        test_args = ['main.py', 'availability-report', '--data-folder', '/tmp/test']
        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging"):
                with patch(
                    "peeps_scheduler.availability_report.run_availability_report"
                ) as mock_report:
                    main()

        # Verify default filename was used
        call_kwargs = mock_report.call_args.kwargs
        assert call_kwargs['cancellations_file'] == 'cancellations.json'  # default


@pytest.mark.integration
class TestMainIntegration:
    """Integration tests for main() with multiple commands and flags."""

    def test_run_command_all_flags_combined(self):
        """Test run command with all flags specified together."""
        test_args = [
            'main.py', '--verbose', 'run',
            '--data-folder', '/tmp/test',
            '--generate-tests',
            '--load-from-csv',
            '--max-events', '15',
            '--cancellations-file', 'custom_cancel.json',
            '--partnerships-file', 'custom_partners.json'
        ]

        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.main.Scheduler") as mock_scheduler_class:
                mock_instance = Mock()
                mock_scheduler_class.return_value = mock_instance
                with patch("peeps_scheduler.utils.setup_logging") as mock_setup:
                    main()

        # Verify all parameters were passed correctly
        mock_setup.assert_called_with(verbose=True)

        call_kwargs = mock_scheduler_class.call_args.kwargs
        assert call_kwargs['data_folder'] == '/tmp/test'
        assert call_kwargs['max_events'] == 15
        assert call_kwargs['cancellations_file'] == 'custom_cancel.json'
        assert call_kwargs['partnerships_file'] == 'custom_partners.json'

        mock_instance.run.assert_called_once()
        run_kwargs = mock_instance.run.call_args.kwargs
        assert run_kwargs['generate_test_data'] is True
        assert run_kwargs['load_from_csv'] is True

    def test_apply_results_all_options(self, temp_period_dir):
        """Test apply-results command with all options specified."""
        test_args = [
            'main.py', '--verbose', 'apply-results',
            '--period-folder', str(temp_period_dir),
            '--results-file', 'my_results.json'
        ]

        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging") as mock_setup:
                with patch("peeps_scheduler.main.apply_results") as mock_apply:
                    mock_apply.return_value = True
                    main()

        # Verify all parameters
        mock_setup.assert_called_with(verbose=True)

        # Verify results filename was passed (second positional arg)
        call_args = mock_apply.call_args
        assert call_args[0][0] == str(temp_period_dir)
        assert call_args[0][1] == 'my_results.json'

    def test_availability_report_all_options(self):
        """Test availability-report command with all options."""
        test_args = [
            'main.py', '--verbose', 'availability-report',
            '--data-folder', '/data/folder',
            '--cancellations-file', 'custom_cancel.json'
        ]

        with patch.object(sys, 'argv', test_args):
            with patch("peeps_scheduler.utils.setup_logging") as mock_setup:
                with patch(
                    "peeps_scheduler.availability_report.run_availability_report"
                ) as mock_report:
                    main()

        # Verify parameters
        mock_setup.assert_called_with(verbose=True)

        call_args = mock_report.call_args
        assert call_args[0][0] == '/data/folder'
        assert call_args[1]['cancellations_file'] == 'custom_cancel.json'
