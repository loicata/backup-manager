"""Tests for drive_serial module."""

from unittest.mock import MagicMock, patch

from src.storage.drive_serial import (
    _enumerate_drive_serials,
    find_drive_by_serial,
    get_hardware_serial,
    resolve_local_path,
)


class TestGetHardwareSerial:
    """get_hardware_serial queries PowerShell for device serial."""

    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_non_windows(self, mock_sys):
        mock_sys.platform = "linux"
        assert get_hardware_serial("G") is None

    def test_returns_none_for_empty_letter(self):
        assert get_hardware_serial("") is None

    def test_returns_none_for_invalid_letter(self):
        assert get_hardware_serial("123") is None

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_serial_on_success(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="  ABC123DEF456  \n",
        )
        result = get_hardware_serial("G")
        assert result == "ABC123DEF456"

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_subprocess_uses_create_no_window(self, mock_sys, mock_run):
        """PowerShell must run hidden to avoid CMD window flash."""
        import subprocess

        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(returncode=0, stdout="SERIAL\n")
        get_hardware_serial("G")
        _, kwargs = mock_run.call_args
        assert kwargs.get("creationflags") == subprocess.CREATE_NO_WINDOW

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_powershell_error(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert get_hardware_serial("G") is None

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_empty_output(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(returncode=0, stdout="  \n")
        assert get_hardware_serial("G") is None

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_timeout(self, mock_sys, mock_run):
        import subprocess

        mock_sys.platform = "win32"
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ps", timeout=5)
        assert get_hardware_serial("G") is None

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_when_powershell_missing(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.side_effect = FileNotFoundError("powershell")
        assert get_hardware_serial("G") is None

    def test_normalizes_drive_letter(self):
        with (
            patch("src.storage.drive_serial.subprocess.run") as mock_run,
            patch("src.storage.drive_serial.sys") as mock_sys,
        ):
            mock_sys.platform = "win32"
            mock_run.return_value = MagicMock(returncode=0, stdout="SER1")
            get_hardware_serial("g")
            cmd = mock_run.call_args[0][0]
            assert "-DriveLetter G" in " ".join(cmd)


class TestFindDriveBySerial:
    """find_drive_by_serial uses a single enumeration PowerShell call."""

    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_non_windows(self, mock_sys):
        mock_sys.platform = "linux"
        assert find_drive_by_serial("ABC123") is None

    def test_returns_none_for_empty_serial(self):
        assert find_drive_by_serial("") is None

    @patch("src.storage.drive_serial._enumerate_drive_serials")
    @patch("src.storage.drive_serial.sys")
    def test_finds_matching_drive(self, mock_sys, mock_enum):
        mock_sys.platform = "win32"
        mock_enum.return_value = {"D": "OTHER", "H": "TARGET_SERIAL"}
        assert find_drive_by_serial("TARGET_SERIAL") == "H"

    @patch("src.storage.drive_serial._enumerate_drive_serials")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_when_not_found(self, mock_sys, mock_enum):
        mock_sys.platform = "win32"
        mock_enum.return_value = {"C": "WRONG_SERIAL"}
        assert find_drive_by_serial("TARGET_SERIAL") is None

    @patch("src.storage.drive_serial._enumerate_drive_serials")
    @patch("src.storage.drive_serial.sys")
    def test_case_insensitive_match(self, mock_sys, mock_enum):
        mock_sys.platform = "win32"
        mock_enum.return_value = {"E": "abc123"}
        assert find_drive_by_serial("ABC123") == "E"

    @patch("src.storage.drive_serial._enumerate_drive_serials")
    @patch("src.storage.drive_serial.sys")
    def test_single_subprocess_call(self, mock_sys, mock_enum):
        """Enumeration is done in a single call, not one per letter.

        Before the fix, find_drive_by_serial ran PowerShell up to 24
        times in sequence, each with a 5s timeout. The new implementation
        uses ``_enumerate_drive_serials`` exactly once.
        """
        mock_sys.platform = "win32"
        mock_enum.return_value = {"H": "X"}
        find_drive_by_serial("X")
        assert mock_enum.call_count == 1


class TestEnumerateDriveSerials:
    """_enumerate_drive_serials parses the PowerShell output."""

    @patch("src.storage.drive_serial.sys")
    def test_returns_empty_dict_on_non_windows(self, mock_sys):
        mock_sys.platform = "linux"
        assert _enumerate_drive_serials() == {}

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_parses_tab_separated_output(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="C\tSERIAL_C\nG\tSERIAL_G\n",
        )
        result = _enumerate_drive_serials()
        assert result == {"C": "SERIAL_C", "G": "SERIAL_G"}

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_skips_lines_without_serial(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="C\tSERIAL_C\nD\t\nmalformed\n",
        )
        result = _enumerate_drive_serials()
        assert result == {"C": "SERIAL_C"}

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_empty_on_timeout(self, mock_sys, mock_run):
        import subprocess

        mock_sys.platform = "win32"
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ps", timeout=5)
        assert _enumerate_drive_serials() == {}

    @patch("src.storage.drive_serial.subprocess.run")
    @patch("src.storage.drive_serial.sys")
    def test_returns_empty_on_nonzero_exit(self, mock_sys, mock_run):
        mock_sys.platform = "win32"
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        assert _enumerate_drive_serials() == {}


class TestResolveLocalPath:
    """resolve_local_path rewrites drive letter if serial matches."""

    def test_returns_path_when_no_serial(self):
        assert resolve_local_path("G:\\Backups", "") == "G:\\Backups"

    @patch("src.storage.drive_serial.Path")
    def test_returns_path_when_exists(self, mock_path):
        mock_path.return_value.exists.return_value = True
        assert resolve_local_path("G:\\Backups", "SER1") == "G:\\Backups"

    @patch("src.storage.drive_serial.find_drive_by_serial")
    @patch("src.storage.drive_serial.Path")
    def test_resolves_to_new_letter(self, mock_path, mock_find):
        mock_path.return_value.exists.return_value = False
        mock_find.return_value = "H"
        result = resolve_local_path("G:\\Backups\\Data", "SER1")
        assert result == "H:\\Backups\\Data"

    @patch("src.storage.drive_serial.find_drive_by_serial")
    @patch("src.storage.drive_serial.Path")
    def test_returns_original_when_drive_not_found(self, mock_path, mock_find):
        mock_path.return_value.exists.return_value = False
        mock_find.return_value = None
        assert resolve_local_path("G:\\Backups", "SER1") == "G:\\Backups"

    @patch("src.storage.drive_serial.find_drive_by_serial")
    @patch("src.storage.drive_serial.Path")
    def test_returns_original_when_same_letter(self, mock_path, mock_find):
        mock_path.return_value.exists.return_value = False
        mock_find.return_value = "G"
        assert resolve_local_path("G:\\Backups", "SER1") == "G:\\Backups"

    def test_returns_path_for_short_input(self):
        assert resolve_local_path("C", "SER1") == "C"

    def test_returns_path_for_non_drive_format(self):
        assert resolve_local_path("/usr/backup", "SER1") == "/usr/backup"
