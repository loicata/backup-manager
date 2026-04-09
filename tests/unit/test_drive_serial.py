"""Tests for drive_serial module."""

from unittest.mock import MagicMock, patch

from src.storage.drive_serial import (
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
    """find_drive_by_serial scans drive letters for a matching serial."""

    @patch("src.storage.drive_serial.sys")
    def test_returns_none_on_non_windows(self, mock_sys):
        mock_sys.platform = "linux"
        assert find_drive_by_serial("ABC123") is None

    def test_returns_none_for_empty_serial(self):
        assert find_drive_by_serial("") is None

    @patch("src.storage.drive_serial.get_hardware_serial")
    @patch("src.storage.drive_serial.os.path.isdir")
    @patch("src.storage.drive_serial.sys")
    def test_finds_matching_drive(self, mock_sys, mock_isdir, mock_serial):
        mock_sys.platform = "win32"
        mock_isdir.side_effect = lambda p: p in ("D:\\", "H:\\")
        mock_serial.side_effect = lambda letter: "TARGET_SERIAL" if letter == "H" else "OTHER"
        assert find_drive_by_serial("TARGET_SERIAL") == "H"

    @patch("src.storage.drive_serial.get_hardware_serial")
    @patch("src.storage.drive_serial.os.path.isdir")
    @patch("src.storage.drive_serial.sys")
    def test_returns_none_when_not_found(self, mock_sys, mock_isdir, mock_serial):
        mock_sys.platform = "win32"
        mock_isdir.side_effect = lambda p: p == "C:\\"
        mock_serial.return_value = "WRONG_SERIAL"
        assert find_drive_by_serial("TARGET_SERIAL") is None

    @patch("src.storage.drive_serial.get_hardware_serial")
    @patch("src.storage.drive_serial.os.path.isdir")
    @patch("src.storage.drive_serial.sys")
    def test_case_insensitive_match(self, mock_sys, mock_isdir, mock_serial):
        mock_sys.platform = "win32"
        mock_isdir.side_effect = lambda p: p == "E:\\"
        mock_serial.return_value = "abc123"
        assert find_drive_by_serial("ABC123") == "E"


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
