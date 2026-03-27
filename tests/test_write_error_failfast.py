"""Tests for fail-fast write behavior and scheduled backup notifications.

Verifies that WriteError is raised on any file write/upload failure,
and that scheduled backups send tray notifications and emails.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.exceptions import WriteError
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import write_flat
from src.core.phases.remote_writer import write_remote


def _make_file(tmp_path: Path, name: str = "test.txt") -> FileInfo:
    """Create a real file and return a FileInfo pointing to it."""
    src = tmp_path / "source" / name
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("data", encoding="utf-8")
    return FileInfo(
        source_path=src,
        relative_path=name,
        size=src.stat().st_size,
        mtime=src.stat().st_mtime,
        source_root=str(tmp_path / "source"),
    )


def _make_files(tmp_path: Path, count: int = 3) -> list[FileInfo]:
    """Create multiple FileInfo backed by real temp files."""
    files = []
    for i in range(count):
        files.append(_make_file(tmp_path, f"file_{i}.txt"))
    return files


# ---------------------------------------------------------------------------
# WriteError exception
# ---------------------------------------------------------------------------


class TestWriteError:
    """WriteError stores file path and original exception."""

    def test_message_contains_file_path(self):
        err = WriteError("docs/readme.md", OSError("disk full"))
        assert "docs/readme.md" in str(err)
        assert "disk full" in str(err)

    def test_original_exception_preserved(self):
        original = ConnectionError("timeout")
        err = WriteError("data.bin", original)
        assert err.original is original
        assert err.file_path == "data.bin"

    def test_chained_exception(self):
        original = PermissionError("access denied")
        err = WriteError("secret.txt", original)
        assert err.__cause__ is None  # Not chained yet, only via raise...from


# ---------------------------------------------------------------------------
# local_writer fail-fast
# ---------------------------------------------------------------------------


class TestLocalWriterFailFast:
    """write_flat raises WriteError on any copy failure."""

    def test_permission_error_raises(self, tmp_path):
        fi = _make_file(tmp_path)
        with patch(
            "src.core.phases.local_writer.shutil.copy2",
            side_effect=PermissionError("access denied"),
        ):
            with pytest.raises(WriteError, match="test.txt") as exc_info:
                write_flat([fi], tmp_path / "dst", "bk1")
            assert isinstance(exc_info.value.original, PermissionError)

    def test_oserror_raises(self, tmp_path):
        fi = _make_file(tmp_path)
        with patch(
            "src.core.phases.local_writer.shutil.copy2",
            side_effect=OSError("I/O error"),
        ):
            with pytest.raises(WriteError, match="test.txt"):
                write_flat([fi], tmp_path / "dst", "bk1")

    def test_first_file_failure_stops_pipeline(self, tmp_path):
        """When 3 files are queued and first fails, only 1 copy is attempted."""
        files = _make_files(tmp_path, count=3)
        mock_copy = MagicMock(side_effect=OSError("fail"))

        with patch("src.core.phases.local_writer.shutil.copy2", mock_copy):
            with pytest.raises(WriteError):
                write_flat(files, tmp_path / "dst", "bk1")

        assert mock_copy.call_count == 1

    def test_success_still_works(self, tmp_path):
        """Normal case: all files copied successfully."""
        files = _make_files(tmp_path, count=2)
        dest = tmp_path / "backups"
        dest.mkdir()

        result = write_flat(files, dest, "ok_backup")

        assert result == dest / "ok_backup"
        assert (result / "file_0.txt").exists()
        assert (result / "file_1.txt").exists()


# ---------------------------------------------------------------------------
# remote_writer fail-fast
# ---------------------------------------------------------------------------


class TestRemoteWriterFailFast:
    """write_remote raises WriteError on any upload failure."""

    def test_first_upload_failure_stops(self, tmp_path):
        """First file fails — no further uploads attempted."""
        files = _make_files(tmp_path, count=3)
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("connection reset")

        with pytest.raises(WriteError, match="file_0.txt"):
            write_remote(files, backend, "backup_01")

        assert backend.upload_file.call_count == 1

    def test_connection_error_raises(self, tmp_path):
        files = _make_files(tmp_path, count=1)
        backend = MagicMock()
        backend.upload_file.side_effect = ConnectionError("refused")

        with pytest.raises(WriteError) as exc_info:
            write_remote(files, backend, "backup_01")

        assert isinstance(exc_info.value.original, ConnectionError)

    def test_success_still_works(self, tmp_path):
        """Normal case: all files uploaded successfully."""
        files = _make_files(tmp_path, count=3)
        backend = MagicMock()

        result = write_remote(files, backend, "backup_01")

        assert result == "backup_01"
        assert backend.upload_file.call_count == 3


# ---------------------------------------------------------------------------
# Scheduled backup notifications
# ---------------------------------------------------------------------------


class TestScheduledBackupNotifications:
    """_scheduled_backup sends tray notifications and emails."""

    def _make_app(self):
        """Create a minimal mock of BackupManagerApp."""
        app = MagicMock()
        app.config_manager = MagicMock()
        app.events = MagicMock()
        app.tray = MagicMock()
        app.scheduler = MagicMock()
        app.engine = None
        return app

    def _make_instance(self, app):
        """Create a BackupManagerApp instance with mocked internals."""
        from src.ui.app import BackupManagerApp

        instance = BackupManagerApp.__new__(BackupManagerApp)
        instance.tray = app.tray
        instance.scheduler = app.scheduler
        instance.config_manager = app.config_manager
        instance.events = app.events
        instance.engine = None
        return instance

    def test_failure_sends_tray_notification_then_reraises(self):
        app = self._make_app()
        profile = MagicMock()
        profile.name = "TestProfile"
        profile.email.enabled = False

        mock_engine = MagicMock()
        mock_engine.run_backup.side_effect = WriteError("data.txt", OSError("disk full"))

        instance = self._make_instance(app)

        with patch("src.ui.app.BackupEngine", return_value=mock_engine):
            with pytest.raises(WriteError):
                instance._scheduled_backup(profile)

        app.tray.notify.assert_called_once()
        call_args = app.tray.notify.call_args
        assert "failed" in call_args[0][0].lower()
        assert "TestProfile" in call_args[0][1]

    def test_failure_sends_email_then_reraises(self):
        app = self._make_app()
        profile = MagicMock()
        profile.name = "TestProfile"
        profile.email.enabled = True

        mock_engine = MagicMock()
        mock_engine.run_backup.side_effect = RuntimeError("SFTP down")

        instance = self._make_instance(app)

        with (
            patch("src.ui.app.BackupEngine", return_value=mock_engine),
            patch("src.notifications.email_notifier.send_backup_report") as mock_email,
        ):
            with pytest.raises(RuntimeError):
                instance._scheduled_backup(profile)

        mock_email.assert_called_once()
        call_args = mock_email.call_args
        assert call_args[0][2] is False  # success=False

    def test_success_sends_tray_notification(self):
        app = self._make_app()
        profile = MagicMock()
        profile.name = "TestProfile"
        profile.email.enabled = False

        stats = MagicMock()
        stats.files_processed = 42
        stats.duration_seconds = 5.0

        mock_engine = MagicMock()
        mock_engine.run_backup.return_value = stats

        instance = self._make_instance(app)

        with patch("src.ui.app.BackupEngine", return_value=mock_engine):
            instance._scheduled_backup(profile)

        app.tray.notify.assert_called_once()
        call_args = app.tray.notify.call_args
        assert "complete" in call_args[0][0].lower()
