"""Tests for fail-fast write behavior and scheduled backup notifications.

Verifies that WriteError is raised on any file write/upload failure,
and that scheduled backups send tray notifications and emails.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.exceptions import CancelledError, WriteError
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import WRITE_FLAT_WORKERS, write_flat
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
    """write_flat raises WriteError on any copy failure.

    Since v3.3.19 ``write_flat`` is a pure ``shutil.copy2`` loop;
    the integrity manifest is built upstream by parallel source
    hashing in ``_phase_integrity``. These tests mock the kernel-copy
    primitive directly.
    """

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
        with (
            patch(
                "src.core.phases.local_writer.shutil.copy2",
                side_effect=OSError("I/O error"),
            ),
            pytest.raises(WriteError, match="test.txt"),
        ):
            write_flat([fi], tmp_path / "dst", "bk1")

    def test_first_file_failure_stops_pipeline(self, tmp_path):
        """A failing copy short-circuits the pool: pending futures
        get cancelled and only the workers that were already mid-copy
        when the first exception surfaced finish their current task.

        Pre-v3.7.1 (sequential loop) this assertion was ``== 1``: only
        the first file's copy was attempted before the error broke the
        loop. With the v3.7.1 ThreadPoolExecutor(4), the strict bound
        is ``≤ WRITE_FLAT_WORKERS`` calls *if* the main thread reacts
        to the first exception before any worker can rotate to a
        second file. On a tmp_path NVMe where ``OSError`` is raised
        in microseconds, the workers can churn through several files
        before the main thread's ``as_completed`` loop yields the
        first future — giving counts of 5-10 instead of 4. To make
        the bound deterministic, the mock sleeps for 50 ms before
        raising, giving the main thread comfortable headroom to cancel
        pending futures before any worker can dequeue a second file.

        Contract pinned: copies attempted ≤ workers, and strictly less
        than the total queue (so cancellation is observable as a real
        short-circuit).
        """
        import time

        files = _make_files(tmp_path, count=20)

        def slow_fail(*_args, **_kwargs):
            # 50 ms is large enough that the main thread observes the
            # first future's exception (microseconds) before any
            # worker can finish its sleep and dequeue another file.
            time.sleep(0.05)
            raise OSError("fail")

        mock_copy = MagicMock(side_effect=slow_fail)

        with (
            patch("src.core.phases.local_writer.shutil.copy2", mock_copy),
            pytest.raises(WriteError),
        ):
            write_flat(files, tmp_path / "dst", "bk1")

        # At least 1 attempt (the one that surfaced the error).
        assert mock_copy.call_count >= 1
        # Strictly less than the queued total — the cancellation must
        # be observable, not a no-op.
        assert mock_copy.call_count < len(files)
        # And at most WRITE_FLAT_WORKERS: every worker fires exactly
        # one copy, sleeps 50 ms, raises, then the queue is empty.
        assert mock_copy.call_count <= WRITE_FLAT_WORKERS

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
        instance.root = MagicMock()
        instance.tab_run = MagicMock()
        instance._update_health_dashboard = MagicMock()
        # Per-profile engine model + queue drain wiring touched by
        # ``_scheduled_backup``. Mocked so the test exercises only the
        # notification/email contract, not health repoll or log I/O.
        instance._active_engines = {}
        instance._backup_running = False
        instance._launch_in_progress = False
        instance._repoll_destinations_after_backup_start = MagicMock()
        instance._save_backup_log = MagicMock()
        return instance

    def test_failure_sends_tray_notification_then_reraises(self):
        app = self._make_app()
        profile = MagicMock()
        profile.name = "TestProfile"
        profile.email.enabled = False

        mock_engine = MagicMock()
        mock_engine.run_backup.side_effect = WriteError("data.txt", OSError("disk full"))

        instance = self._make_instance(app)

        with (
            patch("src.ui.app.BackupEngine", return_value=mock_engine),
            pytest.raises(WriteError),
        ):
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
            pytest.raises(RuntimeError),
        ):
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

    def test_cancellation_sends_tray_notification(self):
        app = self._make_app()
        profile = MagicMock()
        profile.name = "TestProfile"
        profile.email.enabled = False

        mock_engine = MagicMock()
        mock_engine.run_backup.side_effect = CancelledError()

        instance = self._make_instance(app)

        with patch("src.ui.app.BackupEngine", return_value=mock_engine):
            instance._scheduled_backup(profile)

        app.tray.notify.assert_called_once()
        call_args = app.tray.notify.call_args
        assert "cancelled" in call_args[0][0].lower()
        assert "TestProfile" in call_args[0][1]


class TestManualBackupNotifications:
    """_start_backup_thread sends tray notifications on cancel."""

    def _make_instance(self):
        """Create a BackupManagerApp instance with mocked internals."""
        from src.ui.app import BackupManagerApp

        instance = BackupManagerApp.__new__(BackupManagerApp)
        instance.tray = MagicMock()
        instance.scheduler = MagicMock()
        instance.config_manager = MagicMock()
        instance.events = MagicMock()
        instance.engine = MagicMock()
        instance.tab_run = MagicMock()
        # Per-profile engine model + queue drain wiring touched by
        # ``_start_backup_thread`` and its worker thread's finally block.
        instance._active_engines = {}
        instance._backup_running = False
        instance._launch_in_progress = False
        instance.root = MagicMock()
        instance._repoll_destinations_after_backup_start = MagicMock()
        instance._save_backup_log = MagicMock()
        return instance

    def test_cancellation_sends_tray_notification(self):
        instance = self._make_instance()
        profile = MagicMock()
        profile.name = "MyProfile"
        profile.email.enabled = False

        instance.engine.run_backup.side_effect = CancelledError()
        instance.engine._current_result = None

        # Per-profile engine model: the engine is passed in (registered
        # in _active_engines for cancel routing), not created inside.
        instance._start_backup_thread(profile, instance.engine)

        # Wait for the background thread to complete
        import threading

        for t in threading.enumerate():
            if t.name == "Backup":
                t.join(timeout=5)

        instance.tray.notify.assert_called_once()
        call_args = instance.tray.notify.call_args
        assert "cancelled" in call_args[0][0].lower()
        assert "MyProfile" in call_args[0][1]
