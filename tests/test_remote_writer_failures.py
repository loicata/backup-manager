"""Tests for remote_writer failure handling.

Verifies plain/encrypted uploads, fail-fast on errors, temp file cleanup,
progress callbacks, and edge cases.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.exceptions import WriteError
from src.core.phases.collector import FileInfo
from src.core.phases.remote_writer import write_remote


def _make_files(tmp_path: Path, count: int = 3) -> list[FileInfo]:
    """Create a list of FileInfo backed by real temp files."""
    files = []
    for i in range(count):
        p = tmp_path / f"file_{i}.txt"
        p.write_text(f"content_{i}")
        files.append(
            FileInfo(
                source_path=p,
                relative_path=f"dir/file_{i}.txt",
                size=p.stat().st_size,
                mtime=1.0,
                source_root=str(tmp_path),
            )
        )
    return files


# -- Plain upload tests --


def test_upload_plain_all_succeed(tmp_path):
    """All files uploaded successfully via backend.upload_file."""
    files = _make_files(tmp_path, count=3)
    backend = MagicMock()

    result = write_remote(files, backend, "backup_01")

    assert result == "backup_01"
    assert backend.upload_file.call_count == 3


def test_upload_plain_one_fails_raises_write_error(tmp_path):
    """One file raises OSError -- WriteError raised, backup stops immediately."""
    files = _make_files(tmp_path, count=3)
    backend = MagicMock()
    backend.upload_file.side_effect = [None, OSError("disk"), None]

    with pytest.raises(WriteError, match="file_1.txt") as exc_info:
        write_remote(files, backend, "backup_01")

    assert isinstance(exc_info.value.original, OSError)
    # Only 2 calls: first succeeds, second fails, third never attempted
    assert backend.upload_file.call_count == 2


# -- Encrypted tar upload tests --


def _drain_upload(fileobj, remote_path, size=0):
    """Mock upload_file that drains the stream (required for pipe-based uploads)."""
    while fileobj.read(65536):
        pass


def test_upload_encrypted_tar_success(tmp_path):
    """Encrypted upload produces a single .tar.wbenc file via backend.upload_file."""
    files = _make_files(tmp_path, count=3)
    backend = MagicMock()
    backend.upload_file.side_effect = _drain_upload

    write_remote(files, backend, "backup_01", encrypt_password="password12345678")

    backend.upload_file.assert_called_once()
    remote_path = backend.upload_file.call_args[0][1]
    assert remote_path == "backup_01.tar.wbenc"


def test_upload_encrypted_tar_upload_fails_raises(tmp_path):
    """Upload failure during encrypted tar raises WriteError."""
    files = _make_files(tmp_path, count=1)
    backend = MagicMock()

    def _drain_then_fail(fileobj, remote_path, size=0):
        while fileobj.read(65536):
            pass
        raise ConnectionError("timeout")

    backend.upload_file.side_effect = _drain_then_fail

    with pytest.raises(WriteError, match="encrypted-tar"):
        write_remote(files, backend, "backup_01", encrypt_password="password12345678")


# -- Progress and edge cases --


def test_progress_callback_values(tmp_path):
    """Progress events emitted with correct current/total values."""
    files = _make_files(tmp_path, count=3)
    backend = MagicMock()
    events = MagicMock()

    write_remote(files, backend, "backup_01", events=events)

    # PhaseLogger.progress is called via events.emit with PROGRESS event
    progress_calls = [
        c for c in events.emit.call_args_list if len(c.args) > 0 and "current" in c.kwargs
    ]
    assert len(progress_calls) == 3
    for i, pc in enumerate(progress_calls):
        assert pc.kwargs["current"] == i + 1
        assert pc.kwargs["total"] == 3


def test_empty_file_list_returns_immediately(tmp_path):
    """Empty file list returns backup name, no errors."""
    backend = MagicMock()

    result = write_remote([], backend, "backup_01")

    assert result == "backup_01"
    backend.upload_file.assert_not_called()


def test_network_timeout_raises_write_error(tmp_path):
    """Network timeout during upload -- WriteError raised immediately."""
    files = _make_files(tmp_path, count=2)
    backend = MagicMock()
    backend.upload_file.side_effect = TimeoutError("timed out")

    with pytest.raises(WriteError, match="file_0.txt") as exc_info:
        write_remote(files, backend, "backup_01")

    assert isinstance(exc_info.value.original, TimeoutError)
    # Only first file attempted before failure
    assert backend.upload_file.call_count == 1


def test_disconnect_called_even_on_failure(tmp_path):
    """Backend.disconnect() is called even when upload fails."""
    files = _make_files(tmp_path, count=1)
    backend = MagicMock()
    backend.upload_file.side_effect = OSError("connection lost")

    with pytest.raises(WriteError):
        write_remote(files, backend, "backup_01")

    backend.disconnect.assert_called_once()
