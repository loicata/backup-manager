"""Regression test for SFTP download of encrypted (.tar.wbenc) archives.

The previous implementation of ``SFTPStorage.download_backup`` always
treated the remote backup as a directory (``mkdir`` + recursive
``listdir_attr``), which raised ``FileNotFoundError`` on a single-file
encrypted archive because SFTP will not list a regular file.

The fix probes the remote with ``sftp.stat`` and dispatches to
``sftp.get`` for files and the original recursive copy for directories.
These tests lock in both paths and the stat-based dispatch.
"""

import stat as stat_module
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.storage.sftp import SFTPStorage


class _FakeAttr:
    """Lightweight stand-in for paramiko.SFTPAttributes.st_mode."""

    def __init__(self, mode: int, size: int = 0):
        self.st_mode = mode
        self.st_size = size
        self.filename = ""


@pytest.fixture()
def sftp_backend(tmp_path, monkeypatch):
    backend = SFTPStorage(
        host="example.invalid",
        port=22,
        username="u",
        password="p",
        remote_path="/srv/backups",
    )

    # Stub the transport/sftp plumbing so we never touch the network.
    fake_sftp = MagicMock()
    fake_transport = MagicMock()

    # Default channel behaviour: a failed tar-stream (exit != 0) so the
    # directory case falls back to per-file _sftp_download_dir without
    # hanging in an infinite recv() loop (MagicMock recv returns truthy
    # MagicMocks by default — tests that want the tar path must override).
    def _make_failing_channel():
        ch = MagicMock()
        ch.recv = MagicMock(return_value=b"")
        ch.recv_exit_status = MagicMock(return_value=1)
        return ch

    fake_transport.open_session.side_effect = _make_failing_channel

    def _get_transport(self):
        return fake_transport

    def _get_sftp(self, transport):
        return fake_sftp

    monkeypatch.setattr(SFTPStorage, "_get_transport", _get_transport)
    monkeypatch.setattr(SFTPStorage, "_get_sftp", _get_sftp)
    # Force the "not persistent" branch so transport.close() runs.
    backend._persistent_transport = None

    return backend, fake_sftp


class TestDownloadFileLayout:
    """Encrypted archives: remote is a single .tar.wbenc regular file."""

    def test_downloads_as_single_file_not_directory(self, sftp_backend, tmp_path):
        backend, sftp = sftp_backend
        name = "BackupTest_FULL_2026-04-17_233903.tar.wbenc"
        # stat reports a regular file
        sftp.stat.return_value = _FakeAttr(stat_module.S_IFREG | 0o644, size=1024)

        # sftp.get writes the target file at the requested local path
        def fake_get(remote_path, local_path):
            Path(local_path).write_bytes(b"dummy-encrypted-bytes")

        sftp.get.side_effect = fake_get

        result = backend.download_backup(name, tmp_path)

        assert result == tmp_path / name
        assert result.is_file()
        assert result.read_bytes() == b"dummy-encrypted-bytes"
        # listdir_attr must NOT be called on a file — the original bug
        sftp.listdir_attr.assert_not_called()

    def test_overwrites_stale_local_directory_with_same_name(self, sftp_backend, tmp_path):
        backend, sftp = sftp_backend
        name = "BackupTest_FULL_2026-04-17_233903.tar.wbenc"
        stale_dir = tmp_path / name
        stale_dir.mkdir()
        (stale_dir / "leftover.txt").write_text("old")

        sftp.stat.return_value = _FakeAttr(stat_module.S_IFREG | 0o644)
        sftp.get.side_effect = lambda r, local: Path(local).write_bytes(b"new")

        result = backend.download_backup(name, tmp_path)

        assert result.is_file()
        assert result.read_bytes() == b"new"
        # The stale directory is gone
        assert not stale_dir.is_dir()

    def test_file_case_does_not_download_wbverify(self, sftp_backend, tmp_path):
        """Encrypted archives embed their manifest inside the tar, so we
        must not try to ``sftp.get`` a separate .wbverify for them."""
        backend, sftp = sftp_backend
        name = "BackupTest_FULL.tar.wbenc"
        sftp.stat.return_value = _FakeAttr(stat_module.S_IFREG | 0o644)
        sftp.get.side_effect = lambda r, local: Path(local).write_bytes(b"x")

        backend.download_backup(name, tmp_path)

        # Only the archive itself was fetched — no follow-up .wbverify get.
        assert sftp.get.call_count == 1


class TestDownloadDirectoryLayout:
    """Unencrypted backup: remote is a directory tree copied recursively."""

    def test_directory_layout_still_uses_recursive_copy(self, sftp_backend, tmp_path, monkeypatch):
        backend, sftp = sftp_backend
        name = "BackupTest_FULL_2026-04-17"
        sftp.stat.return_value = _FakeAttr(stat_module.S_IFDIR | 0o755)
        # listdir_attr used to exist — keep returning nothing (empty dir).
        sftp.listdir_attr.return_value = []
        # .wbverify fetch is expected to raise FileNotFoundError (no manifest).
        sftp.get.side_effect = FileNotFoundError

        result = backend.download_backup(name, tmp_path)

        assert result == tmp_path / name
        assert result.is_dir()
        sftp.listdir_attr.assert_called()


class TestMissingBackup:
    """A deleted/missing remote surfaces a clear FileNotFoundError."""

    def test_stat_failure_is_reported_clearly(self, sftp_backend, tmp_path):
        backend, sftp = sftp_backend
        sftp.stat.side_effect = FileNotFoundError("remote missing")

        with pytest.raises(FileNotFoundError) as excinfo:
            backend.download_backup("gone.tar.wbenc", tmp_path)

        assert "not found" in str(excinfo.value).lower()
