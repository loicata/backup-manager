"""Tests for mirror phase failure isolation.

Verifies that mirror_backup handles per-mirror failures independently,
encryption flags per mirror, local backends, and edge cases.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import StorageConfig, StorageType
from src.core.phases.collector import FileInfo
from src.core.phases.mirror import mirror_backup


def _make_file_info(tmp_path: Path, name: str = "file.txt") -> FileInfo:
    """Create a FileInfo backed by a real temp file."""
    p = tmp_path / name
    p.write_text("data")
    return FileInfo(
        source_path=p, relative_path=name,
        size=4, mtime=1.0, source_root=str(tmp_path),
    )


def _remote_config() -> StorageConfig:
    return StorageConfig(storage_type=StorageType.SFTP, sftp_host="mirror.example.com")


def _local_config(dest: str = "C:/backups") -> StorageConfig:
    return StorageConfig(storage_type=StorageType.LOCAL, destination_path=dest)


# -- Tests --

def test_mirror1_fails_mirror2_succeeds(tmp_path):
    """Mirror 1 raises ConnectionError at backend creation, Mirror 2 succeeds."""
    files = [_make_file_info(tmp_path)]
    backend_ok = MagicMock()

    call_count = {"n": 0}

    def factory(cfg):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ConnectionError("refused")
        return backend_ok

    results = mirror_backup(
        backup_path=tmp_path, files=files,
        mirror_configs=[_remote_config(), _remote_config()],
        backup_name="bk", get_backend=factory,
    )

    assert len(results) == 2
    assert results[0][1] is False  # Mirror 1 failed
    assert results[1][1] is True   # Mirror 2 succeeded


def test_both_mirrors_fail_no_crash(tmp_path):
    """Both mirrors fail at backend creation -- errors logged, no crash."""
    files = [_make_file_info(tmp_path)]

    def factory(cfg):
        raise ConnectionError("down")

    results = mirror_backup(
        backup_path=tmp_path, files=files,
        mirror_configs=[_remote_config(), _remote_config()],
        backup_name="bk", get_backend=factory,
    )

    assert len(results) == 2
    assert all(not r[1] for r in results)
    assert "ConnectionError" in results[0][2]


def test_local_mirror_uses_upload(tmp_path):
    """Local mirror calls backend.upload (not upload_file)."""
    backend = MagicMock()

    results = mirror_backup(
        backup_path=tmp_path, files=[],
        mirror_configs=[_local_config()],
        backup_name="bk", get_backend=lambda _: backend,
    )

    backend.upload.assert_called_once_with(tmp_path, "bk")
    assert results[0][1] is True


def test_empty_file_list_no_error(tmp_path):
    """Empty file list causes no error on remote mirror."""
    backend = MagicMock()

    results = mirror_backup(
        backup_path=tmp_path, files=[],
        mirror_configs=[_remote_config()],
        backup_name="bk", get_backend=lambda _: backend,
    )

    assert results[0][1] is True


@patch("src.core.phases.mirror.write_remote")
def test_encrypt_flags_per_mirror(mock_wr, tmp_path):
    """encrypt_mirror1=True, encrypt_mirror2=False -- password forwarded correctly."""
    files = [_make_file_info(tmp_path)]
    backend = MagicMock()

    mirror_backup(
        backup_path=tmp_path, files=files,
        mirror_configs=[_remote_config(), _remote_config()],
        backup_name="bk", get_backend=lambda _: backend,
        encrypt_password="secret", encrypt_flags=[True, False],
    )

    # Mirror 1: password forwarded
    assert mock_wr.call_args_list[0].kwargs.get("encrypt_password") == "secret" or \
           mock_wr.call_args_list[0][1].get("encrypt_password") == "secret"
    # Mirror 2: empty password
    call2_pw = mock_wr.call_args_list[1]
    assert call2_pw.kwargs.get("encrypt_password", "") == "" or \
           call2_pw[1].get("encrypt_password", "") == ""


def test_invalid_backend_config_logged_gracefully(tmp_path):
    """get_backend raises ValueError -- error logged, no crash."""
    def factory(cfg):
        raise ValueError("bad config")

    results = mirror_backup(
        backup_path=tmp_path, files=[],
        mirror_configs=[_remote_config()],
        backup_name="bk", get_backend=factory,
    )

    assert len(results) == 1
    assert results[0][1] is False
    assert "ValueError" in results[0][2]


def test_single_mirror_only(tmp_path):
    """Only mirror1 configured -- only one result returned."""
    backend = MagicMock()

    results = mirror_backup(
        backup_path=tmp_path, files=[_make_file_info(tmp_path)],
        mirror_configs=[_remote_config()],
        backup_name="bk", get_backend=lambda _: backend,
    )

    assert len(results) == 1
    assert results[0][0] == "Mirror 1"
    assert results[0][1] is True
