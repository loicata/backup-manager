"""Tests for mirror phase failure handling.

Verifies that mirror_backup attempts all mirrors, raises on any failure,
handles encryption flags per mirror, local backends, edge cases, and
GFS rotation on mirror destinations.
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
        source_path=p,
        relative_path=name,
        size=4,
        mtime=1.0,
        source_root=str(tmp_path),
    )


def _remote_config() -> StorageConfig:
    return StorageConfig(storage_type=StorageType.SFTP, sftp_host="mirror.example.com")


def _local_config(dest: str = "C:/backups") -> StorageConfig:
    return StorageConfig(storage_type=StorageType.LOCAL, destination_path=dest)


# -- Tests --


def test_mirror1_fails_mirror2_still_attempted(tmp_path):
    """Mirror 1 fails but Mirror 2 is still attempted, then RuntimeError raised."""
    files = [_make_file_info(tmp_path)]
    backend_ok = MagicMock()

    call_count = {"n": 0}

    def factory(cfg):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ConnectionError("refused")
        return backend_ok

    with pytest.raises(RuntimeError, match="Mirror upload failed"):
        mirror_backup(
            backup_path=tmp_path,
            files=files,
            mirror_configs=[_remote_config(), _remote_config()],
            backup_name="bk",
            get_backend=factory,
        )

    # Both mirrors were attempted (factory called twice)
    assert call_count["n"] == 2


def test_both_mirrors_fail_raises(tmp_path):
    """Both mirrors fail -- RuntimeError raised with details for both."""
    files = [_make_file_info(tmp_path)]

    def factory(cfg):
        raise ConnectionError("down")

    with pytest.raises(RuntimeError, match="Mirror upload failed") as exc_info:
        mirror_backup(
            backup_path=tmp_path,
            files=files,
            mirror_configs=[_remote_config(), _remote_config()],
            backup_name="bk",
            get_backend=factory,
        )

    assert "Mirror 1" in str(exc_info.value)
    assert "Mirror 2" in str(exc_info.value)


def test_local_mirror_copies_files(tmp_path):
    """Local mirror copies files to destination with progress."""
    # Create a backup directory with a file
    backup_dir = tmp_path / "backup_src"
    backup_dir.mkdir()
    (backup_dir / "test.txt").write_text("hello")

    mirror_dest = tmp_path / "mirror_dest"
    mirror_dest.mkdir()

    from src.storage.local import LocalStorage

    backend = LocalStorage(str(mirror_dest))

    results = mirror_backup(
        backup_path=backup_dir,
        files=[],
        mirror_configs=[_local_config(str(mirror_dest))],
        backup_name="bk",
        get_backend=lambda _: backend,
    )

    assert results[0][1] is True
    assert (mirror_dest / "bk" / "test.txt").read_text() == "hello"


def test_empty_file_list_no_error(tmp_path):
    """Empty file list causes no error on remote mirror."""
    backend = MagicMock()

    results = mirror_backup(
        backup_path=tmp_path,
        files=[],
        mirror_configs=[_remote_config()],
        backup_name="bk",
        get_backend=lambda _: backend,
    )

    assert results[0][1] is True


@patch("src.core.phases.mirror.write_remote")
def test_encrypt_flags_per_mirror(mock_wr, tmp_path):
    """encrypt_mirror1=True, encrypt_mirror2=False -- password forwarded correctly."""
    files = [_make_file_info(tmp_path)]
    backend = MagicMock()

    mirror_backup(
        backup_path=tmp_path,
        files=files,
        mirror_configs=[_remote_config(), _remote_config()],
        backup_name="bk",
        get_backend=lambda _: backend,
        encrypt_password="secret",
        encrypt_flags=[True, False],
    )

    # Mirror 1: password forwarded
    assert (
        mock_wr.call_args_list[0].kwargs.get("encrypt_password") == "secret"
        or mock_wr.call_args_list[0][1].get("encrypt_password") == "secret"
    )
    # Mirror 2: empty password
    call2_pw = mock_wr.call_args_list[1]
    assert (
        call2_pw.kwargs.get("encrypt_password", "") == ""
        or call2_pw[1].get("encrypt_password", "") == ""
    )


def test_invalid_backend_config_raises(tmp_path):
    """get_backend raises ValueError -- RuntimeError raised."""

    def factory(cfg):
        raise ValueError("bad config")

    with pytest.raises(RuntimeError, match="Mirror upload failed"):
        mirror_backup(
            backup_path=tmp_path,
            files=[],
            mirror_configs=[_remote_config()],
            backup_name="bk",
            get_backend=factory,
        )


def test_single_mirror_only(tmp_path):
    """Only mirror1 configured -- only one result returned."""
    backend = MagicMock()

    results = mirror_backup(
        backup_path=tmp_path,
        files=[_make_file_info(tmp_path)],
        mirror_configs=[_remote_config()],
        backup_name="bk",
        get_backend=lambda _: backend,
    )

    assert len(results) == 1
    assert results[0][0] == "Mirror 1"
    assert results[0][1] is True


# ---------------------------------------------------------------------------
# Mirror GFS rotation tests (BackupEngine._phase_rotate)
# ---------------------------------------------------------------------------


class TestMirrorRotation:
    """GFS rotation is applied to mirror destinations too."""

    @pytest.fixture
    def engine_env(self, tmp_path):
        """Minimal env for BackupEngine with source files."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            RetentionConfig,
            RetentionPolicy,
            VerificationConfig,
        )
        from src.core.events import EventBus

        source = tmp_path / "source"
        source.mkdir()
        (source / "a.txt").write_text("hello")

        dest = tmp_path / "backups"
        dest.mkdir()

        config_dir = tmp_path / "config"
        for sub in ("profiles", "logs", "manifests"):
            (config_dir / sub).mkdir(parents=True, exist_ok=True)

        cm = ConfigManager(config_dir=config_dir)
        events = EventBus()
        engine = BackupEngine(cm, events=events)

        profile = BackupProfile(
            id="mirror_rot",
            name="MirrorRot",
            source_paths=[str(source)],
            exclude_patterns=[],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
            verification=VerificationConfig(auto_verify=True),
            retention=RetentionConfig(
                policy=RetentionPolicy.GFS,
                gfs_daily=1,
                gfs_weekly=0,
                gfs_monthly=0,
            ),
        )

        return {
            "engine": engine,
            "profile": profile,
            "dest": dest,
        }

    @patch("src.core.backup_engine.rotate_backups")
    def test_mirror_rotation_called(self, mock_rotate, engine_env):
        """rotate_backups is called for each mirror destination."""
        mock_rotate.return_value = 0
        profile = engine_env["profile"]
        mirror_dest = engine_env["dest"] / "mirror1"
        mirror_dest.mkdir()

        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(mirror_dest),
            ),
        ]

        engine = engine_env["engine"]
        engine.run_backup(profile)

        # Called twice: once for primary, once for mirror
        assert mock_rotate.call_count == 2

    @patch("src.core.backup_engine.rotate_backups")
    def test_mirror_rotation_failure_does_not_fail_backup(self, mock_rotate, engine_env):
        """Mirror rotation failure is logged but does not fail the backup."""
        call_count = {"n": 0}

        def side_effect(backend, retention, events=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return 0  # Primary rotation OK
            raise ConnectionError("mirror unreachable")

        mock_rotate.side_effect = side_effect

        profile = engine_env["profile"]
        mirror_dest = engine_env["dest"] / "mirror1"
        mirror_dest.mkdir()
        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(mirror_dest),
            ),
        ]

        engine = engine_env["engine"]
        # Should NOT raise despite mirror rotation failure
        result = engine.run_backup(profile)
        assert result.files_processed > 0

    @patch("src.core.backup_engine.rotate_backups")
    def test_no_mirrors_no_extra_rotation(self, mock_rotate, engine_env):
        """Without mirrors, rotate_backups is called only once (primary)."""
        mock_rotate.return_value = 0
        profile = engine_env["profile"]
        profile.mirror_destinations = []

        engine = engine_env["engine"]
        engine.run_backup(profile)

        assert mock_rotate.call_count == 1
