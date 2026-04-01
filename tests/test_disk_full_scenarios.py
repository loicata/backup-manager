"""Tests for disk-full (ENOSPC) scenarios across core modules.

Covers: local_writer, encryptor, manifest, config, local storage.
"""

import errno
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.exceptions import WriteError
from src.core.phases.collector import FileInfo


def _make_file_info(tmp_path: Path, name: str = "file.txt") -> FileInfo:
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


# ---------------------------------------------------------------------------
# 1-2  local_writer: write_flat
# ---------------------------------------------------------------------------


class TestLocalWriterDiskFull:
    """Disk-full errors during flat copy."""

    def test_copy2_enospc_raises_write_error(self, tmp_path):
        """shutil.copy2 raises ENOSPC — WriteError raised immediately."""
        from src.core.phases.local_writer import write_flat

        fi = _make_file_info(tmp_path)
        enospc = OSError(errno.ENOSPC, "No space left on device")

        with (
            patch("src.core.phases.local_writer.shutil.copy2", side_effect=enospc),
            pytest.raises(WriteError, match="file.txt") as exc_info,
        ):
            write_flat([fi], tmp_path / "dst", "bk1")

        assert isinstance(exc_info.value.original, OSError)

    def test_makedirs_enospc_propagates(self, tmp_path):
        """os.makedirs (via mkdir) raises ENOSPC — error propagates."""
        from src.core.phases.local_writer import write_flat

        fi = _make_file_info(tmp_path)
        enospc = OSError(errno.ENOSPC, "No space left on device")

        with (
            patch("os.makedirs", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            write_flat([fi], tmp_path / "dst", "bk2")


# ---------------------------------------------------------------------------
# 3  encryptor: encrypt_backup
# ---------------------------------------------------------------------------


class TestEncryptorDiskFull:
    """Disk full during encryption output write."""

    def test_encrypt_backup_disk_full_raises(self, tmp_path):
        """ENOSPC during tar.wbenc write propagates as an error."""
        from src.core.phases.encryptor import encrypt_backup

        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()
        (backup_dir / "data.txt").write_text("secret", encoding="utf-8")

        enospc = OSError(errno.ENOSPC, "No space left on device")
        with (
            patch("builtins.open", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            encrypt_backup(backup_dir, "password12345678")


# ---------------------------------------------------------------------------
# 4  manifest: save_integrity_manifest
# ---------------------------------------------------------------------------


class TestManifestDiskFull:
    """Disk full when writing .wbverify file."""

    def test_save_manifest_enospc_raises(self, tmp_path):
        from src.core.phases.manifest import save_integrity_manifest

        manifest = {"version": 1, "files": {}, "total_checksum": "abc"}
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        enospc = OSError(errno.ENOSPC, "No space left on device")
        with (
            patch.object(Path, "write_text", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            save_integrity_manifest(manifest, backup_dir)


# ---------------------------------------------------------------------------
# 5  config: ConfigManager._atomic_write / save_profile
# ---------------------------------------------------------------------------


class TestConfigSaveDiskFull:
    """Disk full when saving profile JSON."""

    def test_atomic_write_enospc_on_tmp(self, tmp_path):
        from src.core.config import ConfigManager

        cm = ConfigManager(config_dir=tmp_path / "cfg")
        filepath = cm.profiles_dir / "test.json"

        enospc = OSError(errno.ENOSPC, "No space left on device")
        with (
            patch.object(Path, "write_text", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            cm._atomic_write(filepath, {"key": "value"})

    def test_save_profile_enospc(self, tmp_path):
        from src.core.config import BackupProfile, ConfigManager

        cm = ConfigManager(config_dir=tmp_path / "cfg")
        profile = BackupProfile(name="Test")

        enospc = OSError(errno.ENOSPC, "No space left on device")
        with (
            patch.object(Path, "write_text", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            cm.save_profile(profile)


# ---------------------------------------------------------------------------
# 6  LocalStorage: upload (copytree) disk full
# ---------------------------------------------------------------------------


class TestLocalStorageDiskFull:
    """copytree / copy2 fails with ENOSPC during upload."""

    def test_upload_directory_enospc(self, tmp_path):
        from src.storage.local import LocalStorage

        src_dir = tmp_path / "src_dir"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa", encoding="utf-8")

        storage = LocalStorage(str(tmp_path / "dest"))
        (tmp_path / "dest").mkdir()

        enospc = OSError(errno.ENOSPC, "No space left on device")
        with (
            patch("src.storage.local.shutil.copytree", side_effect=enospc),
            pytest.raises(OSError, match="No space left"),
        ):
            storage.upload(src_dir, "backup1")


# ---------------------------------------------------------------------------
# 7  LocalStorage: get_free_space
# ---------------------------------------------------------------------------


class TestLocalStorageFreeSpace:
    """get_free_space returns correct value or None on error."""

    def test_free_space_returns_value(self, tmp_path):
        from src.storage.local import LocalStorage

        storage = LocalStorage(str(tmp_path))
        free = storage.get_free_space()
        assert isinstance(free, int)
        assert free > 0

    def test_free_space_returns_none_on_error(self, tmp_path):
        from src.storage.local import LocalStorage

        storage = LocalStorage(str(tmp_path))
        with patch("src.storage.local.shutil.disk_usage", side_effect=OSError("fail")):
            assert storage.get_free_space() is None
