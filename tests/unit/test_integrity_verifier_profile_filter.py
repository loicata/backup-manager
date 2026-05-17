"""Tests for the v3.7.4 profile-name filter in IntegrityVerifier.

Regression (v3.7.3 and earlier): ``IntegrityVerifier.verify_iter``
called ``backend.list_backups()`` with no filtering, so a periodic
verify triggered for profile A re-hashed every backup present on the
destination — including backups belonging to other profiles that
happen to share the same drive (a common one-USB-many-profiles
setup). On the user's 17/05/2026 run, a freshly-created ``TestLoic``
profile re-hashed 39 873 + 3 339 files from two unrelated profiles
during its own backup hash phase.

Fix: ``verify_iter`` filters the backup list by the sanitized profile
name prefix, the same way ``rotator.rotate_backups`` already does.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.config import BackupProfile, ConfigManager, StorageConfig, StorageType
from src.core.integrity_verifier import IntegrityVerifier
from src.core.phases.commit_marker import write_commit_marker

# Stable test HMAC key — mirrors test_integrity_verifier.py.
_TEST_KEY = b"\x33" * 32


@pytest.fixture(autouse=True)
def _patch_hmac_key():
    """Avoid touching the real DPAPI-wrapped HMAC key during tests."""
    with patch(
        "src.core.phases.commit_marker.get_app_hmac_key",
        return_value=_TEST_KEY,
    ):
        yield


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _create_committed_flat_backup(dest: Path, name: str, files: dict[str, bytes]) -> None:
    """Helper: write a flat backup + matching ``.wbverify`` + ``.wbcommit``."""
    backup_dir = dest / name
    backup_dir.mkdir(parents=True)
    manifest = {"version": 1, "algorithm": "sha256", "files": {}}
    for rel, content in files.items():
        target = backup_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        manifest["files"][rel] = {"hash": _sha256(content), "size": len(content)}
    parts = []
    for rel_path in sorted(manifest["files"].keys()):
        entry = manifest["files"][rel_path]
        parts.append(f"{rel_path}\x00{entry['hash']}\x00{entry['size']}")
    manifest["total_checksum"] = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
    (dest / f"{name}.wbverify").write_text(json.dumps(manifest), encoding="utf-8")
    write_commit_marker(
        backup_path=backup_dir,
        manifest_sha256=manifest["total_checksum"],
        files_count=len(manifest["files"]),
        destination_label="storage",
        writer_version="3.7.4",
    )


class TestVerifyFilterByProfileName:
    """The periodic-verify list must only include the caller's backups."""

    def test_verify_skips_foreign_profile_backups(self, tmp_path: Path) -> None:
        """A profile named ``TestLoic`` must not verify ``TestBackup_*``
        backups that happen to live in the same destination directory.

        This is the exact scenario from the 17/05/2026 case study: a
        new profile sharing ``G:\\Backup Manager`` with two pre-existing
        profiles re-verified all three sets of backups on every tick.
        """
        dest = tmp_path / "shared_drive"
        dest.mkdir()
        _create_committed_flat_backup(dest, "TestLoic_FULL_2026-05-17_154910", {"a.txt": b"loic"})
        _create_committed_flat_backup(
            dest, "TestBackup_FULL_2026-05-15_205528", {"x.txt": b"other"}
        )
        _create_committed_flat_backup(
            dest, "TestBackup2_FULL_2026-05-15_211836", {"y.txt": b"other"}
        )

        profile = BackupProfile(
            name="TestLoic",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        # Only the TestLoic_* backup is in scope.
        assert result.total_backups == 1
        assert len(result.results) == 1
        assert result.results[0].backup_name.startswith("TestLoic_")
        assert result.success

    def test_verify_uses_prefix_anchored_at_underscore(self, tmp_path: Path) -> None:
        """The filter prefix is ``<name>_`` — a profile named ``Foo``
        must not match a backup named ``FooBar_FULL_…`` that belongs
        to a different profile whose sanitized name happens to share
        the leading characters.
        """
        dest = tmp_path / "drive"
        dest.mkdir()
        _create_committed_flat_backup(dest, "Foo_FULL_2026-01-01_120000", {"f.txt": b"f"})
        _create_committed_flat_backup(dest, "FooBar_FULL_2026-01-02_120000", {"fb.txt": b"fb"})

        profile = BackupProfile(
            name="Foo",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.total_backups == 1
        assert result.results[0].backup_name == "Foo_FULL_2026-01-01_120000"

    def test_verify_sanitizes_profile_name_for_match(self, tmp_path: Path) -> None:
        """``sanitize_profile_name`` is applied to the profile name
        before matching, so a profile called ``My Profile`` matches
        ``My_Profile_FULL_…`` on disk (spaces collapse to underscore).
        """
        dest = tmp_path / "drive"
        dest.mkdir()
        _create_committed_flat_backup(dest, "My_Profile_FULL_2026-01-01_120000", {"a.txt": b"a"})

        profile = BackupProfile(
            name="My Profile",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.total_backups == 1

    def test_verify_empty_when_no_matching_backups(self, tmp_path: Path) -> None:
        """A destination holding only foreign backups yields zero
        results, not an error — periodic verify is a no-op for the
        caller, exactly as desired."""
        dest = tmp_path / "drive"
        dest.mkdir()
        _create_committed_flat_backup(dest, "Other_FULL_2026-01-01_120000", {"o.txt": b"o"})

        profile = BackupProfile(
            name="Mine",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.total_backups == 0
        assert result.success  # zero errors, zero results — vacuously OK
