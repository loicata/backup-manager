"""Tests for integrity verification fail-fast behavior.

Verifies that hash failures in build_integrity_manifest and
verification mismatches in _phase_verify cause the backup to fail.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases.collector import FileInfo
from src.core.phases.manifest import build_integrity_manifest


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


class TestBuildIntegrityManifestFailFast:
    """build_integrity_manifest must raise if any file cannot be hashed."""

    def test_hash_oserror_raises(self, tmp_path):
        """OSError during hashing propagates immediately."""
        fi = _make_file(tmp_path)

        with (
            patch(
                "src.core.phases.manifest.compute_sha256",
                side_effect=OSError("disk read error"),
            ),
            pytest.raises(OSError, match="disk read error"),
        ):
            build_integrity_manifest([fi])

    def test_hash_permission_error_raises(self, tmp_path):
        """PermissionError during hashing propagates immediately."""
        fi = _make_file(tmp_path)

        with (
            patch(
                "src.core.phases.manifest.compute_sha256",
                side_effect=PermissionError("access denied"),
            ),
            pytest.raises(PermissionError, match="access denied"),
        ):
            build_integrity_manifest([fi])

    def test_success_still_works(self, tmp_path):
        """Normal case: all files hashed successfully."""
        fi = _make_file(tmp_path)

        manifest = build_integrity_manifest([fi])

        assert manifest["version"] == 1
        assert len(manifest["files"]) == 1
        assert manifest["files"]["test.txt"]["hash"]
        assert manifest["total_checksum"]


class TestVerifyBackupFailFast:
    """verify_backup correctly detects mismatches and missing files."""

    def test_mismatch_returns_false(self, tmp_path):
        """Modified file detected as mismatch."""
        import json

        from src.core.hashing import compute_sha256
        from src.core.phases.verifier import verify_backup

        backup = tmp_path / "backup"
        backup.mkdir()
        (backup / "a.txt").write_text("original", encoding="utf-8")

        manifest = {
            "files": {
                "a.txt": {
                    "hash": compute_sha256(backup / "a.txt"),
                    "size": 8,
                }
            }
        }

        # Corrupt the file after hashing
        (backup / "a.txt").write_text("CORRUPTED", encoding="utf-8")

        manifest_path = tmp_path / "test.wbverify"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is False
        assert "Mismatch" in msg

    def test_missing_file_returns_false(self, tmp_path):
        """Missing file detected."""
        import json

        from src.core.phases.verifier import verify_backup

        backup = tmp_path / "backup"
        backup.mkdir()

        manifest = {"files": {"gone.txt": {"hash": "abc", "size": 4}}}
        manifest_path = tmp_path / "test.wbverify"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is False
        assert "Missing" in msg


class TestBuildIntegrityManifestVanishedFile:
    """A source that vanished before hashing is SKIPPED (recorded), not
    fatal — the 06/05/2026 incident where one deleted .ico aborted a
    256k-file run. Genuine I/O errors (OSError/PermissionError) still
    abort, as the fail-fast tests above assert."""

    def test_vanished_file_skipped_not_raised(self, tmp_path):
        good = _make_file(tmp_path, "good.txt")
        gone = FileInfo(
            source_path=tmp_path / "source" / "gone.txt",  # never created
            relative_path="gone.txt",
            size=4,
            mtime=0.0,
            source_root=str(tmp_path / "source"),
        )
        manifest = build_integrity_manifest([good, gone])
        assert "good.txt" in manifest["files"]
        assert "gone.txt" not in manifest["files"]
        skipped = {e["path"] for e in manifest.get("skipped_files", [])}
        assert "gone.txt" in skipped

    def test_all_files_vanished_yields_empty_files_with_skipped(self, tmp_path):
        gone = FileInfo(
            source_path=tmp_path / "nope.txt",
            relative_path="nope.txt",
            size=1,
            mtime=0.0,
            source_root=str(tmp_path),
        )
        manifest = build_integrity_manifest([gone])
        assert manifest["files"] == {}
        assert manifest.get("skipped_files")
        assert manifest["total_checksum"]  # still a valid checksum
