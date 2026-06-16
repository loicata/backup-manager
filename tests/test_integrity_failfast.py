"""Tests for integrity verification fail-fast behavior.

Verifies that hash failures in build_integrity_manifest and
verification mismatches in _phase_verify cause the backup to fail.
"""

from pathlib import Path
from unittest.mock import patch

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


class TestBuildIntegrityManifestUnreadableSkipped:
    """An unreadable source is skipped + recorded, never fatal.

    Changed 2026-06-16: before, any non-ENOENT OSError during hashing
    propagated, so one unreadable file among millions aborted the whole
    backup, which the scheduler + crash-recovery then retried forever
    (the Loic15062026 storm: 1773 unreadable files in a forensic image).
    """

    def test_hash_oserror_skipped_not_raised(self, tmp_path):
        """A generic OSError ([Errno 22]) is skipped and recorded."""
        fi = _make_file(tmp_path)

        with patch(
            "src.core.phases.manifest.compute_sha256",
            side_effect=OSError(22, "Invalid argument"),
        ):
            manifest = build_integrity_manifest([fi])

        assert manifest["files"] == {}
        skipped = {e["path"]: e["reason"] for e in manifest.get("skipped_files", [])}
        assert skipped == {fi.relative_path: "unreadable_before_hash"}
        assert manifest["total_checksum"]

    def test_hash_permission_error_skipped_not_raised(self, tmp_path):
        """PermissionError (an OSError subclass) is skipped and recorded."""
        fi = _make_file(tmp_path)

        with patch(
            "src.core.phases.manifest.compute_sha256",
            side_effect=PermissionError("access denied"),
        ):
            manifest = build_integrity_manifest([fi])

        assert manifest["files"] == {}
        skipped = {e["path"]: e["reason"] for e in manifest.get("skipped_files", [])}
        assert skipped == {fi.relative_path: "unreadable_before_hash"}

    def test_one_unreadable_among_good_files_skipped(self, tmp_path):
        """One unreadable file is skipped; the others still hash and the
        run produces a valid manifest — the real 2026-06-16 scenario."""
        from src.core.hashing import compute_sha256 as real_hash

        files = [_make_file(tmp_path, f"f{i}.txt") for i in range(4)]
        bad = files[1].source_path

        def selective(path):
            if path == bad:
                raise OSError(22, "Invalid argument")
            return real_hash(path)

        with patch("src.core.phases.manifest.compute_sha256", side_effect=selective):
            manifest = build_integrity_manifest(files)

        assert len(manifest["files"]) == 3
        assert files[1].relative_path not in manifest["files"]
        skipped = {e["path"]: e["reason"] for e in manifest["skipped_files"]}
        assert skipped == {files[1].relative_path: "unreadable_before_hash"}

    def test_vanished_and_unreadable_both_recorded(self, tmp_path):
        """Vanished (ENOENT) and unreadable (other OSError) sources land in
        skipped_files with distinct reasons; good files still hash."""
        from src.core.hashing import compute_sha256 as real_hash

        good = _make_file(tmp_path, "good.txt")
        unreadable = _make_file(tmp_path, "bad.txt")
        gone = FileInfo(
            source_path=tmp_path / "source" / "gone.txt",  # never created
            relative_path="gone.txt",
            size=4,
            mtime=0.0,
            source_root=str(tmp_path / "source"),
        )

        def selective(path):
            if path == unreadable.source_path:
                raise OSError(13, "Permission denied")
            return real_hash(path)  # good hashes; gone raises FileNotFoundError

        with patch("src.core.phases.manifest.compute_sha256", side_effect=selective):
            manifest = build_integrity_manifest([good, unreadable, gone])

        assert set(manifest["files"]) == {"good.txt"}
        reasons = {e["path"]: e["reason"] for e in manifest["skipped_files"]}
        assert reasons == {
            "bad.txt": "unreadable_before_hash",
            "gone.txt": "vanished_before_hash",
        }

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
    256k-file run. Unreadable files (other OSError) are likewise skipped
    and recorded since 2026-06-16, see
    TestBuildIntegrityManifestUnreadableSkipped."""

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
