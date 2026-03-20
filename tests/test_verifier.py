"""Tests for the verifier pipeline phase."""

import json

import pytest

from src.core.events import EventBus
from src.core.phases.collector import collect_files
from src.core.phases.local_writer import write_flat
from src.core.phases.manifest import build_integrity_manifest, save_integrity_manifest
from src.core.phases.verifier import verify_backup


@pytest.fixture
def verified_backup(sample_files, tmp_path):
    """Create a backup with a matching manifest."""
    files = collect_files([str(sample_files)])
    dest = tmp_path / "backups"
    dest.mkdir()
    backup = write_flat(files, dest, "test_verify")
    manifest = build_integrity_manifest(files)
    manifest_path = save_integrity_manifest(manifest, backup)
    return backup, manifest_path


class TestVerifyBackup:
    """Test backup verification against manifest."""

    def test_verify_ok(self, verified_backup):
        """Intact backup should verify successfully."""
        backup, manifest_path = verified_backup
        ok, msg = verify_backup(backup, manifest_path)
        assert ok is True
        assert "OK" in msg

    def test_verify_missing_manifest(self, tmp_path):
        """Missing manifest should skip verification."""
        ok, msg = verify_backup(tmp_path, tmp_path / "nonexistent.wbverify")
        assert ok is True
        assert "skipping" in msg.lower()

    def test_verify_corrupt_manifest(self, verified_backup, tmp_path):
        """Corrupt manifest file should fail."""
        backup, _ = verified_backup
        corrupt_manifest = tmp_path / "corrupt.wbverify"
        corrupt_manifest.write_text("not json!", encoding="utf-8")
        ok, msg = verify_backup(backup, corrupt_manifest)
        assert ok is False
        assert "Could not read" in msg

    def test_verify_missing_file(self, verified_backup):
        """Missing file in backup should be reported."""
        backup, manifest_path = verified_backup
        # Delete a file from backup
        for f in backup.rglob("file1.txt"):
            f.unlink()
            break

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is False
        assert "Missing" in msg

    def test_verify_modified_file(self, verified_backup):
        """Modified file should be detected as mismatch."""
        backup, manifest_path = verified_backup
        # Modify a file in the backup
        for f in backup.rglob("file1.txt"):
            f.write_text("TAMPERED CONTENT", encoding="utf-8")
            break

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is False
        assert "Mismatch" in msg

    def test_verify_emits_progress(self, verified_backup):
        """Progress events should be emitted during verification."""
        backup, manifest_path = verified_backup
        events = EventBus()
        progress = []
        events.subscribe("progress", lambda **kw: progress.append(kw))

        verify_backup(backup, manifest_path, events)
        assert len(progress) > 0
        assert all(p["phase"] == "verification" for p in progress)

    def test_verify_many_errors_truncated(self, tmp_path):
        """Error list should be truncated when > 10 errors."""
        backup = tmp_path / "backup"
        backup.mkdir()

        # Create manifest with 15 files that don't exist in backup
        manifest_data = {"files": {f"missing_{i}.txt": {"hash": "abc123"} for i in range(15)}}
        manifest_path = tmp_path / "test.wbverify"
        manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is False
        assert "... and" in msg
        assert "5 more" in msg
