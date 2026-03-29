"""Thorough tests for differential backup mode.

Covers:
- Basic differential: unchanged files skipped, changed files included
- File content change (same size, different content)
- File size change
- New file added between runs
- File deleted between runs
- File renamed / moved
- Empty files
- mtime changed but content identical (touch)
- Manifest corruption / missing
- First run without manifest (= full backup)
- Multiple differential runs in sequence
- Large number of files
- File disappears during backup (OSError handling)
"""

import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases.collector import FileInfo, collect_files
from src.core.phases.filter import (
    build_updated_manifest,
    filter_changed_files,
    load_manifest,
    save_manifest,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_dir(tmp_path):
    """Create a source directory with sample files."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "doc.txt").write_text("Hello World", encoding="utf-8")
    (src / "data.bin").write_bytes(b"\x00" * 1024)
    subdir = src / "subdir"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("Nested content", encoding="utf-8")
    return src


@pytest.fixture()
def manifest_path(tmp_path):
    """Path for the delta manifest."""
    return tmp_path / "manifest.json"


def _collect(source_dir: Path) -> list[FileInfo]:
    """Collect files from source directory."""
    return collect_files([str(source_dir)])


def _do_backup_cycle(source_dir: Path, manifest_path: Path):
    """Simulate a backup cycle saving ALL files to manifest.

    The manifest contains all known files so that differential
    detection works correctly across multiple runs.
    """
    files = _collect(source_dir)
    changed = filter_changed_files(files, manifest_path)
    # Save ALL files so differential detection works across runs.
    manifest = build_updated_manifest(files)
    save_manifest(manifest, manifest_path)
    return files, changed


# ===========================================================================
# Basic differential behavior
# ===========================================================================


class TestDifferentialBasic:
    """Core differential detection: unchanged, changed, new, deleted."""

    def test_first_run_no_manifest_backs_up_all(self, source_dir, manifest_path):
        """First run without manifest should include all files (= full)."""
        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == len(files)

    def test_second_run_unchanged_skips_all(self, source_dir, manifest_path):
        """Second run with no changes should skip all files."""
        _do_backup_cycle(source_dir, manifest_path)

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 0

    def test_modified_content_detected(self, source_dir, manifest_path):
        """File with modified content should be included."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "doc.txt").write_text("Modified!", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 1
        assert changed[0].relative_path == "doc.txt"

    def test_new_file_included(self, source_dir, manifest_path):
        """New file added after first backup should be included."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "new_file.txt").write_text("Brand new", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 1
        assert changed[0].relative_path == "new_file.txt"

    def test_deleted_file_not_in_changed(self, source_dir, manifest_path):
        """Deleted file should not appear in changed list (not collected)."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "doc.txt").unlink()

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 0
        # Verify the file is not in collected files either
        assert all(f.relative_path != "doc.txt" for f in files)

    def test_multiple_changes_detected(self, source_dir, manifest_path):
        """Multiple files changed should all be detected."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "doc.txt").write_text("Changed 1", encoding="utf-8")
        (source_dir / "data.bin").write_bytes(b"\xff" * 512)
        (source_dir / "brand_new.txt").write_text("New", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        changed_names = {f.relative_path for f in changed}
        assert "doc.txt" in changed_names
        assert "data.bin" in changed_names
        assert "brand_new.txt" in changed_names
        assert len(changed) == 3


# ===========================================================================
# Content vs size vs mtime edge cases
# ===========================================================================


class TestChangeDetectionEdgeCases:
    """Edge cases in two-stage detection (size → hash)."""

    def test_same_size_different_content(self, source_dir, manifest_path):
        """File with same size but different content must be detected via hash."""
        (source_dir / "doc.txt").write_text("AAAAAAAAAA", encoding="utf-8")
        _do_backup_cycle(source_dir, manifest_path)

        # Same length, different content
        (source_dir / "doc.txt").write_text("BBBBBBBBBB", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) >= 1
        assert any(f.relative_path == "doc.txt" for f in changed)

    def test_different_size_detected_without_hash(self, source_dir, manifest_path):
        """File with different size should be detected by size check alone."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "doc.txt").write_text("Much longer content now!", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert any(f.relative_path == "doc.txt" for f in changed)

    def test_mtime_changed_content_same_skipped(self, source_dir, manifest_path):
        """File with changed mtime but identical content should be skipped."""
        _do_backup_cycle(source_dir, manifest_path)

        # Touch the file (update mtime without changing content)
        doc = source_dir / "doc.txt"
        content = doc.read_text(encoding="utf-8")
        time.sleep(0.05)  # Ensure different mtime
        doc.write_text(content, encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        # Content is the same, hash matches → should be skipped
        assert not any(f.relative_path == "doc.txt" for f in changed)

    def test_empty_file_handled(self, source_dir, manifest_path):
        """Empty file should be tracked and detected correctly."""
        (source_dir / "empty.txt").write_bytes(b"")
        _do_backup_cycle(source_dir, manifest_path)

        # Verify empty file is in manifest
        manifest = load_manifest(manifest_path)
        assert "empty.txt" in manifest
        assert manifest["empty.txt"]["size"] == 0

        # Second run: empty file unchanged
        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)
        assert not any(f.relative_path == "empty.txt" for f in changed)

        # Modify empty file to non-empty
        (source_dir / "empty.txt").write_text("Now has content", encoding="utf-8")
        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)
        assert any(f.relative_path == "empty.txt" for f in changed)

    def test_file_truncated_to_empty(self, source_dir, manifest_path):
        """File truncated to 0 bytes should be detected."""
        _do_backup_cycle(source_dir, manifest_path)

        (source_dir / "doc.txt").write_bytes(b"")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert any(f.relative_path == "doc.txt" for f in changed)


# ===========================================================================
# File rename / move
# ===========================================================================


class TestFileRenameAndMove:
    """Files renamed or moved should be treated as new."""

    def test_renamed_file_detected_as_new(self, source_dir, manifest_path):
        """Renamed file = old path deleted + new path is new file."""
        _do_backup_cycle(source_dir, manifest_path)

        old = source_dir / "doc.txt"
        new = source_dir / "document.txt"
        old.rename(new)

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        changed_names = {f.relative_path for f in changed}
        assert "document.txt" in changed_names
        assert "doc.txt" not in changed_names

    def test_moved_file_detected_as_new(self, source_dir, manifest_path):
        """File moved to different directory = new relative path."""
        _do_backup_cycle(source_dir, manifest_path)

        new_dir = source_dir / "moved"
        new_dir.mkdir()
        (source_dir / "doc.txt").rename(new_dir / "doc.txt")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        changed_names = {f.relative_path for f in changed}
        assert any("moved" in name for name in changed_names)


# ===========================================================================
# Manifest corruption and edge cases
# ===========================================================================


class TestManifestEdgeCases:
    """Manifest corruption, missing, or malformed data."""

    def test_corrupted_manifest_triggers_full_backup(self, source_dir, manifest_path):
        """Corrupted JSON manifest should trigger full backup."""
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("{invalid json!!!", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        # All files should be backed up (treated as first run)
        assert len(changed) == len(files)

    def test_empty_manifest_file_triggers_full(self, source_dir, manifest_path):
        """Empty manifest file should trigger full backup."""
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text("", encoding="utf-8")

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == len(files)

    def test_manifest_with_missing_hash_field(self, source_dir, manifest_path):
        """Manifest entry without hash should detect file as changed."""
        files = _collect(source_dir)
        manifest = {}
        for f in files:
            manifest[f.relative_path] = {
                "size": f.size,
                "mtime": f.mtime,
                # No "hash" field
            }
        save_manifest(manifest, manifest_path)

        changed = filter_changed_files(files, manifest_path)

        # Missing hash means prev.get("hash", "") won't match → all files changed
        # Actually: size matches, hash compare: compute_sha256(file) != "" → changed
        assert len(changed) == len(files)

    def test_manifest_with_extra_entries_ignored(self, source_dir, manifest_path):
        """Manifest entries for non-existent files should be harmlessly ignored."""
        _do_backup_cycle(source_dir, manifest_path)

        # Add fake entry to manifest
        manifest = load_manifest(manifest_path)
        manifest["ghost_file.txt"] = {
            "hash": "abc123",
            "size": 999,
            "mtime": 0,
        }
        save_manifest(manifest, manifest_path)

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 0  # Real files unchanged

    def test_load_manifest_nonexistent(self, tmp_path):
        """Loading a non-existent manifest should return empty dict."""
        result = load_manifest(tmp_path / "nonexistent.json")
        assert result == {}


# ===========================================================================
# Multiple sequential backup cycles
# ===========================================================================


class TestMultipleBackupCycles:
    """Simulate realistic multi-run scenarios."""

    def test_three_differential_runs(self, source_dir, manifest_path):
        """Three backup cycles: full → no change → one change."""
        # Run 1: full backup (no manifest)
        _, changed1 = _do_backup_cycle(source_dir, manifest_path)
        assert len(changed1) == 3  # All files

        # Run 2: no changes — use full cycle to preserve manifest
        _, changed2 = _do_backup_cycle(source_dir, manifest_path)
        assert len(changed2) == 0

        # Run 3: modify one file
        (source_dir / "doc.txt").write_text("Updated content", encoding="utf-8")
        _, changed3 = _do_backup_cycle(source_dir, manifest_path)
        assert len(changed3) == 1
        assert changed3[0].relative_path == "doc.txt"

    def test_differential_after_add_modify_delete(self, source_dir, manifest_path):
        """Complex scenario: add + modify + delete in one cycle."""
        _do_backup_cycle(source_dir, manifest_path)

        # Add new file
        (source_dir / "added.txt").write_text("New file", encoding="utf-8")
        # Modify existing
        (source_dir / "data.bin").write_bytes(b"\x01" * 2048)
        # Delete existing
        (source_dir / "subdir" / "nested.txt").unlink()

        files = _collect(source_dir)
        changed = filter_changed_files(files, manifest_path)

        changed_names = {f.relative_path for f in changed}
        assert "added.txt" in changed_names
        assert "data.bin" in changed_names
        assert "nested.txt" not in changed_names  # Deleted = not collected
        assert len(changed) == 2

    def test_five_cycles_progressive_changes(self, source_dir, manifest_path):
        """Five backup cycles with progressive file changes."""
        # Cycle 1: initial
        _, c1 = _do_backup_cycle(source_dir, manifest_path)
        assert len(c1) == 3

        # Cycle 2: add file
        (source_dir / "file4.txt").write_text("Fourth", encoding="utf-8")
        _, c2 = _do_backup_cycle(source_dir, manifest_path)
        assert len(c2) == 1

        # Cycle 3: modify 2 files
        (source_dir / "doc.txt").write_text("V2", encoding="utf-8")
        (source_dir / "file4.txt").write_text("V2", encoding="utf-8")
        _, c3 = _do_backup_cycle(source_dir, manifest_path)
        assert len(c3) == 2

        # Cycle 4: no change
        _, c4 = _do_backup_cycle(source_dir, manifest_path)
        assert len(c4) == 0

        # Cycle 5: delete + add
        (source_dir / "data.bin").unlink()
        (source_dir / "file5.txt").write_text("Fifth", encoding="utf-8")
        _, c5 = _do_backup_cycle(source_dir, manifest_path)
        assert len(c5) == 1  # Only the new file
        assert c5[0].relative_path == "file5.txt"


# ===========================================================================
# Cached hash reuse in manifest building
# ===========================================================================


class TestCachedHashInManifest:
    """Verify hash caching from Phase 3 works in manifest building."""

    def test_cached_hashes_produce_correct_manifest(self, source_dir, manifest_path):
        """Manifest built with cached hashes should match non-cached version."""
        files = _collect(source_dir)

        # Build without cache
        m_no_cache = build_updated_manifest(files)

        # Build with cache (simulating Phase 3 output)
        from src.core.hashing import compute_sha256

        cached = {f.relative_path: compute_sha256(f.source_path) for f in files}
        m_cached = build_updated_manifest(files, cached_hashes=cached)

        # Both manifests should be identical
        assert m_no_cache == m_cached


# ===========================================================================
# Engine-level differential tests (full → differential cycle)
# ===========================================================================


class TestDifferentialEngine:
    """Test differential via BackupEngine (integration with manifest logic)."""

    def test_first_differential_without_full_writes_manifest(self, tmp_path):
        """First differential with no prior full should write the manifest."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        source = tmp_path / "source"
        source.mkdir()
        (source / "a.txt").write_text("AAA", encoding="utf-8")
        (source / "b.txt").write_text("BBB", encoding="utf-8")

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="DiffNoFull",
            source_paths=[str(source)],
            backup_type=BackupType.DIFFERENTIAL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        mgr = ConfigManager(config_dir=tmp_path / "config")

        # First differential: no manifest → acts as full, writes manifest
        engine = BackupEngine(mgr)
        stats1 = engine.run_backup(profile)
        assert stats1.files_processed == 2

        manifest_path = mgr.get_manifest_path(profile.id)
        assert manifest_path.exists()

        # Second differential: manifest exists → skips unchanged files
        engine2 = BackupEngine(mgr)
        stats2 = engine2.run_backup(profile)
        assert stats2.files_skipped == 2
        assert stats2.files_processed == 0

    def test_differential_always_compares_to_last_full(self, tmp_path):
        """Multiple differentials should always compare to the full, not each other."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        source = tmp_path / "source"
        source.mkdir()
        (source / "a.txt").write_text("AAA", encoding="utf-8")
        (source / "b.txt").write_text("BBB", encoding="utf-8")
        (source / "c.txt").write_text("CCC", encoding="utf-8")

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="DiffChain",
            source_paths=[str(source)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        mgr = ConfigManager(config_dir=tmp_path / "config")

        # Full backup (writes manifest)
        engine = BackupEngine(mgr)
        engine.run_backup(profile)

        # Modify file A
        (source / "a.txt").write_text("AAA-modified", encoding="utf-8")

        # Differential 1: detects A changed
        profile.backup_type = BackupType.DIFFERENTIAL
        stats1 = BackupEngine(mgr).run_backup(profile)
        assert stats1.files_processed == 1
        assert stats1.files_skipped == 2

        # Modify file B (A is still modified from full)
        (source / "b.txt").write_text("BBB-modified", encoding="utf-8")

        # Differential 2: detects BOTH A and B changed (from full, not from diff 1)
        stats2 = BackupEngine(mgr).run_backup(profile)
        assert stats2.files_processed == 2
        assert stats2.files_skipped == 1


# ===========================================================================
# Error handling during filter
# ===========================================================================


class TestFilterErrorHandling:
    """Errors during filtering should be handled gracefully."""

    def test_unreadable_file_included_as_changed(self, source_dir, manifest_path):
        """File that can't be hashed should be included (fail-safe)."""
        _do_backup_cycle(source_dir, manifest_path)

        files = _collect(source_dir)
        # Find the doc.txt file
        doc_file = next(f for f in files if f.relative_path == "doc.txt")

        # Mock compute_sha256 to raise for this specific file
        original_sha = None

        def _failing_sha(path):
            if path == doc_file.source_path:
                raise OSError("Permission denied")
            return original_sha(path)

        with patch("src.core.phases.filter.compute_sha256") as mock_sha:
            from src.core.hashing import compute_sha256

            original_sha = compute_sha256
            mock_sha.side_effect = _failing_sha

            changed = filter_changed_files(files, manifest_path)

        # doc.txt should be in changed (fail-safe: can't read = include)
        assert any(f.relative_path == "doc.txt" for f in changed)

    def test_manifest_with_permissions_error_triggers_full(self, source_dir, tmp_path):
        """Unreadable manifest should trigger full backup."""
        bad_path = tmp_path / "bad_manifest.json"
        # Don't create the directory — load_manifest handles this

        files = _collect(source_dir)
        changed = filter_changed_files(files, bad_path)

        assert len(changed) == len(files)


# ===========================================================================
# Many files
# ===========================================================================


class TestManyFiles:
    """Performance with large number of files."""

    def test_hundred_files_differential(self, tmp_path):
        """100 files: first run all, second run detect 5 changes."""
        source = tmp_path / "source"
        source.mkdir()
        manifest_path = tmp_path / "manifest.json"

        # Create 100 files
        for i in range(100):
            (source / f"file_{i:03d}.txt").write_text(f"Content {i}", encoding="utf-8")

        # First run
        _, changed1 = _do_backup_cycle(source, manifest_path)
        assert len(changed1) == 100

        # Modify 5 files
        for i in [10, 25, 50, 75, 99]:
            (source / f"file_{i:03d}.txt").write_text(f"Modified {i}", encoding="utf-8")

        # Second run
        files = _collect(source)
        changed2 = filter_changed_files(files, manifest_path)

        assert len(changed2) == 5
        changed_names = {f.relative_path for f in changed2}
        for i in [10, 25, 50, 75, 99]:
            assert f"file_{i:03d}.txt" in changed_names

    def test_thousand_files_all_unchanged(self, tmp_path):
        """1000 files unchanged on second run: all should be skipped."""
        source = tmp_path / "source"
        source.mkdir()
        manifest_path = tmp_path / "manifest.json"

        for i in range(1000):
            (source / f"f{i}.dat").write_bytes(f"data{i}".encode())

        _do_backup_cycle(source, manifest_path)

        files = _collect(source)
        changed = filter_changed_files(files, manifest_path)

        assert len(changed) == 0


# ===========================================================================
# Destination change detection tests
# ===========================================================================


class TestDestinationChangeForcesFull:
    """Changing destinations forces a full backup."""

    @staticmethod
    def _make_env(tmp_path):
        """Create a minimal backup environment."""
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        source = tmp_path / "source"
        source.mkdir()
        (source / "a.txt").write_text("hello")

        dest = tmp_path / "backups"
        dest.mkdir()

        mgr = ConfigManager(config_dir=tmp_path / "config")
        profile = BackupProfile(
            name="DestChange",
            source_paths=[str(source)],
            backup_type=BackupType.DIFFERENTIAL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        return mgr, profile, source, dest

    def test_destination_change_forces_full(self, tmp_path):
        """Changing the storage path forces a full backup."""
        from src.core.backup_engine import BackupEngine

        mgr, profile, source, dest = self._make_env(tmp_path)

        # Run 1: first differential → auto-promoted to full (no manifest)
        engine = BackupEngine(mgr)
        r1 = engine.run_backup(profile)
        assert r1.files_processed == 1
        assert profile.destinations_hash != ""

        # Run 2: no changes → skipped (differential)
        engine2 = BackupEngine(mgr)
        r2 = engine2.run_backup(profile)
        assert r2.files_skipped == 1

        # Change destination path
        new_dest = tmp_path / "backups2"
        new_dest.mkdir()
        profile.storage.destination_path = str(new_dest)

        # Run 3: destination changed → forced full
        engine3 = BackupEngine(mgr)
        r3 = engine3.run_backup(profile)
        assert r3.files_processed == 1
        assert r3.files_skipped == 0

    def test_mirror_added_forces_full(self, tmp_path):
        """Adding a mirror destination forces a full backup."""
        from src.core.backup_engine import BackupEngine
        from src.core.config import StorageConfig, StorageType

        mgr, profile, source, dest = self._make_env(tmp_path)

        # Run 1: first full
        engine = BackupEngine(mgr)
        engine.run_backup(profile)
        old_hash = profile.destinations_hash

        # Run 2: differential (no changes → skipped)
        engine2 = BackupEngine(mgr)
        r2 = engine2.run_backup(profile)
        assert r2.files_skipped == 1

        # Add a mirror
        mirror_dest = tmp_path / "mirror"
        mirror_dest.mkdir()
        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(mirror_dest),
            ),
        ]

        # Run 3: mirror added → forced full
        engine3 = BackupEngine(mgr)
        r3 = engine3.run_backup(profile)
        assert r3.files_processed == 1
        assert r3.files_skipped == 0
        assert profile.destinations_hash != old_hash

    def test_same_destinations_stays_differential(self, tmp_path):
        """Same destinations: differential stays differential."""
        from src.core.backup_engine import BackupEngine

        mgr, profile, source, dest = self._make_env(tmp_path)

        # Run 1: first full
        engine = BackupEngine(mgr)
        engine.run_backup(profile)

        # Run 2: same config, no file changes → differential, all skipped
        engine2 = BackupEngine(mgr)
        r2 = engine2.run_backup(profile)
        assert r2.files_skipped == 1
        assert r2.files_processed == 0

    def test_destinations_hash_persisted(self, tmp_path):
        """destinations_hash is saved to profile after full backup."""
        from src.core.config import compute_destinations_hash

        mgr, profile, source, dest = self._make_env(tmp_path)

        assert profile.destinations_hash == ""

        from src.core.backup_engine import BackupEngine

        engine = BackupEngine(mgr)
        engine.run_backup(profile)

        expected = compute_destinations_hash(profile)
        assert profile.destinations_hash == expected
        assert len(profile.destinations_hash) == 64
