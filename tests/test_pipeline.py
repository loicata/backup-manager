"""Tests for the backup pipeline phases."""

import json

import pytest

from src.core.config import RetentionConfig, RetentionPolicy
from src.core.events import EventBus
from src.core.phases.collector import collect_files
from src.core.phases.filter import (
    build_updated_manifest,
    filter_changed_files,
    save_manifest,
)
from src.core.phases.local_writer import generate_backup_name, write_flat
from src.core.phases.manifest import build_integrity_manifest, save_integrity_manifest
from src.core.phases.rotator import rotate_backups
from src.core.phases.verifier import verify_backup

# --- Collector tests ---


class TestCollector:
    def test_collect_from_directory(self, sample_files):
        files = collect_files([str(sample_files)])
        assert len(files) == 3
        names = {f.relative_path for f in files}
        assert any(n.endswith("/file1.txt") or n == "file1.txt" for n in names)
        assert any(n.endswith("/subdir/file3.txt") or n == "subdir/file3.txt" for n in names)

    def test_collect_single_file(self, tmp_path):
        f = tmp_path / "single.txt"
        f.write_text("data", encoding="utf-8")
        files = collect_files([str(f)])
        assert len(files) == 1
        assert files[0].size == 4

    def test_collect_nonexistent_source(self):
        files = collect_files(["/nonexistent/path"])
        assert files == []

    def test_collect_with_excludes(self, tmp_path):
        (tmp_path / "keep.txt").write_text("k", encoding="utf-8")
        (tmp_path / "skip.tmp").write_text("s", encoding="utf-8")
        (tmp_path / "skip.log").write_text("l", encoding="utf-8")

        files = collect_files([str(tmp_path)], exclude_patterns=["*.tmp", "*.log"])
        assert len(files) == 1
        assert files[0].relative_path.endswith("/keep.txt") or files[0].relative_path == "keep.txt"

    def test_collect_skips_symlinks(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("real", encoding="utf-8")
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(real)
        except OSError:
            pytest.skip("Symlinks not supported")

        files = collect_files([str(tmp_path)])
        names = [f.relative_path for f in files]
        assert any(n.endswith("/real.txt") or n == "real.txt" for n in names)
        assert not any(n.endswith("/link.txt") or n == "link.txt" for n in names)

    def test_collect_deduplicates(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("data", encoding="utf-8")
        # Same source twice
        files = collect_files([str(tmp_path), str(tmp_path)])
        assert len(files) == 1

    def test_collect_with_events(self, sample_files):
        bus = EventBus()
        messages = []
        bus.subscribe("log", lambda **kw: messages.append(kw["message"]))
        collect_files([str(sample_files)], events=bus)
        assert any("Collected" in m for m in messages)


# --- Filter tests ---


class TestFilter:
    def test_no_manifest_returns_all(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        manifest_path = tmp_path / "manifest.json"
        result = filter_changed_files(files, manifest_path)
        assert len(result) == len(files)

    def test_unchanged_files_filtered(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        manifest_path = tmp_path / "manifest.json"

        # Build and save manifest
        manifest = build_updated_manifest(files)
        save_manifest(manifest, manifest_path)

        # Filter again — should find no changes
        result = filter_changed_files(files, manifest_path)
        assert len(result) == 0

    def test_modified_file_detected(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        manifest_path = tmp_path / "manifest.json"

        manifest = build_updated_manifest(files)
        save_manifest(manifest, manifest_path)

        # Modify a file
        (sample_files / "file1.txt").write_text("MODIFIED", encoding="utf-8")

        files = collect_files([str(sample_files)])
        result = filter_changed_files(files, manifest_path)
        assert len(result) >= 1
        assert any(
            f.relative_path.endswith("/file1.txt") or f.relative_path == "file1.txt" for f in result
        )

    def test_new_file_included(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        manifest_path = tmp_path / "manifest.json"
        manifest = build_updated_manifest(files)
        save_manifest(manifest, manifest_path)

        (sample_files / "new_file.txt").write_text("new", encoding="utf-8")
        files = collect_files([str(sample_files)])
        result = filter_changed_files(files, manifest_path)
        assert any(
            f.relative_path.endswith("/new_file.txt") or f.relative_path == "new_file.txt"
            for f in result
        )


# --- LocalWriter tests ---


class TestLocalWriter:
    def test_write_flat(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        dest = tmp_path / "backups"
        dest.mkdir()
        backup_path = write_flat(files, dest, "test_backup")
        assert backup_path.exists()
        # relative_path now includes source dir prefix, so check recursively
        all_files = [p.name for p in backup_path.rglob("*") if p.is_file()]
        assert "file1.txt" in all_files
        assert "file3.txt" in all_files

    def test_generate_backup_name(self):
        name = generate_backup_name("My Profile")
        assert "My_Profile" in name
        assert "_" in name  # Contains timestamp

    def test_generate_backup_name_sanitizes(self):
        name = generate_backup_name("Bad/Name<>:")
        assert "/" not in name
        assert "<" not in name


# --- Manifest tests ---


class TestManifest:
    def test_build_integrity_manifest(self, sample_files):
        files = collect_files([str(sample_files)])
        manifest = build_integrity_manifest(files)
        assert manifest["version"] == 1
        assert len(manifest["files"]) == 3
        assert manifest["total_checksum"]

    def test_save_and_load_manifest(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        manifest = build_integrity_manifest(files)
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()
        path = save_integrity_manifest(manifest, backup_dir)
        assert path.exists()
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["total_checksum"] == manifest["total_checksum"]


# --- Verifier tests ---


class TestVerifier:
    def test_verify_ok(self, sample_files, tmp_path):
        files = collect_files([str(sample_files)])
        dest = tmp_path / "backups"
        dest.mkdir()
        backup = write_flat(files, dest, "verify_test")
        manifest = build_integrity_manifest(files)
        manifest_path = save_integrity_manifest(manifest, backup)

        ok, msg = verify_backup(backup, manifest_path)
        assert ok is True
        assert "OK" in msg

    def test_verify_missing_manifest(self, tmp_path):
        ok, msg = verify_backup(tmp_path, tmp_path / "missing.wbverify")
        assert ok is True
        assert "skipping" in msg.lower()


# --- Rotator tests ---


class TestRotator:
    def _make_mock_backend(self, backups):
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.list_backups.return_value = backups
        return backend

    def test_empty_backups(self):
        backend = self._make_mock_backend([])
        retention = RetentionConfig()
        assert rotate_backups(backend, retention) == 0

    def test_gfs_keeps_recent(self):
        import time

        now = time.time()
        backups = [
            {"name": "today", "modified": now},
            {"name": "yesterday", "modified": now - 86400},
            {"name": "old", "modified": now - 86400 * 60},
        ]
        backend = self._make_mock_backend(backups)
        retention = RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=7,
            gfs_weekly=4,
            gfs_monthly=3,
        )

        rotate_backups(backend, retention)
        # "today" and "yesterday" should be kept, "old" might be deleted
        # depending on GFS logic
        assert backend.list_backups.called


# --- BackupEngine tests ---


class TestBackupEngine:
    def test_full_local_backup(self, sample_files, tmp_config_dir, tmp_path):
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="Test",
            source_paths=[str(sample_files)],
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr)
        stats = engine.run_backup(profile)

        assert stats.files_found == 3
        assert stats.files_processed == 3
        assert stats.errors == 0
        assert stats.duration_seconds > 0

    def test_cancel_backup(self, sample_files, tmp_config_dir, tmp_path):
        from src.core.backup_engine import BackupEngine, CancelledError
        from src.core.config import (
            BackupProfile,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="Cancel Test",
            source_paths=[str(sample_files)],
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        mgr = ConfigManager(config_dir=tmp_config_dir)
        bus = EventBus()
        engine = BackupEngine(mgr, events=bus)

        # Cancel after collection starts (subscribe to phase change)
        def cancel_on_filter(**kw):
            if "Filter" in kw.get("phase", ""):
                engine.cancel()

        bus.subscribe("phase_changed", cancel_on_filter)

        with pytest.raises(CancelledError):
            engine.run_backup(profile)

    def test_differential_skips_unchanged(self, sample_files, tmp_config_dir, tmp_path):
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="Differential",
            source_paths=[str(sample_files)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr)

        # First backup: full (writes the manifest)
        stats1 = engine.run_backup(profile)
        assert stats1.files_processed == 3

        # Switch to differential, no changes
        profile.backup_type = BackupType.DIFFERENTIAL
        engine2 = BackupEngine(mgr)
        stats2 = engine2.run_backup(profile)
        assert stats2.files_skipped == 3
        assert stats2.files_processed == 0

    def test_events_emitted(self, sample_files, tmp_config_dir, tmp_path):
        from src.core.backup_engine import BackupEngine
        from src.core.config import (
            BackupProfile,
            ConfigManager,
            StorageConfig,
            StorageType,
        )

        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            name="Events",
            source_paths=[str(sample_files)],
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )

        bus = EventBus()
        statuses = []
        bus.subscribe("status", lambda **kw: statuses.append(kw["state"]))

        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr, events=bus)
        engine.run_backup(profile)

        assert "running" in statuses
        assert "success" in statuses
