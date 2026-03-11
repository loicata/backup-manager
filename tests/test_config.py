"""
Tests for src.core.config — BackupProfile defaults, ConfigManager CRUD,
RetentionConfig, ScheduleConfig, and StorageConfig dataclass defaults.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    RetentionConfig,
    RetentionPolicy,
    ScheduleConfig,
    ScheduleFrequency,
    StorageConfig,
    StorageType,
)


class TestBackupProfileDefaults(unittest.TestCase):
    """Verify that BackupProfile fields have the expected defaults."""

    def test_default_name(self):
        p = BackupProfile()
        self.assertEqual(p.name, "New Profile")

    def test_default_backup_type(self):
        p = BackupProfile()
        self.assertEqual(p.backup_type, BackupType.FULL.value)

    def test_default_source_paths_empty(self):
        p = BackupProfile()
        self.assertEqual(p.source_paths, [])

    def test_default_exclude_patterns(self):
        p = BackupProfile()
        self.assertIn("*.tmp", p.exclude_patterns)
        self.assertIn("__pycache__", p.exclude_patterns)
        self.assertIn(".git", p.exclude_patterns)

    def test_default_compress_false(self):
        p = BackupProfile()
        self.assertFalse(p.compress)

    def test_default_bandwidth_unlimited(self):
        p = BackupProfile()
        self.assertEqual(p.bandwidth_limit_kbps, 0)

    def test_default_encryption_mode(self):
        p = BackupProfile()
        self.assertEqual(p.encryption_mode, "none")

    def test_default_last_backup_none(self):
        p = BackupProfile()
        self.assertIsNone(p.last_backup)

    def test_id_generated(self):
        p = BackupProfile()
        self.assertTrue(len(p.id) > 0)

    def test_two_profiles_different_ids(self):
        p1 = BackupProfile()
        p2 = BackupProfile()
        self.assertNotEqual(p1.id, p2.id)


class TestStorageConfigDefaults(unittest.TestCase):
    """Verify StorageConfig dataclass defaults."""

    def test_default_storage_type(self):
        sc = StorageConfig()
        self.assertEqual(sc.storage_type, StorageType.LOCAL.value)

    def test_default_destination_path_empty(self):
        sc = StorageConfig()
        self.assertEqual(sc.destination_path, "")

    def test_default_sftp_port(self):
        sc = StorageConfig()
        self.assertEqual(sc.sftp_port, 22)

    def test_default_s3_region(self):
        sc = StorageConfig()
        self.assertEqual(sc.s3_region, "eu-west-1")

    def test_default_mirror_encrypt_false(self):
        sc = StorageConfig()
        self.assertFalse(sc.mirror_encrypt)


class TestScheduleConfigDefaults(unittest.TestCase):
    """Verify ScheduleConfig dataclass defaults."""

    def test_default_frequency(self):
        sc = ScheduleConfig()
        self.assertEqual(sc.frequency, ScheduleFrequency.MANUAL.value)

    def test_default_time(self):
        sc = ScheduleConfig()
        self.assertEqual(sc.time, "02:00")

    def test_default_enabled_false(self):
        sc = ScheduleConfig()
        self.assertFalse(sc.enabled)

    def test_default_retry_enabled(self):
        sc = ScheduleConfig()
        self.assertTrue(sc.retry_enabled)

    def test_default_retry_delay_minutes(self):
        sc = ScheduleConfig()
        self.assertEqual(sc.retry_delay_minutes, [2, 10, 30])

    def test_default_retry_max_attempts(self):
        sc = ScheduleConfig()
        self.assertEqual(sc.retry_max_attempts, 3)


class TestRetentionConfigDefaults(unittest.TestCase):
    """Verify RetentionConfig dataclass defaults."""

    def test_default_policy(self):
        rc = RetentionConfig()
        self.assertEqual(rc.policy, RetentionPolicy.SIMPLE.value)

    def test_default_max_backups(self):
        rc = RetentionConfig()
        self.assertEqual(rc.max_backups, 10)

    def test_default_gfs_daily(self):
        rc = RetentionConfig()
        self.assertEqual(rc.gfs_daily, 7)

    def test_default_gfs_weekly(self):
        rc = RetentionConfig()
        self.assertEqual(rc.gfs_weekly, 4)

    def test_default_gfs_monthly(self):
        rc = RetentionConfig()
        self.assertEqual(rc.gfs_monthly, 12)


class TestConfigManagerCRUD(unittest.TestCase):
    """Test ConfigManager save/load/delete using a temp directory."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmpdir.name)
        # Patch ConfigManager class attributes to use temp directory
        self._patches = [
            patch.object(ConfigManager, "CONFIG_DIR", tmp_path),
            patch.object(ConfigManager, "CONFIG_FILE", tmp_path / "config.json"),
            patch.object(ConfigManager, "PROFILES_DIR", tmp_path / "profiles"),
            patch.object(ConfigManager, "LOG_DIR", tmp_path / "logs"),
            patch.object(ConfigManager, "MANIFEST_DIR", tmp_path / "manifests"),
        ]
        for p in self._patches:
            p.start()
        self.cm = ConfigManager()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self.tmpdir.cleanup()

    def test_save_and_load_profile(self):
        profile = BackupProfile(name="Test Profile")
        profile.storage.destination_path = "/tmp/backup"
        self.cm.save_profile(profile)

        loaded = self.cm.get_all_profiles()
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].name, "Test Profile")
        self.assertEqual(loaded[0].storage.destination_path, "/tmp/backup")

    def test_delete_profile(self):
        profile = BackupProfile(name="To Delete")
        self.cm.save_profile(profile)

        self.cm.delete_profile(profile.id)
        loaded = self.cm.get_all_profiles()
        self.assertEqual(len(loaded), 0)

    def test_save_multiple_profiles(self):
        for i in range(3):
            p = BackupProfile(name=f"Profile {i}")
            self.cm.save_profile(p)
        loaded = self.cm.get_all_profiles()
        self.assertEqual(len(loaded), 3)

    def test_overwrite_existing_profile(self):
        profile = BackupProfile(name="Original")
        self.cm.save_profile(profile)

        profile.name = "Updated"
        self.cm.save_profile(profile)

        loaded = self.cm.get_all_profiles()
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].name, "Updated")

    def test_delete_nonexistent_profile_no_error(self):
        # Should not raise
        self.cm.delete_profile("nonexistent-id")


if __name__ == "__main__":
    unittest.main()
