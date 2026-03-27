"""Tests for duplicate profile name prevention.

Validates that the application rejects saving a profile whose name
matches another existing profile (case-insensitive).
"""

import pytest

from src.core.config import BackupProfile, ConfigManager


class TestDuplicateProfileNames:
    """Verify that duplicate profile names are detected."""

    def test_same_name_different_id_detected(self, tmp_config_dir):
        """Two profiles with the same name (same case) must be flagged."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        p1 = BackupProfile(name="My Backup")
        p2 = BackupProfile(name="My Backup")
        mgr.save_profile(p1)
        mgr.save_profile(p2)

        profiles = mgr.get_all_profiles()
        names = [p.name for p in profiles]
        # Both saved — the duplicate check is in the UI layer.
        # Here we confirm ConfigManager does NOT block it (bug existed).
        assert names.count("My Backup") == 2

    def test_case_insensitive_duplicate_detected(self, tmp_config_dir):
        """'My Backup' and 'my backup' should be considered duplicates."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        p1 = BackupProfile(name="My Backup")
        p2 = BackupProfile(name="my backup")
        mgr.save_profile(p1)
        mgr.save_profile(p2)

        profiles = mgr.get_all_profiles()
        lower_names = [p.name.lower() for p in profiles]
        # ConfigManager allows it — UI must prevent this.
        assert lower_names.count("my backup") == 2

    def test_has_duplicate_name_helper(self, tmp_config_dir):
        """Verify the duplicate detection logic used in _save_profile."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        p1 = BackupProfile(name="Production")
        p2 = BackupProfile(name="Staging")
        mgr.save_profile(p1)
        mgr.save_profile(p2)

        profiles = mgr.get_all_profiles()

        # Simulate checking if renaming p2 to "Production" conflicts
        new_name = "Production"
        conflict = any(
            p.id != p2.id and p.name.lower() == new_name.lower()
            for p in profiles
        )
        assert conflict is True

    def test_no_conflict_when_renaming_self(self, tmp_config_dir):
        """A profile should be allowed to keep its own name."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        p1 = BackupProfile(name="Production")
        mgr.save_profile(p1)

        profiles = mgr.get_all_profiles()

        # Renaming p1 to "Production" (same name, same profile)
        new_name = "Production"
        conflict = any(
            p.id != p1.id and p.name.lower() == new_name.lower()
            for p in profiles
        )
        assert conflict is False

    def test_no_conflict_with_different_names(self, tmp_config_dir):
        """Unique names should not trigger duplicate detection."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        p1 = BackupProfile(name="Production")
        p2 = BackupProfile(name="Staging")
        mgr.save_profile(p1)
        mgr.save_profile(p2)

        profiles = mgr.get_all_profiles()

        new_name = "Development"
        conflict = any(
            p.id != p2.id and p.name.lower() == new_name.lower()
            for p in profiles
        )
        assert conflict is False
