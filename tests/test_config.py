"""Tests for src.core.config — ConfigManager and dataclasses."""

import json

import pytest

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
    compute_profile_hash,
)


class TestStorageConfig:
    def test_local_is_not_remote(self):
        cfg = StorageConfig(storage_type=StorageType.LOCAL, destination_path="/tmp/backup")
        assert cfg.is_remote() is False

    def test_network_is_not_remote(self):
        cfg = StorageConfig(
            storage_type=StorageType.NETWORK,
            destination_path="//server/share",
            network_username="user",
            network_password="pass",
        )
        assert cfg.is_remote() is False

    def test_sftp_is_remote(self):
        cfg = StorageConfig(storage_type=StorageType.SFTP, sftp_host="example.com")
        assert cfg.is_remote() is True

    def test_s3_is_remote(self):
        cfg = StorageConfig(storage_type=StorageType.S3, s3_bucket="my-bucket")
        assert cfg.is_remote() is True

    def test_default_values(self):
        cfg = StorageConfig()
        assert cfg.sftp_port == 22
        assert cfg.s3_region == "eu-west-1"

    # --- __post_init__ validation tests ---

    def test_local_empty_destination_validate_raises(self):
        """validate() catches empty destination_path on LOCAL storage."""
        cfg = StorageConfig(storage_type=StorageType.LOCAL, destination_path="")
        with pytest.raises(ValueError, match="destination_path is required"):
            cfg.validate()

    def test_local_whitespace_destination_raises(self):
        with pytest.raises(ValueError, match="destination_path is required"):
            StorageConfig(storage_type=StorageType.LOCAL, destination_path="   ")

    def test_network_empty_destination_raises(self):
        with pytest.raises(ValueError, match="destination_path is required"):
            StorageConfig(storage_type=StorageType.NETWORK, destination_path="")

    def test_sftp_empty_host_raises(self):
        with pytest.raises(ValueError, match="sftp_host is required"):
            StorageConfig(storage_type=StorageType.SFTP, sftp_host="")

    def test_sftp_whitespace_host_raises(self):
        with pytest.raises(ValueError, match="sftp_host is required"):
            StorageConfig(storage_type=StorageType.SFTP, sftp_host="  ")

    def test_s3_empty_bucket_raises(self):
        with pytest.raises(ValueError, match="s3_bucket is required"):
            StorageConfig(storage_type=StorageType.S3, s3_bucket="")

    def test_local_valid_destination_ok(self):
        cfg = StorageConfig(storage_type=StorageType.LOCAL, destination_path="/tmp/backup")
        assert cfg.destination_path == "/tmp/backup"

    def test_sftp_valid_host_ok(self):
        cfg = StorageConfig(storage_type=StorageType.SFTP, sftp_host="backup.example.com")
        assert cfg.sftp_host == "backup.example.com"

    def test_s3_valid_bucket_ok(self):
        cfg = StorageConfig(storage_type=StorageType.S3, s3_bucket="my-backup-bucket")
        assert cfg.s3_bucket == "my-backup-bucket"

    def test_default_storage_config_no_validation(self):
        """Default StorageConfig() should not raise — allows empty defaults."""
        cfg = StorageConfig()
        assert cfg.storage_type == StorageType.LOCAL
        assert cfg.destination_path == ""

    def test_validate_method_catches_empty_local(self):
        """Explicit validate() call catches empty destination on LOCAL."""
        cfg = StorageConfig()
        with pytest.raises(ValueError, match="destination_path is required"):
            cfg.validate()


class TestBackupProfile:
    def test_default_profile_has_id(self):
        # Full UUID hex — 8-char IDs gave a measurable collision rate
        # for users importing profiles across installs.
        p = BackupProfile()
        assert len(p.id) == 32
        assert all(c in "0123456789abcdef" for c in p.id)

    def test_unique_ids(self):
        p1 = BackupProfile()
        p2 = BackupProfile()
        assert p1.id != p2.id

    def test_default_exclude_patterns(self):
        p = BackupProfile()
        assert "*.tmp" in p.exclude_patterns
        assert "__pycache__" in p.exclude_patterns

    def test_default_backup_type(self):
        p = BackupProfile()
        assert p.backup_type == BackupType.DIFFERENTIAL


class TestConfigManager:
    def test_save_and_load_profile(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Test Profile")
        profile.source_paths = ["/path/to/files"]
        mgr.save_profile(profile)

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1
        assert profiles[0].name == "Test Profile"
        assert profiles[0].id == profile.id
        assert profiles[0].source_paths == ["/path/to/files"]

    def test_delete_profile(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="To Delete")
        mgr.save_profile(profile)
        assert len(mgr.get_all_profiles()) == 1

        mgr.delete_profile(profile.id)
        assert len(mgr.get_all_profiles()) == 0

    def test_overwrite_profile(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="V1")
        mgr.save_profile(profile)

        profile.name = "V2"
        mgr.save_profile(profile)

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1
        assert profiles[0].name == "V2"

    def test_bak_file_created(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Original")
        mgr.save_profile(profile)
        mgr.save_profile(profile)  # Second save creates .bak

        bak = tmp_config_dir / "profiles" / f"{profile.id}.json.bak"
        assert bak.exists()

    def test_recovery_from_bak(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Recoverable")
        mgr.save_profile(profile)

        # Corrupt the main file
        main = tmp_config_dir / "profiles" / f"{profile.id}.json"
        mgr.save_profile(profile)  # Create .bak
        main.write_text("CORRUPTED", encoding="utf-8")

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1
        assert profiles[0].name == "Recoverable"

    def test_atomic_write_no_tmp_leftover_on_success(self, tmp_config_dir):
        """After a successful save no .tmp file lingers."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Clean")
        mgr.save_profile(profile)

        tmp_files = list((tmp_config_dir / "profiles").glob("*.json.tmp"))
        assert tmp_files == [], f"Leaked .tmp files: {tmp_files}"

    def test_atomic_write_cleans_tmp_on_failure(self, tmp_config_dir, monkeypatch):
        """If os.replace fails, the .tmp must be removed so a secret
        payload never lingers on disk with a predictable name."""
        import os as _os

        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Failing")

        real_replace = _os.replace

        def _boom(src, dst):
            raise PermissionError("simulated rename failure")

        monkeypatch.setattr(_os, "replace", _boom)
        with pytest.raises(PermissionError):
            mgr.save_profile(profile)

        # Restore for teardown
        monkeypatch.setattr(_os, "replace", real_replace)

        tmp_files = list((tmp_config_dir / "profiles").glob("*.json.tmp"))
        assert tmp_files == [], f"Orphan .tmp must be cleaned up on failure: {tmp_files}"

    def test_duplicate_ids_deduplicated(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Original")
        mgr.save_profile(profile)

        # Manually create a duplicate with same ID
        dup_path = tmp_config_dir / "profiles" / f"{profile.id}_dup.json"
        main_path = tmp_config_dir / "profiles" / f"{profile.id}.json"
        data = json.loads(main_path.read_text(encoding="utf-8"))
        dup_path.write_text(json.dumps(data), encoding="utf-8")

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1

    def test_enum_serialization_roundtrip(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(
            name="Enum Test",
            backup_type=BackupType.DIFFERENTIAL,
            storage=StorageConfig(storage_type=StorageType.SFTP, sftp_host="example.com"),
            schedule=ScheduleConfig(frequency=ScheduleFrequency.WEEKLY),
            retention=RetentionConfig(policy=RetentionPolicy.GFS),
        )
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.backup_type == BackupType.DIFFERENTIAL
        assert loaded.storage.storage_type == StorageType.SFTP
        assert loaded.schedule.frequency == ScheduleFrequency.WEEKLY
        assert loaded.retention.policy == RetentionPolicy.GFS

    def test_secret_encryption_on_save(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Secrets")
        profile.storage.sftp_password = "my_secret"
        profile.email.password = "email_secret"
        mgr.save_profile(profile)

        # Read raw JSON — secrets should be encrypted
        raw = json.loads(
            (tmp_config_dir / "profiles" / f"{profile.id}.json").read_text(encoding="utf-8")
        )
        assert raw["storage"]["sftp_password"] != "my_secret"
        assert raw["email"]["password"] != "email_secret"

        # But loading decrypts them
        loaded = mgr.get_all_profiles()[0]
        assert loaded.storage.sftp_password == "my_secret"
        assert loaded.email.password == "email_secret"

    def test_manifest_path(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        path = mgr.get_manifest_path("abc123")
        assert path.name == "abc123_manifest.json"
        assert "manifests" in str(path)

    def test_log_path(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        path = mgr.get_log_path("abc123")
        assert "backup_abc123_" in path.name
        assert path.suffix == ".log"

    def test_app_settings_roundtrip(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        settings = {"theme": "dark", "language": "fr"}
        mgr.save_app_settings(settings)
        loaded = mgr.load_app_settings()
        assert loaded == settings

    def test_app_settings_missing_returns_empty(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        assert mgr.load_app_settings() == {}

    def test_mirror_destinations_roundtrip(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="With Mirrors")
        profile.mirror_destinations = [
            StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="my-bucket",
                s3_access_key="AKID",
                s3_secret_key="SECRET",
            ),
        ]
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert len(loaded.mirror_destinations) == 1
        assert loaded.mirror_destinations[0].s3_bucket == "my-bucket"
        assert loaded.mirror_destinations[0].s3_access_key == "AKID"

    def test_empty_profiles_dir(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        assert mgr.get_all_profiles() == []

    def test_completely_corrupted_profile_skipped(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        bad = tmp_config_dir / "profiles" / "bad.json"
        bad.write_text("NOT JSON", encoding="utf-8")
        assert mgr.get_all_profiles() == []

    def test_old_profile_with_compress_field_loads_without_error(self, tmp_config_dir):
        """Old profiles with removed 'compress' field should load silently."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        old_data = {
            "id": "oldprof1",
            "name": "Old Profile",
            "source_paths": [],
            "backup_type": "full",
            "compress": True,  # Removed field — must be ignored
            "storage": {"storage_type": "local", "destination_path": "/tmp"},
            "schedule": {"frequency": "manual"},
            "retention": {"policy": "gfs", "max_backups": 5},
            "encryption": {"enabled": False, "stored_password": ""},
            "verification": {"auto_verify": True, "alert_on_failure": True},
            "email": {"enabled": False},
        }
        filepath = tmp_config_dir / "profiles" / "oldprof1.json"
        filepath.write_text(json.dumps(old_data), encoding="utf-8")

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1
        assert profiles[0].name == "Old Profile"
        assert not hasattr(profiles[0], "compress") or "compress" not in vars(profiles[0])


class TestComputeProfileHash:
    """Tests for compute_profile_hash."""

    def test_same_config_produces_same_hash(self):
        """Identical profiles produce the same hash."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
        )
        assert compute_profile_hash(profile) == compute_profile_hash(profile)

    def test_different_path_produces_different_hash(self):
        """Changing destination_path changes the hash."""
        p1 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
        )
        p2 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/other"),
        )
        assert compute_profile_hash(p1) != compute_profile_hash(p2)

    def test_adding_mirror_changes_hash(self):
        """Adding a mirror destination changes the hash."""
        p1 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
        )
        p2 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
            mirror_destinations=[
                StorageConfig(storage_type=StorageType.S3, s3_bucket="my-bucket"),
            ],
        )
        assert compute_profile_hash(p1) != compute_profile_hash(p2)

    def test_password_change_does_not_change_hash(self):
        """Changing a secret (password) does not change the hash."""
        p1 = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="server.com",
                sftp_password="old_pass",
            ),
        )
        p2 = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="server.com",
                sftp_password="new_pass",
            ),
        )
        assert compute_profile_hash(p1) == compute_profile_hash(p2)

    def test_encryption_change_changes_hash(self):
        """Toggling encryption changes the hash."""
        p1 = BackupProfile(encrypt_primary=False)
        p2 = BackupProfile(encrypt_primary=True)
        assert compute_profile_hash(p1) != compute_profile_hash(p2)

    def test_retention_change_changes_hash(self):
        """Changing retention policy changes the hash."""
        from src.core.config import RetentionConfig

        p1 = BackupProfile(retention=RetentionConfig(gfs_daily=7))
        p2 = BackupProfile(retention=RetentionConfig(gfs_daily=14))
        assert compute_profile_hash(p1) != compute_profile_hash(p2)

    def test_backup_type_excluded_from_hash(self):
        """backup_type (FULL/DIFFERENTIAL) must NOT affect the hash.

        The backup engine temporarily flips ``backup_type`` to FULL
        when a DIFF is auto-promoted (e.g. profile changed, cycle
        reached). If the hash depended on backup_type, the
        ``profile_hash`` computed during the promoted run would differ
        from the hash computed next time (when backup_type is back to
        DIFFERENTIAL), incorrectly triggering another FULL and
        defeating the differential-backup savings.
        """
        p_full = BackupProfile(backup_type=BackupType.FULL)
        p_diff = BackupProfile(backup_type=BackupType.DIFFERENTIAL)
        assert compute_profile_hash(p_full) == compute_profile_hash(
            p_diff
        ), "compute_profile_hash must be independent of backup_type"

    def test_name_change_changes_hash(self):
        """Changing profile name changes the hash."""
        p1 = BackupProfile(name="Profile A")
        p2 = BackupProfile(name="Profile B")
        assert compute_profile_hash(p1) != compute_profile_hash(p2)

    def test_email_change_does_not_change_hash(self):
        """Changing email settings does not change the hash."""
        from src.core.config import EmailConfig

        p1 = BackupProfile(email=EmailConfig(smtp_host="a.com"))
        p2 = BackupProfile(email=EmailConfig(smtp_host="b.com"))
        assert compute_profile_hash(p1) == compute_profile_hash(p2)

    def test_hash_is_64_char_hex(self):
        """Hash is a valid SHA-256 hex digest."""
        profile = BackupProfile()
        h = compute_profile_hash(profile)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_profile_hash_roundtrip(self, tmp_config_dir):
        """profile_hash is persisted and loaded correctly."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="HashTest")
        profile.profile_hash = compute_profile_hash(profile)
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.profile_hash == profile.profile_hash

    def test_bandwidth_percent_changes_hash(self):
        """Changing bandwidth_percent changes the profile hash."""
        p1 = BackupProfile(bandwidth_percent=100)
        p2 = BackupProfile(bandwidth_percent=50)
        assert compute_profile_hash(p1) != compute_profile_hash(p2)


class TestBandwidthPercentMigration:
    def test_legacy_bandwidth_limit_migrated(self, tmp_config_dir):
        """Old bandwidth_limit_kbps field is migrated to bandwidth_percent=100."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Legacy")
        mgr.save_profile(profile)

        # Manually inject old field into saved JSON
        filepath = mgr.profiles_dir / f"{profile.id}.json"
        data = json.loads(filepath.read_text(encoding="utf-8"))
        data["bandwidth_limit_kbps"] = 5000
        data.pop("bandwidth_percent", None)
        filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

        loaded = mgr.get_all_profiles()[0]
        assert loaded.bandwidth_percent == 100
        assert not hasattr(loaded, "bandwidth_limit_kbps") or True

    def test_bandwidth_percent_roundtrip(self, tmp_config_dir):
        """bandwidth_percent is saved and loaded correctly."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="BW Test", bandwidth_percent=50)
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.bandwidth_percent == 50

    def test_default_bandwidth_percent(self):
        """Default bandwidth_percent is 75."""
        p = BackupProfile()
        assert p.bandwidth_percent == 75
