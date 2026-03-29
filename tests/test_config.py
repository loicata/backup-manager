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
    compute_destinations_hash,
)


class TestStorageConfig:
    def test_local_is_not_remote(self):
        cfg = StorageConfig(storage_type=StorageType.LOCAL, destination_path="/tmp/backup")
        assert cfg.is_remote() is False

    def test_network_is_not_remote(self):
        cfg = StorageConfig(storage_type=StorageType.NETWORK, destination_path="//server/share")
        assert cfg.is_remote() is False

    def test_sftp_is_remote(self):
        cfg = StorageConfig(storage_type=StorageType.SFTP, sftp_host="example.com")
        assert cfg.is_remote() is True

    def test_s3_is_remote(self):
        cfg = StorageConfig(storage_type=StorageType.S3, s3_bucket="my-bucket")
        assert cfg.is_remote() is True

    def test_proton_is_remote(self):
        cfg = StorageConfig(storage_type=StorageType.PROTON, proton_username="user@proton.me")
        assert cfg.is_remote() is True

    def test_default_values(self):
        cfg = StorageConfig()
        assert cfg.sftp_port == 22
        assert cfg.s3_region == "eu-west-1"
        assert cfg.proton_remote_path == "/Backups"

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

    def test_proton_empty_username_raises(self):
        with pytest.raises(ValueError, match="proton_username is required"):
            StorageConfig(storage_type=StorageType.PROTON, proton_username="")

    def test_local_valid_destination_ok(self):
        cfg = StorageConfig(storage_type=StorageType.LOCAL, destination_path="/tmp/backup")
        assert cfg.destination_path == "/tmp/backup"

    def test_sftp_valid_host_ok(self):
        cfg = StorageConfig(storage_type=StorageType.SFTP, sftp_host="backup.example.com")
        assert cfg.sftp_host == "backup.example.com"

    def test_s3_valid_bucket_ok(self):
        cfg = StorageConfig(storage_type=StorageType.S3, s3_bucket="my-backup-bucket")
        assert cfg.s3_bucket == "my-backup-bucket"

    def test_proton_valid_username_ok(self):
        cfg = StorageConfig(storage_type=StorageType.PROTON, proton_username="user@proton.me")
        assert cfg.proton_username == "user@proton.me"

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
        p = BackupProfile()
        assert len(p.id) == 8

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
            "retention": {"policy": "simple", "max_backups": 5},
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

    def test_migrate_encryption_mode_all(self, tmp_config_dir):
        """Old 'encryption_mode: all' migrates to 3 boolean flags."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        old_data = {
            "id": "encall1",
            "name": "Enc All",
            "backup_type": "full",
            "encryption_mode": "all",
            "storage": {"storage_type": "local", "destination_path": "/tmp"},
            "encryption": {"enabled": True, "stored_password": "secret"},
        }
        filepath = tmp_config_dir / "profiles" / "encall1.json"
        filepath.write_text(json.dumps(old_data), encoding="utf-8")

        profiles = mgr.get_all_profiles()
        assert len(profiles) == 1
        p = profiles[0]
        assert p.encrypt_primary is True
        assert p.encrypt_mirror1 is True
        assert p.encrypt_mirror2 is True

    def test_migrate_encryption_mode_mirror1(self, tmp_config_dir):
        """Old 'encryption_mode: mirror1_only' migrates correctly."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        old_data = {
            "id": "encm1",
            "name": "Mirror1 Enc",
            "backup_type": "full",
            "encryption_mode": "mirror1_only",
            "storage": {"storage_type": "local", "destination_path": "/tmp"},
            "encryption": {"enabled": True, "stored_password": "pw"},
        }
        filepath = tmp_config_dir / "profiles" / "encm1.json"
        filepath.write_text(json.dumps(old_data), encoding="utf-8")

        profiles = mgr.get_all_profiles()
        p = profiles[0]
        assert p.encrypt_primary is False
        assert p.encrypt_mirror1 is True
        assert p.encrypt_mirror2 is False

    def test_migrate_encryption_mode_none(self, tmp_config_dir):
        """Old 'encryption_mode: none' results in all flags False."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        old_data = {
            "id": "encnone",
            "name": "No Enc",
            "backup_type": "full",
            "encryption_mode": "none",
            "storage": {"storage_type": "local", "destination_path": "/tmp"},
        }
        filepath = tmp_config_dir / "profiles" / "encnone.json"
        filepath.write_text(json.dumps(old_data), encoding="utf-8")

        profiles = mgr.get_all_profiles()
        p = profiles[0]
        assert p.encrypt_primary is False
        assert p.encrypt_mirror1 is False
        assert p.encrypt_mirror2 is False


class TestComputeDestinationsHash:
    """Tests for compute_destinations_hash."""

    def test_same_config_produces_same_hash(self):
        """Identical profiles produce the same hash."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
        )
        assert compute_destinations_hash(profile) == compute_destinations_hash(profile)

    def test_different_path_produces_different_hash(self):
        """Changing destination_path changes the hash."""
        p1 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/backups"),
        )
        p2 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.LOCAL, destination_path="/other"),
        )
        assert compute_destinations_hash(p1) != compute_destinations_hash(p2)

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
        assert compute_destinations_hash(p1) != compute_destinations_hash(p2)

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
        assert compute_destinations_hash(p1) == compute_destinations_hash(p2)

    def test_s3_provider_change_changes_hash(self):
        """Changing S3 provider changes the hash."""
        p1 = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="bk", s3_provider="aws"),
        )
        p2 = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="bk",
                s3_provider="scaleway",
            ),
        )
        assert compute_destinations_hash(p1) != compute_destinations_hash(p2)

    def test_hash_is_64_char_hex(self):
        """Hash is a valid SHA-256 hex digest."""
        profile = BackupProfile()
        h = compute_destinations_hash(profile)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_destinations_hash_roundtrip(self, tmp_config_dir):
        """destinations_hash is persisted and loaded correctly."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="HashTest")
        profile.destinations_hash = compute_destinations_hash(profile)
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.destinations_hash == profile.destinations_hash
