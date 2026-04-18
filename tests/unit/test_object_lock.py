"""Tests for S3 Object Lock integration.

Covers: config fields, s3_setup provisioning, rotator skip,
backup engine retention, wizard profile creation, cost estimation.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from src.core.backup_result import BackupResult
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    RetentionConfig,
    StorageConfig,
    StorageType,
)
from src.core.events import EventBus
from src.core.phases.base import PipelineContext
from src.storage.s3_setup import (
    RETENTION_OPTIONS,
    S3ObjectLockSetup,
    estimate_total_cost,
)

# ---------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------


class TestConfigObjectLockFields:
    """Verify new Object Lock fields persist through JSON roundtrip."""

    def test_storage_config_defaults(self):
        config = StorageConfig()
        assert config.s3_object_lock is False
        assert config.s3_object_lock_mode == "COMPLIANCE"
        assert config.s3_object_lock_days == 30
        assert config.s3_object_lock_full_extra_days == 30
        assert config.s3_speedtest_bucket == ""

    def test_retention_config_gfs_enabled_default(self):
        r = RetentionConfig()
        assert r.gfs_enabled is True

    def test_profile_object_lock_default(self):
        p = BackupProfile()
        assert p.object_lock_enabled is False

    def test_roundtrip_object_lock_fields(self, tmp_config_dir):
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(
            name="ProTest",
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="test-bucket",
                s3_region="eu-west-1",
                s3_access_key="AKIA",
                s3_secret_key="secret",
                s3_object_lock=True,
                s3_object_lock_mode="COMPLIANCE",
                s3_object_lock_days=395,
                s3_object_lock_full_extra_days=30,
            ),
            retention=RetentionConfig(gfs_enabled=False),
        )
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.object_lock_enabled is True
        assert loaded.storage.s3_object_lock is True
        assert loaded.storage.s3_object_lock_mode == "COMPLIANCE"
        assert loaded.storage.s3_object_lock_days == 395
        assert loaded.storage.s3_object_lock_full_extra_days == 30
        assert loaded.retention.gfs_enabled is False

    def test_old_profile_without_object_lock_defaults(self, tmp_config_dir):
        """Profiles saved before Object Lock feature get correct defaults."""
        import json

        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(name="Legacy")
        mgr.save_profile(profile)

        filepath = mgr.profiles_dir / f"{profile.id}.json"
        data = json.loads(filepath.read_text(encoding="utf-8"))
        data.pop("object_lock_enabled", None)
        data.get("storage", {}).pop("s3_object_lock", None)
        data.get("retention", {}).pop("gfs_enabled", None)
        filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

        loaded = mgr.get_all_profiles()[0]
        assert loaded.object_lock_enabled is False
        assert loaded.storage.s3_object_lock is False
        assert loaded.retention.gfs_enabled is True


# ---------------------------------------------------------------
# S3 Object Lock Setup tests
# ---------------------------------------------------------------


class TestS3ObjectLockSetup:
    """Test S3 bucket provisioning with mocked _get_client."""

    def _make_setup(self, mock_client: MagicMock, region: str = "eu-west-1"):
        """Create an S3ObjectLockSetup with a mocked client."""
        setup = S3ObjectLockSetup("AKIA", "secret", region)
        setup._get_client = lambda: mock_client
        return setup

    def test_validate_credentials_success(self):
        client = MagicMock()
        client.list_buckets.return_value = {"Buckets": [{"Name": "b1"}]}
        setup = self._make_setup(client)
        ok, msg = setup.validate_credentials()
        assert ok is True
        assert "1 bucket" in msg

    def test_validate_credentials_failure(self):
        client = MagicMock()
        client.list_buckets.side_effect = Exception("denied")
        setup = self._make_setup(client)
        ok, msg = setup.validate_credentials()
        assert ok is False
        assert "denied" in msg

    def test_create_bucket_success(self):
        client = MagicMock()
        setup = self._make_setup(client)
        ok, msg = setup.create_bucket("my-bucket")
        assert ok is True
        call_kwargs = client.create_bucket.call_args[1]
        assert call_kwargs["ObjectLockEnabledForBucket"] is True
        assert call_kwargs["Bucket"] == "my-bucket"

    def test_create_bucket_us_east_1_no_location(self):
        """us-east-1 must NOT include LocationConstraint."""
        client = MagicMock()
        setup = self._make_setup(client, region="us-east-1")
        setup.create_bucket("my-bucket")
        call_kwargs = client.create_bucket.call_args[1]
        assert "CreateBucketConfiguration" not in call_kwargs

    def test_configure_retention_compliance(self):
        client = MagicMock()
        setup = self._make_setup(client)
        ok, msg = setup.configure_retention("bucket", "COMPLIANCE", 395)
        assert ok is True
        call_args = client.put_object_lock_configuration.call_args[1]
        config = call_args["ObjectLockConfiguration"]
        assert config["Rule"]["DefaultRetention"]["Mode"] == "COMPLIANCE"
        assert config["Rule"]["DefaultRetention"]["Days"] == 395

    def test_configure_lifecycle(self):
        client = MagicMock()
        setup = self._make_setup(client)
        ok, msg = setup.configure_lifecycle("bucket", 426)
        assert ok is True
        call_args = client.put_bucket_lifecycle_configuration.call_args[1]
        rules = call_args["LifecycleConfiguration"]["Rules"]
        assert rules[0]["Expiration"]["Days"] == 426

    def test_create_speedtest_bucket_no_object_lock(self):
        """Speedtest bucket must be created WITHOUT Object Lock."""
        client = MagicMock()
        setup = self._make_setup(client)
        ok, msg = setup.create_speedtest_bucket("bucket-speedtest")
        assert ok is True
        call_kwargs = client.create_bucket.call_args[1]
        assert "ObjectLockEnabledForBucket" not in call_kwargs
        assert call_kwargs["Bucket"] == "bucket-speedtest"

    def test_create_speedtest_bucket_lifecycle_1_day(self):
        """Speedtest bucket must have a 1-day lifecycle rule."""
        client = MagicMock()
        setup = self._make_setup(client)
        setup.create_speedtest_bucket("bucket-speedtest")
        call_args = client.put_bucket_lifecycle_configuration.call_args[1]
        rules = call_args["LifecycleConfiguration"]["Rules"]
        assert rules[0]["ID"] == "speedtest-auto-cleanup"
        assert rules[0]["Expiration"]["Days"] == 1

    def test_create_speedtest_bucket_us_east_1(self):
        """us-east-1 must NOT include LocationConstraint."""
        client = MagicMock()
        setup = self._make_setup(client, region="us-east-1")
        setup.create_speedtest_bucket("bucket-speedtest")
        call_kwargs = client.create_bucket.call_args[1]
        assert "CreateBucketConfiguration" not in call_kwargs

    def test_create_speedtest_bucket_failure(self):
        client = MagicMock()
        client.create_bucket.side_effect = Exception("access denied")
        setup = self._make_setup(client)
        ok, msg = setup.create_speedtest_bucket("bucket-speedtest")
        assert ok is False
        assert "access denied" in msg

    def test_full_setup_all_steps(self):
        client = MagicMock()
        client.get_object_lock_configuration.return_value = {
            "ObjectLockConfiguration": {
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 395}},
            }
        }
        setup = self._make_setup(client)
        results = setup.full_setup("bucket", 395, full_extra_days=30)
        assert len(results) == 4
        assert all(ok for _, ok, _ in results)

    def test_full_setup_with_speedtest_bucket(self):
        """full_setup with speedtest_bucket_name creates 5 steps."""
        client = MagicMock()
        client.get_object_lock_configuration.return_value = {
            "ObjectLockConfiguration": {
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 30}},
            }
        }
        setup = self._make_setup(client)
        results = setup.full_setup(
            "bucket",
            30,
            full_extra_days=30,
            speedtest_bucket_name="bucket-speedtest",
        )
        assert len(results) == 5
        assert results[4][0] == "Create speedtest bucket"
        assert results[4][1] is True

    def test_full_setup_continues_on_speedtest_failure(self):
        """Main bucket steps succeed even if speedtest bucket fails."""
        client = MagicMock()
        client.get_object_lock_configuration.return_value = {
            "ObjectLockConfiguration": {
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 30}},
            }
        }
        setup = self._make_setup(client)

        # Mock create_speedtest_bucket to fail directly
        setup.create_speedtest_bucket = MagicMock(return_value=(False, "speedtest bucket failed"))

        results = setup.full_setup(
            "bucket",
            30,
            speedtest_bucket_name="bucket-speedtest",
        )
        # All 5 steps present, main steps OK, speedtest failed
        assert len(results) == 5
        main_steps = results[:4]
        assert all(ok for _, ok, _ in main_steps)
        assert results[4][0] == "Create speedtest bucket"
        assert results[4][1] is False

    def test_full_setup_stops_on_failure(self):
        client = MagicMock()
        client.create_bucket.side_effect = Exception("access denied")
        setup = self._make_setup(client)
        results = setup.full_setup("bucket", 30)
        assert len(results) == 1
        assert results[0][1] is False

    def test_invalid_mode_rejected(self):
        client = MagicMock()
        setup = self._make_setup(client)
        ok, msg = setup.configure_retention("bucket", "INVALID", 30)
        assert ok is False


# ---------------------------------------------------------------
# Cost estimation tests
# ---------------------------------------------------------------


class TestCostEstimation:
    """Test pricing simulation functions."""

    def test_estimate_total_cost_basic(self):
        total = estimate_total_cost(100, "eu-west-1", 13)
        assert total > 0

    def test_estimate_total_cost_increases_with_retention(self):
        short = estimate_total_cost(100, "eu-west-1", 1)
        long = estimate_total_cost(100, "eu-west-1", 13)
        assert long > short

    def test_unknown_region_uses_default_price(self):
        cost = estimate_total_cost(100, "unknown-region-99", 1)
        assert cost > 0

    def test_retention_options_have_correct_structure(self):
        for label, months, days in RETENTION_OPTIONS:
            assert isinstance(label, str)
            assert months > 0
            assert days > 0


# ---------------------------------------------------------------
# Rotator skip tests
# ---------------------------------------------------------------


class TestRotatorSkipObjectLock:
    """Test that GFS rotation is skipped when gfs_enabled=False."""

    def test_rotation_skipped_when_gfs_disabled(self):
        from src.core.phases.rotator import rotate_backups

        mock_backend = MagicMock()
        retention = RetentionConfig(gfs_enabled=False)

        deleted = rotate_backups(
            mock_backend,
            retention,
            events=EventBus(),
            current_backup_name="test",
            profile_name="test",
        )

        assert deleted == 0
        mock_backend.list_backups.assert_not_called()

    def test_rotation_proceeds_when_gfs_enabled(self):
        from src.core.phases.rotator import rotate_backups

        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = []
        retention = RetentionConfig(gfs_enabled=True)

        deleted = rotate_backups(
            mock_backend,
            retention,
            events=EventBus(),
            current_backup_name="test",
            profile_name="test",
        )

        assert deleted == 0
        mock_backend.list_backups.assert_called_once()


# ---------------------------------------------------------------
# Backup engine Object Lock retention tests
# ---------------------------------------------------------------


class TestBackupEngineObjectLock:
    """Test retain_until_date calculation in the backup engine."""

    def _make_ctx(self, profile, mgr):
        return PipelineContext(
            profile=profile,
            config_manager=mgr,
            events=EventBus(),
            result=BackupResult(),
        )

    def test_no_retention_when_object_lock_disabled(self, tmp_config_dir):
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(name="Standard", object_lock_enabled=False)
        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)
        ctx.backend = MagicMock(spec=["set_retain_until", "set_cancel_check"])

        engine._apply_object_lock_retention(ctx)

        ctx.backend.set_retain_until.assert_not_called()

    def test_differential_retention_days(self, tmp_config_dir):
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Pro",
            backup_type=BackupType.DIFFERENTIAL,
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="bucket",
                s3_object_lock=True,
                s3_object_lock_days=395,
                s3_object_lock_full_extra_days=30,
            ),
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)
        ctx.backend = MagicMock()

        engine._apply_object_lock_retention(ctx)

        ctx.backend.set_retain_until.assert_called_once()
        retain = ctx.backend.set_retain_until.call_args[0][0]
        expected_min = datetime.now(UTC) + timedelta(days=394)
        assert retain > expected_min

    def test_full_backup_gets_extra_days(self, tmp_config_dir):
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Pro",
            backup_type=BackupType.FULL,
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="bucket",
                s3_object_lock=True,
                s3_object_lock_days=395,
                s3_object_lock_full_extra_days=30,
            ),
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)
        ctx.backend = MagicMock()

        engine._apply_object_lock_retention(ctx)

        retain = ctx.backend.set_retain_until.call_args[0][0]
        expected_min = datetime.now(UTC) + timedelta(days=424)
        assert retain > expected_min

    def test_forced_full_gets_extra_days(self, tmp_config_dir):
        from src.core.backup_engine import BackupEngine

        profile = BackupProfile(
            name="Pro",
            backup_type=BackupType.DIFFERENTIAL,
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="bucket",
                s3_object_lock=True,
                s3_object_lock_days=30,
                s3_object_lock_full_extra_days=30,
            ),
        )
        mgr = ConfigManager(config_dir=tmp_config_dir)
        engine = BackupEngine(mgr, events=EventBus())
        ctx = self._make_ctx(profile, mgr)
        ctx.forced_full = True  # Auto-promoted
        ctx.backend = MagicMock()

        engine._apply_object_lock_retention(ctx)

        retain = ctx.backend.set_retain_until.call_args[0][0]
        expected_min = datetime.now(UTC) + timedelta(days=59)
        assert retain > expected_min


# ---------------------------------------------------------------
# S3 backend Object Lock upload tests
# ---------------------------------------------------------------


class TestS3BackendObjectLock:
    """Test that S3 uploads include Object Lock headers when set."""

    def test_upload_without_lock(self):
        from src.storage.s3 import S3Storage

        backend = S3Storage(bucket="b", region="eu-west-1")
        assert backend._build_lock_extra_args() == {}

    def test_upload_with_lock(self):
        from src.storage.s3 import S3Storage

        backend = S3Storage(bucket="b", region="eu-west-1")
        retain = datetime(2027, 1, 1, tzinfo=UTC)
        backend.set_retain_until(retain)

        args = backend._build_lock_extra_args()
        assert args["ObjectLockMode"] == "COMPLIANCE"
        assert args["ObjectLockRetainUntilDate"] == retain

    def test_set_retain_until_none_clears(self):
        from src.storage.s3 import S3Storage

        backend = S3Storage(bucket="b", region="eu-west-1")
        backend.set_retain_until(datetime(2027, 1, 1, tzinfo=UTC))
        backend.set_retain_until(None)
        assert backend._build_lock_extra_args() == {}


# ---------------------------------------------------------------
# Wizard professional profile creation test
# ---------------------------------------------------------------


class TestBandwidthSpeedtestBucketRouting:
    """Test that bandwidth measurement uses the speedtest bucket for Object Lock."""

    def _make_engine(self, tmp_config_dir):
        from src.core.backup_engine import BackupEngine

        mgr = ConfigManager(config_dir=tmp_config_dir)
        return BackupEngine(mgr, events=EventBus())

    def test_s3_lock_with_speedtest_uses_test_bucket(self, tmp_config_dir):
        """S3 Object Lock + speedtest bucket → measure on speedtest bucket."""
        from unittest.mock import patch

        from src.storage.s3 import S3Storage

        profile = BackupProfile(
            name="Pro",
            bandwidth_percent=75,
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="main-bucket",
                s3_region="eu-west-1",
                s3_access_key="AKIA",
                s3_secret_key="secret",
                s3_object_lock=True,
                s3_speedtest_bucket="main-bucket-speedtest",
            ),
        )
        engine = self._make_engine(tmp_config_dir)
        main_backend = MagicMock(spec=S3Storage)

        with patch(
            "src.core.backup_engine.measure_bandwidth", return_value=5_000_000
        ) as mock_measure:
            engine._apply_bandwidth_throttle(main_backend, profile)

            # measure_bandwidth was called with a DIFFERENT backend (not main)
            mock_measure.assert_called_once()
            test_backend = mock_measure.call_args[0][0]
            assert test_backend is not main_backend
            assert isinstance(test_backend, S3Storage)
            assert test_backend._bucket == "main-bucket-speedtest"
            # Throttle applied to the MAIN backend
            main_backend.set_bandwidth_limit.assert_called_once()

    def test_s3_lock_without_speedtest_raises(self, tmp_config_dir):
        """S3 Object Lock + no speedtest bucket → raises ValueError."""

        import pytest

        from src.storage.s3 import S3Storage

        profile = BackupProfile(
            name="Pro",
            bandwidth_percent=75,
            object_lock_enabled=True,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="main-bucket",
                s3_region="eu-west-1",
                s3_access_key="AKIA",
                s3_secret_key="secret",
                s3_object_lock=True,
                s3_speedtest_bucket="",
            ),
        )
        engine = self._make_engine(tmp_config_dir)
        main_backend = MagicMock(spec=S3Storage)

        with pytest.raises(ValueError, match="missing s3_speedtest_bucket"):
            engine._apply_bandwidth_throttle(main_backend, profile)

    def test_s3_no_lock_uses_main(self, tmp_config_dir):
        """S3 without Object Lock → measure on main bucket."""
        from unittest.mock import patch

        from src.storage.s3 import S3Storage

        profile = BackupProfile(
            name="Standard",
            bandwidth_percent=75,
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="main-bucket",
                s3_region="eu-west-1",
                s3_access_key="AKIA",
                s3_secret_key="secret",
                s3_object_lock=False,
            ),
        )
        engine = self._make_engine(tmp_config_dir)
        main_backend = MagicMock(spec=S3Storage)

        with patch(
            "src.core.backup_engine.measure_bandwidth", return_value=5_000_000
        ) as mock_measure:
            engine._apply_bandwidth_throttle(main_backend, profile)
            mock_measure.assert_called_once_with(main_backend)


# ---------------------------------------------------------------
# Wizard professional profile creation test
# ---------------------------------------------------------------


class TestWizardProProfile:
    """Test that the wizard creates correct professional profiles."""

    def test_pro_profile_has_object_lock_fields(self):
        """Simulate wizard data and verify profile creation."""
        from src.ui.wizard import SetupWizard

        wizard = SetupWizard.__new__(SetupWizard)
        wizard._data = {
            "name": "Pro Backup",
            "sources": ["/data"],
            "pro_aws_key": "AKIA_TEST",
            "pro_aws_secret": "SECRET_TEST",
            "pro_region": "eu-west-1",
            "pro_bucket": "test-bucket",
            "pro_retention_idx": 1,  # 13 months
            "pro_encrypt": False,
            "pro_encrypt_password": "",
            "pro_mirror_local": False,
            "pro_mirror_path": "",
        }
        wizard.result_profile = None

        # Mock the window methods used by _create_pro_profile
        wizard._canvas = MagicMock()
        wizard._win = MagicMock()

        wizard._create_pro_profile()

        p = wizard.result_profile
        assert p is not None
        assert p.object_lock_enabled is True
        assert p.storage.s3_object_lock is True
        assert p.storage.s3_object_lock_mode == "COMPLIANCE"
        assert p.storage.s3_object_lock_days == 395  # 13 months
        assert p.retention.gfs_enabled is False
        assert p.full_backup_every == 30
        assert p.backup_type == BackupType.DIFFERENTIAL
        assert p.storage.s3_speedtest_bucket == "test-bucket-speedtest"

    def test_pro_profile_with_encryption(self):
        from src.ui.wizard import SetupWizard

        wizard = SetupWizard.__new__(SetupWizard)
        wizard._data = {
            "name": "Encrypted Pro",
            "sources": ["/data"],
            "pro_aws_key": "AKIA",
            "pro_aws_secret": "SECRET",
            "pro_region": "us-east-1",
            "pro_bucket": "enc-bucket",
            "pro_retention_idx": 0,  # 4 months
            "pro_encrypt": True,
            "pro_encrypt_password": "strongpass123",
            "pro_mirror_local": False,
            "pro_mirror_path": "",
        }
        wizard.result_profile = None
        wizard._canvas = MagicMock()
        wizard._win = MagicMock()

        wizard._create_pro_profile()

        p = wizard.result_profile
        assert p.encryption.enabled is True
        assert p.encrypt_primary is True

    def test_pro_profile_with_local_mirror(self):
        from src.ui.wizard import SetupWizard

        wizard = SetupWizard.__new__(SetupWizard)
        wizard._data = {
            "name": "Mirrored Pro",
            "sources": ["/data"],
            "pro_aws_key": "AKIA",
            "pro_aws_secret": "SECRET",
            "pro_region": "eu-west-1",
            "pro_bucket": "mirror-bucket",
            "pro_retention_idx": 0,  # 4 months
            "pro_encrypt": False,
            "pro_encrypt_password": "",
            "pro_mirror_local": True,
            "mirror1": {
                "type": "local",
                "vars": {"destination_path": "D:/Backups"},
            },
        }
        wizard.result_profile = None
        wizard._canvas = MagicMock()
        wizard._win = MagicMock()

        wizard._create_pro_profile()

        p = wizard.result_profile
        assert len(p.mirror_destinations) == 1
        assert p.mirror_destinations[0].storage_type == StorageType.LOCAL
        assert p.mirror_destinations[0].destination_path == "D:/Backups"

    def test_pro_profile_speedtest_failed_sets_empty(self):
        """When speedtest bucket creation failed, field is empty."""
        from src.ui.wizard import SetupWizard

        wizard = SetupWizard.__new__(SetupWizard)
        wizard._data = {
            "name": "Failed Speedtest",
            "sources": ["/data"],
            "pro_aws_key": "AKIA",
            "pro_aws_secret": "SECRET",
            "pro_region": "eu-west-1",
            "pro_bucket": "fail-bucket",
            "pro_retention_idx": 0,
            "pro_encrypt": False,
            "pro_encrypt_password": "",
            "pro_mirror_local": False,
            "pro_mirror_path": "",
            "pro_speedtest_failed": True,
        }
        wizard.result_profile = None
        wizard._canvas = MagicMock()
        wizard._win = MagicMock()

        wizard._create_pro_profile()

        p = wizard.result_profile
        assert p.storage.s3_speedtest_bucket == ""

    def test_roundtrip_speedtest_bucket_field(self, tmp_config_dir):
        """Speedtest bucket field persists through JSON save/load."""
        mgr = ConfigManager(config_dir=tmp_config_dir)
        profile = BackupProfile(
            name="SpeedtestRoundtrip",
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="main-bucket",
                s3_region="eu-west-1",
                s3_access_key="AKIA",
                s3_secret_key="secret",
                s3_speedtest_bucket="main-bucket-speedtest",
            ),
        )
        mgr.save_profile(profile)

        loaded = mgr.get_all_profiles()[0]
        assert loaded.storage.s3_speedtest_bucket == "main-bucket-speedtest"
