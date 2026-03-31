"""Tests for target pre-check before backup.

Verifies that precheck_targets() tests all configured destinations
and that _describe_target() produces correct messages per storage type.
"""

from unittest.mock import MagicMock, patch

from src.core.backup_engine import BackupEngine
from src.core.config import (
    BackupProfile,
    ConfigManager,
    StorageConfig,
    StorageType,
)
from src.core.events import EventBus


def _engine() -> BackupEngine:
    """Create a BackupEngine with mocked config manager."""
    cm = MagicMock(spec=ConfigManager)
    return BackupEngine(cm, events=EventBus())


def _mock_backend(ok: bool, msg: str) -> MagicMock:
    """Create a mock backend with a test_connection result."""
    backend = MagicMock()
    backend.test_connection.return_value = (ok, msg)
    return backend


# ---------------------------------------------------------------------------
# precheck_targets()
# ---------------------------------------------------------------------------


class TestPrecheckTargets:
    """precheck_targets tests all configured destinations."""

    def test_all_ok_no_mirrors(self):
        """Storage only, reachable — all results ok."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="D:\\Backups",
            ),
        )
        engine = _engine()
        backend = _mock_backend(True, "Connected — 50.0 GB free")

        with patch.object(engine, "_get_backend", return_value=backend):
            results = engine.precheck_targets(profile)

        assert len(results) == 1
        assert results[0][0] == "Storage"
        assert results[0][2] is True

    def test_storage_fails(self):
        """Storage unreachable — returned in results."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="D:\\Backups",
            ),
        )
        engine = _engine()
        backend = _mock_backend(False, "Path does not exist: D:\\Backups")

        with patch.object(engine, "_get_backend", return_value=backend):
            results = engine.precheck_targets(profile)

        assert len(results) == 1
        assert results[0][2] is False
        assert "Path does not exist" in results[0][3]

    def test_with_mirrors(self):
        """Storage + 2 mirrors, all tested."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="D:\\Backups",
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.SFTP,
                    sftp_host="192.168.3.243",
                    sftp_username="user",
                ),
                StorageConfig(
                    storage_type=StorageType.S3,
                    s3_bucket="my-bucket",
                ),
            ],
        )
        engine = _engine()
        backend = _mock_backend(True, "OK")

        with patch.object(engine, "_get_backend", return_value=backend):
            results = engine.precheck_targets(profile)

        assert len(results) == 3
        assert results[0][0] == "Storage"
        assert results[1][0] == "Mirror 1"
        assert results[2][0] == "Mirror 2"

    def test_mirror_fails_storage_ok(self):
        """Storage ok, Mirror 1 fails — only mirror in failures."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="D:\\Backups",
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.SFTP,
                    sftp_host="192.168.3.243",
                    sftp_username="user",
                ),
            ],
        )
        engine = _engine()
        call_count = {"n": 0}

        def mock_backend(config):
            call_count["n"] += 1
            b = MagicMock()
            if call_count["n"] == 1:
                b.test_connection.return_value = (True, "OK")
            else:
                b.test_connection.return_value = (False, "Connection timeout")
            return b

        with patch.object(engine, "_get_backend", side_effect=mock_backend):
            results = engine.precheck_targets(profile)

        failures = [r for r in results if not r[2]]
        assert len(failures) == 1
        assert failures[0][0] == "Mirror 1"

    def test_backend_creation_fails(self):
        """Backend creation raises — caught and returned as failure."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="bad-host",
                sftp_username="user",
            ),
        )
        engine = _engine()

        with patch.object(
            engine,
            "_get_backend",
            side_effect=ValueError("bad config"),
        ):
            results = engine.precheck_targets(profile)

        assert len(results) == 1
        assert results[0][2] is False
        assert "bad config" in results[0][3]

    def test_multiple_failures(self):
        """Storage + Mirror 2 both fail."""
        profile = BackupProfile(
            name="Test",
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="G:\\Backups",
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.LOCAL,
                    destination_path="E:\\Mirror",
                ),
                StorageConfig(
                    storage_type=StorageType.SFTP,
                    sftp_host="192.168.3.243",
                    sftp_username="user",
                ),
            ],
        )
        engine = _engine()
        call_count = {"n": 0}

        def mock_backend(config):
            call_count["n"] += 1
            b = MagicMock()
            if call_count["n"] == 2:
                b.test_connection.return_value = (True, "OK")
            else:
                b.test_connection.return_value = (False, "Unreachable")
            return b

        with patch.object(engine, "_get_backend", side_effect=mock_backend):
            results = engine.precheck_targets(profile)

        failures = [r for r in results if not r[2]]
        assert len(failures) == 2
        assert failures[0][0] == "Storage"
        assert failures[1][0] == "Mirror 2"


# ---------------------------------------------------------------------------
# _describe_target()
# ---------------------------------------------------------------------------


class TestDescribeTarget:
    """_describe_target produces correct messages per storage type."""

    def test_local(self):
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="D:\\Backups",
        )
        msg = BackupEngine._describe_target(config)
        assert "USB drive" in msg
        assert "D:\\Backups" in msg

    def test_network(self):
        config = StorageConfig(
            storage_type=StorageType.NETWORK,
            destination_path="\\\\SERVER\\Share",
            network_username="user",
            network_password="pass",
        )
        msg = BackupEngine._describe_target(config)
        assert "network share" in msg
        assert "\\\\SERVER\\Share" in msg

    def test_sftp(self):
        config = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="192.168.3.243",
            sftp_port=22,
            sftp_username="cipango56",
        )
        msg = BackupEngine._describe_target(config)
        assert "SSH server" in msg
        assert "cipango56@192.168.3.243:22" in msg

    def test_s3(self):
        config = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="my-bucket",
            s3_provider="aws",
            s3_region="eu-west-1",
        )
        msg = BackupEngine._describe_target(config)
        assert "S3 bucket" in msg
        assert "my-bucket" in msg
        assert "aws" in msg
