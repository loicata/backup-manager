"""Tests for health_checker module."""

import threading
from unittest.mock import MagicMock, patch

from src.core.config import StorageConfig, StorageType
from src.core.health_checker import (
    DestinationHealth,
    _check_destination,
    _parse_free_space,
    check_destinations_async,
    format_bytes,
)


class TestFormatBytes:
    """format_bytes converts byte counts to human-readable strings."""

    def test_zero(self):
        assert format_bytes(0) == "0 B"

    def test_bytes(self):
        assert format_bytes(512) == "512 B"

    def test_kilobytes(self):
        assert format_bytes(1024) == "1 KB"

    def test_megabytes(self):
        assert format_bytes(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        result = format_bytes(48_500_000_000)
        assert "GB" in result
        assert "45" in result

    def test_terabytes(self):
        result = format_bytes(2 * 1024**4)
        assert "TB" in result

    def test_negative(self):
        assert format_bytes(-100) == "0 B"


class TestParseFreeSpace:
    """_parse_free_space extracts bytes from test_connection messages."""

    def test_local_message(self):
        result = _parse_free_space("Connected — 83.8 GB free")
        assert result is not None
        expected = int(83.8 * 1024**3)
        assert result == expected

    def test_sftp_message(self):
        msg = "SFTP connected: user@host:22\n45.2 GB free"
        result = _parse_free_space(msg)
        assert result is not None
        expected = int(45.2 * 1024**3)
        assert result == expected

    def test_s3_no_space(self):
        result = _parse_free_space("Connected to my-bucket (aws)")
        assert result is None

    def test_empty_message(self):
        assert _parse_free_space("") is None

    def test_zero_gb(self):
        result = _parse_free_space("Connected — 0.1 GB free")
        assert result is not None
        assert result > 0


class TestCheckDestination:
    """_check_destination tests connectivity and free space."""

    def test_local_success_with_free_space(self):
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="/tmp/test",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            True,
            "Connected — 46.6 GB free",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Storage")

        assert health.label == "Storage"
        assert health.backend_type == "local"
        assert health.online is True
        assert health.free_bytes == int(46.6 * 1024**3)

    def test_sftp_success_with_free_space(self):
        config = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="myserver",
            sftp_username="user",
            sftp_remote_path="/backup",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            True,
            "SFTP connected: user@myserver:22\n12.3 GB free",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Mirror 1")

        assert health.online is True
        assert health.free_bytes == int(12.3 * 1024**3)

    def test_s3_returns_none_free_space(self):
        config = StorageConfig(
            storage_type=StorageType.S3,
            s3_bucket="test-bucket",
            s3_region="eu-west-1",
            s3_access_key="key",
            s3_secret_key="secret",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            True,
            "Connected to test-bucket (aws)",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Mirror 2")

        assert health.online is True
        assert health.free_bytes is None

    def test_connection_failure(self):
        config = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="unreachable",
            sftp_username="user",
            sftp_remote_path="/backup",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (False, "Connection refused")

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Mirror 1")

        assert health.online is False
        assert "Connection refused" in health.error

    def test_exception_during_check(self):
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="/nonexistent",
        )

        with patch(
            "src.core.health_checker.create_backend",
            side_effect=OSError("No such device"),
        ):
            health = _check_destination(config, "Storage")

        assert health.online is False
        assert "No such device" in health.error


class TestCheckDestinationsAsync:
    """check_destinations_async runs checks in background threads."""

    def test_calls_callback_for_each_destination(self):
        storage = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="/tmp/backup",
        )
        mirror = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="server",
            sftp_username="user",
            sftp_remote_path="/backup",
        )

        results = {}
        event = threading.Event()

        def callback(idx, health):
            results[idx] = health
            if len(results) >= 2:
                event.set()

        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            True,
            "Connected — 10.0 GB free",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            check_destinations_async(storage, [mirror], callback)
            event.wait(timeout=5)

        assert 0 in results
        assert 1 in results
        assert results[0].label == "Storage"
        assert results[1].label == "Mirror 1"

    def test_skips_unconfigured_destinations(self):
        storage = StorageConfig()  # Default empty — will fail validate
        mirror = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="server",
            sftp_username="user",
            sftp_remote_path="/backup",
        )

        results = {}
        event = threading.Event()

        def callback(idx, health):
            results[idx] = health
            event.set()

        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            True,
            "Connected to bucket (aws)",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            check_destinations_async(storage, [mirror], callback)
            event.wait(timeout=5)

        # Only mirror should be checked (index 1)
        assert 0 not in results
        assert 1 in results


class TestDestinationHealth:
    """DestinationHealth dataclass basic tests."""

    def test_default_values(self):
        h = DestinationHealth(label="Test", backend_type="local")
        assert h.online is None
        assert h.free_bytes is None
        assert h.error == ""

    def test_with_values(self):
        h = DestinationHealth(
            label="Storage",
            backend_type="sftp",
            online=True,
            free_bytes=1024,
        )
        assert h.online is True
        assert h.free_bytes == 1024
