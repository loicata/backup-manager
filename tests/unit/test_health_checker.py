"""Tests for health_checker module."""

import threading
from unittest.mock import MagicMock, patch

from src.core.config import StorageConfig, StorageType
from src.core.health_checker import (
    DestinationHealth,
    _check_destination,
    _is_transient_wakeup_error,
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


class TestTransientWakeupRetry:
    """v3.7.2 silent retry on USB-HDD spin-up timeouts.

    The first probe of a sleeping HDD triggers its wake-up sequence
    (10-30 s on deep-power-save drives) and can exceed
    ``CONNECTION_TIMEOUT = 30 s``. The retry that follows almost
    always finds the drive already spinning. Only transient markers
    trigger the silent retry — real failures surface immediately.
    """

    def test_is_transient_matches_timeout_message(self):
        assert _is_transient_wakeup_error(
            "Connection test timed out after 30s. The drive may be …"
        )

    def test_is_transient_matches_drive_not_ready(self):
        assert _is_transient_wakeup_error(
            "Drive not ready after wake-up retries: G:\\Backup Manager"
        )

    def test_is_transient_case_insensitive(self):
        assert _is_transient_wakeup_error("TIMED OUT after 30s")

    def test_is_transient_rejects_permission_denied(self):
        assert not _is_transient_wakeup_error(
            "Destination is read-only or locked (permission denied)"
        )

    def test_is_transient_rejects_connection_refused(self):
        assert not _is_transient_wakeup_error("Connection refused")

    def test_is_transient_rejects_empty(self):
        assert not _is_transient_wakeup_error("")

    def test_retry_succeeds_after_transient_timeout(self):
        """First probe times out (drive spinning up), second succeeds.

        Mirrors the real-world v3.7.1 post-backup flash: backup ends,
        drive starts spinning down, first health poll lands during
        spin-up and times out, retry finds the drive ready.
        """
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="G:\\Backup Manager",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.side_effect = [
            (False, "Connection test timed out after 30s. The drive …"),
            (True, "Connected — 2456.3 GB free"),
        ]

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Storage")

        assert health.online is True
        assert health.free_bytes == int(2456.3 * 1024**3)
        # Called twice — once for the failing probe, once for the retry.
        assert mock_backend.test_connection.call_count == 2

    def test_retry_reports_failure_when_both_fail(self):
        """Two transient timeouts in a row → reported offline.

        Pins that the retry is *one-shot*, not an infinite loop. If
        the drive is genuinely unresponsive (real unplug rather than
        spin-up), the second timeout surfaces.
        """
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="G:\\Backup Manager",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.side_effect = [
            (False, "Connection test timed out after 30s. The drive …"),
            (False, "Drive not ready after wake-up retries: G:\\…"),
        ]

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Storage")

        assert health.online is False
        # The error reported is the SECOND probe's message — the
        # most recent state of the world wins. Caller does not see
        # the first transient blip.
        assert "Drive not ready" in health.error
        assert mock_backend.test_connection.call_count == 2

    def test_no_retry_on_permission_error(self):
        """Permission-denied is NOT a transient marker: skip retry."""
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="G:\\Backup Manager",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (
            False,
            "Destination is read-only or locked (permission denied)",
        )

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Storage")

        assert health.online is False
        # No retry — exactly one probe.
        assert mock_backend.test_connection.call_count == 1

    def test_no_retry_on_connection_refused(self):
        """ConnectionRefused (SFTP/network) is a real failure: no retry."""
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
        assert mock_backend.test_connection.call_count == 1

    def test_no_retry_on_success(self):
        """Successful probe does not trigger a retry."""
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="G:\\Backup Manager",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.return_value = (True, "Connected — 100.0 GB free")

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            health = _check_destination(config, "Storage")

        assert health.online is True
        assert mock_backend.test_connection.call_count == 1


class TestHealthPollingIdempotent:
    """_check_destination is idempotent — safe for continuous polling."""

    def test_poll_recovers_after_initial_failure(self):
        """Simulates a destination going from offline to online on re-poll."""
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path="/tmp/backup",
        )
        mock_backend = MagicMock()
        mock_backend.test_connection.side_effect = [
            (False, "Permission denied: G:\\"),
            (True, "Connected — 75.8 GB free"),
        ]

        with patch(
            "src.core.health_checker.create_backend",
            return_value=mock_backend,
        ):
            # First check: offline
            health1 = _check_destination(config, "Storage")
            assert health1.online is False
            assert "Permission denied" in health1.error

            # Retry: now online
            health2 = _check_destination(config, "Storage")
            assert health2.online is True
            assert health2.free_bytes == int(75.8 * 1024**3)

    def test_repeated_failures_produce_consistent_results(self):
        """Multiple failures return the same error each time."""
        config = StorageConfig(
            storage_type=StorageType.SFTP,
            sftp_host="unreachable",
            sftp_username="user",
            sftp_remote_path="/backup",
        )

        with patch(
            "src.core.health_checker.create_backend",
            side_effect=ConnectionRefusedError("Connection refused"),
        ):
            results = [_check_destination(config, "Mirror 1") for _ in range(3)]

        for h in results:
            assert h.online is False
            assert "Connection refused" in h.error
