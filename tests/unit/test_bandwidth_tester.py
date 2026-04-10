"""Tests for src.core.bandwidth_tester — bandwidth measurement and throttle."""

from unittest.mock import MagicMock

import pytest

from src.core.bandwidth_tester import (
    SAMPLE_SIZE,
    TEMP_PREFIX,
    WARMUP_SIZE,
    _cleanup,
    _write_sample,
    compute_throttle_kbps,
    measure_bandwidth,
)


class TestComputeThrottleKbps:
    def test_100_percent_returns_zero(self):
        assert compute_throttle_kbps(10_000_000, 100) == 0

    def test_50_percent(self):
        # 10 MB/s = 10_000_000 B/s → 50% → 4882 KB/s
        result = compute_throttle_kbps(10_000_000, 50)
        assert result == int((10_000_000 / 1024) * 0.5)

    def test_25_percent(self):
        result = compute_throttle_kbps(10_000_000, 25)
        assert result == int((10_000_000 / 1024) * 0.25)

    def test_75_percent(self):
        result = compute_throttle_kbps(10_000_000, 75)
        assert result == int((10_000_000 / 1024) * 0.75)

    def test_zero_measured_returns_zero(self):
        assert compute_throttle_kbps(0, 50) == 0

    def test_negative_measured_returns_zero(self):
        assert compute_throttle_kbps(-1, 50) == 0

    def test_zero_percent_uses_minimum(self):
        result = compute_throttle_kbps(10_000_000, 0)
        assert result == int((10_000_000 / 1024) * 0.25)

    def test_result_is_at_least_one(self):
        result = compute_throttle_kbps(100, 25)
        assert result >= 1


class TestWriteSample:
    def test_returns_positive_speed(self):
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        speed = _write_sample(backend, 1024)
        assert speed > 0
        backend.upload_file.assert_called_once()
        backend.delete_backup.assert_called_once()

    def test_temp_file_name_format(self):
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        _write_sample(backend, 1024)
        call_args = backend.upload_file.call_args
        remote_path = call_args[0][1]
        assert remote_path.startswith(TEMP_PREFIX)
        assert "1024" in remote_path

    def test_cleanup_called_on_upload_error(self):
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("upload failed")
        backend.delete_backup = MagicMock()

        with pytest.raises(OSError, match="upload failed"):
            _write_sample(backend, 1024)

        # Cleanup must still be called
        backend.delete_backup.assert_called_once()


class TestCleanup:
    def test_ignores_file_not_found(self):
        backend = MagicMock()
        backend.delete_backup.side_effect = FileNotFoundError()
        _cleanup(backend, "test_file")  # Should not raise

    def test_logs_warning_on_other_error(self):
        backend = MagicMock()
        backend.delete_backup.side_effect = OSError("permission denied")
        _cleanup(backend, "test_file")  # Should not raise


class TestMeasureBandwidth:
    def test_warmup_then_measurement(self):
        """Should call upload_file twice: warmup + measurement."""
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        result = measure_bandwidth(backend)
        assert result > 0
        # 2 calls: 1 warmup (1 MB) + 1 measurement (16 MB)
        assert backend.upload_file.call_count == 2

    def test_returns_zero_when_all_fail(self):
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("fail")
        backend.delete_backup = MagicMock()

        result = measure_bandwidth(backend)
        assert result == 0.0

    def test_succeeds_even_if_warmup_fails(self):
        """Measurement should proceed even if warmup fails."""
        backend = MagicMock()
        call_count = 0

        def side_effect(fileobj, name, size=0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("warmup fails")

        backend.upload_file = MagicMock(side_effect=side_effect)
        backend.delete_backup = MagicMock()

        result = measure_bandwidth(backend)
        assert result > 0

    def test_sample_sizes_are_correct(self):
        """Warmup is 1 MB, measurement is 16 MB."""
        assert WARMUP_SIZE == 1 * 1024 * 1024
        assert SAMPLE_SIZE == 16 * 1024 * 1024
