"""Tests for src.core.bandwidth_tester — bandwidth measurement and throttle."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.core.bandwidth_tester import (
    FAST_LINK_THRESHOLD,
    FULL_SAMPLE_SIZE,
    PROBE_SIZE,
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


def _fake_monotonic_factory(step: float = 0.01):
    """Return a fake monotonic clock that advances by *step* each call.

    Guarantees elapsed > 0 even when the mock runs instantly.
    """
    t = [0.0]

    def _fake():
        t[0] += step
        return t[0]

    return _fake


class TestWriteSample:
    def test_returns_positive_speed(self):
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        with patch(
            "src.core.bandwidth_tester.time.monotonic", side_effect=_fake_monotonic_factory()
        ):
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
    def test_warmup_probe_and_full(self):
        """Should call upload_file 3 times: warmup + probe + full sample.

        The mock returns instantly (simulating a fast link), so the
        adaptive logic runs the full 512 MB sample after the 128 MB probe.
        """
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        with patch(
            "src.core.bandwidth_tester.time.monotonic", side_effect=_fake_monotonic_factory()
        ):
            result = measure_bandwidth(backend)
        assert result > 0
        # 3 calls: 1 warmup (1 MB) + 1 probe (128 MB) + 1 full (512 MB)
        assert backend.upload_file.call_count == 3

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

        with patch(
            "src.core.bandwidth_tester.time.monotonic", side_effect=_fake_monotonic_factory()
        ):
            result = measure_bandwidth(backend)
        assert result > 0

    def test_sample_sizes_are_correct(self):
        """Warmup is 1 MB, probe is 128 MB, full sample is 512 MB."""
        assert WARMUP_SIZE == 1 * 1024 * 1024
        assert PROBE_SIZE == 128 * 1024 * 1024
        assert FULL_SAMPLE_SIZE == 512 * 1024 * 1024

    def test_threshold_is_20mbps(self):
        assert FAST_LINK_THRESHOLD == 20 * 1024 * 1024
