"""Tests for src.core.bandwidth_tester — bandwidth measurement and throttle."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.core.bandwidth_tester import (
    FAST_LINK_THRESHOLD,
    FULL_SAMPLE_SIZE,
    PROBE_SIZE,
    TEMP_PREFIX,
    WARMUP_SIZE,
    _cleanup,
    _RandomStream,
    _remote_sync,
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

    def test_zero_probe_returns_zero(self):
        """A probe that completes in zero time produces speed=0 and the
        outer ``measure_bandwidth`` short-circuits to 0 immediately."""
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()

        # Force ``time.monotonic`` to return the same instant every call
        # so warmup AND probe both compute ``elapsed = 0`` → ``_write_sample``
        # returns 0 → outer function logs and returns 0.0.
        with patch("src.core.bandwidth_tester.time.monotonic", return_value=42.0):
            result = measure_bandwidth(backend)
        assert result == 0.0

    def test_full_sample_failure_falls_back_to_probe(self):
        """If the 512 MB full sample upload errors, the probe value is
        used as the final answer rather than the function returning 0."""
        backend = MagicMock()
        call_count = 0

        def upload_side_effect(fileobj, name, size=0):
            nonlocal call_count
            call_count += 1
            # Warmup (1) and probe (2) succeed; full sample (3) fails.
            if call_count == 3:
                raise OSError("full-sample upload failed")

        backend.upload_file = MagicMock(side_effect=upload_side_effect)
        backend.delete_backup = MagicMock()

        with patch(
            "src.core.bandwidth_tester.time.monotonic", side_effect=_fake_monotonic_factory()
        ):
            result = measure_bandwidth(backend)
        # Three upload attempts, but the result is the probe speed,
        # not zero — the fallback path on lines 88–97 kicked in.
        assert backend.upload_file.call_count == 3
        assert result > 0


# ---------------------------------------------------------------------------
# _RandomStream
# ---------------------------------------------------------------------------


class TestRandomStream:
    """The lazy random byte stream used by ``_write_sample``.

    The whole point of this class is to AVOID pre-allocating 512 MB of
    randomness in RAM (which the previous ``os.urandom(size)`` call did
    and which made the bandwidth test a memory hog). The tests below
    pin every readability contract so a future refactor cannot
    re-introduce the memory regression.
    """

    def test_readable_returns_true(self) -> None:
        # ``io.RawIOBase`` defaults to ``readable() -> False``; the
        # subclass must override or ``upload_file`` rejects it.
        assert _RandomStream(0).readable() is True

    def test_total_bytes_yielded_equals_size(self) -> None:
        size = 16 * 1024  # 16 KB — large enough to span multiple reads
        stream = _RandomStream(size)
        chunks: list[bytes] = []
        while True:
            data = stream.read(4096)
            if not data:
                break
            chunks.append(data)
        assert sum(len(c) for c in chunks) == size

    def test_zero_size_yields_empty(self) -> None:
        stream = _RandomStream(0)
        assert stream.read(8192) == b""
        assert stream.read(-1) == b""

    def test_read_unbounded_returns_all_remaining(self) -> None:
        # Negative ``n`` (or ``None``) drains the stream in one call —
        # used by non-streaming backends that buffer the full payload.
        size = 3 * 1024 * 1024 + 17  # 3 MB + odd bytes so it crosses
        # at least two internal CHUNK refills (1 MB each) AND ends mid-buffer.
        stream = _RandomStream(size)
        all_bytes = stream.read(-1)
        assert len(all_bytes) == size
        # Stream is exhausted afterwards.
        assert stream.read(-1) == b""

    def test_partial_reads_eventually_exhaust(self) -> None:
        size = 1024
        stream = _RandomStream(size)
        out = bytearray()
        while True:
            data = stream.read(64)
            if not data:
                break
            out.extend(data)
        assert len(out) == size
        # And further reads still return empty.
        assert stream.read(64) == b""

    def test_random_bytes_actually_random(self) -> None:
        """Two reads from independent streams produce different bytes —
        a regression on this would mean the stream is seeded statically."""
        a = _RandomStream(64).read(64)
        b = _RandomStream(64).read(64)
        # 64 random bytes colliding by chance is ~2^-512 — safe to assert.
        assert a != b
        assert len(a) == 64

    def test_read_more_than_buffer_returns_partial(self) -> None:
        """``read(n)`` may return fewer bytes than requested — that's
        the standard ``RawIOBase`` contract. Caller must loop."""
        stream = _RandomStream(8)
        # Ask for 1 MB but only 8 are available — the stream gives us
        # what it has on this call.
        chunk = stream.read(1024 * 1024)
        assert 0 < len(chunk) <= 8


# ---------------------------------------------------------------------------
# _write_sample — Object Lock guard + edge cases
# ---------------------------------------------------------------------------


class TestWriteSampleObjectLock:
    """Refuse to probe a bucket whose retain-until is a real datetime.

    The ``isinstance(retain_until, datetime)`` check (rather than a
    truthiness check) is deliberate — MagicMock attributes are truthy
    but not real datetimes, so the test backend stays exercisable.
    """

    def test_real_datetime_retain_aborts(self) -> None:
        backend = MagicMock()
        backend._retain_until = datetime(2030, 1, 1, tzinfo=UTC)
        with pytest.raises(RuntimeError, match="Object Lock"):
            _write_sample(backend, 1024)
        # The probe must NOT be uploaded — the test aborts before any I/O.
        backend.upload_file.assert_not_called()

    def test_mock_attribute_does_not_abort(self) -> None:
        # MagicMock auto-creates ``_retain_until`` as a Mock; the guard
        # must NOT fire on that path or every test using a MagicMock
        # backend would mysteriously fail with "Object Lock".
        backend = MagicMock()
        # Sanity: MagicMock returns a Mock for unknown attribute access.
        assert not isinstance(backend._retain_until, datetime)

        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()
        # No exception — the function proceeds normally.
        speed = _write_sample(backend, 1024)
        assert speed >= 0

    def test_zero_elapsed_returns_zero(self) -> None:
        """If the upload completes in zero time (or the clock didn't
        advance), the function returns 0.0 instead of dividing by zero."""
        backend = MagicMock()
        backend.upload_file = MagicMock()
        backend.delete_backup = MagicMock()
        with patch("src.core.bandwidth_tester.time.monotonic", return_value=10.0):
            speed = _write_sample(backend, 4096)
        assert speed == 0.0


# ---------------------------------------------------------------------------
# _remote_sync
# ---------------------------------------------------------------------------


class TestRemoteSync:
    """Forces a remote ``sync`` on SFTP backends, no-op everywhere else."""

    def test_no_get_transport_attribute_is_noop(self) -> None:
        # Generic backend (LocalStorage, S3Storage) — the function
        # short-circuits and does nothing observable.
        backend = MagicMock(spec=["upload_file"])  # no _get_transport
        # Should not raise and should not touch anything.
        _remote_sync(backend)

    def test_sftp_backend_runs_sync_command(self) -> None:
        backend = MagicMock()
        backend._get_transport = MagicMock()
        transport = MagicMock()
        backend._get_transport.return_value = transport
        channel = MagicMock()
        transport.open_session.return_value = channel
        channel.recv_exit_status.return_value = 0

        _remote_sync(backend)

        backend._get_transport.assert_called_once()
        channel.settimeout.assert_called_once_with(30)
        channel.exec_command.assert_called_once_with("sync")
        channel.recv_exit_status.assert_called_once()
        channel.close.assert_called_once()

    def test_channel_closed_even_on_exception(self) -> None:
        backend = MagicMock()
        backend._get_transport = MagicMock()
        transport = MagicMock()
        backend._get_transport.return_value = transport
        channel = MagicMock()
        transport.open_session.return_value = channel
        channel.exec_command.side_effect = OSError("ssh closed")

        # Must NOT propagate — sync is best-effort and a failure here
        # would mask the real upload result.
        _remote_sync(backend)
        channel.close.assert_called_once()

    def test_get_transport_failure_is_swallowed(self) -> None:
        """``transport`` itself blowing up must not propagate either."""
        backend = MagicMock()
        backend._get_transport = MagicMock(side_effect=OSError("no transport"))
        # Best-effort: logs at debug and returns silently.
        _remote_sync(backend)
