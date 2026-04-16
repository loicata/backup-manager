"""Tests for the storage base module (ThrottledReader, with_retry)."""

import io
import os
import time

import pytest

from src.storage.base import ThrottledReader, with_retry

_IN_CI = os.environ.get("CI") == "true"


class TestThrottledReader:
    """Test bandwidth throttling wrapper."""

    def test_reads_data_correctly(self):
        """ThrottledReader should pass through data correctly."""
        data = b"Hello World" * 100
        reader = ThrottledReader(io.BytesIO(data), limit_kbps=0)
        result = reader.read()
        assert result == data

    @pytest.mark.skipif(_IN_CI, reason="Timing-sensitive test unreliable in CI")
    def test_throttles_speed(self):
        """Reading should be slowed down with a tight limit."""
        data = b"x" * 4096  # 4 KB
        reader = ThrottledReader(io.BytesIO(data), limit_kbps=1)  # 1 KB/s
        start = time.monotonic()
        result = b""
        while True:
            chunk = reader.read(1024)
            if not chunk:
                break
            result += chunk
        elapsed = time.monotonic() - start
        assert result == data
        # Should take at least ~3s for 4KB at 1KB/s (with some margin)
        assert elapsed >= 2.0

    def test_no_throttle_when_zero(self):
        """Zero limit should not throttle."""
        data = b"x" * 10000
        reader = ThrottledReader(io.BytesIO(data), limit_kbps=0)
        start = time.monotonic()
        reader.read()
        elapsed = time.monotonic() - start
        assert elapsed < 1.0

    def test_seek_and_tell(self):
        """Seek and tell should delegate to underlying file."""
        data = b"0123456789"
        reader = ThrottledReader(io.BytesIO(data), limit_kbps=100)
        reader.seek(5)
        assert reader.tell() == 5
        assert reader.read() == b"56789"

    def test_context_manager(self):
        """Should work as context manager."""
        data = b"test"
        with ThrottledReader(io.BytesIO(data), limit_kbps=100) as reader:
            assert reader.read() == data

    def test_name_property(self):
        """Name property should return underlying name or fallback."""
        reader = ThrottledReader(io.BytesIO(b""), limit_kbps=100)
        assert reader.name == "<throttled>"

    def test_empty_read(self):
        """Empty BytesIO should return empty bytes."""
        reader = ThrottledReader(io.BytesIO(b""), limit_kbps=100)
        assert reader.read() == b""

    def test_concurrent_reads_do_not_corrupt_stream(self):
        """Parallel reads on a non-atomic fileobj must stay serialized.

        Simulates boto3's ``upload_fileobj`` multipart behavior, which
        can dispatch concurrent ``read()`` calls from its worker pool.
        A fileobj whose ``read`` is not atomic (captures position,
        yields, advances) would produce duplicated or overlapping data
        if ``ThrottledReader`` did not serialize access.
        """
        import threading

        class _NonAtomicFileobj:
            """Fileobj whose read is intentionally non-atomic."""

            def __init__(self, data: bytes) -> None:
                self._data = data
                self._pos = 0

            def read(self, size: int = -1) -> bytes:
                if self._pos >= len(self._data):
                    return b""
                if size < 0:
                    size = len(self._data) - self._pos
                pos = self._pos
                # Forced context switch: any caller not holding an
                # external lock can preempt here and read the same
                # offset, producing duplicate bytes on the wire.
                time.sleep(0.001)
                self._pos = pos + size
                return self._data[pos : pos + size]

        data = bytes((i % 256) for i in range(8192))
        reader = ThrottledReader(_NonAtomicFileobj(data), limit_kbps=0)

        chunks: list[bytes] = []
        chunks_lock = threading.Lock()

        def worker() -> None:
            while True:
                chunk = reader.read(256)
                if not chunk:
                    return
                with chunks_lock:
                    chunks.append(chunk)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        total_read = sum(len(c) for c in chunks)
        assert total_read == len(data), (
            "ThrottledReader must not produce duplicated or missing bytes " "under concurrent reads"
        )

    def test_concurrent_reads_track_counter_consistently(self):
        """Window-byte counter must match the total bytes returned."""
        import threading

        data = b"x" * 4096
        # High limit — throttling logic runs (exercises _window_bytes)
        # but shouldn't meaningfully slow the test.
        reader = ThrottledReader(io.BytesIO(data), limit_kbps=100_000)

        total_read = [0]
        counter_lock = threading.Lock()

        def worker() -> None:
            while True:
                chunk = reader.read(64)
                if not chunk:
                    return
                with counter_lock:
                    total_read[0] += len(chunk)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert total_read[0] == len(data)
        # Internal counter must not exceed the bytes we actually read.
        assert reader._window_bytes <= len(data)


class TestWithRetry:
    """Test retry decorator."""

    def test_succeeds_first_try(self):
        """Successful call should not retry."""
        call_count = [0]

        @with_retry(max_retries=3, base_delay=0.01)
        def func():
            call_count[0] += 1
            return "ok"

        assert func() == "ok"
        assert call_count[0] == 1

    def test_retries_on_failure(self):
        """Should retry specified number of times."""
        call_count = [0]

        @with_retry(max_retries=2, base_delay=0.01)
        def func():
            call_count[0] += 1
            raise ValueError("fail")

        with pytest.raises(ValueError, match="fail"):
            func()

        assert call_count[0] == 3  # 1 initial + 2 retries

    def test_succeeds_after_retry(self):
        """Should succeed if later attempt works."""
        call_count = [0]

        @with_retry(max_retries=3, base_delay=0.01)
        def func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise RuntimeError("not yet")
            return "finally"

        assert func() == "finally"
        assert call_count[0] == 3

    def test_preserves_function_name(self):
        """Decorated function should keep its name."""

        @with_retry(max_retries=1, base_delay=0.01)
        def my_special_func():
            pass

        assert my_special_func.__name__ == "my_special_func"
