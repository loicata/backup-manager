"""Tests for parallel hashing in build_integrity_manifest.

The legacy sequential ``for`` loop was the dominant cost on large
backups (270 K files = ~3 hours on a SATA SSD with Defender real-time
scan).  Parallelising via ``ThreadPoolExecutor`` lifts throughput
roughly N× because ``hashlib.sha256`` releases the GIL during the
C-level update and file I/O syscalls release it during read.

These tests verify the parallel path's contract:

* Output dict is identical to the sequential implementation.
* Cache hits stay on the main thread (no pool overhead).
* Cancellation propagates through the pool.
* Read errors fail fast.
* Worker count cap is respected.
* Empty / all-cached inputs short-circuit cleanly.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.exceptions import CancelledError
from src.core.phases.collector import FileInfo
from src.core.phases.manifest import (
    _HASH_WORKERS_MAX,
    _resolve_worker_count,
    build_integrity_manifest,
)


def _make_files(tmp_path: Path, count: int, prefix: str = "f") -> list[FileInfo]:
    """Create ``count`` small text files and return their FileInfo entries."""
    out: list[FileInfo] = []
    for i in range(count):
        p = tmp_path / f"{prefix}_{i:04d}.txt"
        p.write_text(f"content-{i}\n", encoding="utf-8")
        st = p.stat()
        out.append(
            FileInfo(
                source_path=p,
                relative_path=p.name,
                size=st.st_size,
                mtime=st.st_mtime,
                source_root=tmp_path,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Equivalence with the sequential implementation
# ---------------------------------------------------------------------------


class TestParallelResultMatchesSequential:
    """The parallel path must produce byte-identical output."""

    def test_hash_values_match_sequential_reference(self, tmp_path: Path) -> None:
        """For a deterministic input, the manifest must equal the
        manifest a hand-rolled sequential loop would produce."""
        files = _make_files(tmp_path, count=20)

        manifest = build_integrity_manifest(files)

        # Recompute hashes sequentially for the reference.
        from src.core.hashing import compute_sha256

        expected = {fi.relative_path: compute_sha256(fi.source_path) for fi in files}
        actual = {rel: entry["hash"] for rel, entry in manifest["files"].items()}
        assert actual == expected

    def test_total_checksum_is_stable(self, tmp_path: Path) -> None:
        """Checksum must be deterministic across runs (sorts internally)."""
        files = _make_files(tmp_path, count=15)
        first = build_integrity_manifest(files)["total_checksum"]
        second = build_integrity_manifest(files)["total_checksum"]
        assert first == second

    def test_all_files_present_in_output(self, tmp_path: Path) -> None:
        """No silent drop: every input file must appear in the manifest."""
        files = _make_files(tmp_path, count=50)
        manifest = build_integrity_manifest(files)
        assert set(manifest["files"].keys()) == {fi.relative_path for fi in files}


# ---------------------------------------------------------------------------
# Cache lookup remains on main thread (no pool work for hits)
# ---------------------------------------------------------------------------


class TestCacheHitsBypassPool:
    """Cache hits resolved on main thread skip ``compute_sha256`` entirely."""

    def test_all_cached_skips_compute(self, tmp_path: Path) -> None:
        """If everything is cached, ``compute_sha256`` must NOT be called."""
        files = _make_files(tmp_path, count=10)
        cache = {fi.relative_path: "deadbeef" * 8 for fi in files}  # 64 hex

        with patch("src.core.phases.manifest.compute_sha256") as mock_hash:
            manifest = build_integrity_manifest(files, cached_hashes=cache)
            mock_hash.assert_not_called()

        for fi in files:
            assert manifest["files"][fi.relative_path]["hash"] == cache[fi.relative_path]

    def test_partial_cache_only_hashes_misses(self, tmp_path: Path) -> None:
        """Half cached → ``compute_sha256`` called only for the misses."""
        files = _make_files(tmp_path, count=10)
        # Cache the first 4 files
        cache = {fi.relative_path: "f" * 64 for fi in files[:4]}

        with patch(
            "src.core.phases.manifest.compute_sha256",
            return_value="0" * 64,
        ) as mock_hash:
            build_integrity_manifest(files, cached_hashes=cache)
            assert mock_hash.call_count == 6  # 10 - 4 = 6 misses


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class TestCancellation:
    """``cancel_check`` raising must abort the build promptly."""

    def test_cancel_during_cache_pass_raises(self, tmp_path: Path) -> None:
        """A cancellation during the cache-drain pass must propagate."""
        files = _make_files(tmp_path, count=20)
        # Cancel as soon as the very first cancel_check runs.
        call_count = {"n": 0}

        def cancel():
            call_count["n"] += 1
            if call_count["n"] >= 3:
                raise CancelledError("user cancelled")

        with pytest.raises(CancelledError):
            build_integrity_manifest(files, cancel_check=cancel)

    def test_cancel_during_pool_pass_raises(self, tmp_path: Path) -> None:
        """Cancellation while the thread pool is hashing must surface."""
        files = _make_files(tmp_path, count=30)
        # Allow the cache pass (one call per file) to finish, then
        # raise on the pool pass (one call per completed future).
        cache_pass_calls = len(files)
        call_count = {"n": 0}

        def cancel():
            call_count["n"] += 1
            if call_count["n"] > cache_pass_calls:
                raise CancelledError("user cancelled mid-pool")

        with pytest.raises(CancelledError):
            build_integrity_manifest(files, cancel_check=cancel)


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    """Worker errors must surface to the caller (fail-fast contract)."""

    def test_oserror_in_worker_propagates(self, tmp_path: Path) -> None:
        """A file disappearing mid-build must raise, not silently skip."""
        files = _make_files(tmp_path, count=5)
        # Delete one file so the worker hits FileNotFoundError.
        files[2].source_path.unlink()

        with pytest.raises((FileNotFoundError, OSError)):
            build_integrity_manifest(files)


# ---------------------------------------------------------------------------
# Speed proof: parallel really does run concurrently
# ---------------------------------------------------------------------------


class TestParallelism:
    """Demonstrate the workers run concurrently, not interleaved."""

    def test_concurrent_workers_observed(self, tmp_path: Path) -> None:
        """Patch ``compute_sha256`` to record overlapping execution.

        Without parallelism, max concurrency is 1.  With the
        ThreadPoolExecutor we expect concurrency ≥ 2 (and up to
        ``_resolve_worker_count()``).
        """
        files = _make_files(tmp_path, count=12)

        active = {"now": 0, "peak": 0}
        lock = threading.Lock()

        def slow_hash(_path):
            with lock:
                active["now"] += 1
                if active["now"] > active["peak"]:
                    active["peak"] = active["now"]
            time.sleep(0.05)  # simulate AV-stalled read
            with lock:
                active["now"] -= 1
            return "a" * 64

        with patch("src.core.phases.manifest.compute_sha256", side_effect=slow_hash):
            build_integrity_manifest(files)

        # On a single-CPU container _resolve_worker_count() may be 1
        # (no parallelism possible).  Skip the assertion in that
        # degenerate case rather than failing the test.
        if _resolve_worker_count() > 1:
            assert active["peak"] >= 2, (
                f"Expected concurrent execution, got peak={active['peak']}"
            )


# ---------------------------------------------------------------------------
# Worker-count helper
# ---------------------------------------------------------------------------


class TestWorkerCount:
    """Bounded by the ``_HASH_WORKERS_MAX`` cap and ``os.cpu_count``."""

    def test_workers_capped_at_max(self) -> None:
        """Even on a 64-core box we never spawn more than the cap."""
        with patch("src.core.phases.manifest.os.cpu_count", return_value=64):
            assert _resolve_worker_count() == _HASH_WORKERS_MAX

    def test_workers_respect_cpu_count_below_cap(self) -> None:
        """Don't oversubscribe a small box."""
        with patch("src.core.phases.manifest.os.cpu_count", return_value=2):
            assert _resolve_worker_count() == 2

    def test_workers_floor_when_cpu_count_unknown(self) -> None:
        """``os.cpu_count`` may return None on exotic platforms."""
        with patch("src.core.phases.manifest.os.cpu_count", return_value=None):
            assert _resolve_worker_count() == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Degenerate inputs must not crash."""

    def test_empty_file_list_returns_empty_manifest(self) -> None:
        manifest = build_integrity_manifest([])
        assert manifest["files"] == {}
        assert "total_checksum" in manifest

    def test_single_file_no_pool_overhead(self, tmp_path: Path) -> None:
        """One file in, one entry out — pool spins up briefly but works."""
        files = _make_files(tmp_path, count=1)
        manifest = build_integrity_manifest(files)
        assert len(manifest["files"]) == 1
