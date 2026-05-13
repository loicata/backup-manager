"""Tests for the parallel ``verify_backup_files`` path.

The remote-side ``sha256sum`` dominates a backup's runtime on commodity
hardware: a Pi 4 hashes ~45 files/s, so 231 908 files take ~85 min when
the client sends one sequential batch at a time. The 3.5.10 refactor
fans the batches out across :data:`_VERIFY_HASH_WORKERS` SSH channels
on the shared paramiko transport so the server runs N concurrent
``sha256sum`` processes (one per channel).

These tests pin three things:

1. **Concurrency** — multiple batches are in flight at the same time
   (the workers actually run in parallel, not serialised behind a
   lock the way the old sequential loop did).

2. **Correctness** — the aggregated hash map is identical to what the
   sequential implementation would have produced, regardless of which
   worker finishes first.

3. **Failure semantics** — a single worker raising or returning a
   non-zero exit code falls the whole verify back to size-only, same
   as the sequential implementation.

No real paramiko Transport is touched. We stub ``open_session`` to
return a fake channel that records when its ``exec_command`` ran, so
we can prove batches ran concurrently by checking timestamp overlap.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.storage.sftp import (
    _VERIFY_HASH_BATCH_SIZE,
    SFTPStorage,
)


def _make_backend() -> SFTPStorage:
    """Construct an SFTPStorage without touching paramiko.

    ``__init__`` builds a config and lazily connects, so we use
    ``__new__`` to avoid any I/O.
    """
    backend = SFTPStorage.__new__(SFTPStorage)
    backend._persistent_transport = None
    backend._cancel_check = None
    backend._remote_path = "/home/u/backups"
    return backend


class _FakeChannel:
    """Stand-in for a paramiko channel.

    Records the wall-clock window during which ``exec_command`` to
    ``recv_exit_status`` was active, so concurrency tests can detect
    overlap between batches. Each instance hashes its assigned files
    by deterministic ``f"hash-of-{path}"`` so result correctness is
    easy to assert without computing real SHA-256s.
    """

    _registry: list[_FakeChannel] = []

    def __init__(self, work_seconds: float, exit_status: int = 0):
        self._work_seconds = work_seconds
        self._exit_status = exit_status
        self._paths: list[str] = []
        # The production loop in ``_sha256_batch`` calls ``recv`` until
        # it returns ``b""`` (EOF). We emit the full output on the
        # first call and EOF afterwards — without this gate, the loop
        # spins forever because there's no real socket to close.
        self._output_drained = False
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self._closed = False
        _FakeChannel._registry.append(self)

    @classmethod
    def reset(cls) -> None:
        cls._registry.clear()

    def settimeout(self, _t: float) -> None:
        pass

    def exec_command(self, cmd: str) -> None:
        # cmd format: ``sha256sum 'p1' 'p2' ...``
        self.started_at = time.monotonic()
        # Pull the paths out of the shell-escaped command. Each
        # _shell_escape wraps the value in single quotes.
        import shlex

        tokens = shlex.split(cmd)
        assert tokens[0] == "sha256sum"
        self._paths = tokens[1:]

    def recv(self, _n: int) -> bytes:
        if self._output_drained:
            return b""
        # Simulate the server taking ``work_seconds`` to compute
        # all hashes for this batch.
        time.sleep(self._work_seconds)
        self.finished_at = time.monotonic()
        self._output_drained = True
        if self._exit_status != 0:
            return b""
        # Output one line per file in sha256sum's format
        lines = [f"hash-of-{p}  {p}\n" for p in self._paths]
        return "".join(lines).encode("utf-8")

    def recv_exit_status(self) -> int:
        return self._exit_status

    def close(self) -> None:
        self._closed = True


class _FakeTransport:
    """Returns ``_FakeChannel`` instances on ``open_session``.

    Thread-safe — paramiko's real ``Transport.open_session`` is also
    thread-safe (channel creation is internally serialised) so this
    matches the production contract.
    """

    def __init__(self, work_seconds: float, exit_status: int = 0):
        self._work_seconds = work_seconds
        self._exit_status = exit_status
        self._lock = threading.Lock()
        self.closed = False

    def open_session(self) -> _FakeChannel:
        with self._lock:
            return _FakeChannel(self._work_seconds, self._exit_status)

    def close(self) -> None:
        # ``verify_backup_files`` calls this when the transport it
        # used is not the persistent one. A no-op on our fake is
        # enough; production paramiko releases the TCP socket here.
        self.closed = True


def _stub_listing(backend: SFTPStorage, files: list[tuple[str, int]]) -> None:
    """Make ``list_backup_files`` return the supplied list without I/O."""
    backend.list_backup_files = MagicMock(return_value=files)


class TestParallelVerifyConcurrency:
    """The N workers must actually run at the same time."""

    def test_multiple_batches_run_concurrently(self):
        """If two batches each take ~0.2 s and run sequentially the
        total would be ~0.4 s. With ``_VERIFY_HASH_WORKERS >= 2`` the
        total drops below ~0.3 s because the second batch starts
        while the first is still hashing.
        """
        backend = _make_backend()
        # Two batches of size 1 (we only need enough work to be measurable)
        files = [(f"f{i}", 1) for i in range(_VERIFY_HASH_BATCH_SIZE + 1)]
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.2)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            start = time.monotonic()
            result = backend.verify_backup_files("backup_dir")
            elapsed = time.monotonic() - start

        assert len(result) == len(files)
        # Sequential lower bound is 2 × 0.2 = 0.4s. Parallel ceiling
        # (with overhead) should stay well under 0.35s — anything
        # higher means we accidentally serialised the workers.
        assert elapsed < 0.35, (
            f"Verify took {elapsed:.2f}s — workers did not run in parallel "
            f"(sequential lower bound was 0.4s)"
        )

    def test_at_least_two_channels_overlap_in_time(self):
        """Direct proof of concurrency: two channels' active windows overlap."""
        backend = _make_backend()
        files = [(f"f{i}", 1) for i in range(_VERIFY_HASH_BATCH_SIZE + 1)]
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.2)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            backend.verify_backup_files("backup_dir")

        # Find any two channels whose [started_at, finished_at] windows
        # overlap. If they never overlap, the workers were serial.
        windows = [
            (c.started_at, c.finished_at)
            for c in _FakeChannel._registry
            if c.started_at is not None and c.finished_at is not None
        ]
        assert len(windows) >= 2
        overlapping = any(
            a_start < b_finish and b_start < a_finish
            for i, (a_start, a_finish) in enumerate(windows)
            for b_start, b_finish in windows[i + 1 :]
        )
        assert overlapping, "No two channel windows overlapped — workers ran serially"


class TestParallelVerifyCorrectness:
    """Aggregated result must match a sequential run exactly."""

    def test_single_batch_returns_all_hashes(self):
        backend = _make_backend()
        files = [(f"f{i}", 100 + i) for i in range(50)]  # < batch_size
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.001)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            result = backend.verify_backup_files("backup_dir")

        assert len(result) == len(files)
        for rel, size, sha in result:
            assert sha == f"hash-of-/home/u/backups/backup_dir/{rel}"
            assert (rel, size) in files

    def test_many_batches_preserves_size_and_relpath_ordering(self):
        """Result order must follow the input file list, not the
        completion order of the batches.
        """
        backend = _make_backend()
        files = [(f"file_{i:04d}", i * 100) for i in range(500)]  # >2 batches
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.005)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            result = backend.verify_backup_files("backup_dir")

        assert len(result) == len(files)
        # Same order as input.
        for (rel_in, size_in), (rel_out, size_out, _sha) in zip(files, result, strict=True):
            assert rel_in == rel_out
            assert size_in == size_out

    def test_empty_file_list_returns_empty(self):
        backend = _make_backend()
        _stub_listing(backend, [])

        result = backend.verify_backup_files("empty_backup")

        assert result == []


class TestParallelVerifyFailureSemantics:
    """A failing worker degrades to size-only — same as the old code."""

    def test_nonzero_exit_falls_back_to_size_only(self):
        backend = _make_backend()
        files = [(f"f{i}", 10 + i) for i in range(_VERIFY_HASH_BATCH_SIZE + 1)]
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.001, exit_status=1)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            result = backend.verify_backup_files("backup_dir")

        # Size-only fallback: every (rel, size, sha) tuple has sha="".
        assert len(result) == len(files)
        for rel, size, sha in result:
            assert sha == ""
            assert (rel, size) in files

    def test_worker_count_caps_at_batch_count(self):
        """When the file list yields fewer batches than workers, we
        spin up only enough workers to cover the batches (avoids the
        ``ThreadPoolExecutor(max_workers=0)`` crash and keeps the SSH
        channel count minimal on small backups).
        """
        backend = _make_backend()
        files = [(f"f{i}", 1) for i in range(3)]  # 1 batch
        _stub_listing(backend, files)

        transport = _FakeTransport(work_seconds=0.001)
        _FakeChannel.reset()

        with patch.object(backend, "_get_transport", return_value=transport):
            backend.verify_backup_files("backup_dir")

        # Only one batch's worth of channels should have been opened.
        assert len(_FakeChannel._registry) == 1


@pytest.mark.parametrize(
    "n_files, expected_batches",
    [
        (0, 0),
        (1, 1),
        (_VERIFY_HASH_BATCH_SIZE, 1),
        (_VERIFY_HASH_BATCH_SIZE + 1, 2),
        (_VERIFY_HASH_BATCH_SIZE * 4, 4),
    ],
)
def test_batch_count_matches_file_count(n_files, expected_batches):
    """Anchor the batch math so a future refactor of batch_size or
    file slicing doesn't silently double the number of SSH sessions.
    """
    backend = _make_backend()
    files = [(f"f{i}", 1) for i in range(n_files)]
    _stub_listing(backend, files)

    transport = _FakeTransport(work_seconds=0.001)
    _FakeChannel.reset()

    with patch.object(backend, "_get_transport", return_value=transport):
        backend.verify_backup_files("backup_dir")

    assert len(_FakeChannel._registry) == expected_batches
