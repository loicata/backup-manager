"""Tests for the hash-phase timeout in manifest + verifier.

Without a deadline on ``ThreadPoolExecutor``'s ``as_completed`` the
manifest and verify phases hang indefinitely when a worker is stuck
inside ``compute_sha256`` — typically because the file is locked by an
antivirus mid-scan, the OS is rehydrating a OneDrive placeholder, or a
NAS share dropped mid-read. Windows file I/O has no kernel-level
deadline, so the only way out is a userspace timeout.

These tests pin three things:

1. ``build_integrity_manifest`` raises a ``RuntimeError`` with a
   user-actionable message when the budget expires (lists the pending
   files, names the likely culprit, mentions the budget). The message
   shape is part of the contract the run-tab log surfaces verbatim.
2. ``verify_backup`` returns ``(False, "...timed out...")`` rather than
   raising — the destination bytes are on disk, we just couldn't
   re-verify them in time, so the failure is graceful.
3. The pool shuts down without ``wait=True`` so a stuck worker cannot
   block the pipeline from raising — the test would hang otherwise.

To avoid leaking threads past the test session, every "stuck" worker
waits on a ``threading.Event`` that is ``set()`` in the test's
``finally`` block. The pool itself is already shut down with
``cancel_futures=True``; releasing the event just lets the workers
return naturally instead of leaving zombie threads behind.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from src.core.phases import manifest, verifier
from src.core.phases.collector import FileInfo
from src.core.phases.manifest import build_integrity_manifest
from src.core.phases.verifier import verify_backup


def _patch_short_timeout(target_module, monkeypatch, seconds: float = 0.5) -> None:
    """Force a tiny timeout on the target phase module for the test.

    ``manifest`` and ``verifier`` use different constant names but the
    same semantics (per-file budget + minimum). Both are reduced to
    ``seconds`` so the test waits at most ~``seconds`` for the timeout
    to fire instead of the production minute.
    """
    if target_module is manifest:
        monkeypatch.setattr(manifest, "_HASH_TIMEOUT_MIN_SECONDS", seconds)
        monkeypatch.setattr(manifest, "_HASH_TIMEOUT_PER_FILE", seconds)
    elif target_module is verifier:
        monkeypatch.setattr(verifier, "_VERIFY_TIMEOUT_MIN_SECONDS", seconds)
        monkeypatch.setattr(verifier, "_VERIFY_TIMEOUT_PER_FILE", seconds)
    else:
        raise ValueError(f"Unsupported target module: {target_module!r}")


def _make_files(tmp_path: Path, count: int = 1) -> list[FileInfo]:
    """Create ``count`` real files under ``tmp_path`` and return FileInfos."""
    src = tmp_path / "src"
    src.mkdir()
    out: list[FileInfo] = []
    for i in range(count):
        p = src / f"f_{i}.txt"
        p.write_text(f"payload {i}", encoding="utf-8")
        st = p.stat()
        out.append(
            FileInfo(
                source_path=p,
                relative_path=p.name,
                size=st.st_size,
                mtime=st.st_mtime,
                source_root=str(src),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Manifest phase: timeout raises a clear RuntimeError
# ---------------------------------------------------------------------------


class TestManifestHashTimeout:
    """A stuck worker must trigger a clean RuntimeError, not a hang."""

    def test_blocked_hash_raises_runtime_error_with_pending_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        files = _make_files(tmp_path, count=1)
        _patch_short_timeout(manifest, monkeypatch, seconds=0.3)

        release = threading.Event()

        def stuck_hash(_path):
            release.wait(timeout=10.0)
            return "0" * 64

        monkeypatch.setattr("src.core.phases.manifest.compute_sha256", stuck_hash)

        try:
            with pytest.raises(RuntimeError, match=r"Hash phase timed out"):
                build_integrity_manifest(files)
        finally:
            # Let the worker thread exit instead of leaking past the test.
            release.set()

    def test_timeout_message_lists_pending_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """The error must surface at least one pending file by name —
        the run-tab log shows this verbatim and the user uses it to
        identify the culprit (which folder to remove from sources, or
        which AV exclusion to add)."""
        files = _make_files(tmp_path, count=3)
        _patch_short_timeout(manifest, monkeypatch, seconds=0.3)

        release = threading.Event()

        def stuck_hash(_path):
            release.wait(timeout=10.0)
            return "0" * 64

        monkeypatch.setattr("src.core.phases.manifest.compute_sha256", stuck_hash)

        try:
            with pytest.raises(RuntimeError) as exc_info:
                build_integrity_manifest(files)
        finally:
            release.set()

        msg = str(exc_info.value)
        # Mentions the budget and at least one of the file names.
        assert "timed out" in msg
        assert "Pending:" in msg
        assert any(f.relative_path in msg for f in files)

    def test_happy_path_still_completes_without_timeout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Sanity guard: a tiny timeout must not break the fast path.

        A real hash that returns in microseconds must complete well
        before the (already small) timeout fires, otherwise the
        production path with a 60 s minimum would be similarly fragile.
        """
        files = _make_files(tmp_path, count=3)
        _patch_short_timeout(manifest, monkeypatch, seconds=2.0)

        result = build_integrity_manifest(files)

        assert len(result["files"]) == 3
        assert result["total_checksum"]


# ---------------------------------------------------------------------------
# Verify phase: timeout becomes a verification failure (graceful)
# ---------------------------------------------------------------------------


class TestVerifyHashTimeout:
    """Verify must surface the timeout as ``(False, msg)``, not raise.

    Rationale: at the verify phase the destination bytes have already
    been written. The backup itself is intact. A timeout means the
    pipeline could not RE-CHECK all files within the budget — that is
    a verification failure (graceful), not a corruption (which would
    abort with an exception).
    """

    def test_blocked_verify_returns_failure_with_timed_out_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        backup = tmp_path / "backup"
        backup.mkdir()
        (backup / "x.txt").write_text("data", encoding="utf-8")

        # A minimal manifest that references the file we just wrote.
        # ``total_checksum`` omitted so verify_backup skips the global
        # recomputation (this test focuses on the timeout path).
        manifest_doc = {
            "version": 1,
            "algorithm": "sha256",
            "files": {"x.txt": {"hash": "0" * 64, "size": 4}},
        }
        manifest_path = tmp_path / "backup.wbverify"
        manifest_path.write_text(json.dumps(manifest_doc), encoding="utf-8")

        _patch_short_timeout(verifier, monkeypatch, seconds=0.3)

        release = threading.Event()

        def stuck_hash(_path):
            release.wait(timeout=10.0)
            return "0" * 64

        monkeypatch.setattr("src.core.phases.verifier.compute_sha256", stuck_hash)

        try:
            ok, msg = verify_backup(backup, manifest_path)
        finally:
            release.set()

        assert ok is False
        assert "timed out" in msg
        assert "x.txt" in msg

    def test_happy_path_returns_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """A fast-completing verify must return ``(True, ...)`` even
        with a tight timeout — confirms the budget logic is not eating
        legitimate runs."""
        from src.core.hashing import compute_sha256

        backup = tmp_path / "backup"
        backup.mkdir()
        f = backup / "x.txt"
        f.write_text("data", encoding="utf-8")
        real_hash = compute_sha256(f)

        # ``total_checksum`` omitted: this test exercises the happy-path
        # timeout logic, not the manifest-tamper detection added later.
        manifest_doc = {
            "version": 1,
            "algorithm": "sha256",
            "files": {"x.txt": {"hash": real_hash, "size": 4}},
        }
        manifest_path = tmp_path / "backup.wbverify"
        manifest_path.write_text(json.dumps(manifest_doc), encoding="utf-8")

        _patch_short_timeout(verifier, monkeypatch, seconds=2.0)

        ok, msg = verify_backup(backup, manifest_path)

        assert ok is True
        assert "1/1" in msg


# ---------------------------------------------------------------------------
# Pool shutdown does not block on stuck workers
# ---------------------------------------------------------------------------


class TestTimeoutOverflowCap:
    """Regression: huge workloads (>143 k files) used to compute a
    multi-month ``total_timeout`` that Python refused to schedule —
    ``concurrent.futures.as_completed`` ultimately calls
    ``threading.Condition.wait`` which on Windows lands in
    ``WaitForMultipleObjectsEx`` whose DWORD millisecond argument
    saturates around 49.7 days. CPython surfaces this as an
    ``OverflowError("timeout value is too large")`` and the engine
    blocks every backup above that threshold.

    The fix is an absolute ceiling on ``total_timeout`` (4 h) that
    stays four orders of magnitude below the OS limit while still
    being generous enough for any realistic hash pass — see the
    constants block in ``manifest.py`` / ``verifier.py``.
    """

    def test_manifest_timeout_capped_at_max_for_262k_files(self):
        """262 k files × 30 s = 7.8 M s ≈ 91 days → must clamp to 4 h."""
        n = 262_654
        computed = min(
            manifest._HASH_TIMEOUT_MAX_SECONDS,
            max(
                manifest._HASH_TIMEOUT_MIN_SECONDS,
                n * manifest._HASH_TIMEOUT_PER_FILE,
            ),
        )
        assert computed == manifest._HASH_TIMEOUT_MAX_SECONDS
        # Sanity: the cap stays well below the Windows DWORD ms limit
        # (~49.7 days). Anything below ~30 days is safe.
        assert computed < 30 * 24 * 3600

    def test_verifier_timeout_capped_at_max_for_huge_workload(self):
        """Verify mirror of the manifest cap — keep them in sync."""
        n = 1_000_000
        computed = min(
            verifier._VERIFY_TIMEOUT_MAX_SECONDS,
            max(
                verifier._VERIFY_TIMEOUT_MIN_SECONDS,
                n * verifier._VERIFY_TIMEOUT_PER_FILE,
            ),
        )
        assert computed == verifier._VERIFY_TIMEOUT_MAX_SECONDS
        assert computed < 30 * 24 * 3600


class TestPoolShutdownNonBlocking:
    """The exception path must NOT wait for stuck workers to finish.

    Without ``shutdown(wait=False, cancel_futures=True)`` the
    ``with ThreadPoolExecutor`` block on exit calls
    ``shutdown(wait=True)`` — which would block forever on a stuck
    worker, hiding the very timeout the user is supposed to see.

    We measure that the manifest call returns within a few seconds of
    the configured timeout (not the worker's blocking duration).
    """

    def test_runtime_error_returned_promptly(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        import time

        files = _make_files(tmp_path, count=2)
        _patch_short_timeout(manifest, monkeypatch, seconds=0.3)

        release = threading.Event()

        def stuck_hash(_path):
            # Workers would naturally block ~10 s if the pool waited
            # on them, masking any premature exit.
            release.wait(timeout=10.0)
            return "0" * 64

        monkeypatch.setattr("src.core.phases.manifest.compute_sha256", stuck_hash)

        t0 = time.monotonic()
        try:
            with pytest.raises(RuntimeError):
                build_integrity_manifest(files)
        finally:
            release.set()
        elapsed = time.monotonic() - t0

        # Generous: timeout is 0.3 s, allow up to 3 s for scheduling
        # overhead. A ``shutdown(wait=True)`` regression would push
        # this to ~10 s (the worker's full block).
        assert elapsed < 3.0, f"Pool shutdown blocked on stuck workers: {elapsed:.1f}s"
