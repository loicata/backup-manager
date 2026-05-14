"""Regression tests for the verify-mirror race condition.

Before the fix, ``_verify_mirror_checksums`` and ``_verify_remote_checksums``
re-hashed ``f.source_path`` and compared the fresh digest to the remote
checksum. Any source file that mutated between the manifest phase and the
verify phase (e.g. ``.claude/settings.local.json`` while Claude Code is
running, an editor autosave) produced a guaranteed false-positive
``Hash mismatch`` and aborted the backup.

The fix is to compare the **remote checksum** to the **hash already
captured in the integrity manifest**. The manifest is the canonical
description of the backup contents; the live source is irrelevant once
collection has run.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.backup_engine import BackupEngine
from src.core.phases.collector import FileInfo


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _engine() -> BackupEngine:
    engine = BackupEngine.__new__(BackupEngine)
    engine._events = MagicMock()
    engine._cancelled = False
    return engine


def _file_on_disk(tmp_path: Path, rel: str, content: bytes) -> FileInfo:
    source = tmp_path / rel
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(content)
    return FileInfo(
        source_path=source,
        relative_path=rel,
        size=len(content),
        mtime=source.stat().st_mtime,
        source_root=str(tmp_path),
    )


def _ctx(files: list[FileInfo], manifest: dict) -> MagicMock:
    ctx = MagicMock()
    ctx.files = files
    ctx.integrity_manifest = {"files": manifest}
    return ctx


class TestMirrorVerifyIgnoresLiveSourceMutation:
    """The race the v3.5.7 incident hit on 2026-05-12."""

    def test_source_modified_after_manifest_does_not_fail_verify(self, tmp_path: Path) -> None:
        """Source rewritten between manifest and verify → still OK.

        Reproduces ``.claude/settings.local.json`` being touched by an
        editor mid-backup. The remote bytes are intact (= manifest
        hash); the live source has drifted. Pre-fix this fired a
        ``Hash mismatch on Mirror 1`` error.
        """
        original = b"committed at manifest time"
        f = _file_on_disk(tmp_path, "live.json", original)
        manifest_hash = _sha256(original)

        # Simulate live mutation AFTER the manifest captured the hash.
        f.source_path.write_bytes(b"editor wrote a new line while uploading")
        assert _sha256(f.source_path.read_bytes()) != manifest_hash

        engine = _engine()
        engine._verify_mirror_checksums(
            ctx=_ctx(
                [f],
                {"live.json": {"hash": manifest_hash, "size": len(original)}},
            ),
            remote_files=[("live.json", len(original), manifest_hash)],
            mirror_name="Mirror 1",
        )

    def test_remote_checksum_drift_from_manifest_raises(self, tmp_path: Path) -> None:
        """Remote bytes differ from manifest → real corruption, must raise."""
        content = b"good bytes"
        f = _file_on_disk(tmp_path, "real.txt", content)
        manifest_hash = _sha256(content)
        bad_remote_hash = "0" * 64

        engine = _engine()
        with pytest.raises(RuntimeError, match="Hash mismatch on Mirror 1"):
            engine._verify_mirror_checksums(
                ctx=_ctx(
                    [f],
                    {"real.txt": {"hash": manifest_hash, "size": len(content)}},
                ),
                remote_files=[("real.txt", len(content), bad_remote_hash)],
                mirror_name="Mirror 1",
            )

    def test_missing_manifest_entry_falls_back_to_size_check(self, tmp_path: Path) -> None:
        """Manifest has no entry for the file → defensive size compare.

        Should not be reachable in practice (manifest is built from
        the same FileInfo list), but the fallback prevents a silent
        accept if upstream code ever desyncs.
        """
        content = b"hello"
        f = _file_on_disk(tmp_path, "x.txt", content)
        any_hash = "a" * 64

        engine = _engine()
        # Same size → falls through as size_verified
        engine._verify_mirror_checksums(
            ctx=_ctx([f], {}),
            remote_files=[("x.txt", len(content), any_hash)],
            mirror_name="Mirror 1",
        )
        # Wrong size → raises
        with pytest.raises(RuntimeError, match="Size mismatch on Mirror 1"):
            engine._verify_mirror_checksums(
                ctx=_ctx([f], {}),
                remote_files=[("x.txt", len(content) + 1, any_hash)],
                mirror_name="Mirror 1",
            )

    def test_missing_remote_file_raises(self, tmp_path: Path) -> None:
        content = b"abc"
        f = _file_on_disk(tmp_path, "lost.txt", content)
        engine = _engine()
        with pytest.raises(RuntimeError, match="Missing on Mirror 1"):
            engine._verify_mirror_checksums(
                ctx=_ctx(
                    [f],
                    {"lost.txt": {"hash": _sha256(content), "size": len(content)}},
                ),
                remote_files=[],
                mirror_name="Mirror 1",
            )


class TestRemoteVerifyIgnoresLiveSourceMutation:
    """Same race, same fix, on the primary remote-verify path."""

    def test_source_modified_after_manifest_does_not_fail_verify(self, tmp_path: Path) -> None:
        original = b"v1"
        f = _file_on_disk(tmp_path, "a.txt", original)
        manifest_hash = _sha256(original)

        f.source_path.write_bytes(b"v2 written after the manifest closed")
        assert _sha256(f.source_path.read_bytes()) != manifest_hash

        engine = _engine()
        engine._verify_remote_checksums(
            ctx=_ctx(
                [f],
                {"a.txt": {"hash": manifest_hash, "size": len(original)}},
            ),
            remote_files=[("a.txt", len(original), manifest_hash)],
        )

    def test_remote_checksum_drift_from_manifest_raises(self, tmp_path: Path) -> None:
        content = b"good"
        f = _file_on_disk(tmp_path, "b.txt", content)
        manifest_hash = _sha256(content)

        engine = _engine()
        with pytest.raises(RuntimeError, match="Hash mismatch"):
            engine._verify_remote_checksums(
                ctx=_ctx(
                    [f],
                    {"b.txt": {"hash": manifest_hash, "size": len(content)}},
                ),
                remote_files=[("b.txt", len(content), "f" * 64)],
            )

    def test_missing_manifest_entry_falls_back_to_size(self, tmp_path: Path) -> None:
        content = b"123"
        f = _file_on_disk(tmp_path, "c.txt", content)

        engine = _engine()
        # Matching size — OK
        engine._verify_remote_checksums(
            ctx=_ctx([f], {}),
            remote_files=[("c.txt", len(content), "e" * 64)],
        )
        # Mismatched size — raises
        with pytest.raises(RuntimeError, match="Size mismatch"):
            engine._verify_remote_checksums(
                ctx=_ctx([f], {}),
                remote_files=[("c.txt", len(content) - 1, "e" * 64)],
            )


class TestVerifyRemoteThrottlesProgress:
    """Regression guard for the 2026-05-14 87-min verify bottleneck.

    Before the fix ``_verify_remote_checksums`` and ``_verify_remote_sizes``
    emitted one PROGRESS event per file directly through
    ``self._events.emit(PROGRESS, ...)``, bypassing the 10 Hz
    ``PhaseLogger.progress`` throttler that every other pipeline phase
    uses. On a 231 k-file backup that flood of Tk.after(0) callbacks
    dominated the verify wall time (~22 ms per widget update × 231908
    ≈ 87 min) for a loop that is otherwise pure dict lookups.

    The fix: route both loops through ``PhaseLogger`` so the same 10 Hz
    cap applies. The first and the last events still fire so the UI
    sees the start and the end of the phase even when it completes
    inside a single throttle window.
    """

    def _make_engine_with_real_bus(self):
        """Engine with a real EventBus so we can count PROGRESS emits."""
        from src.core.events import EventBus

        engine = BackupEngine.__new__(BackupEngine)
        engine._events = EventBus()
        engine._cancelled = False
        return engine

    def _count_progress(self, engine):
        """Subscribe a counter to PROGRESS, return the counter list."""
        from src.core.events import PROGRESS

        emitted: list[dict] = []

        def _capture(**kw):
            emitted.append(kw)

        engine._events.subscribe(PROGRESS, _capture)
        return emitted

    def test_checksum_verify_throttles_per_file_progress(self, tmp_path: Path) -> None:
        """1 000 files must NOT produce 1 000 PROGRESS events.

        The loop is pure dict lookups so it finishes in well under one
        throttle window (100 ms); we expect exactly the boundary events
        (first + last) plus at most a handful from any 100 ms slice
        that happened to fall mid-loop. 50 is a very generous ceiling
        that still rejects the un-throttled implementation by 20×.
        """
        engine = self._make_engine_with_real_bus()
        emitted = self._count_progress(engine)

        n = 1000
        files = []
        manifest = {}
        remote_files = []
        for i in range(n):
            rel = f"f{i:04d}.txt"
            content = f"data-{i}".encode()
            h = _sha256(content)
            files.append(_file_on_disk(tmp_path, rel, content))
            manifest[rel] = {"hash": h, "size": len(content)}
            remote_files.append((rel, len(content), h))

        engine._verify_remote_checksums(
            ctx=_ctx(files, manifest),
            remote_files=remote_files,
        )

        # Loose upper bound: the throttler caps emissions at ~10 Hz
        # and the loop finishes well under a second on any modern box.
        assert len(emitted) <= 50, (
            f"Expected <=50 throttled PROGRESS emits for {n} files, got {len(emitted)}. "
            "_verify_remote_checksums must use PhaseLogger.progress, not direct events.emit."
        )
        # First (current=1) and last (current==total) always fire.
        currents = [e.get("current") for e in emitted]
        assert 1 in currents, f"First PROGRESS event missing: {currents[:5]}"
        assert n in currents, f"Last PROGRESS event (current={n}) missing: {currents[-5:]}"

    def test_size_verify_throttles_per_file_progress(self, tmp_path: Path) -> None:
        """Same regression guard on the size-only fallback path."""
        engine = self._make_engine_with_real_bus()
        emitted = self._count_progress(engine)

        n = 1000
        files = []
        remote_files: list[tuple[str, int]] = []
        for i in range(n):
            rel = f"g{i:04d}.txt"
            content = f"data-{i}".encode()
            files.append(_file_on_disk(tmp_path, rel, content))
            remote_files.append((rel, len(content)))

        engine._verify_remote_sizes(
            ctx=_ctx(files, {}),
            remote_files=remote_files,
        )

        assert len(emitted) <= 50, (
            f"Expected <=50 throttled PROGRESS emits for {n} files, got {len(emitted)}. "
            "_verify_remote_sizes must use PhaseLogger.progress, not direct events.emit."
        )
        currents = [e.get("current") for e in emitted]
        assert 1 in currents
        assert n in currents
