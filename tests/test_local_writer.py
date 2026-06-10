"""Tests for src.core.phases.local_writer — flat copy and encrypted tar."""

import json
import os
import tarfile
from pathlib import Path

import pytest

from src.core.events import EventBus
from src.core.exceptions import CancelledError, WriteError
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import (
    WRITE_FLAT_WORKERS,
    generate_backup_name,
    write_encrypted_tar,
    write_flat,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_file(path: Path, content: str = "data") -> None:
    """Create a file with content, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_file_info(source_path: Path, relative_path: str) -> FileInfo:
    """Create a FileInfo from an existing file on disk."""
    return FileInfo(
        source_path=source_path,
        relative_path=relative_path,
        size=source_path.stat().st_size,
        mtime=source_path.stat().st_mtime,
        source_root=str(source_path.parent),
    )


# ---------------------------------------------------------------------------
# write_flat
# ---------------------------------------------------------------------------


class TestWriteFlat:
    """Tests for plain (unencrypted) flat directory copy."""

    def test_copies_files_to_backup_dir(self, tmp_path: Path) -> None:
        """Files are copied with correct relative paths."""
        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        _make_file(src / "sub" / "b.txt", "beta")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "sub" / "b.txt", "sub/b.txt"),
        ]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "TestBackup_FULL_2026-04-02_120000")

        assert result.is_dir()
        assert (result / "a.txt").read_text(encoding="utf-8") == "alpha"
        assert (result / "sub" / "b.txt").read_text(encoding="utf-8") == "beta"

    def test_returns_correct_backup_path(self, tmp_path: Path) -> None:
        """Returned path matches destination/backup_name."""
        src = tmp_path / "source"
        _make_file(src / "f.txt", "x")

        files = [_make_file_info(src / "f.txt", "f.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "MyProfile_FULL_2026-04-02_100000")
        assert result == dest / "MyProfile_FULL_2026-04-02_100000"

    def test_empty_file_list(self, tmp_path: Path) -> None:
        """Empty file list creates an empty backup directory."""
        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat([], dest, "Empty_FULL_2026-04-02_100000")
        assert result.is_dir()
        assert list(result.iterdir()) == []

    def test_emits_progress_events(self, tmp_path: Path) -> None:
        """Progress events are emitted for each file."""
        src = tmp_path / "source"
        _make_file(src / "a.txt", "a")
        _make_file(src / "b.txt", "b")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "b.txt", "b.txt"),
        ]

        events = EventBus()
        progress_data: list[dict] = []
        events.subscribe("progress", lambda **kw: progress_data.append(kw))

        dest = tmp_path / "dest"
        dest.mkdir()
        write_flat(files, dest, "Backup", events=events)

        assert len(progress_data) == 2
        assert progress_data[0]["current"] == 1
        assert progress_data[1]["current"] == 2

    def test_source_file_missing_is_skipped_not_fatal(self, tmp_path: Path) -> None:
        """A vanished source file is SKIPPED (recorded), not fatal.

        Changed in the #9 fix: a single file deleted between collection and
        copy must not abort the whole run (the 18/05/2026 WinError 2
        incident). Destination errors and unreadable sources stay fatal —
        see test_source_permission_error_raises_write_error below."""
        missing = tmp_path / "source" / "gone.txt"
        fi = FileInfo(
            source_path=missing,
            relative_path="gone.txt",
            size=0,
            mtime=0.0,
            source_root=str(tmp_path / "source"),
        )

        dest = tmp_path / "dest"
        dest.mkdir()
        skipped: set[str] = set()
        backup_dir = write_flat([fi], dest, "Backup", skipped_out=skipped)  # must not raise
        assert skipped == {"gone.txt"}
        assert backup_dir.exists()

    def test_source_permission_error_raises_write_error(self, tmp_path: Path) -> None:
        """WriteError when source file cannot be read.

        Since v3.3.19 ``write_flat`` uses ``shutil.copy2`` directly
        (the integrity manifest is built upstream by parallel hashing
        in ``_phase_integrity``). The mock therefore targets the
        kernel-copy primitive itself.
        """
        from unittest.mock import patch

        src = tmp_path / "source"
        _make_file(src / "a.txt", "data")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        with (
            patch(
                "src.core.phases.local_writer.shutil.copy2",
                side_effect=PermissionError("Access denied"),
            ),
            pytest.raises(WriteError),
        ):
            write_flat(files, dest, "Backup")

    def test_preserves_file_content_large_file(self, tmp_path: Path) -> None:
        """Binary content is preserved exactly (1 MB file)."""
        src = tmp_path / "source"
        src.mkdir()
        data = os.urandom(1024 * 1024)
        (src / "big.bin").write_bytes(data)

        files = [_make_file_info(src / "big.bin", "big.bin")]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "Backup")
        assert (result / "big.bin").read_bytes() == data


# ---------------------------------------------------------------------------
# write_flat — parallel pool behaviour (v3.7.1)
# ---------------------------------------------------------------------------


class TestWriteFlatParallel:
    """Contracts pinned for the v3.7.1 ThreadPoolExecutor refactor.

    The point of these tests is *not* to measure throughput (pytest
    runs on NVMe / tmp_path where pool4 and single-thread are within
    rounding error — see ``feedback_no_silent_perf_regressions.md``).
    They pin the structural properties that a green local run cannot
    catch on its own:

    - The pool is sized at ``WRITE_FLAT_WORKERS`` (4).
    - All files land in the destination regardless of completion order.
    - ``cancel_check`` raised by any worker propagates.
    - The first ``WriteError`` surfaces; pending work is cancelled.
    - PROGRESS events still emit once per file (with throttle disabled).
    """

    def test_workers_constant_is_4(self) -> None:
        """``WRITE_FLAT_WORKERS`` is the sweet spot from the 2026-05-17
        bench. Changing it requires a re-bench on a real HDD/USB
        target — see scripts/bench_copy_strategies.py."""
        assert WRITE_FLAT_WORKERS == 4

    def test_pool_constructed_with_4_workers(self, tmp_path: Path) -> None:
        """The executor is built with ``max_workers=WRITE_FLAT_WORKERS``.

        Pins the wiring between the constant and the executor so a
        refactor that drops the kwarg (Python's default scales with
        CPU count, which is wildly wrong for an HDD target) cannot
        silently land.
        """
        from unittest.mock import patch

        src = tmp_path / "source"
        _make_file(src / "a.txt", "a")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()

        with patch(
            "src.core.phases.local_writer.ThreadPoolExecutor",
            wraps=__import__(
                "concurrent.futures", fromlist=["ThreadPoolExecutor"]
            ).ThreadPoolExecutor,
        ) as mock_pool:
            write_flat(files, dest, "Backup")

        # Inspect the call kwargs to find max_workers
        assert mock_pool.call_count == 1
        kwargs = mock_pool.call_args.kwargs
        assert kwargs.get("max_workers") == 4

    def test_all_files_copied_with_many(self, tmp_path: Path) -> None:
        """50 files all land in the destination regardless of which
        worker copies which one. Catches a counter / loop bug that
        could lose a file silently when the pool reorders work.
        """
        src = tmp_path / "source"
        files = []
        for i in range(50):
            f = src / f"f_{i:02d}.txt"
            _make_file(f, f"content-{i}")
            files.append(_make_file_info(f, f"f_{i:02d}.txt"))

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "Backup")

        produced = sorted(p.name for p in result.iterdir())
        assert produced == sorted(f"f_{i:02d}.txt" for i in range(50))
        # And each file has its expected content (no cross-pollination
        # via a shared buffer or filename swap).
        for i in range(50):
            assert (result / f"f_{i:02d}.txt").read_text(
                encoding="utf-8"
            ) == f"content-{i}"

    def test_cancellation_propagates_from_worker(self, tmp_path: Path) -> None:
        """``cancel_check`` raised by any worker surfaces as
        ``CancelledError``. The pool is drained promptly and at least
        one file may have been copied before the cancel was observed
        — that is acceptable.
        """
        src = tmp_path / "source"
        files = []
        for i in range(20):
            f = src / f"f_{i:02d}.txt"
            _make_file(f, "data")
            files.append(_make_file_info(f, f"f_{i:02d}.txt"))

        # ``cancel_check`` is called from multiple worker threads. The
        # call counter must be guarded by a lock so the "raise after N"
        # rule is deterministic — without the lock the threshold can
        # be crossed by several workers concurrently, but the contract
        # (a CancelledError surfaces) still holds.
        import threading

        call_lock = threading.Lock()
        call_count = [0]

        def cancel_after_three() -> None:
            with call_lock:
                call_count[0] += 1
                n = call_count[0]
            if n > 3:
                raise CancelledError("user cancelled")

        dest = tmp_path / "dest"
        dest.mkdir()

        with pytest.raises(CancelledError):
            write_flat(files, dest, "Backup", cancel_check=cancel_after_three)

    def test_first_write_error_propagates(self, tmp_path: Path) -> None:
        """If multiple files would fail, only the first observed
        ``WriteError`` surfaces; pending files are cancelled. The test
        is order-agnostic on which file's path appears in the
        exception (the pool may reorder).
        """
        from unittest.mock import patch

        src = tmp_path / "source"
        files = []
        for i in range(20):
            f = src / f"f_{i:02d}.txt"
            _make_file(f, "data")
            files.append(_make_file_info(f, f"f_{i:02d}.txt"))

        dest = tmp_path / "dest"
        dest.mkdir()

        with patch(
            "src.core.phases.local_writer.shutil.copy2",
            side_effect=PermissionError("Access denied"),
        ), pytest.raises(WriteError) as exc_info:
            write_flat(files, dest, "Backup")

        # The relative path on the exception comes from the
        # file_info passed to whichever worker failed first — must
        # match one of the inputs.
        msg = str(exc_info.value)
        assert any(f"f_{i:02d}.txt" in msg for i in range(20))

    def test_progress_emits_one_per_file_without_throttle(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """With the throttle disabled, every file produces a PROGRESS
        event regardless of which worker copies it. Catches a counter
        bug that would lose events when workers race on the lock.
        """
        monkeypatch.setattr("src.core.phase_logger._PROGRESS_THROTTLE_MS", 0)

        src = tmp_path / "source"
        files = []
        for i in range(20):
            f = src / f"f_{i:02d}.txt"
            _make_file(f, "data")
            files.append(_make_file_info(f, f"f_{i:02d}.txt"))

        events = EventBus()
        progress_data: list[dict] = []
        events.subscribe("progress", lambda **kw: progress_data.append(kw))

        dest = tmp_path / "dest"
        dest.mkdir()
        write_flat(files, dest, "Backup", events=events)

        # Exactly one PROGRESS per file
        assert len(progress_data) == 20
        # ``current`` values are 1..20 in some order (pool may reorder)
        current_values = sorted(p["current"] for p in progress_data)
        assert current_values == list(range(1, 21))

    def test_progress_current_max_equals_total(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """At least one PROGRESS event has ``current == total`` so the
        UI bar reaches 100%, even when the throttle would otherwise
        squelch the last event.
        """
        monkeypatch.setattr("src.core.phase_logger._PROGRESS_THROTTLE_MS", 0)

        src = tmp_path / "source"
        files = []
        for i in range(8):
            f = src / f"f_{i}.txt"
            _make_file(f, "data")
            files.append(_make_file_info(f, f"f_{i}.txt"))

        events = EventBus()
        progress_data: list[dict] = []
        events.subscribe("progress", lambda **kw: progress_data.append(kw))

        dest = tmp_path / "dest"
        dest.mkdir()
        write_flat(files, dest, "Backup", events=events)

        max_current = max(p["current"] for p in progress_data)
        assert max_current == 8

    def test_long_path_destination_directories_created(
        self, tmp_path: Path
    ) -> None:
        """Files in nested subdirectories all land properly even when
        multiple workers create parent directories concurrently — the
        ``long_path_mkdir`` call inside ``_copy_one`` is the race
        target. ``mkdir(parents=True, exist_ok=True)`` is the safety
        net but the test pins the property explicitly.
        """
        src = tmp_path / "source"
        files = []
        for i in range(20):
            # Same parent for many files → highest concurrency on
            # the mkdir of that one directory.
            rel = f"deep/nested/path/f_{i:02d}.txt"
            f = src / rel
            _make_file(f, "data")
            files.append(_make_file_info(f, rel))

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "Backup")

        for i in range(20):
            assert (result / "deep" / "nested" / "path" / f"f_{i:02d}.txt").exists()


# ---------------------------------------------------------------------------
# write_encrypted_tar
# ---------------------------------------------------------------------------


class TestWriteEncryptedTar:
    """Tests for encrypted .tar.wbenc archive creation."""

    def test_creates_tar_wbenc_file(self, tmp_path: Path) -> None:
        """A .tar.wbenc file is created at the expected path."""
        src = tmp_path / "source"
        _make_file(src / "hello.txt", "world")
        files = [_make_file_info(src / "hello.txt", "hello.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_encrypted_tar(files, dest, "Backup_FULL_2026", "secret123")

        assert result.exists()
        assert result.suffix == ".wbenc"
        assert result.name == "Backup_FULL_2026.tar.wbenc"

    def test_encrypted_archive_is_not_plain_tar(self, tmp_path: Path) -> None:
        """Encrypted output should not be a valid plain tar file."""
        src = tmp_path / "source"
        _make_file(src / "hello.txt", "world")
        files = [_make_file_info(src / "hello.txt", "hello.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_encrypted_tar(files, dest, "Backup", "password")

        assert not tarfile.is_tarfile(str(result))

    def test_round_trip_decrypt(self, tmp_path: Path) -> None:
        """Encrypted archive can be decrypted and files extracted."""
        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        _make_file(src / "sub" / "b.txt", "beta")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "sub" / "b.txt", "sub/b.txt"),
        ]

        dest = tmp_path / "dest"
        dest.mkdir()
        password = "test-password-2026"
        archive = write_encrypted_tar(files, dest, "Backup", password)

        # Decrypt and extract
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, password)
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                tar.extractall(path=extract_dir)

        assert (extract_dir / "a.txt").read_text(encoding="utf-8") == "alpha"
        assert (extract_dir / "sub" / "b.txt").read_text(encoding="utf-8") == "beta"

    def test_wrong_password_fails(self, tmp_path: Path) -> None:
        """Decrypting with wrong password raises an error."""
        from cryptography.exceptions import InvalidTag

        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "data")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        archive = write_encrypted_tar(files, dest, "Backup", "correct")

        with pytest.raises((InvalidTag, Exception)), open(archive, "rb") as f:
            reader = DecryptingReader(f, "wrong-password")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    tar.extract(member, path=tmp_path / "fail")

    def test_embeds_integrity_manifest(self, tmp_path: Path) -> None:
        """Integrity manifest is embedded as ``.wbverify`` inside the archive.

        The single-pass writer builds the manifest itself from the
        hashes computed during tar streaming, so the embedded
        ``.wbverify`` accurately describes what was written. The legacy
        ``integrity_manifest`` parameter is ignored.
        """
        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        password = "manifest-test"
        archive = write_encrypted_tar(files, dest, "Backup", password)

        # Extract and check for .wbverify with the writer-computed hash.
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, password)
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                tar.extractall(path=extract_dir)

        wbverify = extract_dir / ".wbverify"
        assert wbverify.exists()
        loaded = json.loads(wbverify.read_text(encoding="utf-8"))
        assert loaded["algorithm"] == "sha256"
        assert "a.txt" in loaded["files"]
        # Hash matches the actual content (single-pass writer
        # computed it from the bytes written).
        import hashlib

        expected = hashlib.sha256(b"alpha").hexdigest()
        assert loaded["files"]["a.txt"]["hash"] == expected
        assert loaded["files"]["a.txt"]["size"] == 5

    def test_manifest_is_always_embedded(self, tmp_path: Path) -> None:
        """The writer always embeds an integrity manifest now.

        Previously, omitting ``integrity_manifest`` produced an archive
        with no ``.wbverify`` entry. With hash-during-write the manifest
        is built from streaming hashes and always embedded — the
        archive is self-describing on every path.
        """
        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "data")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        password = "test"
        archive = write_encrypted_tar(files, dest, "Backup", password)

        names: list[str] = []
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, password)
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    names.append(member.name)
                    tar.extract(member, path=extract_dir)

        assert ".wbverify" in names

    def test_cancellation_leaves_no_partial_or_final(self, tmp_path: Path) -> None:
        """Cancel mid-write removes the .partial file and leaves no archive."""
        from src.core.exceptions import CancelledError

        src = tmp_path / "source"
        _make_file(src / "a.txt", "a")
        _make_file(src / "b.txt", "b")
        _make_file(src / "c.txt", "c")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "b.txt", "b.txt"),
            _make_file_info(src / "c.txt", "c.txt"),
        ]

        call_count = {"n": 0}

        def cancel_after_two() -> None:
            call_count["n"] += 1
            if call_count["n"] > 2:
                raise CancelledError("user cancelled")

        dest = tmp_path / "dest"
        dest.mkdir()

        with pytest.raises(CancelledError):
            write_encrypted_tar(
                files, dest, "Backup_FULL_2026", "pw", cancel_check=cancel_after_two
            )

        # No final archive and no .partial residue in destination.
        assert not (dest / "Backup_FULL_2026.tar.wbenc").exists()
        assert not (dest / "Backup_FULL_2026.tar.wbenc.partial").exists()
        assert list(dest.iterdir()) == []

    def test_success_leaves_no_partial_residue(self, tmp_path: Path) -> None:
        """A completed encrypted write only produces the final archive."""
        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        archive = write_encrypted_tar(files, dest, "Backup_FULL_2026", "pw")

        assert archive.exists()
        assert not archive.with_name(archive.name + ".partial").exists()
        # Only the final archive sits in the destination.
        assert {p.name for p in dest.iterdir()} == {archive.name}

    def test_emits_progress_events(self, tmp_path: Path, monkeypatch) -> None:
        """Progress events are emitted for each file in the archive.

        ``PhaseLogger.progress`` throttles intermediate events to 10 Hz
        in production (see ``_PROGRESS_THROTTLE_MS``); on a 3-file
        fixture the whole loop fits inside a single 100 ms window, so
        only the first and the terminal events would survive. Disable
        the throttle here to keep the per-file contract observable.
        """
        monkeypatch.setattr("src.core.phase_logger._PROGRESS_THROTTLE_MS", 0)

        src = tmp_path / "source"
        _make_file(src / "a.txt", "a")
        _make_file(src / "b.txt", "b")
        _make_file(src / "c.txt", "c")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "b.txt", "b.txt"),
            _make_file_info(src / "c.txt", "c.txt"),
        ]

        events = EventBus()
        progress_data: list[dict] = []
        events.subscribe("progress", lambda **kw: progress_data.append(kw))

        dest = tmp_path / "dest"
        dest.mkdir()
        write_encrypted_tar(files, dest, "Backup", "pw", events=events)

        assert len(progress_data) == 3
        assert progress_data[-1]["current"] == 3

    def test_empty_file_list(self, tmp_path: Path) -> None:
        """Empty file list creates a valid encrypted archive containing
        only an empty integrity manifest.

        With the single-pass writer the ``.wbverify`` entry is always
        present (even when no files were archived), so a restore tool
        can still authenticate the archive's intent rather than facing
        an unsigned, ambiguous artefact.
        """
        from src.security.encryption import DecryptingReader

        dest = tmp_path / "dest"
        dest.mkdir()
        archive = write_encrypted_tar([], dest, "Empty", "pw")

        assert archive.exists()
        assert archive.stat().st_size > 0

        # Should be decryptable; only the (empty) manifest is inside.
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, "pw")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                members = list(tar)
                assert [m.name for m in members] == [".wbverify"]


# ---------------------------------------------------------------------------
# Embedded manifest consistency on vanishing files
# ---------------------------------------------------------------------------


class TestVanishingFileManifestSync:
    """A file that vanishes between collection and write must NOT
    appear in the embedded ``.wbverify``.

    Under the legacy two-pass writer this required explicit pruning
    of a pre-built manifest. Under the single-pass writer the property
    is automatic: a file we cannot ``open`` is skipped, no hash is
    computed, no manifest entry is created. The test verifies the
    invariant from the outside (only the produced artefact matters).
    """

    def test_vanished_file_not_in_embedded_manifest(self, tmp_path: Path) -> None:
        from src.security.encryption import DecryptingReader

        src_dir = tmp_path / "src"
        src_dir.mkdir()
        f1 = src_dir / "a.txt"
        f2 = src_dir / "b.txt"
        f1.write_text("aaa", encoding="utf-8")
        f2.write_text("bbb", encoding="utf-8")

        files = [_make_file_info(f1, "a.txt"), _make_file_info(f2, "b.txt")]

        # Remove b.txt BEFORE the write phase runs, so the writer
        # encounters OSError on os.path.getsize and skips it.
        f2.unlink()

        dest = tmp_path / "dest"
        dest.mkdir()
        archive = write_encrypted_tar(
            files=files,
            destination=dest,
            backup_name="Prof_FULL_2026-04-17_000000",
            password="pw",
        )

        # Read the embedded manifest back and verify it only lists a.txt.
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, "pw")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    if member.name == ".wbverify":
                        body = tar.extractfile(member).read()
                        embedded = json.loads(body)
                        break
                else:
                    pytest.fail("No .wbverify entry in archive")

        assert "a.txt" in embedded["files"]
        assert (
            "b.txt" not in embedded["files"]
        ), "Vanished file must not appear in the embedded manifest"
        # Hash matches the actual content (writer computed it during stream).
        import hashlib

        assert embedded["files"]["a.txt"]["hash"] == hashlib.sha256(b"aaa").hexdigest()


# ---------------------------------------------------------------------------
# generate_backup_name
# ---------------------------------------------------------------------------


class TestGenerateBackupName:
    def test_full_backup_name(self) -> None:
        name = generate_backup_name("My Profile", "FULL")
        assert name.startswith("My_Profile_FULL_")

    def test_diff_backup_name(self) -> None:
        name = generate_backup_name("Test", "DIFF")
        assert "_DIFF_" in name

    def test_default_is_full(self) -> None:
        name = generate_backup_name("Test")
        assert "_FULL_" in name

    def test_special_characters_sanitized(self) -> None:
        name = generate_backup_name("Pro/file<>:test")
        assert "/" not in name
        assert "<" not in name
        assert ">" not in name
        assert ":" not in name

    def test_timestamp_format(self) -> None:
        """Name contains a valid date/time pattern."""
        import re

        name = generate_backup_name("X", "FULL")
        # Pattern: _FULL_YYYY-MM-DD_HHMMSS
        assert re.search(r"_FULL_\d{4}-\d{2}-\d{2}_\d{6}$", name)


class TestWriteFlatVanishedSource:
    """A source file that disappears between collection and copy is
    skipped (recorded in skipped_out), not fatal — the 18/05/2026
    WinError 2 incident. Destination errors stay fatal."""

    def test_vanished_source_skipped_and_recorded(self, tmp_path: Path) -> None:
        src = tmp_path / "source"
        _make_file(src / "present.txt", "here")
        present = _make_file_info(src / "present.txt", "present.txt")
        # FileInfo for a file that never existed → copy raises FNF on source.
        gone = FileInfo(
            source_path=src / "gone.txt",
            relative_path="gone.txt",
            size=4,
            mtime=0.0,
            source_root=str(src),
        )
        skipped: set[str] = set()
        backup_dir = write_flat(
            [present, gone],
            tmp_path / "dest",
            "Bk_FULL_x",
            skipped_out=skipped,
        )
        # The present file landed; the vanished one was skipped, not fatal.
        assert (backup_dir / "present.txt").read_text(encoding="utf-8") == "here"
        assert not (backup_dir / "gone.txt").exists()
        assert skipped == {"gone.txt"}

    def test_vanished_source_without_skipped_out_still_does_not_raise(
        self, tmp_path: Path
    ) -> None:
        src = tmp_path / "source"
        _make_file(src / "a.txt", "a")
        present = _make_file_info(src / "a.txt", "a.txt")
        gone = FileInfo(
            source_path=src / "missing.txt",
            relative_path="missing.txt",
            size=1,
            mtime=0.0,
            source_root=str(src),
        )
        # No skipped_out passed — must still tolerate the vanished source.
        backup_dir = write_flat([present, gone], tmp_path / "dest2", "Bk_FULL_y")
        assert (backup_dir / "a.txt").exists()
