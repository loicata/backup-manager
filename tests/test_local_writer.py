"""Tests for src.core.phases.local_writer — flat copy and encrypted tar."""

import json
import os
import tarfile
from pathlib import Path

import pytest

from src.core.events import EventBus
from src.core.exceptions import WriteError
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import (
    generate_backup_name,
    write_encrypted_tar,
    write_flat,
    write_flat_with_hashes,
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

    def test_source_file_missing_raises_write_error(self, tmp_path: Path) -> None:
        """WriteError is raised when a source file does not exist."""
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
        with pytest.raises(WriteError):
            write_flat([fi], dest, "Backup")

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
# write_flat_with_hashes — single-pass copy that defeats manifest→write TOCTOU
# ---------------------------------------------------------------------------


class TestWriteFlatWithHashes:
    """Single-pass copy + hash returns the dict that will become the
    integrity manifest. The hash is computed from the bytes written to
    the destination, not a second source read — so a source mutation
    after the write either lands inside our pass (and is reflected in
    the hash + bytes coherently) or after our pass (and never affects
    the backup).
    """

    def test_returns_path_and_hashes_dict(self, tmp_path: Path) -> None:
        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        _make_file(src / "sub" / "b.txt", "beta")

        files = [
            _make_file_info(src / "a.txt", "a.txt"),
            _make_file_info(src / "sub" / "b.txt", "sub/b.txt"),
        ]

        dest = tmp_path / "dest"
        dest.mkdir()
        backup_dir, hashes = write_flat_with_hashes(files, dest, "Backup_FULL_2026-05-08_120000")

        assert backup_dir == dest / "Backup_FULL_2026-05-08_120000"
        assert backup_dir.is_dir()
        assert set(hashes.keys()) == {"a.txt", "sub/b.txt"}
        # Each hash is a 64-char lowercase hex digest.
        for h in hashes.values():
            assert len(h) == 64
            assert all(c in "0123456789abcdef" for c in h)

    def test_hash_matches_destination_bytes(self, tmp_path: Path) -> None:
        """The returned hash equals SHA-256 of the file on disk."""
        import hashlib

        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        backup_dir, hashes = write_flat_with_hashes(files, dest, "bk")

        actual = hashlib.sha256((backup_dir / "a.txt").read_bytes()).hexdigest()
        assert hashes["a.txt"] == actual

    def test_empty_file_list_returns_empty_dict(self, tmp_path: Path) -> None:
        dest = tmp_path / "dest"
        dest.mkdir()
        backup_dir, hashes = write_flat_with_hashes([], dest, "Empty_FULL")
        assert backup_dir.is_dir()
        assert hashes == {}

    def test_hashes_are_independent_per_file(self, tmp_path: Path) -> None:
        """Different content → different hashes, same content → same hash."""
        src = tmp_path / "source"
        _make_file(src / "x.txt", "xxx")
        _make_file(src / "y.txt", "xxx")  # same content as x.txt
        _make_file(src / "z.txt", "different")

        files = [
            _make_file_info(src / "x.txt", "x.txt"),
            _make_file_info(src / "y.txt", "y.txt"),
            _make_file_info(src / "z.txt", "z.txt"),
        ]

        dest = tmp_path / "dest"
        dest.mkdir()
        _, hashes = write_flat_with_hashes(files, dest, "bk")
        assert hashes["x.txt"] == hashes["y.txt"]
        assert hashes["x.txt"] != hashes["z.txt"]

    def test_hash_bound_to_what_was_written_not_what_remains_at_source(
        self, tmp_path: Path
    ) -> None:
        """TOCTOU defence: if the source is modified AFTER the writer
        finishes, the destination still has the original bytes and the
        returned hash is for those bytes — no inconsistency."""
        import hashlib

        src = tmp_path / "source"
        _make_file(src / "moving.txt", "first version")
        files = [_make_file_info(src / "moving.txt", "moving.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        backup_dir, hashes = write_flat_with_hashes(files, dest, "bk")

        # Mutate source AFTER the write finishes.
        (src / "moving.txt").write_text("mutated content unrelated", encoding="utf-8")

        dest_bytes = (backup_dir / "moving.txt").read_bytes()
        assert dest_bytes == b"first version"
        assert hashes["moving.txt"] == hashlib.sha256(dest_bytes).hexdigest()

    def test_wrapper_write_flat_returns_path_only(self, tmp_path: Path) -> None:
        """The legacy ``write_flat`` API returns just the Path; hashes
        are silently dropped. Used by tests and any caller that does
        not need the manifest hashes."""
        src = tmp_path / "source"
        _make_file(src / "a.txt", "data")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        result = write_flat(files, dest, "bk")
        # write_flat returns Path, not tuple.
        assert isinstance(result, Path)
        assert result.is_dir()

    def test_destination_subdirs_created(self, tmp_path: Path) -> None:
        """Nested relative paths trigger parent-dir creation under dest."""
        src = tmp_path / "source"
        _make_file(src / "a" / "b" / "c" / "deep.txt", "buried")
        files = [_make_file_info(src / "a" / "b" / "c" / "deep.txt", "a/b/c/deep.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        backup_dir, hashes = write_flat_with_hashes(files, dest, "bk")
        assert (backup_dir / "a" / "b" / "c" / "deep.txt").read_text(encoding="utf-8") == "buried"
        assert "a/b/c/deep.txt" in hashes


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

    def test_caller_supplied_manifest_is_ignored(self, tmp_path: Path) -> None:
        """Writer ignores any caller-supplied manifest and builds its own.

        Defeats the manifest→write TOCTOU: a caller-supplied manifest
        would describe a *snapshot* of source hashes from before the
        write, which can diverge from what actually lands in the tar
        if the source mutates in between. The writer's own manifest
        is bound to the bytes it actually streamed.
        """
        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        # Caller supplies a bogus manifest claiming the file's hash is
        # all zeroes — the writer must NOT honour this.
        bogus_manifest = {
            "version": 1,
            "algorithm": "sha256",
            "files": {"a.txt": {"hash": "0" * 64, "size": 5}},
            "total_checksum": "0" * 64,
        }

        dest = tmp_path / "dest"
        dest.mkdir()
        password = "ignore-test"
        archive = write_encrypted_tar(
            files, dest, "Backup", password, integrity_manifest=bogus_manifest
        )

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, password)
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                tar.extractall(path=extract_dir)

        loaded = json.loads((extract_dir / ".wbverify").read_text(encoding="utf-8"))
        # The embedded hash is the REAL one, not the caller's bogus one.
        assert loaded["files"]["a.txt"]["hash"] != "0" * 64

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
