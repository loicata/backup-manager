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
        """WriteError when source file cannot be read."""
        from unittest.mock import patch

        src = tmp_path / "source"
        _make_file(src / "a.txt", "data")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        dest = tmp_path / "dest"
        dest.mkdir()
        with patch("src.core.phases.local_writer.shutil.copy2") as mock_copy:
            mock_copy.side_effect = PermissionError("Access denied")
            with pytest.raises(WriteError):
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
        """Integrity manifest is embedded as .wbverify inside the archive."""
        from src.security.encryption import DecryptingReader

        src = tmp_path / "source"
        _make_file(src / "a.txt", "alpha")
        files = [_make_file_info(src / "a.txt", "a.txt")]

        manifest = {
            "version": 1,
            "algorithm": "sha256",
            "files": {"a.txt": {"hash": "abc123", "size": 5}},
        }

        dest = tmp_path / "dest"
        dest.mkdir()
        password = "manifest-test"
        archive = write_encrypted_tar(files, dest, "Backup", password, integrity_manifest=manifest)

        # Extract and check for .wbverify
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

    def test_no_manifest_when_none(self, tmp_path: Path) -> None:
        """No .wbverify entry when integrity_manifest is None."""
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

        assert ".wbverify" not in names

    def test_emits_progress_events(self, tmp_path: Path) -> None:
        """Progress events are emitted for each file in the archive."""
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
        """Empty file list creates a valid (small) encrypted archive."""
        from src.security.encryption import DecryptingReader

        dest = tmp_path / "dest"
        dest.mkdir()
        archive = write_encrypted_tar([], dest, "Empty", "pw")

        assert archive.exists()
        assert archive.stat().st_size > 0

        # Should be decryptable with no files inside
        with open(archive, "rb") as f:
            reader = DecryptingReader(f, "pw")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                members = list(tar)
                assert len(members) == 0


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
