"""Integration test: full backup → encrypt → restore → verify.

Tests the complete recovery workflow: create source files, run
write_encrypted_tar, then decrypt the .tar.wbenc archive and
verify that all files are restored correctly.
"""

import hashlib
import json
import os
import tarfile
from pathlib import Path

import pytest

from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import write_encrypted_tar
from src.security.encryption import DecryptingReader

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_file(path: Path, content: bytes) -> None:
    """Create a file with binary content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _sha256(data: bytes) -> str:
    """Return hex SHA-256 of data."""
    return hashlib.sha256(data).hexdigest()


def _make_file_info(source_path: Path, relative_path: str) -> FileInfo:
    """Build a FileInfo from an existing file on disk."""
    stat = source_path.stat()
    return FileInfo(
        source_path=source_path,
        relative_path=relative_path,
        size=stat.st_size,
        mtime=stat.st_mtime,
        source_root=str(source_path.parent),
    )


def _restore_archive(archive: Path, password: str, dest: Path) -> list[str]:
    """Decrypt and extract a .tar.wbenc archive, return member names."""
    dest.mkdir(parents=True, exist_ok=True)
    names: list[str] = []
    with open(archive, "rb") as f:
        reader = DecryptingReader(f, password)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for member in tar:
                names.append(member.name)
                tar.extract(member, path=dest, filter="data")
    return names


# ---------------------------------------------------------------------------
# Recovery round-trip tests
# ---------------------------------------------------------------------------


class TestRecoveryRoundTrip:
    """End-to-end: backup encrypted → restore → compare files."""

    @pytest.fixture()
    def source_tree(self, tmp_path: Path) -> tuple[Path, dict[str, bytes]]:
        """Create a realistic source tree with various file types.

        Returns:
            Tuple of (source_dir, {relative_path: content_bytes}).
        """
        src = tmp_path / "source"
        files: dict[str, bytes] = {
            "readme.txt": b"Hello world",
            "docs/notes.md": b"# Notes\nSome content here.\n",
            "data/binary.bin": os.urandom(128 * 1024),  # 128 KB
            "data/empty.dat": b"",
            "deep/nested/folder/file.txt": b"deep content",
        }
        for rel, content in files.items():
            _make_file(src / rel, content)
        return src, files

    def test_all_files_restored_with_correct_content(
        self,
        tmp_path: Path,
        source_tree: tuple[Path, dict[str, bytes]],
    ) -> None:
        """Every file is restored with identical content."""
        src, expected_files = source_tree
        password = "recovery-test-password"

        file_infos = [_make_file_info(src / rel, rel) for rel in expected_files]

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(file_infos, dest, "Recovery_FULL", password)

        restore_dir = tmp_path / "restored"
        _restore_archive(archive, password, restore_dir)

        for rel, expected_content in expected_files.items():
            restored_file = restore_dir / rel
            assert restored_file.exists(), f"Missing restored file: {rel}"
            assert restored_file.read_bytes() == expected_content, f"Content mismatch for {rel}"

    def test_embedded_manifest_is_valid(
        self,
        tmp_path: Path,
        source_tree: tuple[Path, dict[str, bytes]],
    ) -> None:
        """Embedded .wbverify manifest is present and parseable."""
        src, expected_files = source_tree
        password = "manifest-recovery"

        file_infos = [_make_file_info(src / rel, rel) for rel in expected_files]

        # Build a manifest
        manifest = {
            "version": 1,
            "algorithm": "sha256",
            "files": {},
        }
        for rel, content in expected_files.items():
            manifest["files"][rel] = {
                "hash": _sha256(content),
                "size": len(content),
            }

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(
            file_infos,
            dest,
            "ManifestTest",
            password,
            integrity_manifest=manifest,
        )

        restore_dir = tmp_path / "restored"
        names = _restore_archive(archive, password, restore_dir)

        assert ".wbverify" in names
        loaded = json.loads((restore_dir / ".wbverify").read_text("utf-8"))
        assert loaded["algorithm"] == "sha256"
        assert len(loaded["files"]) == len(expected_files)

    def test_manifest_hashes_match_restored_files(
        self,
        tmp_path: Path,
        source_tree: tuple[Path, dict[str, bytes]],
    ) -> None:
        """Manifest hashes match SHA-256 of restored files."""
        src, expected_files = source_tree
        password = "verify-hashes"

        file_infos = [_make_file_info(src / rel, rel) for rel in expected_files]

        manifest = {
            "version": 1,
            "algorithm": "sha256",
            "files": {
                rel: {"hash": _sha256(content), "size": len(content)}
                for rel, content in expected_files.items()
            },
        }

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(
            file_infos, dest, "HashVerify", password, integrity_manifest=manifest
        )

        restore_dir = tmp_path / "restored"
        _restore_archive(archive, password, restore_dir)

        loaded = json.loads((restore_dir / ".wbverify").read_text("utf-8"))
        for rel, meta in loaded["files"].items():
            restored = restore_dir / rel
            actual_hash = _sha256(restored.read_bytes())
            assert actual_hash == meta["hash"], f"Hash mismatch for {rel}"

    def test_wrong_password_fails_to_restore(
        self,
        tmp_path: Path,
        source_tree: tuple[Path, dict[str, bytes]],
    ) -> None:
        """Decryption with wrong password raises an error."""
        from cryptography.exceptions import InvalidTag

        src, expected_files = source_tree

        file_infos = [_make_file_info(src / rel, rel) for rel in expected_files]

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(file_infos, dest, "WrongPw", "correct")

        restore_dir = tmp_path / "restored"
        with pytest.raises((InvalidTag, Exception)):
            _restore_archive(archive, "wrong-password", restore_dir)

    def test_large_binary_file_roundtrip(self, tmp_path: Path) -> None:
        """A 2 MB binary file survives the encrypt/decrypt cycle."""
        src = tmp_path / "source"
        large_data = os.urandom(2 * 1024 * 1024)
        _make_file(src / "large.bin", large_data)

        file_infos = [_make_file_info(src / "large.bin", "large.bin")]
        password = "large-file-test"

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(file_infos, dest, "LargeFile", password)

        restore_dir = tmp_path / "restored"
        _restore_archive(archive, password, restore_dir)

        assert (restore_dir / "large.bin").read_bytes() == large_data

    def test_empty_backup_restores_cleanly(self, tmp_path: Path) -> None:
        """An encrypted backup with no files can be restored."""
        password = "empty"
        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar([], dest, "Empty", password)

        restore_dir = tmp_path / "restored"
        names = _restore_archive(archive, password, restore_dir)
        assert names == []

    def test_unicode_filenames_roundtrip(self, tmp_path: Path) -> None:
        """Files with unicode names survive the encrypt/decrypt cycle."""
        src = tmp_path / "source"
        unicode_files = {
            "café.txt": b"french",
            "données/résumé.md": b"accents",
        }
        for rel, content in unicode_files.items():
            _make_file(src / rel, content)

        file_infos = [_make_file_info(src / rel, rel) for rel in unicode_files]
        password = "unicode-test"

        dest = tmp_path / "backup_dest"
        dest.mkdir()
        archive = write_encrypted_tar(file_infos, dest, "Unicode", password)

        restore_dir = tmp_path / "restored"
        _restore_archive(archive, password, restore_dir)

        for rel, expected in unicode_files.items():
            restored = restore_dir / rel
            assert restored.exists(), f"Missing: {rel}"
            assert restored.read_bytes() == expected
