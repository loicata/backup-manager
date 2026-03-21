"""Tests for src.core.hashing — SHA-256 file hashing utility."""

import hashlib
from pathlib import Path

import pytest

from src.core.hashing import HASH_CHUNK_SIZE, compute_sha256


class TestComputeSha256:
    """Tests for the compute_sha256 function."""

    def test_hash_known_content(self, tmp_path: Path) -> None:
        """Hash of known content matches hashlib reference."""
        content = b"Hello, Backup Manager!"
        filepath = tmp_path / "known.txt"
        filepath.write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        assert compute_sha256(filepath) == expected

    def test_hash_empty_file(self, tmp_path: Path) -> None:
        """Empty file produces the SHA-256 of empty bytes."""
        filepath = tmp_path / "empty.txt"
        filepath.write_bytes(b"")

        expected = hashlib.sha256(b"").hexdigest()
        assert compute_sha256(filepath) == expected

    def test_hash_large_file_spans_multiple_chunks(self, tmp_path: Path) -> None:
        """File larger than HASH_CHUNK_SIZE is hashed correctly."""
        # Create a file larger than 2 chunks
        content = b"X" * (HASH_CHUNK_SIZE * 2 + 42)
        filepath = tmp_path / "large.bin"
        filepath.write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        assert compute_sha256(filepath) == expected

    def test_hash_binary_content(self, tmp_path: Path) -> None:
        """Binary content (all byte values) is hashed correctly."""
        content = bytes(range(256)) * 100
        filepath = tmp_path / "binary.bin"
        filepath.write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        assert compute_sha256(filepath) == expected

    def test_deterministic(self, tmp_path: Path) -> None:
        """Same file hashed twice produces the same digest."""
        filepath = tmp_path / "stable.txt"
        filepath.write_bytes(b"deterministic content")

        hash1 = compute_sha256(filepath)
        hash2 = compute_sha256(filepath)
        assert hash1 == hash2

    def test_different_content_different_hash(self, tmp_path: Path) -> None:
        """Different content produces different hashes."""
        file_a = tmp_path / "a.txt"
        file_b = tmp_path / "b.txt"
        file_a.write_bytes(b"content A")
        file_b.write_bytes(b"content B")

        assert compute_sha256(file_a) != compute_sha256(file_b)

    def test_nonexistent_file_raises_file_not_found(self, tmp_path: Path) -> None:
        """Missing file raises FileNotFoundError."""
        missing = tmp_path / "does_not_exist.txt"
        with pytest.raises(FileNotFoundError):
            compute_sha256(missing)

    def test_rejects_none_path(self) -> None:
        """None path raises TypeError."""
        with pytest.raises(TypeError):
            compute_sha256(None)  # type: ignore[arg-type]

    def test_rejects_directory(self, tmp_path: Path) -> None:
        """Directory path raises ValueError."""
        with pytest.raises(ValueError, match="not a directory"):
            compute_sha256(tmp_path)

    def test_returns_hex_string(self, tmp_path: Path) -> None:
        """Result is a 64-character lowercase hex string."""
        filepath = tmp_path / "hex.txt"
        filepath.write_bytes(b"hex test")

        result = compute_sha256(filepath)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)


class TestHashChunkSize:
    """Verify the chunk size constant."""

    def test_chunk_size_is_128kb(self) -> None:
        """HASH_CHUNK_SIZE is 128 KiB."""
        assert HASH_CHUNK_SIZE == 128 * 1024
