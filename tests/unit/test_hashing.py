"""Tests for src.core.hashing — SHA-256 file hashing utility."""

import hashlib
import os
from pathlib import Path

import pytest

from src.core.hashing import HASH_CHUNK_SIZE, compute_sha256, copy_and_hash


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
    """Verify the chunk size constant.

    The chunk size matters for write throughput: ``copy_and_hash``
    feeds each chunk into both ``hashlib`` and the destination's
    ``open(..., "wb").write``, so a too-small chunk caps USB/SSD
    throughput by syscall overhead. 4 MiB is large enough to saturate
    consumer-grade external SSDs and small enough to keep the
    rolling buffer cheap on memory-constrained machines.
    """

    def test_chunk_size_is_4mib(self) -> None:
        """HASH_CHUNK_SIZE is 4 MiB (matches sustained-throughput sweet spot)."""
        assert HASH_CHUNK_SIZE == 4 * 1024 * 1024


class TestCopyAndHash:
    """Single-pass copy + hash that defeats manifest→write TOCTOU."""

    def test_destination_bytes_match_source(self, tmp_path: Path) -> None:
        """Trivial round-trip: copied bytes equal source bytes."""
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(b"hello world\n" * 100)

        copy_and_hash(src, dst)
        assert dst.read_bytes() == src.read_bytes()

    def test_returned_hash_matches_source_content(self, tmp_path: Path) -> None:
        """Hash returned equals SHA-256 of the bytes that landed on dst."""
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        content = b"some content here"
        src.write_bytes(content)

        h = copy_and_hash(src, dst)
        assert h == hashlib.sha256(content).hexdigest()
        assert h == hashlib.sha256(dst.read_bytes()).hexdigest()

    def test_empty_file_round_trip(self, tmp_path: Path) -> None:
        """Empty source produces empty dest and SHA-256 of empty bytes."""
        src = tmp_path / "empty.bin"
        dst = tmp_path / "empty_dst.bin"
        src.write_bytes(b"")

        h = copy_and_hash(src, dst)
        assert dst.read_bytes() == b""
        assert h == hashlib.sha256(b"").hexdigest()

    def test_large_file_spans_chunks(self, tmp_path: Path) -> None:
        """Files > HASH_CHUNK_SIZE are streamed correctly."""
        src = tmp_path / "big.bin"
        dst = tmp_path / "big_dst.bin"
        # Use deterministic-but-non-uniform content so any off-by-one
        # in chunk boundaries surfaces as a hash mismatch.
        content = bytes(range(256)) * (HASH_CHUNK_SIZE // 256 + 1) * 3
        src.write_bytes(content)

        h = copy_and_hash(src, dst)
        assert dst.read_bytes() == content
        assert h == hashlib.sha256(content).hexdigest()

    def test_preserves_mtime(self, tmp_path: Path) -> None:
        """copystat-equivalent: destination mtime tracks source."""
        src = tmp_path / "stat_src.txt"
        dst = tmp_path / "stat_dst.txt"
        src.write_text("hello", encoding="utf-8")
        target_mtime = 1_400_000_000.0  # 2014-05-13 in UTC, well in the past
        os.utime(src, (target_mtime, target_mtime))

        copy_and_hash(src, dst)
        # NTFS truncates mtime to ~100ns; allow a small tolerance.
        assert abs(dst.stat().st_mtime - target_mtime) < 1.0

    def test_hash_matches_compute_sha256_for_same_file(self, tmp_path: Path) -> None:
        """copy_and_hash on src is equivalent to compute_sha256 on src."""
        src = tmp_path / "compare.bin"
        dst = tmp_path / "compare_dst.bin"
        src.write_bytes(b"compare me" * 200)

        h_combined = copy_and_hash(src, dst)
        h_separate = compute_sha256(src)
        assert h_combined == h_separate

    def test_overwrites_existing_destination(self, tmp_path: Path) -> None:
        """If dst already exists with different content, it is replaced."""
        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(b"new content")
        dst.write_bytes(b"old content that is longer than the new")

        copy_and_hash(src, dst)
        assert dst.read_bytes() == b"new content"

    def test_missing_source_raises(self, tmp_path: Path) -> None:
        """Source absent → FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            copy_and_hash(tmp_path / "ghost.bin", tmp_path / "out.bin")

    def test_directory_source_raises(self, tmp_path: Path) -> None:
        """Directory source → ValueError."""
        d = tmp_path / "dir"
        d.mkdir()
        with pytest.raises(ValueError, match="directory"):
            copy_and_hash(d, tmp_path / "out.bin")

    def test_rejects_non_path_arguments(self, tmp_path: Path) -> None:
        """str/None inputs are TypeErrors, not silent stringification."""
        f = tmp_path / "f.bin"
        f.write_bytes(b"x")
        with pytest.raises(TypeError, match="src_path"):
            copy_and_hash(str(f), tmp_path / "out.bin")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="dst_path"):
            copy_and_hash(f, str(tmp_path / "out.bin"))  # type: ignore[arg-type]

    def test_returns_hex_lowercase_64(self, tmp_path: Path) -> None:
        """Output format contract — lowercase, 64 hex chars."""
        src = tmp_path / "fmt.bin"
        dst = tmp_path / "fmt_dst.bin"
        src.write_bytes(b"abc")

        h = copy_and_hash(src, dst)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_hash_reflects_original_source_not_post_copy_mutation(self, tmp_path: Path) -> None:
        """The returned hash is for the original source bytes — a
        post-copy mutation of the source can't retroactively change
        what the manifest claims.
        """
        src = tmp_path / "moving.bin"
        dst = tmp_path / "moving_dst.bin"
        original = b"first version"
        src.write_bytes(original)

        h = copy_and_hash(src, dst)
        # Mutate source AFTER the call — must not affect the returned hash.
        src.write_bytes(b"mutated content that is unrelated")

        assert h == hashlib.sha256(original).hexdigest()
        assert dst.read_bytes() == original

    def test_returned_hash_lets_verify_detect_silent_kernel_corruption(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """v3.3.18 INVARIANT: if the kernel copy silently corrupts the
        destination, ``compute_sha256(dst)`` MUST differ from the
        manifest hash returned by ``copy_and_hash``.

        v3.3.17 hashed the destination instead of the source, which
        meant a corrupt copy would hash to its own corruption and
        the verify phase would happily accept the bad backup. The
        v3.3.18 model — hash source first, then copy — restores the
        v3.3.14 detection: any divergence between source and
        destination triggers a verify-phase mismatch.
        """
        import shutil

        # Capture the real ``copy2`` BEFORE the monkeypatch, otherwise
        # the wrapper below would call its own patched version and
        # recurse forever.
        real_copy2 = shutil.copy2

        src = tmp_path / "src.bin"
        dst = tmp_path / "dst.bin"
        src.write_bytes(b"original content that the user wanted to back up")

        # Wrap shutil.copy2 in a fault injector that simulates a
        # kernel-side corruption: copy normally, then overwrite dst
        # with garbage as if a flaky USB controller had flipped bits.
        def _corrupt_copy(s: str, d: str) -> None:
            real_copy2(s, d)
            Path(d).write_bytes(b"corrupted by faulty hardware mid-transfer")

        monkeypatch.setattr("src.core.hashing.shutil.copy2", _corrupt_copy)

        manifest_hash = copy_and_hash(src, dst)

        # The manifest reflects the ORIGINAL source bytes, regardless
        # of what landed on the destination.
        expected = hashlib.sha256(
            b"original content that the user wanted to back up"
        ).hexdigest()
        assert manifest_hash == expected

        # And a verify-style re-hash of the destination diverges,
        # which is the loud failure that protects the user.
        assert compute_sha256(dst) != manifest_hash
