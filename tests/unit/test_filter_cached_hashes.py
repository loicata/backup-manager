"""Tests for build_updated_manifest with cached hashes.

Verifies that pre-computed hashes from Phase 3 (integrity manifest)
are reused in Phase 8 (delta manifest) instead of re-hashing files.
"""

from unittest.mock import patch

import pytest

from src.core.phases.collector import FileInfo
from src.core.phases.filter import build_updated_manifest


@pytest.fixture()
def sample_files(tmp_path):
    """Create sample files and return their FileInfo list."""
    files = []
    for i in range(3):
        p = tmp_path / f"file{i}.txt"
        p.write_text(f"content {i}", encoding="utf-8")
        stat = p.stat()
        files.append(
            FileInfo(
                source_path=p,
                relative_path=f"file{i}.txt",
                size=stat.st_size,
                mtime=stat.st_mtime,
                source_root=tmp_path,
            )
        )
    return files


class TestBuildUpdatedManifestCache:
    """Test cached hash reuse in build_updated_manifest."""

    def test_uses_cached_hashes_when_provided(self, sample_files):
        """Cached hashes should be used instead of computing SHA-256."""
        cached = {
            "file0.txt": "cached_hash_0",
            "file1.txt": "cached_hash_1",
            "file2.txt": "cached_hash_2",
        }

        result = build_updated_manifest(sample_files, cached_hashes=cached)

        assert result["file0.txt"]["hash"] == "cached_hash_0"
        assert result["file1.txt"]["hash"] == "cached_hash_1"
        assert result["file2.txt"]["hash"] == "cached_hash_2"

    def test_cached_hashes_skip_compute(self, sample_files):
        """When all hashes are cached, compute_sha256 must not be called."""
        cached = {f.relative_path: f"hash_{i}" for i, f in enumerate(sample_files)}

        with patch("src.core.phases.filter.compute_sha256") as mock_sha:
            build_updated_manifest(sample_files, cached_hashes=cached)
            mock_sha.assert_not_called()

    def test_falls_back_to_compute_for_missing_cache(self, sample_files):
        """Files not in cache should be hashed normally."""
        cached = {"file0.txt": "cached_hash_0"}
        # file1.txt and file2.txt are not cached

        with patch(
            "src.core.phases.filter.compute_sha256",
            return_value="computed_hash",
        ) as mock_sha:
            result = build_updated_manifest(sample_files, cached_hashes=cached)

        assert result["file0.txt"]["hash"] == "cached_hash_0"
        assert result["file1.txt"]["hash"] == "computed_hash"
        assert result["file2.txt"]["hash"] == "computed_hash"
        assert mock_sha.call_count == 2

    def test_no_cache_computes_all(self, sample_files):
        """Without cache, all files should be hashed (backward compat)."""
        with patch(
            "src.core.phases.filter.compute_sha256",
            return_value="computed",
        ) as mock_sha:
            result = build_updated_manifest(sample_files)

        assert mock_sha.call_count == 3
        for f in sample_files:
            assert result[f.relative_path]["hash"] == "computed"

    def test_empty_cache_computes_all(self, sample_files):
        """Empty cache dict behaves like no cache."""
        with patch(
            "src.core.phases.filter.compute_sha256",
            return_value="computed",
        ) as mock_sha:
            build_updated_manifest(sample_files, cached_hashes={})

        assert mock_sha.call_count == 3

    def test_manifest_contains_size_and_mtime(self, sample_files):
        """Cached hashes should not affect size/mtime storage."""
        cached = {f.relative_path: "hash" for f in sample_files}

        result = build_updated_manifest(sample_files, cached_hashes=cached)

        for f in sample_files:
            entry = result[f.relative_path]
            assert entry["size"] == f.size
            assert entry["mtime"] == f.mtime
            assert entry["hash"] == "hash"
