"""Tests for FileInfo validation in src.core.phases.collector."""

from pathlib import Path

import pytest

from src.core.phases.collector import FileInfo


class TestFileInfoValidation:
    """Tests for FileInfo __post_init__ validation."""

    def test_valid_fileinfo(self, tmp_path: Path) -> None:
        """Valid FileInfo is created without error."""
        filepath = tmp_path / "test.txt"
        filepath.write_text("hello")

        info = FileInfo(
            source_path=filepath,
            relative_path="test.txt",
            size=5,
            mtime=1234567890.0,
            source_root=str(tmp_path),
        )
        assert info.source_path == filepath

    def test_none_source_path_raises(self) -> None:
        """source_path=None raises ValueError."""
        with pytest.raises(ValueError, match="source_path must not be None"):
            FileInfo(
                source_path=None,  # type: ignore[arg-type]
                relative_path="test.txt",
                size=0,
                mtime=0.0,
                source_root="/tmp",
            )

    def test_empty_relative_path_raises(self, tmp_path: Path) -> None:
        """Empty relative_path raises ValueError."""
        with pytest.raises(ValueError, match="relative_path must not be empty"):
            FileInfo(
                source_path=tmp_path / "file.txt",
                relative_path="",
                size=0,
                mtime=0.0,
                source_root=str(tmp_path),
            )

    def test_negative_size_raises(self, tmp_path: Path) -> None:
        """Negative size raises ValueError."""
        with pytest.raises(ValueError, match="size must be >= 0"):
            FileInfo(
                source_path=tmp_path / "file.txt",
                relative_path="file.txt",
                size=-1,
                mtime=0.0,
                source_root=str(tmp_path),
            )

    def test_zero_size_is_valid(self, tmp_path: Path) -> None:
        """size=0 is valid (empty file)."""
        info = FileInfo(
            source_path=tmp_path / "empty.txt",
            relative_path="empty.txt",
            size=0,
            mtime=0.0,
            source_root=str(tmp_path),
        )
        assert info.size == 0
