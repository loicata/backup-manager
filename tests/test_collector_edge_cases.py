"""Additional edge-case tests for the collector phase."""

import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases.collector import collect_files, FileInfo


class TestPermissionDenied:
    def test_permission_denied_subdirectory(self, tmp_path):
        """Files in accessible dirs collected; inaccessible subdirs skipped."""
        (tmp_path / "ok.txt").write_text("ok", encoding="utf-8")
        blocked = tmp_path / "blocked"
        blocked.mkdir()
        (blocked / "secret.txt").write_text("s", encoding="utf-8")

        original_scandir = os.scandir

        def patched_scandir(path):
            if str(path) == str(blocked):
                raise PermissionError("Access denied")
            return original_scandir(path)

        with patch("os.scandir", side_effect=patched_scandir):
            files = collect_files([str(tmp_path)])

        names = [f.relative_path for f in files]
        assert "ok.txt" in names
        assert not any("secret" in n for n in names)


class TestRaceConditions:
    def test_file_deleted_during_stat(self, tmp_path):
        """File vanishing between scandir and stat is silently skipped."""
        f = tmp_path / "ephemeral.txt"
        f.write_text("gone soon", encoding="utf-8")
        (tmp_path / "stable.txt").write_text("here", encoding="utf-8")

        original_stat = Path.stat

        def patched_stat(self, *args, **kwargs):
            if self.name == "ephemeral.txt":
                raise FileNotFoundError("deleted")
            return original_stat(self, *args, **kwargs)

        with patch.object(Path, "stat", patched_stat):
            files = collect_files([str(tmp_path)])

        names = [f.relative_path for f in files]
        assert "stable.txt" in names
        assert "ephemeral.txt" not in names


class TestDeepDirectory:
    def test_deep_structure(self, tmp_path):
        """Files at 12 levels deep are all collected."""
        current = tmp_path
        for i in range(12):
            current = current / f"level{i}"
            current.mkdir()
        (current / "deep.txt").write_text("deep", encoding="utf-8")
        (tmp_path / "top.txt").write_text("top", encoding="utf-8")

        files = collect_files([str(tmp_path)])
        names = [f.relative_path for f in files]
        assert "top.txt" in names
        assert any("deep.txt" in n for n in names)
        assert len(files) == 2


class TestEmptyDirectory:
    def test_empty_dir_returns_empty(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        files = collect_files([str(empty)])
        assert files == []


class TestMixedSources:
    def test_file_and_directory_sources(self, tmp_path):
        """Both a single file and a directory can be given as sources."""
        d = tmp_path / "dir"
        d.mkdir()
        (d / "a.txt").write_text("a", encoding="utf-8")

        single = tmp_path / "solo.txt"
        single.write_text("solo", encoding="utf-8")

        files = collect_files([str(d), str(single)])
        names = {f.relative_path for f in files}
        assert "a.txt" in names
        assert "solo.txt" in names
        assert len(files) == 2


class TestSingleFileSource:
    def test_single_file_as_source(self, tmp_path):
        f = tmp_path / "report.csv"
        f.write_text("col1,col2", encoding="utf-8")
        files = collect_files([str(f)])
        assert len(files) == 1
        assert isinstance(files[0], FileInfo)
        assert files[0].size > 0


class TestExcludePatterns:
    def test_wildcard_patterns(self, tmp_path):
        (tmp_path / "app.log").write_text("log", encoding="utf-8")
        (tmp_path / "temp_cache").mkdir()
        (tmp_path / "temp_cache" / "data.bin").write_text("d", encoding="utf-8")
        (tmp_path / "keep.txt").write_text("k", encoding="utf-8")

        files = collect_files(
            [str(tmp_path)],
            exclude_patterns=["*.log", "temp_*"],
        )
        names = [f.relative_path for f in files]
        assert "keep.txt" in names
        assert "app.log" not in names
        assert not any("data.bin" in n for n in names)


class TestDuplicateSources:
    def test_duplicate_paths_deduplicated(self, tmp_path):
        f = tmp_path / "unique.txt"
        f.write_text("x", encoding="utf-8")
        files = collect_files([str(tmp_path), str(tmp_path), str(tmp_path)])
        assert len(files) == 1


class TestSpecialCharPaths:
    def test_spaces_and_special_chars(self, tmp_path):
        weird = tmp_path / "my folder (2024)" / "sub dir"
        weird.mkdir(parents=True)
        (weird / "file with spaces.txt").write_text("ok", encoding="utf-8")

        files = collect_files([str(tmp_path)])
        assert len(files) == 1
        assert "file with spaces.txt" in files[0].relative_path


class TestSymlinksAndJunctions:
    def test_symlinks_skipped(self, tmp_path):
        real = tmp_path / "real.txt"
        real.write_text("real", encoding="utf-8")
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(real)
        except OSError:
            pytest.skip("Symlinks not supported on this platform")

        files = collect_files([str(tmp_path)])
        names = [f.relative_path for f in files]
        assert "real.txt" in names
        assert "link.txt" not in names
