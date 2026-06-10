"""Additional edge-case tests for the collector phase."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases.collector import (
    FileInfo,
    _add_file,
    _SkippedPaths,
    collect_files,
)


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
        # relative_path is prefixed with source directory name
        assert any(n.endswith("/ok.txt") for n in names)
        assert not any("secret" in n for n in names)


class TestRaceConditions:
    def test_file_deleted_during_stat(self, tmp_path):
        """File vanishing between scandir and stat is skipped (and tracked)."""
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
        assert any(n.endswith("/stable.txt") for n in names)
        assert not any(n.endswith("/ephemeral.txt") for n in names)

    def test_stat_failure_recorded_in_skipped(self, tmp_path):
        """A stat() failure in _add_file is recorded on the skipped
        accumulator instead of being silently swallowed (audit L3)."""
        f = tmp_path / "vanishing.txt"
        f.write_text("x", encoding="utf-8")
        skipped = _SkippedPaths()
        files: list[FileInfo] = []

        original_stat = Path.stat

        def boom(self, *args, **kwargs):
            if self.name == "vanishing.txt":
                raise OSError("WinError 32 sharing violation")
            return original_stat(self, *args, **kwargs)

        with patch.object(Path, "stat", boom):
            _add_file(files, set(), f, tmp_path, str(tmp_path), skipped)

        assert files == []  # not collected
        assert len(skipped.os_errors) == 1  # but NOT silently dropped
        path, message = skipped.os_errors[0]
        assert path.endswith("vanishing.txt")
        assert "sharing violation" in message

    def test_add_file_without_accumulator_does_not_raise(self, tmp_path):
        """Backward compat: skipped defaults to None and a stat failure
        is tolerated (no accumulator to record into)."""
        f = tmp_path / "x.txt"
        f.write_text("x", encoding="utf-8")
        files: list[FileInfo] = []
        original_stat = Path.stat

        def boom(self, *args, **kwargs):
            if self.name == "x.txt":
                raise OSError("gone")
            return original_stat(self, *args, **kwargs)

        with patch.object(Path, "stat", boom):
            _add_file(files, set(), f, tmp_path, str(tmp_path))  # no skipped arg
        assert files == []


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
        assert any(n.endswith("/top.txt") for n in names)
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
        assert any(n.endswith("/a.txt") for n in names)
        assert any(n.endswith("/solo.txt") for n in names)
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
        assert any(n.endswith("/keep.txt") for n in names)
        assert not any(n.endswith("/app.log") for n in names)
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
        assert files[0].relative_path.endswith("/file with spaces.txt")


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
        assert any(n.endswith("/real.txt") for n in names)
        assert not any(n.endswith("/link.txt") for n in names)


class TestExcludePatternStyles:
    """Patterns are matched against the basename by default; patterns
    containing ``/`` are matched against the source-relative POSIX
    path so users can target specific layouts (e.g.
    ``*/evidence/*/volatile``)."""

    def test_basename_pattern_excludes_directory_by_name(self, tmp_path):
        """``__pycache__``-style patterns (no ``/``) prune any subdir
        with that basename, anywhere in the tree."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.py").write_text("x", encoding="utf-8")
        (tmp_path / "src" / "__pycache__").mkdir()
        (tmp_path / "src" / "__pycache__" / "cache.pyc").write_text("c", encoding="utf-8")

        files = collect_files([str(tmp_path)], exclude_patterns=["__pycache__"])
        names = [f.relative_path for f in files]

        assert any(n.endswith("/main.py") for n in names)
        assert not any("__pycache__" in n for n in names)

    def test_basename_pattern_excludes_file_by_glob(self, tmp_path):
        """``*.tmp``-style patterns match file basenames anywhere."""
        (tmp_path / "keep.txt").write_text("k", encoding="utf-8")
        (tmp_path / "drop.tmp").write_text("d", encoding="utf-8")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "also.tmp").write_text("a", encoding="utf-8")

        files = collect_files([str(tmp_path)], exclude_patterns=["*.tmp"])
        names = [f.relative_path for f in files]

        assert any(n.endswith("/keep.txt") for n in names)
        assert not any(n.endswith(".tmp") for n in names)

    def test_path_pattern_excludes_specific_layout(self, tmp_path):
        """Pattern ``*/evidence/*/volatile`` matches any
        ``<root>/.../evidence/<uuid>/volatile`` directory regardless
        of depth above ``evidence``."""
        # Layout: tmp/proj/evidence/abc-uuid/volatile/dump.bin
        ev_dir = tmp_path / "proj" / "evidence" / "abc-uuid" / "volatile"
        ev_dir.mkdir(parents=True)
        (ev_dir / "dump.bin").write_text("vol", encoding="utf-8")
        # Files we DO want to back up: a peer dir under evidence/<uuid>.
        peer_dir = tmp_path / "proj" / "evidence" / "abc-uuid" / "metadata"
        peer_dir.mkdir(parents=True)
        (peer_dir / "info.json").write_text("info", encoding="utf-8")

        files = collect_files(
            [str(tmp_path)],
            exclude_patterns=["*/evidence/*/volatile"],
        )
        names = [f.relative_path for f in files]

        assert any(n.endswith("/info.json") for n in names)
        assert not any("volatile" in n for n in names)

    def test_path_pattern_does_not_match_unrelated_basename(self, tmp_path):
        """A path-style pattern does NOT exclude a basename that
        only happens to coincide with the last component."""
        # ``volatile`` directly under root (no evidence/<uuid> ancestor)
        # must NOT be pruned by ``*/evidence/*/volatile``.
        vol_dir = tmp_path / "volatile"
        vol_dir.mkdir()
        (vol_dir / "data.txt").write_text("ok", encoding="utf-8")

        files = collect_files(
            [str(tmp_path)],
            exclude_patterns=["*/evidence/*/volatile"],
        )
        names = [f.relative_path for f in files]

        assert any(n.endswith("/data.txt") for n in names)

    def test_path_and_basename_patterns_can_combine(self, tmp_path):
        """Both pattern styles can coexist in the same exclude list."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.py").write_text("m", encoding="utf-8")
        cache = tmp_path / "src" / "__pycache__"
        cache.mkdir()
        (cache / "x.pyc").write_text("c", encoding="utf-8")
        # ``*/evidence/*/volatile`` requires at least one ancestor
        # component before ``evidence/`` (fnmatch's ``*`` matches ≥1
        # chars including ``/``). Real-world WardSOAR layouts always
        # have at least one wrapper directory, so this matches the
        # production case.
        ev = tmp_path / "WardSOAR" / "evidence" / "u1" / "volatile"
        ev.mkdir(parents=True)
        (ev / "ram.dmp").write_text("v", encoding="utf-8")

        files = collect_files(
            [str(tmp_path)],
            exclude_patterns=["__pycache__", "*/evidence/*/volatile"],
        )
        names = [f.relative_path for f in files]

        assert any(n.endswith("/main.py") for n in names)
        assert not any("__pycache__" in n for n in names)
        assert not any("volatile" in n for n in names)
