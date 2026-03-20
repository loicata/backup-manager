"""Tests for unicode and special character handling across pipeline phases."""

import json
from pathlib import Path

import pytest

from src.core.phases.collector import collect_files
from src.core.phases.manifest import build_integrity_manifest, save_integrity_manifest
from src.core.phases.local_writer import write_flat, generate_backup_name


class TestCollectorUnicode:
    """Collector handles files with unicode names."""

    def test_cjk_filenames(self, tmp_path):
        """Files with CJK characters are collected."""
        (tmp_path / "\u4e2d\u6587\u6587\u4ef6.txt").write_text("data", encoding="utf-8")
        files = collect_files([str(tmp_path)])
        assert len(files) == 1
        assert "\u4e2d\u6587" in files[0].relative_path

    def test_accented_filenames(self, tmp_path):
        """Files with accented characters (e, a, u) are collected."""
        (tmp_path / "caf\u00e9_r\u00e9sum\u00e9.txt").write_text("data", encoding="utf-8")
        files = collect_files([str(tmp_path)])
        assert len(files) == 1
        assert "\u00e9" in files[0].relative_path

    def test_emoji_filenames(self, tmp_path):
        """Files with emoji in names are collected."""
        try:
            (tmp_path / "\ud83c\udfb5music.txt").write_text("notes", encoding="utf-8")
        except OSError:
            pytest.skip("Filesystem does not support emoji filenames")
        files = collect_files([str(tmp_path)])
        assert len(files) == 1

    def test_exclude_pattern_with_unicode(self, tmp_path):
        """Exclusion patterns work with unicode filenames."""
        (tmp_path / "garder.txt").write_text("keep", encoding="utf-8")
        (tmp_path / "\u00e9l\u00e8ve.tmp").write_text("skip", encoding="utf-8")
        files = collect_files([str(tmp_path)], exclude_patterns=["*.tmp"])
        assert len(files) == 1
        assert files[0].relative_path == "garder.txt"

    def test_paths_with_spaces_and_parens(self, tmp_path):
        """Paths containing spaces and parentheses are handled."""
        subdir = tmp_path / "My Documents (2026)"
        subdir.mkdir()
        (subdir / "file (copy).txt").write_text("data", encoding="utf-8")
        files = collect_files([str(subdir)])
        assert len(files) == 1
        assert "(copy)" in files[0].relative_path


class TestManifestUnicode:
    """Manifest JSON serialization preserves unicode."""

    def test_json_preserves_unicode_filenames(self, tmp_path):
        """Unicode filenames survive JSON round-trip."""
        (tmp_path / "\u4e2d\u6587.txt").write_text("hello", encoding="utf-8")
        (tmp_path / "caf\u00e9.txt").write_text("world", encoding="utf-8")

        files = collect_files([str(tmp_path)])
        manifest = build_integrity_manifest(files)

        # Serialize and deserialize
        raw = json.dumps(manifest, ensure_ascii=False)
        loaded = json.loads(raw)

        paths = set(loaded["files"].keys())
        assert any("\u4e2d\u6587" in p for p in paths)
        assert any("caf\u00e9" in p for p in paths)

    def test_save_manifest_unicode(self, tmp_path):
        """Saved .wbverify file preserves unicode filenames on disk."""
        (tmp_path / "\u00fc\u00f6\u00e4.txt").write_text("umlaut", encoding="utf-8")
        files = collect_files([str(tmp_path)])
        manifest = build_integrity_manifest(files)

        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()
        manifest_path = save_integrity_manifest(manifest, backup_dir)

        content = manifest_path.read_text(encoding="utf-8")
        assert "\u00fc\u00f6\u00e4" in content


class TestLocalWriterUnicode:
    """Local writer copies files with unicode names correctly."""

    def test_copy_unicode_named_files(self, tmp_path):
        """Files with unicode names are copied to the backup."""
        src = tmp_path / "source"
        src.mkdir()
        names = ["\u4e2d\u6587.txt", "r\u00e9sum\u00e9.txt", "normal.txt"]
        for name in names:
            (src / name).write_text(f"content of {name}", encoding="utf-8")

        files = collect_files([str(src)])
        dest = tmp_path / "backups"
        dest.mkdir()
        backup_path = write_flat(files, dest, "unicode_test")

        for name in names:
            target = backup_path / name
            assert target.exists(), f"Missing: {name}"
            assert target.read_text(encoding="utf-8") == f"content of {name}"

    def test_generate_backup_name_with_special_chars(self):
        """Profile names with special characters are sanitized."""
        name = generate_backup_name("Profil \u00e9t\u00e9 <2026>")
        assert "<" not in name
        assert ">" not in name
        # Accented letters are kept (isalnum is True for them)
        assert "\u00e9" in name or "_" in name


class TestConfigUnicode:
    """Config handles profiles with unicode names."""

    def test_profile_name_with_special_characters(self, tmp_path):
        """Profile with unicode name is saved and loaded correctly."""
        from unittest.mock import patch
        from src.core.config import BackupProfile, ConfigManager

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "profiles").mkdir()
        (config_dir / "logs").mkdir()
        (config_dir / "manifests").mkdir()

        profile = BackupProfile(name="Sauvegarde \u00e9t\u00e9 \u2014 2026")

        # Bypass encryption for test
        with (
            patch("src.core.config.store_password", side_effect=lambda x: x),
            patch("src.core.config.retrieve_password", side_effect=lambda x: x),
        ):
            mgr = ConfigManager(config_dir=config_dir)
            mgr.save_profile(profile)

            profiles = mgr.get_all_profiles()
            assert len(profiles) == 1
            assert profiles[0].name == "Sauvegarde \u00e9t\u00e9 \u2014 2026"


class TestMixedEncodings:
    """Mixed filename encodings in the same backup."""

    def test_ascii_and_utf8_filenames_together(self, tmp_path):
        """ASCII and UTF-8 filenames coexist in same collection."""
        src = tmp_path / "mixed"
        src.mkdir()
        (src / "readme.txt").write_text("ascii", encoding="utf-8")
        (src / "\u00e9l\u00e8ve.txt").write_text("accented", encoding="utf-8")
        (src / "\u6d4b\u8bd5.txt").write_text("chinese", encoding="utf-8")

        files = collect_files([str(src)])
        assert len(files) == 3

        # All survive backup round-trip
        dest = tmp_path / "backups"
        dest.mkdir()
        backup_path = write_flat(files, dest, "mixed_test")

        assert (backup_path / "readme.txt").exists()
        assert (backup_path / "\u00e9l\u00e8ve.txt").exists()
        assert (backup_path / "\u6d4b\u8bd5.txt").exists()
