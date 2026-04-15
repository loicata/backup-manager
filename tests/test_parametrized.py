"""Parametrized tests — systematic edge case coverage.

Groups of parametrized tests for collector, encryption, config,
hashing, and backup naming to cover variations that single-case
tests miss.
"""

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.config import (
    BackupProfile,
    BackupType,
    StorageConfig,
    StorageType,
)
from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phases.collector import collect_files
from src.core.phases.local_writer import generate_backup_name, sanitize_profile_name

# ---------------------------------------------------------------------------
# Parametrized: sanitize_profile_name
# ---------------------------------------------------------------------------


class TestSanitizeProfileName:

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Simple", "Simple"),
            ("My Profile", "My_Profile"),
            ("Test/Name", "Test_Name"),
            ("Test\\Name", "Test_Name"),
            ("Test:Name", "Test_Name"),
            ('Test"Name', "Test_Name"),
            ("Test<Name>", "Test_Name_"),
            ("café", "café"),
            ("  spaces  ", "spaces"),
            ("a" * 100, "a" * 100),
        ],
        ids=[
            "simple",
            "spaces",
            "forward_slash",
            "backslash",
            "colon",
            "double_quote",
            "angle_brackets",
            "accented",
            "leading_trailing_spaces",
            "very_long",
        ],
    )
    def test_sanitize_variations(self, raw: str, expected: str):
        """Profile name sanitization handles special characters."""
        result = sanitize_profile_name(raw)
        assert result == expected


# ---------------------------------------------------------------------------
# Parametrized: generate_backup_name
# ---------------------------------------------------------------------------


class TestGenerateBackupName:

    @pytest.mark.parametrize(
        "profile_name, type_tag",
        [
            ("MyBackup", "FULL"),
            ("MyBackup", "DIFF"),
            ("Server 1", "FULL"),
            ("Test/Profile", "DIFF"),
        ],
        ids=[
            "full_simple",
            "diff_simple",
            "full_with_space",
            "diff_with_slash",
        ],
    )
    def test_name_format(self, profile_name: str, type_tag: str):
        """Backup name contains sanitized profile name and type tag."""
        name = generate_backup_name(profile_name, type_tag)
        sanitized = sanitize_profile_name(profile_name)
        assert name.startswith(f"{sanitized}_{type_tag}_")
        # Should have timestamp suffix
        parts = name.split("_")
        assert len(parts) >= 4


# ---------------------------------------------------------------------------
# Parametrized: collector with various exclude patterns
# ---------------------------------------------------------------------------


class TestCollectorExcludes:

    @pytest.mark.parametrize(
        "files_to_create, exclude_patterns, expected_count",
        [
            (["a.txt", "b.log", "c.py"], [], 3),
            (["a.txt", "b.log", "c.py"], ["*.log"], 2),
            (["a.txt", "b.log", "c.py"], ["*.log", "*.py"], 1),
            (["a.txt", "b.txt", "c.txt"], ["*.txt"], 0),
            (["a.txt", "b.log"], ["*.bak"], 2),
            (["readme.md", "notes.md", "data.csv"], ["*.md"], 1),
        ],
        ids=[
            "no_excludes",
            "exclude_logs",
            "exclude_logs_and_py",
            "exclude_all",
            "exclude_nothing_matched",
            "exclude_markdown",
        ],
    )
    def test_exclude_patterns(
        self,
        tmp_path: Path,
        files_to_create: list[str],
        exclude_patterns: list[str],
        expected_count: int,
    ):
        """Collector correctly filters files by exclude patterns."""
        for name in files_to_create:
            (tmp_path / name).write_text("content", encoding="utf-8")

        files = collect_files([str(tmp_path)], exclude_patterns)
        assert len(files) == expected_count


# ---------------------------------------------------------------------------
# Parametrized: compute_sha256 with various contents
# ---------------------------------------------------------------------------


class TestComputeSha256Parametrized:

    @pytest.mark.parametrize(
        "content",
        [
            b"",
            b"hello",
            b"a" * 10000,
            b"\x00\xff\xfe\xfd",
            b"line1\nline2\nline3",
        ],
        ids=[
            "empty",
            "short_text",
            "large_repeated",
            "binary_data",
            "multiline",
        ],
    )
    def test_sha256_matches_hashlib(self, tmp_path: Path, content: bytes):
        """compute_sha256 matches hashlib reference implementation."""
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(content)

        result = compute_sha256(test_file)
        expected = hashlib.sha256(content).hexdigest()
        assert result == expected


# ---------------------------------------------------------------------------
# Parametrized: BackupProfile defaults
# ---------------------------------------------------------------------------


class TestBackupProfileDefaults:

    @pytest.mark.parametrize(
        "backup_type, expected_value",
        [
            (BackupType.FULL, "full"),
            (BackupType.DIFFERENTIAL, "differential"),
        ],
        ids=["full", "differential"],
    )
    def test_backup_type_values(self, backup_type: BackupType, expected_value: str):
        """BackupType enum has correct string values."""
        assert backup_type.value == expected_value


# ---------------------------------------------------------------------------
# Parametrized: StorageType values
# ---------------------------------------------------------------------------


class TestStorageTypeValues:

    @pytest.mark.parametrize(
        "storage_type, expected_value",
        [
            (StorageType.LOCAL, "local"),
            (StorageType.NETWORK, "network"),
            (StorageType.SFTP, "sftp"),
            (StorageType.S3, "s3"),
        ],
        ids=["local", "network", "sftp", "s3"],
    )
    def test_storage_type_values(self, storage_type: StorageType, expected_value: str):
        """StorageType enum has correct string values."""
        assert storage_type.value == expected_value

    @pytest.mark.parametrize(
        "storage_type, extra_kwargs, expected_remote",
        [
            (StorageType.LOCAL, {"destination_path": "/tmp"}, False),
            (
                StorageType.NETWORK,
                {"destination_path": "\\\\s\\s", "network_username": "u", "network_password": "p"},
                False,
            ),
            (StorageType.SFTP, {"sftp_host": "h", "sftp_username": "u"}, True),
            (StorageType.S3, {"s3_bucket": "b"}, True),
        ],
        ids=["local_not_remote", "network_not_remote", "sftp_remote", "s3_remote"],
    )
    def test_is_remote(self, storage_type: StorageType, extra_kwargs: dict, expected_remote: bool):
        """StorageConfig.is_remote() returns correct value per type."""
        config = StorageConfig(storage_type=storage_type, **extra_kwargs)
        assert config.is_remote() == expected_remote


# ---------------------------------------------------------------------------
# Parametrized: _raise_verify_error with different error counts
# ---------------------------------------------------------------------------


class TestRaiseVerifyErrorParametrized:

    @pytest.mark.parametrize(
        "error_count, total, expect_truncation",
        [
            (1, 5, False),
            (5, 10, False),
            (10, 20, False),
            (11, 20, True),
            (50, 100, True),
        ],
        ids=[
            "1_error",
            "5_errors",
            "10_errors_exact",
            "11_errors_truncated",
            "50_errors_truncated",
        ],
    )
    def test_error_message_formatting(
        self,
        error_count: int,
        total: int,
        expect_truncation: bool,
    ):
        """Verify error formatting with various error counts."""
        from src.core.backup_engine import BackupEngine

        errors = [f"Error {i}" for i in range(error_count)]
        with pytest.raises(RuntimeError) as exc_info:
            BackupEngine._raise_verify_error(errors, total)

        msg = str(exc_info.value)
        assert f"{error_count}/{total}" in msg
        if expect_truncation:
            assert "more" in msg
        else:
            assert "more" not in msg


# ---------------------------------------------------------------------------
# Parametrized: collector with nested directory structures
# ---------------------------------------------------------------------------


class TestCollectorNestedDirs:

    @pytest.mark.parametrize(
        "depth",
        [1, 2, 5],
        ids=["depth_1", "depth_2", "depth_5"],
    )
    def test_nested_directory_depth(self, tmp_path: Path, depth: int):
        """Collector finds files at various nesting depths."""
        current = tmp_path
        for i in range(depth):
            current = current / f"level_{i}"
            current.mkdir()
        (current / "deep_file.txt").write_text("deep", encoding="utf-8")

        files = collect_files([str(tmp_path)])
        assert len(files) == 1
        assert files[0].relative_path.endswith("deep_file.txt")
