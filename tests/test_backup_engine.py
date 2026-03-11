"""
Tests for src.core.backup_engine — BackupStats formatting, file filtering
with fnmatch patterns, and basic mock backup scenarios.
"""

import fnmatch
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.core.backup_engine import BackupStats


class TestBackupStatsSizeStr(unittest.TestCase):
    """Test BackupStats.size_str formatting."""

    def setUp(self):
        self.stats = BackupStats()

    def test_bytes(self):
        self.assertEqual(self.stats.size_str(512), "512.0 B")

    def test_kilobytes(self):
        result = self.stats.size_str(2048)
        self.assertEqual(result, "2.0 KB")

    def test_megabytes(self):
        result = self.stats.size_str(5 * 1024 * 1024)
        self.assertEqual(result, "5.0 MB")

    def test_gigabytes(self):
        result = self.stats.size_str(3 * 1024 ** 3)
        self.assertEqual(result, "3.0 GB")

    def test_zero_bytes(self):
        self.assertEqual(self.stats.size_str(0), "0.0 B")


class TestBackupStatsDuration(unittest.TestCase):
    """Test BackupStats.duration_str formatting."""

    def test_duration_with_times(self):
        stats = BackupStats()
        stats.start_time = datetime(2025, 1, 1, 10, 0, 0)
        stats.end_time = datetime(2025, 1, 1, 10, 5, 30)
        self.assertEqual(stats.duration_str, "00:05:30")

    def test_duration_with_hours(self):
        stats = BackupStats()
        stats.start_time = datetime(2025, 1, 1, 10, 0, 0)
        stats.end_time = datetime(2025, 1, 1, 12, 30, 45)
        self.assertEqual(stats.duration_str, "02:30:45")

    def test_duration_no_times(self):
        stats = BackupStats()
        self.assertEqual(stats.duration_seconds, 0.0)
        self.assertEqual(stats.duration_str, "00:00:00")

    def test_compression_ratio_no_data(self):
        stats = BackupStats()
        self.assertEqual(stats.compression_ratio, 0.0)

    def test_compression_ratio_with_data(self):
        stats = BackupStats()
        stats.total_size = 1000
        stats.compressed_size = 600
        self.assertAlmostEqual(stats.compression_ratio, 40.0)


class TestFileFiltering(unittest.TestCase):
    """Test file filtering with fnmatch patterns (same logic used by BackupEngine)."""

    def setUp(self):
        self.exclude_patterns = [
            "*.tmp", "*.log", "~$*", "Thumbs.db", "desktop.ini",
            "__pycache__", ".git", "node_modules",
        ]

    def _should_exclude(self, filename):
        """Check if a filename matches any exclude pattern."""
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True
        return False

    def test_exclude_tmp_files(self):
        self.assertTrue(self._should_exclude("data.tmp"))

    def test_exclude_log_files(self):
        self.assertTrue(self._should_exclude("error.log"))

    def test_exclude_tilde_files(self):
        self.assertTrue(self._should_exclude("~$document.docx"))

    def test_exclude_thumbs_db(self):
        self.assertTrue(self._should_exclude("Thumbs.db"))

    def test_exclude_pycache(self):
        self.assertTrue(self._should_exclude("__pycache__"))

    def test_exclude_git(self):
        self.assertTrue(self._should_exclude(".git"))

    def test_allow_normal_file(self):
        self.assertFalse(self._should_exclude("report.pdf"))

    def test_allow_python_file(self):
        self.assertFalse(self._should_exclude("main.py"))

    def test_allow_zip_file(self):
        self.assertFalse(self._should_exclude("backup.zip"))


class TestBackupStatsMockScenario(unittest.TestCase):
    """Mock a simple backup scenario and verify stats accumulation."""

    def test_stats_accumulation(self):
        stats = BackupStats(
            profile_name="Daily Backup",
            backup_type="full",
            start_time=datetime(2025, 6, 1, 2, 0, 0),
        )

        # Simulate copying files
        files = [("a.txt", 1024), ("b.txt", 2048), ("c.txt", 512)]
        for name, size in files:
            stats.files_copied += 1
            stats.total_size += size

        stats.total_files = 3
        stats.end_time = datetime(2025, 6, 1, 2, 1, 30)

        self.assertEqual(stats.files_copied, 3)
        self.assertEqual(stats.total_size, 3584)
        self.assertEqual(stats.duration_str, "00:01:30")
        self.assertEqual(stats.profile_name, "Daily Backup")

    def test_stats_with_errors(self):
        stats = BackupStats()
        stats.files_copied = 8
        stats.files_failed = 2
        stats.total_files = 10
        stats.errors.append("Permission denied: secret.dat")
        stats.errors.append("File not found: missing.txt")

        self.assertEqual(len(stats.errors), 2)
        self.assertEqual(stats.files_failed, 2)


if __name__ == "__main__":
    unittest.main()
