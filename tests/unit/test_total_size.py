"""Tests for GeneralTab total size calculation and formatting."""

import os
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ui.tabs.general_tab import GeneralTab


class TestFormatSize:
    """Tests for GeneralTab._format_size static method."""

    def test_zero_bytes(self):
        assert GeneralTab._format_size(0) == "0 B"

    def test_negative_bytes(self):
        assert GeneralTab._format_size(-100) == "0 B"

    def test_bytes(self):
        assert GeneralTab._format_size(500) == "500 B"

    def test_one_kb(self):
        result = GeneralTab._format_size(1024)
        assert result == "1.00 KB"

    def test_megabytes(self):
        result = GeneralTab._format_size(1_500_000)
        assert "MB" in result

    def test_gigabytes(self):
        result = GeneralTab._format_size(2_500_000_000)
        assert "GB" in result

    def test_terabytes(self):
        result = GeneralTab._format_size(1_500_000_000_000)
        assert "TB" in result

    def test_exact_values(self):
        assert GeneralTab._format_size(1024 * 1024) == "1.00 MB"
        assert GeneralTab._format_size(1024 * 1024 * 1024) == "1.00 GB"


class TestCalculateDirSize:
    """Tests for GeneralTab._calculate_dir_size static method."""

    def test_nonexistent_path(self, tmp_path):
        cancel = threading.Event()
        result = GeneralTab._calculate_dir_size(tmp_path / "nonexistent", cancel)
        assert result == 0

    def test_single_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        cancel = threading.Event()
        result = GeneralTab._calculate_dir_size(f, cancel)
        assert result == f.stat().st_size

    def test_directory_with_files(self, tmp_path):
        (tmp_path / "a.txt").write_bytes(b"x" * 100)
        (tmp_path / "b.txt").write_bytes(b"y" * 200)
        cancel = threading.Event()
        result = GeneralTab._calculate_dir_size(tmp_path, cancel)
        assert result == 300

    def test_nested_directories(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (tmp_path / "a.txt").write_bytes(b"x" * 100)
        (sub / "b.txt").write_bytes(b"y" * 200)
        cancel = threading.Event()
        result = GeneralTab._calculate_dir_size(tmp_path, cancel)
        assert result == 300

    def test_empty_directory(self, tmp_path):
        cancel = threading.Event()
        result = GeneralTab._calculate_dir_size(tmp_path, cancel)
        assert result == 0

    def test_cancel_stops_calculation(self, tmp_path):
        for i in range(10):
            (tmp_path / f"file_{i}.txt").write_bytes(b"x" * 100)
        cancel = threading.Event()
        cancel.set()  # Already cancelled
        result = GeneralTab._calculate_dir_size(tmp_path, cancel)
        assert result == 0

    def test_permission_error_handled(self, tmp_path):
        cancel = threading.Event()
        with patch("os.scandir", side_effect=OSError("Permission denied")):
            result = GeneralTab._calculate_dir_size(tmp_path, cancel)
        assert result == 0
