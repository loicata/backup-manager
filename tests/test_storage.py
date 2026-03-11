"""
Tests for storage modules — LocalStorage CRUD, get_storage_backend factory,
ThrottledReader basic functionality, and with_retry decorator behavior.
"""

import io
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.core.config import StorageConfig, StorageType
from src.storage.base import (
    StorageBackend,
    ThrottledReader,
    get_storage_backend,
    with_retry,
)
from src.storage.local import LocalStorage


class TestLocalStorageCRUD(unittest.TestCase):
    """Test LocalStorage upload, list, and delete with a temp directory."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.dest_path = Path(self.tmpdir.name) / "backups"
        self.dest_path.mkdir()
        self.config = StorageConfig(
            storage_type=StorageType.LOCAL.value,
            destination_path=str(self.dest_path),
        )
        self.storage = LocalStorage(self.config)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_upload_file(self):
        with tempfile.TemporaryDirectory() as src_tmp:
            src_file = Path(src_tmp) / "test.txt"
            src_file.write_text("backup data")
            result = self.storage.upload(src_file, "test.txt")
            self.assertTrue(result)
            uploaded = self.dest_path / "test.txt"
            self.assertTrue(uploaded.exists())
            self.assertEqual(uploaded.read_text(), "backup data")

    def test_upload_directory(self):
        with tempfile.TemporaryDirectory() as src_tmp:
            src_dir = Path(src_tmp) / "mybackup"
            src_dir.mkdir()
            (src_dir / "a.txt").write_text("aaa")
            (src_dir / "b.txt").write_text("bbb")
            result = self.storage.upload(src_dir, "mybackup")
            self.assertTrue(result)
            self.assertTrue((self.dest_path / "mybackup" / "a.txt").exists())
            self.assertTrue((self.dest_path / "mybackup" / "b.txt").exists())

    def test_list_backups(self):
        (self.dest_path / "backup1.zip").write_text("data1")
        (self.dest_path / "backup2.zip").write_text("data2")
        backups = self.storage.list_backups()
        names = [b["name"] for b in backups]
        self.assertIn("backup1.zip", names)
        self.assertIn("backup2.zip", names)

    def test_list_backups_empty(self):
        backups = self.storage.list_backups()
        self.assertEqual(backups, [])

    def test_delete_backup_file(self):
        target = self.dest_path / "to_delete.zip"
        target.write_text("delete me")
        result = self.storage.delete_backup("to_delete.zip")
        self.assertTrue(result)
        self.assertFalse(target.exists())

    def test_delete_nonexistent(self):
        result = self.storage.delete_backup("nonexistent.zip")
        self.assertTrue(result)

    def test_test_connection(self):
        ok, msg = self.storage.test_connection()
        self.assertTrue(ok)

    def test_get_free_space(self):
        free = self.storage.get_free_space()
        self.assertIsNotNone(free)
        self.assertGreater(free, 0)

    def test_get_file_size(self):
        target = self.dest_path / "sized.txt"
        target.write_text("12345")
        size = self.storage.get_file_size("sized.txt")
        self.assertEqual(size, 5)

    def test_get_file_size_nonexistent(self):
        size = self.storage.get_file_size("nope.txt")
        self.assertIsNone(size)


class TestGetStorageBackendFactory(unittest.TestCase):
    """Test that get_storage_backend returns the correct class for each type."""

    def test_local_backend(self):
        config = StorageConfig(storage_type=StorageType.LOCAL.value)
        backend = get_storage_backend(config)
        self.assertIsInstance(backend, LocalStorage)

    def test_network_backend(self):
        config = StorageConfig(storage_type=StorageType.NETWORK.value)
        backend = get_storage_backend(config)
        from src.storage.network import NetworkStorage
        self.assertIsInstance(backend, NetworkStorage)

    def test_unknown_type_falls_back_to_local(self):
        config = StorageConfig(storage_type="unknown_type")
        backend = get_storage_backend(config)
        self.assertIsInstance(backend, LocalStorage)

    def test_s3_backend(self):
        config = StorageConfig(storage_type=StorageType.S3.value)
        backend = get_storage_backend(config)
        from src.storage.s3 import S3Storage
        self.assertIsInstance(backend, S3Storage)


class TestThrottledReader(unittest.TestCase):
    """Test ThrottledReader basic functionality."""

    def test_read_returns_data(self):
        data = b"Hello, throttled world!"
        file_obj = io.BytesIO(data)
        reader = ThrottledReader(file_obj, limit_kbps=1024)
        result = reader.read(len(data))
        self.assertEqual(result, data)

    def test_read_all(self):
        data = b"ABCDEF"
        file_obj = io.BytesIO(data)
        reader = ThrottledReader(file_obj, limit_kbps=1024)
        result = reader.read(-1)
        self.assertEqual(result, data)

    def test_read_empty(self):
        file_obj = io.BytesIO(b"")
        reader = ThrottledReader(file_obj, limit_kbps=100)
        result = reader.read(-1)
        self.assertEqual(result, b"")

    def test_attribute_forwarding(self):
        file_obj = io.BytesIO(b"test")
        reader = ThrottledReader(file_obj, limit_kbps=100)
        # BytesIO has a 'getvalue' method which should be forwarded
        self.assertEqual(reader.getvalue(), b"test")

    def test_zero_limit_no_throttle(self):
        """When limit is 0, read should return data without throttling."""
        data = b"X" * 10000
        file_obj = io.BytesIO(data)
        reader = ThrottledReader(file_obj, limit_kbps=0)
        result = reader.read(-1)
        self.assertEqual(result, data)


class TestWithRetryDecorator(unittest.TestCase):
    """Test with_retry decorator behavior with mocked functions."""

    def test_success_on_first_try(self):
        mock_fn = MagicMock(return_value="ok")
        decorated = with_retry(max_retries=3, base_delay=0.01)(mock_fn)
        result = decorated()
        self.assertEqual(result, "ok")
        self.assertEqual(mock_fn.call_count, 1)

    def test_success_after_retries(self):
        mock_fn = MagicMock(side_effect=[Exception("fail1"), Exception("fail2"), "ok"])
        decorated = with_retry(max_retries=3, base_delay=0.01)(mock_fn)
        result = decorated()
        self.assertEqual(result, "ok")
        self.assertEqual(mock_fn.call_count, 3)

    def test_all_retries_exhausted(self):
        mock_fn = MagicMock(side_effect=Exception("always fails"))
        decorated = with_retry(max_retries=2, base_delay=0.01)(mock_fn)
        with self.assertRaises(Exception) as ctx:
            decorated()
        self.assertIn("always fails", str(ctx.exception))
        # 1 initial + 2 retries = 3 calls
        self.assertEqual(mock_fn.call_count, 3)

    def test_preserves_function_name(self):
        def my_upload_function():
            pass
        decorated = with_retry(max_retries=1)(my_upload_function)
        self.assertEqual(decorated.__name__, "my_upload_function")


class TestStorageBackendFormatSize(unittest.TestCase):
    """Test the static format_size utility."""

    def test_bytes(self):
        self.assertEqual(StorageBackend.format_size(100), "100.0 B")

    def test_megabytes(self):
        self.assertEqual(StorageBackend.format_size(1024 * 1024), "1.0 MB")

    def test_gigabytes(self):
        self.assertEqual(StorageBackend.format_size(2 * 1024 ** 3), "2.0 GB")


if __name__ == "__main__":
    unittest.main()
