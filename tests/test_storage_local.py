"""Tests for src.storage.local — LocalStorage."""

import io
from pathlib import Path

import pytest

from src.storage.local import LocalStorage


class TestLocalStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        dest = tmp_path / "backups"
        dest.mkdir()
        return LocalStorage(str(dest))

    def test_upload_file(self, storage, tmp_path):
        src = tmp_path / "test.txt"
        src.write_text("hello", encoding="utf-8")
        storage.upload(src, "test.txt")
        assert (Path(storage._dest) / "test.txt").read_text(encoding="utf-8") == "hello"

    def test_upload_directory(self, storage, tmp_path):
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        (src_dir / "a.txt").write_text("aaa", encoding="utf-8")
        sub = src_dir / "sub"
        sub.mkdir()
        (sub / "b.txt").write_text("bbb", encoding="utf-8")

        storage.upload(src_dir, "my_backup")
        assert (Path(storage._dest) / "my_backup" / "a.txt").exists()
        assert (Path(storage._dest) / "my_backup" / "sub" / "b.txt").exists()

    def test_upload_file_streaming(self, storage):
        data = io.BytesIO(b"streaming content")
        storage.upload_file(data, "streamed/file.txt", size=17)
        assert (Path(storage._dest) / "streamed" / "file.txt").read_bytes() == b"streaming content"

    def test_list_backups_empty(self, storage):
        assert storage.list_backups() == []

    def test_list_backups_with_files(self, storage, tmp_path):
        src = tmp_path / "file.txt"
        src.write_text("data", encoding="utf-8")
        storage.upload(src, "backup1.txt")

        backups = storage.list_backups()
        assert len(backups) == 1
        assert backups[0]["name"] == "backup1.txt"
        assert backups[0]["size"] == 4

    def test_list_backups_sorted_newest_first(self, storage, tmp_path):
        import time

        for name in ["old.txt", "new.txt"]:
            src = tmp_path / name
            src.write_text(name, encoding="utf-8")
            storage.upload(src, name)
            time.sleep(0.1)

        backups = storage.list_backups()
        assert backups[0]["name"] == "new.txt"

    def test_delete_file(self, storage, tmp_path):
        src = tmp_path / "to_delete.txt"
        src.write_text("bye", encoding="utf-8")
        storage.upload(src, "to_delete.txt")
        storage.delete_backup("to_delete.txt")
        assert not (Path(storage._dest) / "to_delete.txt").exists()

    def test_delete_directory(self, storage, tmp_path):
        src_dir = tmp_path / "dir_backup"
        src_dir.mkdir()
        (src_dir / "file.txt").write_text("data", encoding="utf-8")
        storage.upload(src_dir, "dir_backup")
        storage.delete_backup("dir_backup")
        assert not (Path(storage._dest) / "dir_backup").exists()

    def test_delete_also_removes_wbverify(self, storage, tmp_path):
        src = tmp_path / "data.txt"
        src.write_text("content", encoding="utf-8")
        storage.upload(src, "my_backup")
        # Create an associated .wbverify manifest
        verify_file = Path(storage._dest) / "my_backup.wbverify"
        verify_file.write_text("{}", encoding="utf-8")
        storage.delete_backup("my_backup")
        assert not (Path(storage._dest) / "my_backup").exists()
        assert not verify_file.exists()

    def test_delete_without_wbverify_no_error(self, storage, tmp_path):
        src = tmp_path / "data.txt"
        src.write_text("content", encoding="utf-8")
        storage.upload(src, "solo_backup")
        storage.delete_backup("solo_backup")
        assert not (Path(storage._dest) / "solo_backup").exists()

    def test_delete_nonexistent_raises(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.delete_backup("nonexistent")

    def test_test_connection_ok(self, storage):
        ok, msg = storage.test_connection()
        assert ok is True
        assert "Connected" in msg

    def test_test_connection_missing_path(self, tmp_path):
        storage = LocalStorage(str(tmp_path / "nonexistent"))
        ok, msg = storage.test_connection()
        assert ok is False

    def test_get_free_space(self, storage):
        free = storage.get_free_space()
        assert free is not None
        assert free > 0

    def test_get_file_size(self, storage, tmp_path):
        src = tmp_path / "sized.txt"
        src.write_text("12345", encoding="utf-8")
        storage.upload(src, "sized.txt")
        assert storage.get_file_size("sized.txt") == 5

    def test_get_file_size_nonexistent(self, storage):
        assert storage.get_file_size("nope") is None

    def test_bandwidth_throttled_upload(self, storage, tmp_path):
        storage.set_bandwidth_limit(1000)  # 1000 KB/s
        src = tmp_path / "throttled.txt"
        src.write_text("data" * 100, encoding="utf-8")
        storage.upload(src, "throttled.txt")
        assert (Path(storage._dest) / "throttled.txt").exists()

    def test_progress_callback(self, storage):
        progress = []
        storage.set_progress_callback(lambda sent, total: progress.append((sent, total)))
        data = io.BytesIO(b"x" * 100)
        storage.upload_file(data, "progress.bin", size=100)
        assert len(progress) > 0
        assert progress[-1][0] == 100

    def test_hidden_files_excluded_from_list(self, storage):
        (Path(storage._dest) / ".hidden").write_text("hidden", encoding="utf-8")
        (Path(storage._dest) / "visible.txt").write_text("visible", encoding="utf-8")
        backups = storage.list_backups()
        names = [b["name"] for b in backups]
        assert "visible.txt" in names
        assert ".hidden" not in names
