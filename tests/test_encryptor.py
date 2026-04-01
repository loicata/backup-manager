"""Tests for the encryptor pipeline phase (.tar.wbenc format)."""

import tarfile

import pytest

from src.core.events import EventBus
from src.core.phases.encryptor import encrypt_backup
from src.security.encryption import DecryptingReader


@pytest.fixture
def backup_dir(tmp_path):
    """Create a backup directory with sample files."""
    d = tmp_path / "backup"
    d.mkdir()
    (d / "file1.txt").write_text("Content one", encoding="utf-8")
    (d / "file2.txt").write_text("Content two", encoding="utf-8")
    sub = d / "subdir"
    sub.mkdir()
    (sub / "file3.txt").write_text("Content three", encoding="utf-8")
    return d


class TestEncryptBackupDirectory:
    """Test directory encryption into .tar.wbenc archive."""

    def test_produces_tar_wbenc_archive(self, backup_dir):
        """encrypt_backup creates a .tar.wbenc file and removes the directory."""
        result = encrypt_backup(backup_dir, "test_password_123!")

        assert result.suffix == ".wbenc"
        assert result.name == "backup.tar.wbenc"
        assert result.exists()
        # Original directory should be removed
        assert not backup_dir.exists()

    def test_archive_contains_all_files(self, backup_dir):
        """The .tar.wbenc archive contains all original files."""
        result = encrypt_backup(backup_dir, "test_password_123!")

        with open(result, "rb") as f:
            reader = DecryptingReader(f, "test_password_123!")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                names = sorted(m.name for m in tar)

        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "subdir/file3.txt" in names

    def test_archive_content_matches(self, backup_dir):
        """Decrypted archive content matches original files."""
        result = encrypt_backup(backup_dir, "test_password_123!")

        with open(result, "rb") as f:
            reader = DecryptingReader(f, "test_password_123!")
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                for member in tar:
                    extracted = tar.extractfile(member)
                    if extracted and member.name == "file1.txt":
                        assert extracted.read() == b"Content one"

    def test_emits_progress_events(self, backup_dir):
        """Progress events should be emitted for each file."""
        events = EventBus()
        progress_calls = []
        events.subscribe("progress", lambda **kw: progress_calls.append(kw))

        encrypt_backup(backup_dir, "test_password_123!", events)
        assert len(progress_calls) == 3
        assert all(p["phase"] == "encryption" for p in progress_calls)

    def test_emits_log_events(self, backup_dir):
        """Log events should be emitted."""
        events = EventBus()
        log_msgs = []
        events.subscribe("log", lambda message="", **kw: log_msgs.append(message))

        encrypt_backup(backup_dir, "test_password_123!", events)
        assert any("Encrypting" in m for m in log_msgs)
        assert any("complete" in m.lower() for m in log_msgs)


class TestEncryptBackupRejectsNonDirectory:
    """Test that encrypt_backup rejects non-directory inputs."""

    def test_file_raises_value_error(self, tmp_path):
        """encrypt_backup on a file should raise ValueError."""
        filepath = tmp_path / "backup.zip"
        filepath.write_bytes(b"PK\x03\x04" + b"\x00" * 100)

        with pytest.raises(ValueError, match="Expected a directory"):
            encrypt_backup(filepath, "test_password_123!")
