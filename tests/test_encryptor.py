"""Tests for the encryptor pipeline phase."""

import pytest

from src.core.events import EventBus
from src.core.phases.encryptor import encrypt_backup


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
    """Test directory encryption."""

    def test_encrypts_all_files(self, backup_dir):
        """All files should be replaced with .wbenc versions."""
        result = encrypt_backup(backup_dir, "test_password_123!")
        assert result == backup_dir

        enc_files = list(backup_dir.rglob("*.wbenc"))
        assert len(enc_files) == 3

        # Originals should be removed
        txt_files = list(backup_dir.rglob("*.txt"))
        assert len(txt_files) == 0

    def test_skips_already_encrypted(self, backup_dir):
        """Already .wbenc files should not be re-encrypted."""
        # Create a .wbenc file that should be skipped
        (backup_dir / "already.wbenc").write_bytes(b"encrypted data")

        encrypt_backup(backup_dir, "test_password_123!")
        # Only 3 txt files should be encrypted, not the .wbenc
        enc_files = list(backup_dir.rglob("*.wbenc"))
        assert len(enc_files) == 4  # 3 new + 1 existing

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


class TestEncryptBackupRejectsFile:
    """Test that encrypt_backup rejects single files (ZIP removed)."""

    def test_file_raises_value_error(self, tmp_path):
        """encrypt_backup on a file should raise ValueError."""
        filepath = tmp_path / "backup.zip"
        filepath.write_bytes(b"PK\x03\x04" + b"\x00" * 100)

        with pytest.raises(ValueError, match="Expected a directory"):
            encrypt_backup(filepath, "test_password_123!")
