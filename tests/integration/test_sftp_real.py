"""Real SFTP integration tests against a live SSH server.

Requires:
- SSH server at SFTP_TEST_HOST with key auth
- Test key at ~/.ssh/test_key (no passphrase)
- Remote directory ~/backups/backup_test writable

These tests create and clean up their own subdirectory under the remote path.
"""

import os
import uuid
from pathlib import Path

import pytest

try:
    import paramiko

    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False

from src.storage.sftp import SFTPStorage

# --- Test configuration ---
SFTP_HOST = "192.168.3.243"
SFTP_PORT = 22
SFTP_USER = "cipango56"
SFTP_KEY_PATH = str(Path.home() / ".ssh" / "test_key")
SFTP_REMOTE_BASE = "/home/cipango56/backups/backup_test"


def _can_connect() -> bool:
    """Quick probe to check if SSH server is reachable."""
    if not HAS_PARAMIKO:
        return False
    if not Path(SFTP_KEY_PATH).exists():
        return False
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(
            username=SFTP_USER,
            pkey=paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH),
        )
        transport.close()
        return True
    except Exception:
        return False


CAN_CONNECT = _can_connect()
pytestmark = pytest.mark.skipif(
    not CAN_CONNECT,
    reason=f"Cannot connect to SFTP server at {SFTP_HOST}",
)


@pytest.fixture
def sftp_backend():
    """Create a real SFTPStorage backend with a unique test subdirectory."""
    test_id = uuid.uuid4().hex[:8]
    remote_path = f"{SFTP_REMOTE_BASE}/{test_id}"

    backend = SFTPStorage(
        host=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USER,
        key_path=SFTP_KEY_PATH,
        remote_path=remote_path,
    )

    # Create remote test directory
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(
        username=SFTP_USER,
        pkey=paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH),
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        sftp.mkdir(remote_path)
    except OSError:
        pass
    sftp.close()
    transport.close()

    yield backend

    # Cleanup: remove test directory
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(
        username=SFTP_USER,
        pkey=paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH),
    )
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        _recursive_rm(sftp, remote_path)
    except Exception:
        pass
    sftp.close()
    transport.close()


def _recursive_rm(sftp, path: str) -> None:
    """Recursively remove a remote directory."""
    import stat as stat_mod

    for entry in sftp.listdir_attr(path):
        full = f"{path}/{entry.filename}"
        if stat_mod.S_ISDIR(entry.st_mode):
            _recursive_rm(sftp, full)
        else:
            sftp.remove(full)
    sftp.rmdir(path)


@pytest.fixture
def sample_file(tmp_path):
    """Create a local sample file."""
    f = tmp_path / "test_upload.txt"
    f.write_text("Hello from SFTP integration test!", encoding="utf-8")
    return f


@pytest.fixture
def sample_dir(tmp_path):
    """Create a local sample directory."""
    d = tmp_path / "test_dir"
    d.mkdir()
    (d / "file_a.txt").write_text("Content A", encoding="utf-8")
    sub = d / "sub"
    sub.mkdir()
    (sub / "file_b.txt").write_text("Content B", encoding="utf-8")
    return d


class TestSFTPConnection:
    """Test connection and probing."""

    def test_test_connection_success(self, sftp_backend):
        """Should connect and report success."""
        ok, msg = sftp_backend.test_connection()
        assert ok is True
        assert SFTP_USER in msg
        assert SFTP_HOST in msg

    def test_test_connection_wrong_host(self):
        """Connection to invalid host should fail."""
        backend = SFTPStorage(
            host="192.168.255.254",
            username="nobody",
            password="wrong",
            remote_path="/tmp",
        )
        ok, msg = backend.test_connection()
        assert ok is False
        assert "failed" in msg.lower() or "Error" in msg

    def test_exec_channel_detection(self, sftp_backend):
        """Should detect exec channel availability."""
        transport = sftp_backend._get_transport()
        try:
            result = sftp_backend._check_exec_channel(transport)
            assert isinstance(result, bool)
        finally:
            transport.close()


class TestSFTPUpload:
    """Test file upload operations."""

    def test_upload_single_file(self, sftp_backend, sample_file):
        """Upload a single file and verify it appears in listing."""
        sftp_backend.upload(sample_file, "uploaded.txt")
        backups = sftp_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "uploaded.txt" in names

    def test_upload_directory(self, sftp_backend, sample_dir):
        """Upload a directory and verify its structure."""
        sftp_backend.upload(sample_dir, "my_dir")
        backups = sftp_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "my_dir" in names

    def test_upload_file_stream(self, sftp_backend):
        """Upload via file-like object (streaming)."""
        import io

        data = b"Streamed SFTP content"
        fileobj = io.BytesIO(data)
        sftp_backend.upload_file(fileobj, "streamed.bin", size=len(data))

        size = sftp_backend.get_file_size("streamed.bin")
        assert size == len(data)

    def test_upload_with_progress(self, sftp_backend, sample_file):
        """Upload with progress callback tracking."""
        progress_calls = []
        sftp_backend.set_progress_callback(lambda sent, total: progress_calls.append((sent, total)))
        sftp_backend.upload(sample_file, "progress_test.txt")
        assert len(progress_calls) > 0


class TestSFTPList:
    """Test backup listing."""

    def test_list_empty(self, sftp_backend):
        """Empty directory should return empty list."""
        backups = sftp_backend.list_backups()
        assert backups == []

    def test_list_after_upload(self, sftp_backend, sample_file):
        """Uploaded files should appear in listing."""
        sftp_backend.upload(sample_file, "listed.txt")
        backups = sftp_backend.list_backups()
        assert len(backups) == 1
        assert backups[0]["name"] == "listed.txt"
        assert backups[0]["size"] > 0


class TestSFTPDelete:
    """Test backup deletion."""

    def test_delete_file(self, sftp_backend, sample_file):
        """Delete a single file."""
        sftp_backend.upload(sample_file, "to_delete.txt")
        sftp_backend.delete_backup("to_delete.txt")
        backups = sftp_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "to_delete.txt" not in names

    def test_delete_directory(self, sftp_backend, sample_dir):
        """Delete a directory recursively."""
        sftp_backend.upload(sample_dir, "dir_to_del")
        sftp_backend.delete_backup("dir_to_del")
        backups = sftp_backend.list_backups()
        names = [b["name"] for b in backups]
        assert "dir_to_del" not in names

    def test_delete_nonexistent(self, sftp_backend):
        """Deleting non-existent backup should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            sftp_backend.delete_backup("ghost")


class TestSFTPMisc:
    """Test utility methods."""

    def test_get_free_space(self, sftp_backend):
        """Free space should be a positive integer or None."""
        space = sftp_backend.get_free_space()
        if space is not None:
            assert space > 0

    def test_get_file_size(self, sftp_backend, sample_file):
        """Get size of uploaded file."""
        sftp_backend.upload(sample_file, "sized.txt")
        size = sftp_backend.get_file_size("sized.txt")
        assert size == sample_file.stat().st_size

    def test_get_file_size_missing(self, sftp_backend):
        """Non-existent file should return None."""
        assert sftp_backend.get_file_size("nope.txt") is None
