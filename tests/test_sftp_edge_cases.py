"""Additional edge-case tests for src.storage.sftp — SFTPStorage.

Covers security validation, connection errors, fallback, progress,
and delete operations NOT already present in test_storage_sftp.py.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from src.storage.sftp import _validate_remote_name

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_mock_paramiko():
    """Inject a mock paramiko into sys.modules and return it."""
    mp = MagicMock()
    mp.Ed25519Key = MagicMock()
    mp.Ed25519Key.__name__ = "Ed25519Key"
    mp.ECDSAKey = MagicMock()
    mp.ECDSAKey.__name__ = "ECDSAKey"
    mp.RSAKey = MagicMock()
    mp.RSAKey.__name__ = "RSAKey"
    mp.Transport = MagicMock()
    mp.SFTPClient = MagicMock()
    mp.HostKeys = MagicMock
    mp.AuthenticationException = type("AuthenticationException", (Exception,), {})
    mp.SSHException = type("SSHException", (Exception,), {})
    sys.modules["paramiko"] = mp
    sys.modules["paramiko.hostkeys"] = MagicMock()
    return mp


def _cleanup_paramiko():
    sys.modules.pop("paramiko", None)
    sys.modules.pop("paramiko.hostkeys", None)


def _make_storage():
    from src.storage.sftp import SFTPStorage

    storage = SFTPStorage(
        host="10.0.0.1",
        port=22,
        username="user",
        password="pass",
        remote_path="/backups",
    )
    storage._verify_host_key = lambda t: None
    return storage


# ---------------------------------------------------------------------------
# 1-3  Path traversal / shell injection / null bytes
# ---------------------------------------------------------------------------


class TestRemoteNameSecurity:
    """Reject dangerous remote names."""

    @pytest.mark.parametrize(
        "name",
        [
            "../etc/passwd",
            "foo/../bar",
            "a/../../b",
        ],
    )
    def test_path_traversal_rejected(self, name):
        with pytest.raises(ValueError, match="traversal"):
            _validate_remote_name(name)

    @pytest.mark.parametrize(
        "name",
        [
            "/absolute/path",
            "//double",
        ],
    )
    def test_leading_slash_stripped(self, name):
        result = _validate_remote_name(name)
        assert not result.startswith("/")

    def test_backslash_allowed_not_traversal(self):
        # Backslash is unusual but not blocked by current validator
        result = _validate_remote_name("back\\slash")
        assert result == "back\\slash"

    @pytest.mark.parametrize("char", [";", "|", ">", "<", "&", "!", "`", "$"])
    def test_shell_injection_rejected(self, char):
        with pytest.raises(ValueError, match="dangerous"):
            _validate_remote_name(f"backup{char}rm -rf")

    def test_null_byte_rejected(self):
        with pytest.raises(ValueError, match="null"):
            _validate_remote_name("backup\x00evil")


# ---------------------------------------------------------------------------
# 4  Very long remote name
# ---------------------------------------------------------------------------


class TestLongRemoteName:
    def test_long_name_accepted_by_validator(self):
        """Validator does not enforce length; server will reject if needed."""
        long_name = "a" * 300
        result = _validate_remote_name(long_name)
        assert len(result) == 300


# ---------------------------------------------------------------------------
# 5  Host key changed (MITM detection)
# ---------------------------------------------------------------------------


class TestHostKeyMismatch:
    def test_host_key_changed_raises(self, tmp_path):
        mp = _setup_mock_paramiko()
        try:
            from src.storage.sftp import SFTPStorage

            storage = SFTPStorage(
                host="10.0.0.1",
                port=22,
                username="user",
                password="pass",
                remote_path="/backups",
            )

            transport = MagicMock()
            remote_key = MagicMock()
            remote_key.get_name.return_value = "ssh-ed25519"
            transport.get_remote_server_key.return_value = remote_key

            # The stored key must differ from remote_key
            stored_key = MagicMock()
            stored_key.__eq__ = lambda self, other: False
            stored_key.__ne__ = lambda self, other: True

            host_entry = MagicMock()
            host_entry.get.return_value = stored_key

            mock_hk_instance = MagicMock()
            mock_hk_instance.lookup.return_value = host_entry
            mp.HostKeys = MagicMock(return_value=mock_hk_instance)

            with patch("src.storage.sftp.Path.home", return_value=tmp_path):
                (tmp_path / ".ssh").mkdir()
                (tmp_path / ".ssh" / "known_hosts").write_text("", encoding="utf-8")
                with pytest.raises(OSError, match="Host key verification failed"):
                    storage._verify_host_key(transport)

            transport.close.assert_called_once()
        finally:
            _cleanup_paramiko()


# ---------------------------------------------------------------------------
# 6  Connection timeout
# ---------------------------------------------------------------------------


class TestConnectionTimeout:
    def test_connect_socket_timeout(self):
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mp.Transport.return_value.connect.side_effect = TimeoutError("Connection timed out")
            ok, msg = storage.test_connection()
            assert ok is False
            assert "failed" in msg.lower() or "timed out" in msg.lower()
        finally:
            _cleanup_paramiko()


# ---------------------------------------------------------------------------
# 7  Authentication failure
# ---------------------------------------------------------------------------


class TestAuthFailure:
    def test_auth_exception(self):
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mp.Transport.return_value.connect.side_effect = mp.AuthenticationException(
                "Bad password"
            )
            ok, msg = storage.test_connection()
            assert ok is False
            assert "failed" in msg.lower()
        finally:
            _cleanup_paramiko()


# ---------------------------------------------------------------------------
# 8  Exec channel unavailable — fallback to SFTP mode
# ---------------------------------------------------------------------------


class TestExecFallback:
    def test_upload_falls_back_to_sftp(self, tmp_path):
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._exec_available = False

            transport = MagicMock()
            mock_sftp = MagicMock()
            mock_remote_file = MagicMock()
            mock_sftp.open.return_value.__enter__ = lambda s: mock_remote_file
            mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)
            mp.SFTPClient.from_transport.return_value = mock_sftp

            src_file = tmp_path / "data.bin"
            src_file.write_bytes(b"hello")

            storage._upload_single_file(transport, src_file, "data.bin")

            # SFTP path was used (not exec)
            transport.open_session.assert_not_called()
            mp.SFTPClient.from_transport.assert_called_once()
        finally:
            _cleanup_paramiko()


# ---------------------------------------------------------------------------
# 9  Upload with progress callback
# ---------------------------------------------------------------------------


class TestProgressCallback:
    def test_fast_upload_calls_progress(self, tmp_path):
        _mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            cb = MagicMock()
            storage.set_progress_callback(cb)

            transport = MagicMock()
            channel = MagicMock()
            transport.open_session.return_value = channel
            channel.recv_exit_status.return_value = 0

            src_file = tmp_path / "big.bin"
            src_file.write_bytes(b"x" * 100)

            storage._fast_upload_file(transport, src_file, "/backups/big.bin")

            assert cb.call_count >= 1
            # Verify callback args: (bytes_sent, total_size)
            args = cb.call_args[0]
            assert args[0] > 0
            assert args[1] == 100
        finally:
            _cleanup_paramiko()


# ---------------------------------------------------------------------------
# 10  Delete backup — correct SFTP commands
# ---------------------------------------------------------------------------


class TestDeleteBackup:
    def test_delete_file_calls_remove(self):
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            mock_sftp = MagicMock()

            mp.Transport.return_value = mock_transport
            mp.SFTPClient.from_transport.return_value = mock_sftp

            file_stat = MagicMock()
            file_stat.st_mode = 0o100644  # Regular file
            mock_sftp.stat.return_value = file_stat

            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                storage.delete_backup("old_backup.tar")

            mock_sftp.remove.assert_called_once_with("/backups/old_backup.tar")
        finally:
            _cleanup_paramiko()

    def test_delete_nonexistent_raises(self):
        mp = _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            mock_sftp = MagicMock()

            mp.Transport.return_value = mock_transport
            mp.SFTPClient.from_transport.return_value = mock_sftp
            mock_sftp.stat.side_effect = FileNotFoundError

            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                with pytest.raises(FileNotFoundError, match="not found"):
                    storage.delete_backup("ghost")
        finally:
            _cleanup_paramiko()
