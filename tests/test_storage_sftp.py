"""Tests for src.storage.sftp — SFTPStorage.

Uses mock paramiko injected via sys.modules since paramiko
may not be installed in the test environment.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from src.storage.sftp import _shell_escape, _validate_remote_name


class TestValidateRemoteName:
    def test_valid_name(self):
        assert _validate_remote_name("backup_2026") == "backup_2026"

    def test_strips_leading_slash(self):
        assert _validate_remote_name("/backup") == "backup"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_remote_name("")

    def test_null_byte_raises(self):
        with pytest.raises(ValueError, match="null"):
            _validate_remote_name("bad\x00name")

    def test_path_traversal_raises(self):
        with pytest.raises(ValueError, match="traversal"):
            _validate_remote_name("../etc/passwd")

    def test_nested_path_ok(self):
        assert _validate_remote_name("2026/03/backup.zip") == "2026/03/backup.zip"


class TestShellEscape:
    def test_simple_string(self):
        assert _shell_escape("hello") == "'hello'"

    def test_string_with_spaces(self):
        assert _shell_escape("hello world") == "'hello world'"

    def test_string_with_quotes(self):
        result = _shell_escape("it's")
        assert "'" in result
        assert "\\" in result

    def test_empty_string(self):
        assert _shell_escape("") == "''"


class TestSFTPStorageWithMock:
    """Tests using mocked paramiko."""

    def _setup_mock_paramiko(self):
        """Create and inject mock paramiko into sys.modules."""
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

        sys.modules["paramiko"] = mp
        sys.modules["paramiko.hostkeys"] = MagicMock()
        return mp

    def _cleanup_paramiko(self):
        sys.modules.pop("paramiko", None)
        sys.modules.pop("paramiko.hostkeys", None)

    def _make_storage(self):
        from src.storage.sftp import SFTPStorage

        storage = SFTPStorage(
            host="192.168.1.100",
            port=22,
            username="testuser",
            password="testpass",
            remote_path="/home/testuser/backups",
        )
        # Skip host key verification in unit tests (mocked transport)
        storage._verify_host_key = lambda transport: None
        return storage

    def test_get_transport_password_auth(self):
        mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                transport = storage._get_transport()
            # settimeout called twice: 60s for connect, then 600s for operations
            assert mock_sock.settimeout.call_count == 2
            mock_sock.settimeout.assert_any_call(60)
            mock_sock.settimeout.assert_any_call(600)
            mock_sock.connect.assert_called_once_with(("192.168.1.100", 22))
            mp.Transport.assert_called_once_with(mock_sock)
            transport.connect.assert_called_once_with(username="testuser", password="testpass")
        finally:
            self._cleanup_paramiko()

    def test_get_transport_key_auth(self):
        mp = self._setup_mock_paramiko()
        try:
            from src.storage.sftp import SFTPStorage

            storage = SFTPStorage(
                host="host",
                username="user",
                key_path="/path/to/key",
            )
            storage._verify_host_key = lambda transport: None
            mp.Ed25519Key.from_private_key_file.return_value = "mock_key"
            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                transport = storage._get_transport()
            transport.connect.assert_called_once()
            assert "pkey" in str(transport.connect.call_args)
        finally:
            self._cleanup_paramiko()

    def test_check_exec_channel_available(self):
        _mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            transport = MagicMock()
            channel = MagicMock()
            transport.open_session.return_value = channel
            channel.recv.return_value = b"ok\n"
            channel.recv_exit_status.return_value = 0

            assert storage._check_exec_channel(transport) is True
            assert storage._exec_available is True
        finally:
            self._cleanup_paramiko()

    def test_check_exec_channel_restricted(self):
        _mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            transport = MagicMock()
            channel = MagicMock()
            transport.open_session.return_value = channel
            channel.recv.return_value = b""
            channel.recv_exit_status.return_value = 1

            assert storage._check_exec_channel(transport) is False
        finally:
            self._cleanup_paramiko()

    def test_check_exec_channel_cached(self):
        _mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            storage._exec_available = True
            transport = MagicMock()
            assert storage._check_exec_channel(transport) is True
            transport.open_session.assert_not_called()
        finally:
            self._cleanup_paramiko()

    def test_check_exec_channel_exception(self):
        _mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            transport = MagicMock()
            transport.open_session.side_effect = Exception("SSH error")
            assert storage._check_exec_channel(transport) is False
        finally:
            self._cleanup_paramiko()

    def test_test_connection_success(self):
        mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            mock_transport = MagicMock()
            mock_sftp = MagicMock()
            mock_channel = MagicMock()

            mp.Transport.return_value = mock_transport
            mp.SFTPClient.from_transport.return_value = mock_sftp

            mock_transport.open_session.return_value = mock_channel
            mock_channel.recv.return_value = b"ok\n"
            mock_channel.recv_exit_status.return_value = 0

            vfs = MagicMock()
            vfs.f_bavail = 1000000
            vfs.f_frsize = 4096
            mock_sftp.statvfs.return_value = vfs

            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                ok, msg = storage.test_connection()
            assert ok is True
            assert "SFTP connected" in msg
        finally:
            self._cleanup_paramiko()

    def test_test_connection_path_not_found(self):
        mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            mock_transport = MagicMock()
            mock_sftp = MagicMock()

            mp.Transport.return_value = mock_transport
            mp.SFTPClient.from_transport.return_value = mock_sftp
            mock_sftp.listdir.side_effect = FileNotFoundError

            mock_sock = MagicMock()
            with patch("src.storage.sftp.socket.socket", return_value=mock_sock):
                ok, msg = storage.test_connection()
            assert ok is False
            assert "not found" in msg
        finally:
            self._cleanup_paramiko()

    def test_join_remote(self):
        _mp = self._setup_mock_paramiko()
        try:
            storage = self._make_storage()
            assert storage._join_remote("backup1") == "/home/testuser/backups/backup1"
        finally:
            self._cleanup_paramiko()
