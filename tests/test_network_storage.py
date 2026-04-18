"""Tests for NetworkStorage backend with SMB credential support."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from src.storage.network import NetworkStorage, _extract_share_root


class TestExtractShareRoot:
    """Tests for UNC share root extraction."""

    def test_simple_unc(self):
        assert _extract_share_root(r"\\server\share") == r"\\server\share"

    def test_unc_with_subfolder(self):
        assert _extract_share_root(r"\\server\share\sub\folder") == r"\\server\share"

    def test_forward_slashes(self):
        assert _extract_share_root("//server/share/sub") == r"\\server\share"

    def test_trailing_backslash(self):
        assert _extract_share_root(r"\\server\share\\") == r"\\server\share"

    def test_not_unc_raises(self):
        with pytest.raises(ValueError, match="Not a UNC path"):
            _extract_share_root("C:\\folder")

    def test_incomplete_unc_raises(self):
        with pytest.raises(ValueError, match="Incomplete UNC path"):
            _extract_share_root(r"\\server")


class TestNetworkStorageInit:
    """Tests for NetworkStorage initialization."""

    def test_with_credentials(self):
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        assert ns._username == "user"
        assert ns._password == "pass"
        assert ns._connected is False

    def test_none_credentials_treated_as_empty(self):
        ns = NetworkStorage(r"\\server\share", username=None, password=None)
        assert ns._username == ""
        assert ns._password == ""


class TestConnect:
    """Tests for _connect (net use) logic."""

    def test_missing_credentials_fails(self):
        ns = NetworkStorage(r"\\server\share")
        ok, msg = ns._connect()
        assert ok is False
        assert "required" in msg.lower()

    def test_missing_password_fails(self):
        ns = NetworkStorage(r"\\server\share", username="user")
        ok, msg = ns._connect()
        assert ok is False
        assert "required" in msg.lower()

    def test_missing_username_fails(self):
        ns = NetworkStorage(r"\\server\share", password="pass")
        ok, msg = ns._connect()
        assert ok is False
        assert "required" in msg.lower()

    @patch("src.storage.network.subprocess.run")
    def test_successful_connection(self, mock_run):
        """cmdkey /add + net use + cmdkey /delete sequence, all green."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /add
            MagicMock(
                returncode=0, stdout="The command completed successfully.", stderr=""
            ),  # net use
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /delete
        ]
        ns = NetworkStorage(r"\\server\share\backups", username="admin", password="secret")
        ok, msg = ns._connect()

        assert ok is True
        assert ns._connected is True

        # Three subprocess.run calls: cmdkey /add, net use, cmdkey /delete
        assert mock_run.call_count == 3

        add_cmd = mock_run.call_args_list[0][0][0]
        net_cmd = mock_run.call_args_list[1][0][0]
        del_cmd = mock_run.call_args_list[2][0][0]

        # Step 1: cmdkey /add carries the password in argv ONLY for this
        # single invocation. That is the narrow window we accepted in
        # exchange for fixing the stdin-pipe timeout.
        assert add_cmd[0] == "cmdkey"
        assert "/add:server" in add_cmd
        assert "/user:admin" in add_cmd
        assert "/pass:secret" in add_cmd

        # Step 2: net use has NO password and NO /user — Credential
        # Manager provides the identity silently.
        assert net_cmd[:3] == ["net", "use", r"\\server\share"]
        assert "secret" not in net_cmd
        assert not any(arg.startswith("/user:") for arg in net_cmd)
        assert "*" not in net_cmd

        # Step 3: always clean up the cached credential.
        assert del_cmd == ["cmdkey", "/delete:server"]

    @patch("src.storage.network.subprocess.run")
    def test_already_connected_error_1219(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /add
            MagicMock(returncode=2, stdout="", stderr="System error 1219 has occurred."),  # net use
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /delete
        ]
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ok, msg = ns._connect()

        assert ok is True
        assert "Already connected" in msg
        assert ns._connected is False  # We didn't create the connection

    @patch("src.storage.network.subprocess.run")
    def test_access_denied(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /add
            MagicMock(returncode=1, stdout="", stderr="Access is denied."),  # net use
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /delete
        ]
        ns = NetworkStorage(r"\\server\share", username="user", password="wrong")
        ok, msg = ns._connect()

        assert ok is False
        assert "Access is denied" in msg

    @patch("src.storage.network.subprocess.run")
    def test_timeout(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /add succeeds
            subprocess.TimeoutExpired(cmd="net use", timeout=15),  # net use hangs
            MagicMock(returncode=0, stdout="", stderr=""),  # cmdkey /delete cleanup
        ]
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ok, msg = ns._connect()

        assert ok is False
        assert "timeout" in msg.lower()

    @patch("src.storage.network.subprocess.run")
    def test_command_not_found(self, mock_run):
        # cmdkey missing = not a Windows system; fail fast before net use.
        mock_run.side_effect = FileNotFoundError("cmdkey not found")
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ok, msg = ns._connect()

        assert ok is False
        assert "not available" in msg or "not a Windows system" in msg.lower()


class TestDisconnect:
    """Tests for _disconnect cleanup logic."""

    @patch("src.storage.network.subprocess.run")
    def test_disconnect_after_connect(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ns._connected = True

        ns._disconnect()

        assert ns._connected is False
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "/delete" in cmd

    def test_disconnect_without_connect_is_noop(self):
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ns._disconnect()  # Should not raise
        assert ns._connected is False

    @patch("src.storage.network.subprocess.run")
    def test_close_calls_disconnect(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        ns = NetworkStorage(r"\\server\share", username="user", password="pass")
        ns._connected = True

        ns.close()

        assert ns._connected is False


class TestTestConnection:
    """Tests for test_connection with credentials."""

    def test_missing_credentials_returns_error(self):
        ns = NetworkStorage(r"\\server\share")
        ok, msg = ns.test_connection()
        assert ok is False
        assert "required" in msg.lower()

    @patch("src.storage.network.subprocess.run")
    def test_auth_failure_returns_error(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Logon failure")
        ns = NetworkStorage(r"\\server\share", username="user", password="bad")
        ok, msg = ns.test_connection()
        assert ok is False
        assert "Logon failure" in msg

    @patch.object(NetworkStorage, "_connect", return_value=(True, "Connected"))
    def test_path_not_found_after_auth(self, mock_connect):
        ns = NetworkStorage(r"\\nonexistent\share", username="u", password="p")
        ok, msg = ns.test_connection()
        assert ok is False
        assert "not found" in msg.lower() or "timeout" in msg.lower()


class TestUploadWithAuth:
    """Tests for upload methods requiring authentication."""

    def test_upload_fails_without_credentials(self):
        ns = NetworkStorage(r"\\server\share")
        with pytest.raises(OSError, match="Cannot connect"):
            ns.upload("/some/path")

    @patch.object(NetworkStorage, "_connect", return_value=(False, "Access denied"))
    def test_upload_fails_if_connect_fails(self, mock_connect):
        ns = NetworkStorage(r"\\server\share", username="user", password="bad")
        with pytest.raises(OSError, match="Cannot connect"):
            ns.upload("/some/path")

    @patch.object(NetworkStorage, "_connect", return_value=(False, "Access denied"))
    def test_upload_file_fails_if_connect_fails(self, mock_connect):
        ns = NetworkStorage(r"\\server\share", username="user", password="bad")
        with pytest.raises(OSError, match="Cannot connect"):
            ns.upload_file(MagicMock(), "remote/path")
