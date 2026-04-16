"""Regression tests: SFTP methods must not close a persistent transport.

Once ``connect()`` has set up a persistent transport, the other
methods are expected to reuse it across calls.  Several methods used
to call ``transport.close()`` unconditionally, silently tearing down
the shared transport and forcing a reconnect on the next operation.
"""

import stat as stat_module
import sys
from unittest.mock import MagicMock

from src.core.phases.collector import FileInfo


def _setup_mock_paramiko():
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

    return SFTPStorage(
        host="10.0.0.1",
        port=22,
        username="user",
        password="pass",
        remote_path="/home/user/backups",
    )


def _attach_persistent(storage, mock_transport: MagicMock) -> None:
    """Inject a persistent transport so _get_transport reuses it."""
    mock_transport.is_active.return_value = True
    storage._persistent_transport = mock_transport


def _install_sftp(storage, mock_sftp: MagicMock) -> None:
    """Stub _get_sftp so tests can assert on SFTP interactions."""
    storage._get_sftp = MagicMock(return_value=mock_sftp)  # type: ignore[method-assign]


class TestPersistentTransportPreserved:
    """Every SFTP method that takes the persistent transport must leave it open."""

    def test_list_backups_does_not_close_persistent(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            mock_sftp = MagicMock()
            mock_sftp.listdir_attr.return_value = []
            _install_sftp(storage, mock_sftp)

            storage.list_backups()
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_delete_backup_does_not_close_persistent(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            attr = MagicMock()
            attr.st_mode = 0o100644  # regular file
            mock_sftp = MagicMock()
            mock_sftp.stat.return_value = attr
            _install_sftp(storage, mock_sftp)

            storage.delete_backup("Backup_FULL_2026-04-16_120000")
            # delete_backup opens the persistent transport twice (backup
            # + manifest); neither call should close it.
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_test_connection_does_not_close_persistent(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            mock_sftp = MagicMock()
            mock_sftp.listdir.return_value = []
            _install_sftp(storage, mock_sftp)

            # Stub exec-channel + free-space probes to keep this test
            # focused on transport lifetime.
            storage._check_exec_channel = MagicMock(return_value=False)  # type: ignore[method-assign]
            storage._get_free_space_from_transport = MagicMock(return_value=None)  # type: ignore[method-assign]

            storage.test_connection()
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_get_free_space_does_not_close_persistent(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            storage._check_exec_channel = MagicMock(return_value=False)  # type: ignore[method-assign]
            storage._get_free_space_from_transport = MagicMock(return_value=1_000_000)  # type: ignore[method-assign]

            assert storage.get_free_space() == 1_000_000
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_get_file_size_does_not_close_persistent(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            attr = MagicMock()
            attr.st_size = 4096
            mock_sftp = MagicMock()
            mock_sftp.stat.return_value = attr
            _install_sftp(storage, mock_sftp)

            assert storage.get_file_size("Backup_FULL_2026-04-16_120000") == 4096
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()

    def test_download_backup_does_not_close_persistent(self, tmp_path):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            mock_transport = MagicMock()
            _attach_persistent(storage, mock_transport)

            mock_sftp = MagicMock()
            mock_sftp.listdir_attr.return_value = []
            mock_sftp.get.side_effect = FileNotFoundError("no manifest")
            _install_sftp(storage, mock_sftp)

            storage.download_backup("Backup_FULL_2026-04-16_120000", tmp_path)
            mock_transport.close.assert_not_called()
        finally:
            _cleanup_paramiko()


class TestAdHocTransportStillClosed:
    """When no persistent transport exists, the ad-hoc one is closed as before."""

    def test_list_backups_closes_adhoc(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._persistent_transport = None

            mock_transport = MagicMock()
            mock_sftp = MagicMock()
            mock_sftp.listdir_attr.return_value = []
            _install_sftp(storage, mock_sftp)
            storage._create_transport = MagicMock(return_value=mock_transport)  # type: ignore[method-assign]

            storage.list_backups()
            mock_transport.close.assert_called_once()
        finally:
            _cleanup_paramiko()

    def test_get_file_size_closes_adhoc(self):
        _setup_mock_paramiko()
        try:
            storage = _make_storage()
            storage._persistent_transport = None

            mock_transport = MagicMock()
            attr = MagicMock()
            attr.st_size = 4096
            mock_sftp = MagicMock()
            mock_sftp.stat.return_value = attr
            _install_sftp(storage, mock_sftp)
            storage._create_transport = MagicMock(return_value=mock_transport)  # type: ignore[method-assign]

            storage.get_file_size("Backup_FULL_2026-04-16_120000")
            mock_transport.close.assert_called_once()
        finally:
            _cleanup_paramiko()


# Silence unused-import warnings on code that is only imported to make
# the module parse identically to the production layout.
_ = FileInfo
_ = stat_module
