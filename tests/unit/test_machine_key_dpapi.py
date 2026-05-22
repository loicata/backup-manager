"""Tests for ``encryption._protect_machine_key`` strict DPAPI handling.

Companion to ``test_integrity_check_dpapi.py`` — covers the SECOND
key on disk (``machine_key.bin``, used by AES password storage) which
shares the strict-DPAPI contract and the plaintext-fallback opt-in
flag with ``.integrity_key``. Without these tests a regression on the
machine-key writer would silently re-introduce the plaintext failure
mode that the audit aimed to close.
"""

from __future__ import annotations

import sys

import pytest

from src.core.exceptions import DPAPIUnavailableError
from src.security import encryption, integrity_check


@pytest.fixture(autouse=True)
def _reset_plaintext_flag():
    """Force the module-wide plaintext flag off around every test."""
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False
    yield
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False


@pytest.fixture
def isolated_appdata(tmp_path, monkeypatch):
    """Point ``%APPDATA%`` at a fresh temp dir so each test starts clean."""
    monkeypatch.setenv("APPDATA", str(tmp_path))
    yield tmp_path


KEY_32 = b"\xaa" * 32


class TestProtectMachineKeyNonWindows:
    """On Linux/macOS DPAPI does not exist — clear key is the only option."""

    def test_returns_raw_on_linux(self, monkeypatch):
        monkeypatch.setattr(encryption.sys, "platform", "linux")
        assert encryption._protect_machine_key(KEY_32) == KEY_32

    def test_returns_raw_on_macos(self, monkeypatch):
        monkeypatch.setattr(encryption.sys, "platform", "darwin")
        assert encryption._protect_machine_key(KEY_32) == KEY_32

    def test_does_not_call_has_dpapi_off_windows(self, monkeypatch):
        """The early return must short-circuit BEFORE ``_has_dpapi``.

        Guards against a future edit that reorders the platform check —
        calling ``_has_dpapi`` on Linux is harmless today (it returns
        False) but invokes ``ctypes.windll`` on Windows which is
        rightly absent off-Windows. A reorder that touched ctypes.windll
        on Linux would raise AttributeError.
        """
        monkeypatch.setattr(encryption.sys, "platform", "linux")
        called = []
        monkeypatch.setattr(
            encryption,
            "_has_dpapi",
            lambda: called.append(1) or False,
        )
        encryption._protect_machine_key(KEY_32)
        assert called == [], "_has_dpapi must not be called on non-Windows"


class TestProtectMachineKeyDpapiAbsent:
    """On Windows when DPAPI module is unavailable."""

    def test_raises_when_dpapi_absent_and_flag_off(self, monkeypatch):
        monkeypatch.setattr(encryption.sys, "platform", "win32")
        monkeypatch.setattr(encryption, "_has_dpapi", lambda: False)

        with pytest.raises(DPAPIUnavailableError) as exc:
            encryption._protect_machine_key(KEY_32)
        assert exc.value.phase == "absent"
        assert exc.value.original is None

    def test_returns_raw_when_dpapi_absent_and_flag_on(self, monkeypatch, caplog):
        monkeypatch.setattr(encryption.sys, "platform", "win32")
        monkeypatch.setattr(encryption, "_has_dpapi", lambda: False)
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        with caplog.at_level("ERROR", logger="src.security.encryption"):
            result = encryption._protect_machine_key(KEY_32)

        assert result == KEY_32
        # The ERROR log must explicitly mention the flag so an operator
        # grepping the rotating log can tell why the key is in clear.
        assert any(
            "--allow-plaintext-keys" in r.getMessage() for r in caplog.records
        ), "ERROR log must reference the opt-in flag for diagnosability"


class TestGetOrCreateMachineKeyPermissions:
    """``machine_key.bin`` written via ``_write_key_atomic`` (0o600 POSIX)."""

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions ignored by NTFS")
    def test_machine_key_file_is_0o600_on_posix(self, isolated_appdata, monkeypatch):
        # Non-Windows path so DPAPI is not invoked; the test isolates
        # the file-creation contract from the DPAPI contract.
        monkeypatch.setattr(encryption.sys, "platform", "linux")
        encryption._get_or_create_machine_key()
        path = isolated_appdata / "BackupManager" / "machine_key.bin"
        assert path.exists()
        mode = path.stat().st_mode & 0o777
        assert mode == 0o600, (
            f"Expected 0o600 on machine_key.bin, got {oct(mode)} — "
            f"local user secrets are world-readable"
        )

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions ignored by NTFS")
    def test_atomic_write_no_residual_tmp(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(encryption.sys, "platform", "linux")
        encryption._get_or_create_machine_key()
        tmp = isolated_appdata / "BackupManager" / "machine_key.bin.tmp"
        assert not tmp.exists(), "Atomic rename left a stale .tmp file"


class TestGetOrCreateMachineKeyPropagatesDpapiError:
    """``DPAPIUnavailableError`` from ``_protect_machine_key`` propagates."""

    def test_propagates_dpapi_unavailable_error(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(encryption.sys, "platform", "win32")
        monkeypatch.setattr(encryption, "_has_dpapi", lambda: False)

        with pytest.raises(DPAPIUnavailableError):
            encryption._get_or_create_machine_key()

        # No file should have been created on disk when raising — caller
        # can safely retry after enabling the flag without colliding
        # with a half-written artefact.
        path = isolated_appdata / "BackupManager" / "machine_key.bin"
        assert not path.exists()
