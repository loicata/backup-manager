"""Tests for the suspicious-regeneration alert in ``_get_hmac_key``.

The risk being defended against: a silent regeneration of the per-install
HMAC key invalidates every ``.wbcommit`` previously signed with the old
key. On LOCAL destinations those backups are then classified as orphans
and DELETED at the next ``_phase_orphan_scan`` — without any UI signal,
just a few ``Orphan removed`` INFO lines in ``backup_manager.log``.

These tests verify that the patched ``_get_hmac_key``:

- stays SILENT on a genuine first run (no key, no sentinel);
- creates the install sentinel as soon as a key is successfully read
  OR generated (so the migration from pre-patch installs takes effect
  on the first patched launch);
- RAISES ``HMACKeyRegeneratedError`` on every suspicious regeneration
  trigger (DPAPI unwrap fail, read denied, malformed file, sentinel
  present but key file gone);
- archives the old key as ``.legacy_<utc_ts>_<reason>`` before
  overwriting so a future recovery tool can attempt re-validation;
- defers entirely to the legacy silent-regen behaviour when
  ``_ALLOW_PLAINTEXT_FALLBACK`` is True (preserves the existing
  ``--allow-plaintext-keys`` CLI contract).

Like ``test_integrity_check_dpapi.py``, this module captures the REAL
``_get_hmac_key`` at import time to bypass the conftest's autouse mock.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from src.core.exceptions import DPAPIUnavailableError, HMACKeyRegeneratedError
from src.security import integrity_check
from src.security.integrity_check import (
    HMAC_KEY_FILE,
    HMAC_KEY_INSTALLED_SENTINEL,
    _DPAPI_MARKER,
)
from src.security.integrity_check import (
    _get_hmac_key as _real_get_hmac_key,
)


@pytest.fixture(autouse=True)
def _reset_plaintext_flag():
    """Force the plaintext-fallback flag OFF before every test.

    Module-level state — a leaking ``enable_plaintext_fallback()`` from
    a previous test would silently mask the strict-DPAPI behaviour
    this whole module exists to pin down.
    """
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False
    yield
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False


@pytest.fixture
def isolated_appdata(tmp_path, monkeypatch):
    """Point ``%APPDATA%`` at a fresh temp directory.

    ``_get_hmac_key`` reads the env var on every call; no caching to
    invalidate.
    """
    monkeypatch.setenv("APPDATA", str(tmp_path))
    return tmp_path


def _key_path(appdata: Path) -> Path:
    return appdata / "BackupManager" / HMAC_KEY_FILE


def _sentinel_path(appdata: Path) -> Path:
    return appdata / "BackupManager" / HMAC_KEY_INSTALLED_SENTINEL


class TestGenuineFirstRunStaysSilent:
    """No key + no sentinel = fresh install. Must NOT alert."""

    def test_first_run_generates_key_without_raising(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")

        key = _real_get_hmac_key()

        assert isinstance(key, bytes)
        assert len(key) == 32
        assert _key_path(isolated_appdata).exists()

    def test_first_run_creates_install_sentinel(self, isolated_appdata, monkeypatch):
        """The sentinel is the evidence used by subsequent runs."""
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")

        _real_get_hmac_key()

        assert _sentinel_path(isolated_appdata).exists(), (
            "Sentinel must be written so a future 'key disappeared' event "
            "can be distinguished from another genuine first run"
        )

    def test_second_call_returns_same_key_no_raise(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")

        first = _real_get_hmac_key()
        second = _real_get_hmac_key()

        assert first == second


class TestMigrationFromPrePatchInstall:
    """Existing key file but no sentinel = upgrade from pre-patch.

    The first successful read on the patched binary must create the
    sentinel so the detection works on this install from now on.
    """

    def test_existing_key_read_ok_creates_sentinel(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")

        # Pre-seed a legacy 32-byte plain key (the format an install
        # created with --allow-plaintext-keys on Windows or a normal
        # POSIX run would leave on disk). NO sentinel yet — simulates
        # an install predating this patch.
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_key = b"\xcd" * 32
        key_path.write_bytes(legacy_key)
        assert not _sentinel_path(isolated_appdata).exists()

        result = _real_get_hmac_key()

        assert result == legacy_key
        assert _sentinel_path(isolated_appdata).exists(), (
            "Migration step: the sentinel must be created the first time "
            "we successfully read a pre-existing key on a patched install"
        )

    def test_existing_dpapi_key_unwrap_ok_creates_sentinel(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_unwrap",
            lambda data: data.removeprefix(b"WRAPPED:"),
        )
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        existing_key = b"\xef" * 32
        key_path.write_bytes(_DPAPI_MARKER + b"WRAPPED:" + existing_key)
        assert not _sentinel_path(isolated_appdata).exists()

        result = _real_get_hmac_key()

        assert result == existing_key
        assert _sentinel_path(isolated_appdata).exists()


class TestDpapiUnwrapFailureRaises:
    """Existing DPAPI-wrapped key + unwrap fails = suspect regen."""

    def test_unwrap_failure_raises_with_archive(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)

        # Foreign blob: written by another user/machine, our DPAPI
        # scope cannot decrypt it.
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(_DPAPI_MARKER + b"FOREIGN_BLOB")

        def failing_unwrap(_data):
            raise OSError("CryptUnprotectData failed (error 0x8009000B)")

        monkeypatch.setattr(integrity_check, "_dpapi_unwrap", failing_unwrap)

        with pytest.raises(HMACKeyRegeneratedError) as exc_info:
            _real_get_hmac_key()

        assert exc_info.value.prior_key_existed is True
        assert exc_info.value.prior_key_path == key_path
        assert isinstance(exc_info.value.cause, OSError)

        # The original wrapped key must be archived before the regen
        # path would have overwritten it — even though the raise
        # prevents that overwrite, the archive lets the user verify
        # the key was preserved.
        archives = sorted(key_path.parent.glob(f"{key_path.name}.legacy_*"))
        assert len(archives) == 1
        assert "unwrap_failed" in archives[0].name
        assert archives[0].read_bytes() == _DPAPI_MARKER + b"FOREIGN_BLOB"

        # Crucially: the live key file must NOT have been overwritten,
        # so a future launch (after the user investigates) can still
        # try to recover.
        assert key_path.read_bytes() == _DPAPI_MARKER + b"FOREIGN_BLOB"

    def test_unwrap_failure_silent_with_plaintext_fallback(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        """``--allow-plaintext-keys`` users get the legacy silent regen."""
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)

        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(_DPAPI_MARKER + b"FOREIGN_BLOB")

        def failing_unwrap(_data):
            raise OSError("CryptUnprotectData failed")

        monkeypatch.setattr(integrity_check, "_dpapi_unwrap", failing_unwrap)
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        # No raise — falls through to regen.
        key = _real_get_hmac_key()
        assert len(key) == 32


class TestReadDeniedRaises:
    """Existing key file but read fails (lock, AV, permission)."""

    def test_read_oserror_raises(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        # Write SOMETHING so ``key_path.exists()`` returns True, then
        # patch ``Path.read_bytes`` to raise.
        key_path.write_bytes(b"placeholder")

        original_read_bytes = Path.read_bytes

        def selective_failing_read(self):
            if self == key_path:
                raise PermissionError("EACCES")
            return original_read_bytes(self)

        monkeypatch.setattr(Path, "read_bytes", selective_failing_read)

        with pytest.raises(HMACKeyRegeneratedError) as exc_info:
            _real_get_hmac_key()

        assert isinstance(exc_info.value.cause, PermissionError)
        # Archive exists (created from the placeholder content).
        archives = sorted(key_path.parent.glob(f"{key_path.name}.legacy_*"))
        assert any("read_failed" in p.name for p in archives)


class TestMalformedFileRaises:
    """Existing key file present, no marker, wrong size."""

    def test_wrong_size_raises(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        # 17 bytes: no marker, not the legacy 32-byte format.
        garbage = b"\x00" * 17
        key_path.write_bytes(garbage)

        with pytest.raises(HMACKeyRegeneratedError) as exc_info:
            _real_get_hmac_key()

        assert "17 bytes" in exc_info.value.reason or "unexpected size" in exc_info.value.reason
        archives = sorted(key_path.parent.glob(f"{key_path.name}.legacy_*"))
        assert any("wrong_size" in p.name for p in archives)
        # Archive content matches the original garbage.
        matching = next(p for p in archives if "wrong_size" in p.name)
        assert matching.read_bytes() == garbage

    def test_wrong_size_silent_with_plaintext_fallback(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(b"\x00" * 17)

        key = _real_get_hmac_key()
        assert len(key) == 32


class TestSentinelOrphan:
    """Sentinel exists but key file gone = manual delete / AV / cleanup."""

    def test_sentinel_without_key_raises(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        sentinel = _sentinel_path(isolated_appdata)
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        assert not _key_path(isolated_appdata).exists()

        with pytest.raises(HMACKeyRegeneratedError) as exc_info:
            _real_get_hmac_key()

        assert exc_info.value.prior_key_existed is True
        assert "missing" in exc_info.value.reason.lower()
        assert exc_info.value.cause is None
        # Nothing to archive — no key file existed.
        archives = list(_key_path(isolated_appdata).parent.glob(f"{HMAC_KEY_FILE}.legacy_*"))
        assert archives == []

    def test_sentinel_without_key_silent_with_plaintext_fallback(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        sentinel = _sentinel_path(isolated_appdata)
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        key = _real_get_hmac_key()
        assert len(key) == 32
        # Regen wrote a new key + the sentinel was already there.
        assert _key_path(isolated_appdata).exists()
        assert _sentinel_path(isolated_appdata).exists()


class TestIdempotenceOfAlertPath:
    """Aborting after the alert must leave disk state unchanged.

    The bootstrap caller is expected to call ``_get_hmac_key`` once,
    catch the exception, and stop without retrying. Verifying this
    explicitly pins down the contract: two consecutive raises must
    leave the same on-disk state, so the next launch re-presents the
    same alert (no silent recovery, no silent destruction).
    """

    def test_two_raises_leave_same_state(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)
        key_path = _key_path(isolated_appdata)
        key_path.parent.mkdir(parents=True, exist_ok=True)
        original_bytes = _DPAPI_MARKER + b"FOREIGN_BLOB"
        key_path.write_bytes(original_bytes)

        def failing_unwrap(_data):
            raise OSError("CryptUnprotectData failed")

        monkeypatch.setattr(integrity_check, "_dpapi_unwrap", failing_unwrap)

        with pytest.raises(HMACKeyRegeneratedError):
            _real_get_hmac_key()
        # Live key untouched, archive #1 created.
        assert key_path.read_bytes() == original_bytes
        first_archives = sorted(key_path.parent.glob(f"{key_path.name}.legacy_*"))

        with pytest.raises(HMACKeyRegeneratedError):
            _real_get_hmac_key()
        # Live key STILL untouched. A second archive may or may not
        # exist depending on the timestamp granularity, but the live
        # state is the invariant that matters: no destructive action
        # ever happened.
        assert key_path.read_bytes() == original_bytes
        second_archives = sorted(key_path.parent.glob(f"{key_path.name}.legacy_*"))
        assert len(second_archives) >= len(first_archives)


class TestDpapiWrapStrictStillRaises:
    """Strict-mode DPAPI wrap failure must still raise DPAPIUnavailableError.

    Distinct from ``HMACKeyRegeneratedError``: this is the
    fresh-key-creation path failing because DPAPI is broken, not the
    read path detecting a pre-existing key it cannot use. Conflating
    them would change the bootstrap dialog wording and confuse the
    user about what is actually wrong.
    """

    def test_wrap_failure_path_raises_dpapi_unavailable_not_regen(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")

        def failing_wrap(_data):
            raise OSError("CryptProtectData failed")

        monkeypatch.setattr(integrity_check, "_dpapi_wrap", failing_wrap)

        # No existing file, no sentinel → genuine first run path.
        # The wrap is the only operation that can fail here.
        with pytest.raises(DPAPIUnavailableError) as exc_info:
            _real_get_hmac_key()
        assert exc_info.value.phase == "wrap"
        # No half-written file.
        assert not _key_path(isolated_appdata).exists()
        # No sentinel (atomic on persistence + sentinel only on success).
        assert not _sentinel_path(isolated_appdata).exists()


class TestFormatRegenMessage:
    """The bootstrap message helper renders without a Tk display."""

    def test_message_includes_archive_path_when_present(self, tmp_path):
        # Need to import lazily to avoid pulling tkinter at module-load.
        from src import __main__ as bm_main

        key_path = tmp_path / "BackupManager" / HMAC_KEY_FILE
        key_path.parent.mkdir(parents=True, exist_ok=True)
        archive = key_path.with_name(f"{key_path.name}.legacy_20260523_100000_unwrap_failed")
        archive.write_bytes(b"old key bytes")

        error = HMACKeyRegeneratedError(
            reason="Existing HMAC key cannot be decrypted by DPAPI: CryptUnprotectData failed.",
            prior_key_existed=True,
            prior_key_path=key_path,
        )
        message = bm_main._format_hmac_regen_message(error)

        assert "CryptUnprotectData failed" in message
        assert str(archive) in message
        assert "Continue and accept loss" in message

    def test_message_omits_archive_hint_when_no_archive(self, tmp_path):
        from src import __main__ as bm_main

        key_path = tmp_path / "BackupManager" / HMAC_KEY_FILE
        key_path.parent.mkdir(parents=True, exist_ok=True)
        # No .legacy_* sibling.

        error = HMACKeyRegeneratedError(
            reason="Sentinel says key existed but file is gone.",
            prior_key_existed=True,
            prior_key_path=key_path,
        )
        message = bm_main._format_hmac_regen_message(error)
        assert "legacy_" not in message
        assert "Sentinel says key existed" in message


class TestHandlerOutcomeWithoutTk:
    """When the dialog cannot be shown, the handler MUST default to abort.

    Better to refuse to start than to silently delete the user's
    backups because the GUI is broken.
    """

    def test_handler_returns_abort_on_dialog_failure(self, monkeypatch, tmp_path):
        from src import __main__ as bm_main

        key_path = tmp_path / "BackupManager" / HMAC_KEY_FILE
        key_path.parent.mkdir(parents=True, exist_ok=True)
        error = HMACKeyRegeneratedError(
            reason="Whatever",
            prior_key_existed=True,
            prior_key_path=key_path,
        )

        def failing_askyesno(*_args, **_kwargs):
            raise RuntimeError("No display")

        import tkinter.messagebox as mb

        monkeypatch.setattr(mb, "askyesno", failing_askyesno)

        outcome = bm_main._handle_hmac_regen_at_startup(error)
        assert outcome == "abort"
