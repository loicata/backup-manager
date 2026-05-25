"""Tests for ``integrity_check._get_hmac_key`` and its DPAPI fallback.

The conftest's session-wide autouse fixture patches ``_get_hmac_key``
and ``get_app_hmac_key`` to a fixed test value so the rest of the suite
never touches the user's real key file. This module needs the real
implementation to exercise:

- the fresh-install path (DPAPI wrap + marker prepend);
- the read-existing-DPAPI path (marker recognised, unwrap returns key);
- the read-legacy-plain path (no marker, exactly 32 bytes, used as-is);
- the regen path on a malformed file (wrong size, no marker);
- the fallback path when ``_dpapi_wrap`` raises OSError — the key must
  be written **in clear** (no spurious DPAPI marker that next run
  cannot unwrap, which would loop forever and silently neutralise
  tamper detection on ``app_checksums.json`` and ``.wbcommit``);
- the regen path when ``_dpapi_unwrap`` fails on an existing wrapped
  key (e.g. machine-key migration, user-profile change).

Trick used to bypass the autouse patch: bind the real function object
to a module-level alias **at import time**. Module imports run during
pytest collection, before any per-test fixture (including
``_isolate_hmac_key``) executes, so the alias still points at the
original implementation when each test calls it. The autouse patch
mutates ``integrity_check._get_hmac_key`` for the duration of the
test, but our local alias is not affected.
"""

from __future__ import annotations

import sys

import pytest

# Capture the REAL functions before the conftest autouse fixture
# replaces them with Mock objects. This binding survives because
# imports happen during collection, before any fixture runs.
from src.core.exceptions import DPAPIUnavailableError
from src.security import integrity_check
from src.security.integrity_check import (
    _DPAPI_MARKER,
)
from src.security.integrity_check import (
    _get_hmac_key as _real_get_hmac_key,
)


@pytest.fixture(autouse=True)
def _reset_plaintext_flag():
    """Force the plaintext-fallback flag OFF before every test.

    The flag is process-wide module state. A previous test that
    enabled it would otherwise leak into the next test and silently
    mask the strict-DPAPI behaviour we want to verify.
    """
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False
    yield
    integrity_check._ALLOW_PLAINTEXT_FALLBACK = False


@pytest.fixture
def isolated_appdata(tmp_path, monkeypatch):
    """Point ``%APPDATA%`` at a fresh temp dir so each test starts clean.

    The integrity-check module reads the env var on every call, so a
    monkeypatch is sufficient (no caching to invalidate).
    """
    monkeypatch.setenv("APPDATA", str(tmp_path))
    yield tmp_path


def _key_path(appdata) -> Path:  # noqa: F821 — type hint string only
    """Return the path where the HMAC key file lives under ``appdata``."""
    from pathlib import Path

    return Path(appdata) / "BackupManager" / ".integrity_key"


class TestFreshInstall:
    """First call generates a 32-byte key and persists it to disk."""

    def test_creates_directory_and_file(self, isolated_appdata):
        key = _real_get_hmac_key()

        assert isinstance(key, bytes)
        assert len(key) == 32
        assert _key_path(isolated_appdata).exists()

    def test_key_is_stable_across_calls(self, isolated_appdata):
        first = _real_get_hmac_key()
        second = _real_get_hmac_key()
        assert first == second

    def test_key_is_random_per_install(self, tmp_path, monkeypatch):
        """Two distinct ``%APPDATA%`` roots produce different keys."""
        monkeypatch.setenv("APPDATA", str(tmp_path / "a"))
        key_a = _real_get_hmac_key()
        monkeypatch.setenv("APPDATA", str(tmp_path / "b"))
        key_b = _real_get_hmac_key()
        assert key_a != key_b


class TestDpapiWrappingOnWindows:
    """When DPAPI is available the file starts with the DPAPI marker."""

    def test_dpapi_marker_present_on_windows(self, isolated_appdata, monkeypatch):
        # Force the win32 branch of ``_get_hmac_key`` regardless of host.
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        # Bypass real DPAPI: our fake wrap is a tag the test can verify.
        # ``_dpapi_wrap`` returns ``data`` unchanged on non-Windows; the
        # ``sys.platform`` patch above forces the function to think it
        # IS Windows but the real ctypes call would fail in CI. Patch
        # the wrap helper to a deterministic transform instead.
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)

        key = _real_get_hmac_key()

        raw = _key_path(isolated_appdata).read_bytes()
        assert raw.startswith(_DPAPI_MARKER), (
            "Wrapped key must carry the DPAPI marker so the next read " "knows to call unwrap"
        )
        assert raw[len(_DPAPI_MARKER) :] == b"WRAPPED:" + key

    def test_existing_wrapped_key_is_unwrapped_on_read(self, isolated_appdata, monkeypatch):
        """Second call reads the marker, unwraps, returns the original key."""
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_wrap",
            lambda data: b"WRAPPED:" + data,
        )
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_unwrap",
            lambda data: data.removeprefix(b"WRAPPED:"),
        )

        first = _real_get_hmac_key()
        second = _real_get_hmac_key()  # reads + unwraps the file written above
        assert first == second


class TestDpapiWrapStrictFailure:
    """When DPAPI wrap fails on Windows, refuse to start by default.

    Hardening introduced after the audit found that the previous
    warning-only fallback wrote the HMAC key in clear to disk —
    silently neutralising tamper detection on ``app_checksums.json``
    and ``.wbcommit``. The strict path now raises
    ``DPAPIUnavailableError`` so the bootstrap surfaces a recovery
    dialog instead of degrading without the user's knowledge.
    """

    def test_raises_dpapi_unavailable_by_default(self, isolated_appdata, monkeypatch):
        """Plaintext-fallback off → wrap failure raises DPAPIUnavailableError."""
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")

        def failing_wrap(_data):
            raise OSError("CryptProtectData failed (error 0x80090020)")

        monkeypatch.setattr(integrity_check, "_dpapi_wrap", failing_wrap)

        with pytest.raises(DPAPIUnavailableError) as exc_info:
            _real_get_hmac_key()
        assert exc_info.value.phase == "wrap"
        # No half-written file on disk when raising — the caller can
        # safely retry after fixing the environment.
        assert not _key_path(isolated_appdata).exists()


class TestDpapiWrapPlaintextFallback:
    """``--allow-plaintext-keys`` opt-in restores the legacy clear-key path.

    The bug the legacy fallback guards against: if the wrap raises
    but the writer still prepends ``_DPAPI_MARKER``, the next
    ``_get_hmac_key`` call sees the marker, calls unwrap (which
    fails again), regenerates a fresh key, and the cycle repeats
    forever — tamper detection silently neutralised because every
    run thinks it is the first run. The plaintext fallback must
    therefore write WITHOUT the marker so the next read recognises
    the file as legacy plain and returns the same bytes.
    """

    def test_clear_key_when_wrap_raises_and_flag_set(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        def failing_wrap(_data):
            raise OSError("CryptProtectData failed (error 0x80090020)")

        monkeypatch.setattr(integrity_check, "_dpapi_wrap", failing_wrap)

        key = _real_get_hmac_key()

        raw = _key_path(isolated_appdata).read_bytes()
        assert not raw.startswith(_DPAPI_MARKER), (
            "Marker must NOT be written when wrap failed — otherwise next "
            "run's unwrap call would loop on an unrecoverable file"
        )
        assert raw == key
        assert len(raw) == 32

    def test_clear_key_is_reused_on_next_read(self, isolated_appdata, monkeypatch):
        """A clear-stored key is recognised as legacy plain on the next read."""
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        def failing_wrap(_data):
            raise OSError("DPAPI down")

        monkeypatch.setattr(integrity_check, "_dpapi_wrap", failing_wrap)

        first = _real_get_hmac_key()  # writes 32 raw bytes (no marker)

        # Second call: DPAPI is "back" but the existing file has no
        # marker. The function must accept it as legacy plain and
        # return the SAME bytes — not regenerate.
        # Patch wrap back to a working fake to be sure the no-regen
        # path is the one taken.
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_wrap",
            lambda data: b"WRAPPED:" + data,
        )
        second = _real_get_hmac_key()

        assert first == second
        # File still in clear (no implicit re-wrap on read).
        raw = _key_path(isolated_appdata).read_bytes()
        assert not raw.startswith(_DPAPI_MARKER)


class TestEnablePlaintextFallback:
    """``enable_plaintext_fallback()`` flips the module-level switch."""

    def test_enable_sets_flag(self, monkeypatch):
        assert integrity_check._ALLOW_PLAINTEXT_FALLBACK is False
        integrity_check.enable_plaintext_fallback()
        assert integrity_check._ALLOW_PLAINTEXT_FALLBACK is True
        assert integrity_check.is_plaintext_fallback_allowed() is True

    def test_is_plaintext_fallback_allowed_reflects_flag(self, monkeypatch):
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = False
        assert integrity_check.is_plaintext_fallback_allowed() is False
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True
        assert integrity_check.is_plaintext_fallback_allowed() is True


class TestKeyFilePermissions:
    """``.integrity_key`` is written with mode 0o600 on POSIX.

    NTFS ignores the POSIX mode (file appears 0o666 to ``stat``), so
    the test is skipped on Windows — the protection there comes from
    DPAPI wrapping + %APPDATA% ACLs. On Linux/macOS the 0o600 mode is
    the only thing standing between the (possibly plaintext-fallback)
    key and other local users.
    """

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions ignored by NTFS")
    def test_integrity_key_file_is_0o600_on_posix(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        _real_get_hmac_key()
        mode = _key_path(isolated_appdata).stat().st_mode & 0o777
        assert mode == 0o600, (
            f"Expected 0o600 on .integrity_key, got {oct(mode)} — "
            f"local user secrets are world-readable"
        )

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions ignored by NTFS")
    def test_atomic_write_does_not_leave_tmp_file(self, isolated_appdata, monkeypatch):
        """``os.replace`` removes the .tmp after successful write."""
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        _real_get_hmac_key()
        tmp = _key_path(isolated_appdata).with_name(_key_path(isolated_appdata).name + ".tmp")
        assert not tmp.exists(), "Atomic rename left a stale .tmp file"


class TestDpapiUnwrapFailureRegen:
    """An existing wrapped key that cannot be unwrapped triggers regen.

    Behaviour change (this patch): the silent regeneration on unwrap
    failure is now gated on ``--allow-plaintext-keys``. Strict mode
    instead raises ``HMACKeyRegeneratedError`` so the bootstrap can
    warn the user BEFORE the next ``_phase_orphan_scan`` would
    classify every historical ``.wbcommit`` as an orphan and delete
    the corresponding backups. The dedicated coverage for the strict
    path lives in ``test_hmac_key_regen_alert.py``; this test pins
    down that the legacy silent-regen path still works under the
    opt-in plaintext flag (CLI contract preserved).
    """

    def test_unwrap_failure_regenerates_key_under_plaintext_fallback(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)

        # ``--allow-plaintext-keys`` users get the legacy silent regen.
        # Without this flag, the call raises HMACKeyRegeneratedError —
        # covered in the dedicated regen-alert test module.
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        # Write a file as if a previous Windows user wrapped it; this
        # user can no longer unwrap (different DPAPI scope).
        _key_path(isolated_appdata).parent.mkdir(parents=True, exist_ok=True)
        _key_path(isolated_appdata).write_bytes(_DPAPI_MARKER + b"FOREIGN_BLOB")

        def failing_unwrap(_data):
            raise OSError("CryptUnprotectData failed (error 0x8009000B)")

        monkeypatch.setattr(integrity_check, "_dpapi_unwrap", failing_unwrap)

        key = _real_get_hmac_key()

        # New key generated, persisted with our (working) wrap.
        assert len(key) == 32
        raw = _key_path(isolated_appdata).read_bytes()
        assert raw.startswith(_DPAPI_MARKER)
        # The new wrapped payload is what our fake wrap produces from
        # the new key — proves the foreign blob was discarded, not
        # silently retained.
        assert raw[len(_DPAPI_MARKER) :] == b"WRAPPED:" + key


class TestMalformedFileRegen:
    """A file that is neither marker-prefixed nor 32 bytes is regenerated.

    Same behaviour-change rationale as ``TestDpapiUnwrapFailureRegen``:
    the silent regen path is now opt-in via plaintext fallback. The
    default-strict path raises ``HMACKeyRegeneratedError`` and is
    covered in ``test_hmac_key_regen_alert.py``.
    """

    def test_malformed_size_regenerates_under_plaintext_fallback(
        self,
        isolated_appdata,
        monkeypatch,
    ):
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)
        integrity_check._ALLOW_PLAINTEXT_FALLBACK = True

        # 17 bytes: no marker, wrong size — clearly garbage.
        _key_path(isolated_appdata).parent.mkdir(parents=True, exist_ok=True)
        _key_path(isolated_appdata).write_bytes(b"\x00" * 17)

        key = _real_get_hmac_key()
        assert len(key) == 32

        raw = _key_path(isolated_appdata).read_bytes()
        # Regenerated and re-wrapped.
        assert raw.startswith(_DPAPI_MARKER)
        assert raw != b"\x00" * 17

    def test_legacy_plain_32_byte_file_kept(self, isolated_appdata, monkeypatch):
        """A pre-DPAPI 32-byte plain key is accepted and returned as-is."""
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        monkeypatch.setattr(integrity_check, "_dpapi_wrap", lambda data: b"WRAPPED:" + data)

        legacy = b"\xaa" * 32
        _key_path(isolated_appdata).parent.mkdir(parents=True, exist_ok=True)
        _key_path(isolated_appdata).write_bytes(legacy)

        key = _real_get_hmac_key()
        assert key == legacy
        # File NOT silently rewritten — production code keeps the
        # legacy file in place; rewrap happens on the next save_*
        # operation, not on the read path.
        assert _key_path(isolated_appdata).read_bytes() == legacy


class TestNonWindowsPlatform:
    """On POSIX, ``_dpapi_*`` are no-ops and the file is stored in clear."""

    def test_posix_writes_clear_key(self, isolated_appdata, monkeypatch):
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        # On POSIX the win32 branch is skipped entirely, so wrap is
        # never called. Patch it to a sentinel that would fail the
        # test if it ever IS called by mistake.
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_wrap",
            lambda _data: pytest.fail("dpapi_wrap must not be called on POSIX"),
        )

        key = _real_get_hmac_key()

        raw = _key_path(isolated_appdata).read_bytes()
        assert raw == key
        assert not raw.startswith(_DPAPI_MARKER)


class TestSaveAndLoadRoundTrip:
    """End-to-end: ``save_checksums`` + ``load_checksums`` use the real key.

    These exercise the public API which calls ``_get_hmac_key`` via
    ``_compute_hmac``. The conftest patch on ``_get_hmac_key`` would
    make the HMAC predictable; here we restore the real function for
    one round-trip to confirm the marker-prefixed file is read back
    correctly without intervening regen.
    """

    def test_save_then_load_returns_same_checksums(self, isolated_appdata, monkeypatch):
        # Restore the real ``_compute_hmac`` path: the conftest only
        # patches ``_get_hmac_key`` itself, but ``_compute_hmac`` is
        # not patched, so it will call the (mocked) ``_get_hmac_key``.
        # Override the module attribute back to the real function for
        # this test only.
        monkeypatch.setattr(integrity_check, "_get_hmac_key", _real_get_hmac_key)
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_wrap",
            lambda data: b"WRAPPED:" + data,
        )
        monkeypatch.setattr(
            integrity_check,
            "_dpapi_unwrap",
            lambda data: data.removeprefix(b"WRAPPED:"),
        )
        monkeypatch.setattr(integrity_check.sys, "platform", "win32")
        # ``compute_checksums`` walks the real source tree to hash files;
        # mock it to a small fixed dict so the round-trip test is fast
        # and independent of source-file content.
        monkeypatch.setattr(
            integrity_check,
            "compute_checksums",
            lambda: {"foo.py": "deadbeef" * 8, "bar.py": "cafe" * 16},
        )

        integrity_check.save_checksums()
        loaded = integrity_check.load_checksums()

        assert loaded == {"foo.py": "deadbeef" * 8, "bar.py": "cafe" * 16}

    def test_tampered_file_returns_none(self, isolated_appdata, monkeypatch):
        """A flipped byte in the JSON must trip the HMAC verification."""
        monkeypatch.setattr(integrity_check, "_get_hmac_key", _real_get_hmac_key)
        monkeypatch.setattr(integrity_check.sys, "platform", "linux")
        monkeypatch.setattr(
            integrity_check,
            "compute_checksums",
            lambda: {"foo.py": "0" * 64},
        )

        integrity_check.save_checksums()
        path = integrity_check._get_checksum_path()
        raw = path.read_text(encoding="utf-8")
        # Flip one character inside the checksum so the HMAC over the
        # canonical JSON differs from the stored one.
        tampered = raw.replace('"0000', '"1000', 1)
        assert tampered != raw, "Test fixture must actually modify the file"
        path.write_text(tampered, encoding="utf-8")

        assert integrity_check.load_checksums() is None
