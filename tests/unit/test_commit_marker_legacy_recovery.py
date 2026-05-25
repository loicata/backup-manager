"""Tests for the legacy-HMAC-key recovery branch in ``read_commit_marker``.

After a per-install HMAC key regeneration (Windows reinstall, AV
quarantine, accidental delete confirmed at the bootstrap alert),
every previously-signed ``.wbcommit`` fails HMAC verification with
the new current key. Without recovery, all those markers are
classified as orphans by ``LocalStorage.list_orphan_backups`` and
DELETED at the next ``_phase_orphan_scan``.

The recovery path defended by these tests:

1. ``read_commit_marker`` notices the current-key HMAC mismatch.
2. It iterates the ``.integrity_key.legacy_*`` archives via
   ``get_legacy_hmac_keys()``.
3. The first archive whose HMAC matches is accepted as authentic.
4. The marker is re-signed in place with the CURRENT key so the
   next read takes the fast path.
5. The validated payload is returned, the backup survives the
   orphan scan.

Honest scope (re-asserted in test_hmac_key_regen_alert.py):
on a Windows reinstall the legacy archives were wrapped with the
OLD DPAPI scope which the new user cannot unwrap — the recovery
path is then a no-op (``get_legacy_hmac_keys`` returns empty) and
the original "orphan" behaviour applies. The recovery succeeds
only when the failures are local to the LIVE key (corruption, AV
on the live file only).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.phases import commit_marker
from src.core.phases.commit_marker import (
    read_commit_marker,
    write_commit_marker,
)
from src.security.integrity_check import HMAC_KEY_FILE


# 64-char placeholder digest with valid hex characters, used as a
# stand-in ``manifest_sha256`` in marker payloads. The recovery
# tests only care about the HMAC chain, not the manifest binding.
_FAKE_MANIFEST_SHA = "0" * 64


def _backup_dir(tmp_path: Path) -> Path:
    """Create a stub backup directory and return its path."""
    backup = tmp_path / "Profile_FULL_2026-05-23_100000"
    backup.mkdir()
    (backup / "file.txt").write_text("payload", encoding="utf-8")
    return backup


def _write_marker_signed_with(backup: Path, signing_key: bytes) -> Path:
    """Write a ``.wbcommit`` next to ``backup`` signed by ``signing_key``.

    Bypasses the ``get_app_hmac_key`` indirection so the caller can
    pick exactly which key was in force when the marker was created
    — essential for simulating "this marker was written under an
    older key, and the current key has rotated".

    Only ``write_commit_marker`` is invoked (not ``build_commit_marker``
    separately) because ``build_commit_marker`` embeds
    ``completed_at = datetime.now(UTC)`` — calling it twice produces
    two different HMACs for the same logical marker.
    """
    with patch(
        "src.core.phases.commit_marker.get_app_hmac_key",
        return_value=signing_key,
    ):
        return write_commit_marker(
            backup_path=backup,
            manifest_sha256=_FAKE_MANIFEST_SHA,
            files_count=42,
            destination_label="storage",
        )


@pytest.fixture
def current_key() -> bytes:
    return b"\x11" * 32


@pytest.fixture
def legacy_key_a() -> bytes:
    return b"\x22" * 32


@pytest.fixture
def legacy_key_b() -> bytes:
    return b"\x33" * 32


class TestCurrentKeyMatchSkipsRecovery:
    """When the current key validates, no legacy lookup happens."""

    def test_current_key_match_returns_payload_without_legacy_call(
        self,
        tmp_path,
        current_key,
    ):
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, current_key)

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
            ) as mock_legacy,
        ):
            result = read_commit_marker(marker_path)

        assert result is not None
        assert result["files_count"] == 42
        # The recovery import must NOT have been triggered on the fast
        # path. This pin protects against an accidental "always try
        # legacy" regression that would slow every read by N DPAPI
        # unwraps on installs that never regenerated.
        mock_legacy.assert_not_called()

    def test_current_key_match_does_not_rewrite_marker(self, tmp_path, current_key):
        """No re-sign happens when the current key already matches."""
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, current_key)
        original_mtime = marker_path.stat().st_mtime_ns

        with patch(
            "src.core.phases.commit_marker.get_app_hmac_key",
            return_value=current_key,
        ):
            result = read_commit_marker(marker_path)

        assert result is not None
        assert marker_path.stat().st_mtime_ns == original_mtime


class TestLegacyKeyRecoversAndResigns:
    """When current key fails, the first matching legacy key recovers."""

    def test_legacy_key_validates_and_marker_is_resigned(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
    ):
        backup = _backup_dir(tmp_path)
        # Marker was signed when ``legacy_key_a`` was the current key.
        marker_path = _write_marker_signed_with(backup, legacy_key_a)
        old_payload = json.loads(marker_path.read_text(encoding="utf-8"))
        old_hmac = old_payload["hmac_sha256"]

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a],
            ),
        ):
            result = read_commit_marker(marker_path)

        # Recovery succeeded: the validated payload is returned.
        assert result is not None
        assert result["files_count"] == 42
        # Marker has been re-signed with ``current_key`` so the next
        # read takes the fast path.
        new_payload = json.loads(marker_path.read_text(encoding="utf-8"))
        assert new_payload["hmac_sha256"] != old_hmac, (
            "Marker must be re-signed after a successful legacy-key "
            "recovery — otherwise subsequent reads pay the legacy lookup "
            "cost forever"
        )
        # Sanity: the new HMAC equals the one ``current_key`` would
        # produce on the same payload.
        from src.core.phases.commit_marker import _payload_to_sign

        expected_new = __import__("hmac").new(
            current_key,
            _payload_to_sign(new_payload),
            __import__("hashlib").sha256,
        ).hexdigest()
        assert new_payload["hmac_sha256"] == expected_new

    def test_recovered_marker_passes_subsequent_read_on_fast_path(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
    ):
        """After re-sign, a second read with NO legacy keys must still pass."""
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, legacy_key_a)

        # First read: legacy is available, recovery happens.
        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a],
            ),
        ):
            assert read_commit_marker(marker_path) is not None

        # Second read: pretend the legacy archive has been removed.
        # If the re-sign worked, the current key matches now and the
        # read succeeds without touching the legacy list.
        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[],
            ),
        ):
            assert read_commit_marker(marker_path) is not None


class TestLegacyKeyOrderAndMultiplicity:
    """The first matching legacy key wins, regardless of position."""

    def test_second_legacy_key_validates_when_first_does_not(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
        legacy_key_b,
    ):
        backup = _backup_dir(tmp_path)
        # Signed with B. A is the most recent archive but does not match.
        marker_path = _write_marker_signed_with(backup, legacy_key_b)

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a, legacy_key_b],
            ),
        ):
            result = read_commit_marker(marker_path)
        assert result is not None

    def test_no_legacy_keys_returns_none(self, tmp_path, current_key, legacy_key_a):
        """Empty legacy list = original "untrusted" path."""
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, legacy_key_a)

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[],
            ),
        ):
            result = read_commit_marker(marker_path)
        assert result is None

    def test_legacy_keys_present_but_none_match_returns_none(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
        legacy_key_b,
    ):
        """Foreign legacy archives must NOT cause a false acceptance."""
        backup = _backup_dir(tmp_path)
        # Marker signed with a key NEITHER in the current nor legacy list.
        unknown_key = b"\x99" * 32
        marker_path = _write_marker_signed_with(backup, unknown_key)

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a, legacy_key_b],
            ),
        ):
            result = read_commit_marker(marker_path)
        assert result is None


class TestResignFailureFallsBackGracefully:
    """If the re-sign step fails, the validated payload is still returned."""

    def test_resign_write_failure_does_not_invalidate_recovery(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
        monkeypatch,
    ):
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, legacy_key_a)
        original_bytes = marker_path.read_bytes()

        # Patch ``os.replace`` to fail — simulates the marker location
        # being momentarily locked by an antivirus scan during recovery.
        def failing_replace(*_a, **_kw):
            raise PermissionError("EBUSY")

        monkeypatch.setattr(commit_marker.os, "replace", failing_replace)

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                return_value=current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a],
            ),
        ):
            result = read_commit_marker(marker_path)

        # Recovery still succeeded — the marker was validated via the
        # legacy key. Only the optimisation (re-sign) failed.
        assert result is not None
        assert result["files_count"] == 42
        # On-disk content is unchanged because the rename failed
        # cleanly (no half-written marker, no .tmp lingering).
        assert marker_path.read_bytes() == original_bytes
        tmp_residual = marker_path.with_name(marker_path.name + ".tmp")
        assert not tmp_residual.exists(), (
            "Failed re-sign must clean up its .tmp so the next read does "
            "not see a stray sibling"
        )

    def test_current_key_unavailable_during_resign_logged_not_raised(
        self,
        tmp_path,
        current_key,
        legacy_key_a,
    ):
        """If ``get_app_hmac_key`` raises OSError mid-resign, we log + return payload."""
        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, legacy_key_a)

        call_count = {"n": 0}

        def flaky_current_key():
            call_count["n"] += 1
            # First call (verify path) returns current key — so HMAC
            # mismatches and we fall into legacy recovery.
            # Second call (re-sign path) raises — re-sign aborts but
            # recovery already succeeded.
            if call_count["n"] == 1:
                return current_key
            raise OSError("DPAPI down momentarily")

        with (
            patch(
                "src.core.phases.commit_marker.get_app_hmac_key",
                side_effect=flaky_current_key,
            ),
            patch(
                "src.security.integrity_check.get_legacy_hmac_keys",
                return_value=[legacy_key_a],
            ),
        ):
            result = read_commit_marker(marker_path)

        assert result is not None
        assert result["files_count"] == 42


class TestArchiveDiscoveryIntegration:
    """End-to-end with real ``.integrity_key.legacy_*`` files on disk.

    The previous classes mock ``get_legacy_hmac_keys`` directly to
    keep the per-test setup simple. This class exercises the full
    chain: archive files written to ``%APPDATA%``, picked up by
    ``list_legacy_key_archives``, decoded by ``_load_key_from_archive``,
    and tried by the marker reader.
    """

    def test_plain_legacy_archive_recovers_marker(
        self,
        tmp_path,
        monkeypatch,
        current_key,
        legacy_key_a,
    ):
        # Simulate POSIX-style plaintext archives (no DPAPI marker).
        appdata = tmp_path / "appdata"
        monkeypatch.setenv("APPDATA", str(appdata))
        key_dir = appdata / "BackupManager"
        key_dir.mkdir(parents=True)

        # Live key is the CURRENT key, plaintext.
        (key_dir / HMAC_KEY_FILE).write_bytes(current_key)
        # Archive carrying the OLD key that signed our marker.
        archive = key_dir / f"{HMAC_KEY_FILE}.legacy_20260520_100000_unwrap_failed"
        archive.write_bytes(legacy_key_a)

        backup = _backup_dir(tmp_path)
        marker_path = _write_marker_signed_with(backup, legacy_key_a)

        # NB: only patch ``get_app_hmac_key`` to return ``current_key``.
        # Do NOT patch ``get_legacy_hmac_keys`` — we want the real
        # discovery path to find ``archive`` on its own.
        with patch(
            "src.core.phases.commit_marker.get_app_hmac_key",
            return_value=current_key,
        ):
            result = read_commit_marker(marker_path)

        assert result is not None
        assert result["files_count"] == 42

    def test_unloadable_archive_skipped_silently(self, tmp_path, monkeypatch, current_key):
        """A garbage archive in the dir must not poison the recovery loop."""
        appdata = tmp_path / "appdata"
        monkeypatch.setenv("APPDATA", str(appdata))
        key_dir = appdata / "BackupManager"
        key_dir.mkdir(parents=True)
        (key_dir / HMAC_KEY_FILE).write_bytes(current_key)
        # Corrupted archive: wrong size, no marker.
        bad_archive = key_dir / f"{HMAC_KEY_FILE}.legacy_20260520_100000_wrong_size"
        bad_archive.write_bytes(b"corrupted")

        backup = _backup_dir(tmp_path)
        # Marker signed with a totally unknown key → no recovery possible.
        unknown_key = b"\x77" * 32
        marker_path = _write_marker_signed_with(backup, unknown_key)

        with patch(
            "src.core.phases.commit_marker.get_app_hmac_key",
            return_value=current_key,
        ):
            result = read_commit_marker(marker_path)
        # No legacy key matched → marker is correctly classified as
        # untrusted. The garbage archive is silently skipped.
        assert result is None
