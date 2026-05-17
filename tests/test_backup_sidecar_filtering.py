"""Unit tests for ``src.storage.base.is_backup_sidecar``.

Centralised so adding a new sidecar suffix only requires editing the
helper and one assertion here. Every storage backend funnels its
``list_backups`` filter through this helper -- per-backend integration
tests (test_storage_local etc) confirm the wiring, this file pins the
suffix set itself.
"""

from __future__ import annotations

import pytest

from src.storage.base import BACKUP_SIDECAR_SUFFIXES, is_backup_sidecar


class TestIsBackupSidecar:
    """Pin the exact set of suffixes recognised as sidecars."""

    @pytest.mark.parametrize(
        "name",
        [
            "Prof_FULL_2026-05-14_000000.wbverify",
            "Prof_FULL_2026-05-14_000000.wbcommit",
            "Prof_FULL_2026-05-14_000000.wbcommit.tmp",
            "Prof_FULL_2026-05-14_000000.wbserverhashes",
            "Prof_FULL_2026-05-14_000000.tar.wbenc.partial",
            # Path-like input also OK -- ``endswith`` works the same
            # way on a string with separators.
            "/home/u/backups/Prof.wbserverhashes",
        ],
    )
    def test_known_sidecars_are_filtered(self, name: str) -> None:
        assert is_backup_sidecar(name)

    @pytest.mark.parametrize(
        "name",
        [
            "Prof_FULL_2026-05-14_000000",
            "Prof_FULL_2026-05-14_000000.tar.wbenc",
            "Prof_FULL_2026-05-14_000000.txt",
            "Prof",
            "",
            "my.backup",
            # Confusable but not a sidecar: ``.wbverify-old`` etc.
            "Prof.wbverify-old",
            "Prof.wbcommit_archived",
        ],
    )
    def test_real_backups_are_not_filtered(self, name: str) -> None:
        assert not is_backup_sidecar(name)

    def test_suffix_list_is_explicit(self) -> None:
        """Fail loudly if a future edit shrinks the suffix set silently.

        The exact list is the cross-backend contract: any new sidecar
        type that ships (e.g. an encryption-marker file) MUST be added
        here so every backend's list_backups filters it. Adjusting this
        assertion is intentional -- the reviewer notices.
        """
        assert set(BACKUP_SIDECAR_SUFFIXES) == {
            ".wbverify",
            ".wbcommit",
            ".wbcommit.tmp",
            ".wbserverhashes",
            ".partial",
        }


class TestIntegrityVerifierPropagatesEvents:
    """The Verify tab progress bar stayed at 0 % during the full
    ~10 min local re-hash on a 260 k-file backup because
    IntegrityVerifier instantiated with events=None never forwarded
    progress events from the underlying verify_backup phase.

    This test pins the contract that verify_backup IS called with the
    same EventBus that was passed to the IntegrityVerifier. If a
    future refactor drops the kwarg, the test catches it before users
    see a stuck progress bar.
    """

    def test_verify_backup_receives_event_bus(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock

        from src.core.events import EventBus
        from src.core.integrity_verifier import IntegrityVerifier

        # Skip building a fully realistic profile + commit marker --
        # the production code path is: list_backups -> _verify_single
        # -> _verify_local -> verify_backup. We only care that the
        # last step receives the EventBus, so we stub everything in
        # between with MagicMock and pin the local-storage branch.
        backup_dir = tmp_path / "Prof_FULL"
        backup_dir.mkdir()
        (tmp_path / "Prof_FULL.wbverify").write_text("{}", encoding="utf-8")

        profile = MagicMock()
        # Pin .name explicitly: ``verify_iter`` now filters the backup
        # list by ``sanitize_profile_name(profile.name) + "_"`` (v3.7.4
        # fix), and a default MagicMock.name returns another MagicMock
        # that ``sanitize_profile_name`` cannot encode.
        profile.name = "Prof"
        profile.storage.storage_type.value = "local"
        profile.mirror_destinations = []

        config_manager = MagicMock()
        config_manager.load_verify_hashes.return_value = {}

        bus = EventBus()
        verifier = IntegrityVerifier(profile, config_manager, events=bus)

        # Stub the backend so list_backups returns the one fake backup
        # without needing a real wbcommit.
        backend = MagicMock()
        backend._dest = str(tmp_path)
        backend.list_backups.return_value = [{"name": "Prof_FULL", "is_dir": True}]

        from src.core import integrity_verifier as iv_mod

        monkeypatch.setattr(iv_mod, "_build_backend", lambda _c: backend)

        # Force the LOCAL branch in _verify_single regardless of what
        # the MagicMock profile.storage.storage_type compares to.
        from src.core.config import StorageType

        profile.storage.storage_type = StorageType.LOCAL

        # Spy on verify_backup so we capture its kwargs.
        captured: dict = {}

        def _fake_verify_backup(backup_path, manifest_path, events=None, cancel_check=None):
            captured["events"] = events
            captured["cancel_check"] = cancel_check
            return True, "OK (stubbed)"

        monkeypatch.setattr(iv_mod, "verify_backup", _fake_verify_backup)

        for _ in verifier.verify_iter():
            pass

        assert captured.get("events") is bus, (
            f"verify_backup was called with events={captured.get('events')!r}; "
            "IntegrityVerifier must forward its own event bus so the Verify "
            "tab can render per-file progress during the long local re-hash."
        )
        # Cancellation hook should also be wired so the Cancel button works.
        assert callable(captured.get("cancel_check"))
