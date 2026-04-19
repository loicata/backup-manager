"""Regression tests for the backup_type race condition.

In the wild, the UI (`_save_profile` at src/ui/app.py:1305) mutates
``profile.backup_type`` with UI state. If a save fires while a backup
is running, the shared ``BackupProfile`` instance held by the engine
sees its ``backup_type`` flipped away from FULL back to DIFFERENTIAL
between ``_maybe_force_full`` and ``_phase_update_delta``. The ELSE
branch of ``_phase_update_delta`` then runs, bumping
``differential_count`` instead of resetting it and leaving
``last_full_backup`` stale — the chain between auto-promoted FULLs
and subsequent DIFFs breaks.

Fixes under test:
  1. ``_phase_update_delta`` uses ``ctx.forced_full`` as the
     authoritative source of truth for "auto-promoted to FULL",
     matching the pattern used for Object Lock at line 1585.
  2. ``_on_profile_selected`` skips the save-before-switch path
     when the selection event targets the currently loaded profile
     (breaks the cascade save -> load_profiles -> select_set ->
     ListboxSelect -> save).
  3. ``_save_profile`` drops silent saves and warns on explicit
     saves while ``self._backup_running`` is True.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.backup_engine import BackupEngine
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    RetentionConfig,
    StorageConfig,
    StorageType,
    compute_profile_hash,
)
from src.core.events import EventBus
from src.core.phases.base import PipelineContext

# ---------------------------------------------------------------------------
# Fix 1 — _phase_update_delta uses forced_full as source of truth
# ---------------------------------------------------------------------------


class TestPhaseUpdateDeltaForcedFullSource:
    """_phase_update_delta must take the FULL branch even when profile
    .backup_type has been mutated back to DIFFERENTIAL mid-pipeline."""

    def _build_ctx(
        self, tmp_path: Path, *, backup_type: BackupType, forced_full: bool
    ) -> tuple[PipelineContext, BackupEngine]:
        config_dir = tmp_path / "cfg"
        config_dir.mkdir()
        cfg = ConfigManager(config_dir=config_dir)
        events = EventBus()
        profile = BackupProfile(
            name="T",
            source_paths=[str(tmp_path)],
            backup_type=backup_type,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path / "dest"),
            ),
            retention=RetentionConfig(),
        )
        cfg.save_profile(profile)

        engine = BackupEngine(cfg, events=events)
        ctx = PipelineContext(
            profile=profile, config_manager=cfg, events=events, result=MagicMock()
        )
        ctx.forced_full = forced_full
        ctx.backup_name = "T_FULL_test"
        ctx.all_files = []
        ctx.file_hashes = {}
        ctx.result.files_processed = 0
        return ctx, engine

    def test_forced_full_takes_full_branch_even_if_type_flipped_to_diff(self, tmp_path):
        """Simulate the bug: backup_type was FULL during _maybe_force_full,
        a concurrent UI save flipped it to DIFFERENTIAL, now _phase_update_delta
        runs. It MUST still run the FULL branch (via forced_full sentinel)."""
        ctx, engine = self._build_ctx(
            tmp_path, backup_type=BackupType.DIFFERENTIAL, forced_full=True
        )

        engine._phase_update_delta(ctx)

        # FULL branch ran: last_full_backup set, hash refreshed
        assert ctx.profile.last_full_backup is not None
        assert ctx.profile.profile_hash == compute_profile_hash(ctx.profile)

    def test_honest_full_still_takes_full_branch(self, tmp_path):
        """Sanity: a normal (non-promoted) FULL — no forced_full flag,
        backup_type=FULL — still runs the FULL branch."""
        ctx, engine = self._build_ctx(tmp_path, backup_type=BackupType.FULL, forced_full=False)

        engine._phase_update_delta(ctx)

        assert ctx.profile.last_full_backup is not None

    def test_real_diff_still_takes_diff_branch(self, tmp_path):
        """A real DIFF — backup_type=DIFFERENTIAL, no forced_full —
        must NOT touch last_full_backup."""
        ctx, engine = self._build_ctx(
            tmp_path, backup_type=BackupType.DIFFERENTIAL, forced_full=False
        )
        ctx.profile.last_full_backup = "before"

        engine._phase_update_delta(ctx)

        assert ctx.profile.last_full_backup == "before"  # untouched


# ---------------------------------------------------------------------------
# Fix 2 — _on_profile_selected skips save on same-profile re-selection
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_with_profiles(tk_root, tmp_path, monkeypatch):
    """Spin up a real MainApp against a temp config directory with two profiles.

    We override APPDATA to isolate from the user's real config and populate
    two profiles so the listbox has actual entries to select.
    """
    monkeypatch.setenv("APPDATA", str(tmp_path))

    from src.ui.app import BackupManagerApp

    cfg = ConfigManager(config_dir=tmp_path / "BackupManager")
    p1 = BackupProfile(
        name="P1",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(tmp_path / "d1"),
        ),
    )
    p2 = BackupProfile(
        name="P2",
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(tmp_path / "d2"),
        ),
    )
    cfg.save_profile(p1)
    cfg.save_profile(p2)

    # Build a hidden MainApp — reuse the session tk_root as the parent so
    # we do not leak Tcl interpreters between tests.
    toplevel = __import__("tkinter").Toplevel(tk_root)
    toplevel.withdraw()
    app = BackupManagerApp(toplevel)
    yield app, p1.id, p2.id
    toplevel.destroy()


class TestOnProfileSelectedNoCascade:
    """Reselecting the same profile must not trigger _save_profile."""

    def test_same_profile_reselection_skips_save(self, app_with_profiles):
        app, p1_id, _ = app_with_profiles
        # App starts with the first profile loaded; stub _save_profile so we
        # can count invocations.
        calls = []
        original_save = app._save_profile

        def counting_save(*args, **kwargs):
            calls.append(kwargs.get("silent", False))
            return original_save(*args, **kwargs)

        app._save_profile = counting_save

        # Simulate a programmatic re-selection of the currently loaded
        # profile (what _load_profiles' select_set triggers via
        # <<ListboxSelect>>).
        current_id = app._current_profile.id
        assert current_id == p1_id

        app._on_profile_selected(None)

        # No save should have been issued — we re-selected the same profile.
        assert calls == []

    def test_different_profile_still_saves(self, app_with_profiles):
        app, p1_id, p2_id = app_with_profiles
        calls = []

        def counting_save(*args, **kwargs):
            calls.append(kwargs.get("silent", False))
            return True

        app._save_profile = counting_save

        # Select the second profile by index
        for map_idx, profile in app._listbox_profile_map:
            if profile is not None and profile.id == p2_id:
                app.profile_listbox.selection_clear(0, "end")
                app.profile_listbox.selection_set(map_idx)
                app._on_profile_selected(None)
                break

        # A real switch MUST save the outgoing profile first
        assert calls == [True]


# ---------------------------------------------------------------------------
# Fix 3 — _save_profile drops silent saves while a backup is running
# ---------------------------------------------------------------------------


class TestSaveProfileGuardDuringBackup:
    """_save_profile must not mutate the shared profile while the engine is active."""

    def test_silent_save_is_dropped_during_backup(self, app_with_profiles):
        app, _, _ = app_with_profiles
        original_backup_type = app._current_profile.backup_type
        app._backup_running = True

        # Flip the UI var to something different so if _save_profile ran it
        # would be visible on the profile.
        from src.core.config import BackupType

        other_type = (
            BackupType.DIFFERENTIAL if original_backup_type == BackupType.FULL else BackupType.FULL
        )
        app.tab_general.type_var.set(other_type.value)

        result = app._save_profile(silent=True)

        assert result is True  # silent drop reports success
        # Profile was NOT mutated — the guard kept the engine's view intact.
        assert app._current_profile.backup_type == original_backup_type

    def test_explicit_save_warns_during_backup(self, app_with_profiles, monkeypatch):
        app, _, _ = app_with_profiles
        app._backup_running = True

        shown = []
        monkeypatch.setattr(
            "src.ui.app.messagebox.showwarning",
            lambda *a, **kw: shown.append((a, kw)),
        )

        result = app._save_profile(silent=False)

        assert result is False
        assert shown, "User should have seen a 'Backup in progress' warning"
        assert "Backup in progress" in shown[0][0][0]

    def test_save_works_normally_when_no_backup_running(self, app_with_profiles):
        app, _, _ = app_with_profiles
        assert app._backup_running is False

        # Should not raise and should persist
        result = app._save_profile(silent=True)

        assert result is True
