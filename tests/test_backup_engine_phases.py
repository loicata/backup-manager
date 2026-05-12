"""Tests for the new backup-engine phases and rollback helpers.

Covers the v3.3.15 additions in :mod:`src.core.backup_engine`:

* ``_best_effort_cleanup`` and ``_try_delete`` — partial-backup
  reclamation on the failure path of ``run_backup``.
* ``_phase_orphan_scan`` — phase 0 that deletes any backup without a
  valid ``.wbcommit`` before write phases consume disk.
* ``_phase_commit_primary`` and ``_commit_mirror`` — write/upload of
  the destination-side commit marker that authoritatively marks a
  backup as complete.
* ``_apply_pending_rollback`` and ``_rollback_backup_type_on_failure``
  — backup-type sentinel handling so a forced-FULL promotion never
  permanently sticks after a crash.

All tests use ``BackupEngine.__new__(BackupEngine)`` to bypass the
expensive constructor, then attach the minimal collaborator mocks each
method needs. This keeps each test under a few milliseconds and avoids
allocating real ``ConfigManager`` / ``EventBus`` instances.
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.backup_engine import BackupEngine
from src.core.config import BackupType, StorageType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bare_engine() -> BackupEngine:
    """Return a ``BackupEngine`` with the cheapest set of attributes the
    tested methods need. ``__init__`` is bypassed because every dependency
    (config_manager, EventBus) would slow tests by orders of magnitude.

    Tests that need extra collaborators attach them directly on the
    returned instance (``engine._foo = MagicMock()``).
    """
    engine = BackupEngine.__new__(BackupEngine)
    engine._events = MagicMock()
    engine._cancelled = False
    engine._config = MagicMock()
    return engine


def _make_ctx(**overrides) -> MagicMock:
    """Build a minimal ``PipelineContext`` mock with the attributes
    the new phases read."""
    ctx = MagicMock()
    ctx.profile.mirror_destinations = []
    ctx.profile.storage.storage_type = StorageType.LOCAL
    ctx.profile.storage.s3_object_lock = False
    ctx.backup_name = "Bk_FULL_2026-05-08_120000"
    ctx.backup_path = None
    ctx.backup_remote_name = ""
    ctx.backend = None
    ctx.integrity_manifest = {
        "version": 1,
        "files": {},
        "total_checksum": "a" * 64,
    }
    for k, v in overrides.items():
        setattr(ctx, k, v)
    return ctx


# ---------------------------------------------------------------------------
# _try_delete (static helper)
# ---------------------------------------------------------------------------


class TestTryDelete:
    """Behaviour of the swallow-everything backend deletion helper."""

    def test_success_logs_info_and_does_not_raise(self) -> None:
        backend = MagicMock()
        # No exception → just returns.
        BackupEngine._try_delete(backend, "Bk", "primary")
        backend.delete_backup.assert_called_once_with("Bk")

    def test_file_not_found_swallowed(self) -> None:
        backend = MagicMock()
        backend.delete_backup.side_effect = FileNotFoundError("gone")
        # Must not propagate — FNF is the trivially common case.
        BackupEngine._try_delete(backend, "Bk", "primary")

    def test_other_exception_swallowed_with_warning(self) -> None:
        backend = MagicMock()
        backend.delete_backup.side_effect = OSError("network")
        # The whole point is to NOT mask the original failure that
        # triggered the cleanup. So this MUST not raise.
        BackupEngine._try_delete(backend, "Bk", "primary")

    def test_permission_error_swallowed(self) -> None:
        # PermissionError is its own class and must hit the generic
        # ``except Exception`` arm.
        backend = MagicMock()
        backend.delete_backup.side_effect = PermissionError("locked")
        BackupEngine._try_delete(backend, "Bk", "mirror 1")


# ---------------------------------------------------------------------------
# _best_effort_cleanup
# ---------------------------------------------------------------------------


class TestBestEffortCleanup:
    """The cleanup that runs from ``run_backup``'s except-block."""

    def test_no_backup_name_short_circuits(self) -> None:
        engine = _bare_engine()
        ctx = _make_ctx(backup_name="")
        ctx.backend = MagicMock()
        engine._best_effort_cleanup(ctx)
        # Without a name we have nothing to delete — backend untouched.
        ctx.backend.delete_backup.assert_not_called()

    def test_primary_only_deletes_both_artefact_names(self) -> None:
        engine = _bare_engine()
        backend = MagicMock()
        ctx = _make_ctx()
        ctx.backend = backend
        engine._best_effort_cleanup(ctx)
        # Both the directory name AND the encrypted-archive name are
        # tried — we don't know which artefact landed.
        called = [c.args[0] for c in backend.delete_backup.call_args_list]
        assert ctx.backup_name in called
        assert f"{ctx.backup_name}.tar.wbenc" in called

    def test_mirror_destinations_each_get_cleaned(self) -> None:
        engine = _bare_engine()
        primary_backend = MagicMock()
        mirror_a = MagicMock()
        mirror_b = MagicMock()

        engine._get_backend = MagicMock(side_effect=[mirror_a, mirror_b])

        ctx = _make_ctx()
        ctx.backend = primary_backend
        ctx.profile.mirror_destinations = [MagicMock(), MagicMock()]

        engine._best_effort_cleanup(ctx)

        # Each of the three backends must have seen 2 delete attempts
        # (directory + archive).
        assert primary_backend.delete_backup.call_count == 2
        assert mirror_a.delete_backup.call_count == 2
        assert mirror_b.delete_backup.call_count == 2

    def test_mirror_backend_construction_failure_is_skipped(self) -> None:
        engine = _bare_engine()
        primary_backend = MagicMock()
        # First mirror's backend factory blows up (e.g. credentials gone);
        # cleanup must continue to the second one rather than aborting.
        good_backend = MagicMock()
        engine._get_backend = MagicMock(side_effect=[OSError("creds gone"), good_backend])

        ctx = _make_ctx()
        ctx.backend = primary_backend
        ctx.profile.mirror_destinations = [MagicMock(), MagicMock()]

        engine._best_effort_cleanup(ctx)

        # The skipped mirror produced no delete calls; the good one did.
        assert good_backend.delete_backup.call_count == 2

    def test_no_primary_backend_still_processes_mirrors(self) -> None:
        engine = _bare_engine()
        mirror_backend = MagicMock()
        engine._get_backend = MagicMock(return_value=mirror_backend)
        ctx = _make_ctx()
        ctx.backend = None  # primary not yet wired up
        ctx.profile.mirror_destinations = [MagicMock()]
        engine._best_effort_cleanup(ctx)
        assert mirror_backend.delete_backup.call_count == 2

    def test_individual_delete_failure_does_not_abort(self) -> None:
        # If the first delete blows up, the second one must still be
        # tried — that's what ``_try_delete`` guarantees and we want
        # the integration to behave the same way.
        engine = _bare_engine()
        backend = MagicMock()
        backend.delete_backup.side_effect = [OSError("boom"), None]
        ctx = _make_ctx()
        ctx.backend = backend
        engine._best_effort_cleanup(ctx)
        assert backend.delete_backup.call_count == 2


# ---------------------------------------------------------------------------
# _phase_orphan_scan
# ---------------------------------------------------------------------------


class TestPhaseOrphanScan:
    """Phase 0: clear orphaned backups on every reachable destination."""

    def _make_backend_with_orphans(self, orphans: list[dict]) -> MagicMock:
        backend = MagicMock()
        backend.list_orphan_backups = MagicMock(return_value=orphans)
        return backend

    def test_no_orphans_does_nothing(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans([])
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        backend.delete_backup.assert_not_called()

    def test_single_orphan_deleted_on_primary(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans(
            [{"name": "orphan_FULL_2026-01-01_000000", "size": 1234}]
        )
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        backend.delete_backup.assert_called_once_with("orphan_FULL_2026-01-01_000000")
        # Logger is called via ``self._log`` for visibility.
        engine._log.assert_called_once()

    def test_multiple_orphans_deleted(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        orphans = [{"name": f"orph_{i}", "size": 0} for i in range(5)]
        backend = self._make_backend_with_orphans(orphans)
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        assert backend.delete_backup.call_count == 5

    def test_legacy_backend_without_list_orphan_backups_skipped(self) -> None:
        # Use ``spec`` to make ``hasattr/getattr`` honest: the mock has
        # no ``list_orphan_backups`` attribute, so the function skips it.
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = MagicMock(spec=["delete_backup"])  # no list_orphan_backups
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        backend.delete_backup.assert_not_called()

    def test_object_lock_destination_skipped(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans([{"name": "x", "size": 1}])
        ctx = _make_ctx()
        ctx.backend = backend
        ctx.profile.storage.storage_type = StorageType.S3
        ctx.profile.storage.s3_object_lock = True
        engine._phase_orphan_scan(ctx)
        # The lock would refuse the delete anyway; the lifecycle rule
        # is the GC. We MUST NOT even call ``list_orphan_backups``.
        backend.list_orphan_backups.assert_not_called()
        backend.delete_backup.assert_not_called()

    def test_list_orphan_backups_failure_continues_to_next_dest(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        # Primary blows up listing; mirror still gets processed.
        primary = MagicMock()
        primary.list_orphan_backups = MagicMock(side_effect=OSError("S3 down"))
        mirror_backend = self._make_backend_with_orphans([{"name": "mirror_orphan", "size": 0}])
        engine._get_backend = MagicMock(return_value=mirror_backend)

        mirror_cfg = MagicMock()
        mirror_cfg.storage_type = StorageType.LOCAL
        mirror_cfg.s3_object_lock = False
        ctx = _make_ctx()
        ctx.backend = primary
        ctx.profile.mirror_destinations = [mirror_cfg]
        engine._phase_orphan_scan(ctx)
        primary.delete_backup.assert_not_called()
        mirror_backend.delete_backup.assert_called_once_with("mirror_orphan")

    def test_mirror_backend_construction_failure_skipped(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        primary = self._make_backend_with_orphans([])
        engine._get_backend = MagicMock(side_effect=OSError("no creds"))

        mirror_cfg = MagicMock()
        mirror_cfg.storage_type = StorageType.LOCAL
        mirror_cfg.s3_object_lock = False
        ctx = _make_ctx()
        ctx.backend = primary
        ctx.profile.mirror_destinations = [mirror_cfg]
        engine._phase_orphan_scan(ctx)
        # No crash, primary still examined (no orphans there).
        primary.list_orphan_backups.assert_called_once()

    def test_concurrent_removal_is_swallowed(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans([{"name": "racy", "size": 0}])
        backend.delete_backup.side_effect = FileNotFoundError("gone")
        ctx = _make_ctx()
        ctx.backend = backend
        # The function must NOT propagate FNF — another process removed
        # the orphan in the time between list and delete.
        engine._phase_orphan_scan(ctx)
        backend.delete_backup.assert_called_once()

    def test_per_orphan_delete_failure_does_not_stop_scan(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans(
            [{"name": "a", "size": 0}, {"name": "b", "size": 0}, {"name": "c", "size": 0}]
        )
        # Middle one fails; the others must still be deleted.
        backend.delete_backup.side_effect = [None, OSError("locked"), None]
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        assert backend.delete_backup.call_count == 3


# ---------------------------------------------------------------------------
# _phase_commit_primary
# ---------------------------------------------------------------------------


class TestPhaseCommitPrimary:
    """Phase 6.5: write/upload the primary destination's commit marker."""

    def test_missing_total_checksum_raises_runtime_error(self) -> None:
        engine = _bare_engine()
        ctx = _make_ctx()
        # Total checksum missing → the marker would have nothing to bind.
        ctx.integrity_manifest = {"files": {}}
        with pytest.raises(RuntimeError, match="total_checksum"):
            engine._phase_commit_primary(ctx)

    def test_local_destination_invokes_write_commit_marker(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        backup = tmp_path / "BLoic_FULL_xxx"
        backup.mkdir()
        ctx = _make_ctx()
        ctx.backup_path = backup

        with patch("src.core.phases.commit_marker.write_commit_marker") as mock_write:
            engine._phase_commit_primary(ctx)

        mock_write.assert_called_once()
        kwargs = mock_write.call_args.kwargs
        assert kwargs["backup_path"] == backup
        assert kwargs["manifest_sha256"] == "a" * 64
        assert kwargs["destination_label"] == "storage"

    def test_local_write_failure_propagates(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backup = tmp_path / "BLoic_FULL_yyy"
        backup.mkdir()
        ctx = _make_ctx()
        ctx.backup_path = backup

        with patch(
            "src.core.phases.commit_marker.write_commit_marker",
            side_effect=OSError("disk full"),
        ):
            with pytest.raises(OSError, match="disk full"):
                engine._phase_commit_primary(ctx)
        # The user-visible log must mention orphan handling so the
        # next-run cleanup is no surprise.
        msg = engine._log.call_args[0][0]
        assert "orphan" in msg.lower()

    def test_remote_destination_uploads_marker(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        backend = MagicMock()
        ctx = _make_ctx()
        ctx.backup_path = None
        ctx.backup_remote_name = "RemoteBk"
        ctx.backend = backend

        engine._phase_commit_primary(ctx)

        backend.upload_file.assert_called_once()
        args, kwargs = backend.upload_file.call_args
        # Path-positional argument layout matches the upload contract.
        assert args[1] == "RemoteBk.wbcommit"
        assert isinstance(args[0], BytesIO)
        assert kwargs.get("size", 0) > 0

    def test_remote_upload_failure_propagates(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("connection reset")
        ctx = _make_ctx()
        ctx.backup_path = None
        ctx.backup_remote_name = "RemoteBk"
        ctx.backend = backend
        with pytest.raises(OSError, match="connection reset"):
            engine._phase_commit_primary(ctx)

    def test_local_path_missing_falls_through_to_remote_branch(self, tmp_path: Path) -> None:
        # ``backup_path`` set but the directory was deleted between
        # write and commit — the function falls through to the remote
        # branch which itself short-circuits on missing remote name.
        engine = _bare_engine()
        engine._phase = MagicMock()
        ghost = tmp_path / "ghost"
        # Don't create — exists() is False.
        ctx = _make_ctx()
        ctx.backup_path = ghost
        ctx.backup_remote_name = ""
        ctx.backend = None
        # No exception raised, no marker written.
        engine._phase_commit_primary(ctx)


# ---------------------------------------------------------------------------
# _commit_mirror
# ---------------------------------------------------------------------------


class TestCommitMirror:
    """Per-mirror commit-marker write/upload after each verify."""

    def _mirror_cfg(self, *, remote: bool = False, dest: str = "") -> MagicMock:
        cfg = MagicMock()
        cfg.is_remote = MagicMock(return_value=remote)
        cfg.destination_path = dest
        return cfg

    def test_missing_total_checksum_raises(self) -> None:
        engine = _bare_engine()
        ctx = _make_ctx()
        ctx.integrity_manifest = {"files": {}}  # no total_checksum
        with pytest.raises(RuntimeError, match="total_checksum"):
            engine._commit_mirror(ctx, self._mirror_cfg(), 0, "Mirror 1", False)

    def test_remote_mirror_uploads_with_correct_label(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        backend = MagicMock()
        engine._get_backend = MagicMock(return_value=backend)

        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"

        engine._commit_mirror(
            ctx,
            self._mirror_cfg(remote=True),
            mirror_idx=0,
            mirror_name="Mirror 1",
            is_encrypted=False,
        )
        backend.upload_file.assert_called_once()
        # Remote-name pattern: ``<backup_name>.wbcommit`` (no encryption suffix).
        assert backend.upload_file.call_args.args[1] == "Bk_FULL.wbcommit"

    def test_remote_encrypted_mirror_uploads_with_archive_suffix(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        backend = MagicMock()
        engine._get_backend = MagicMock(return_value=backend)

        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"

        engine._commit_mirror(
            ctx,
            self._mirror_cfg(remote=True),
            mirror_idx=1,
            mirror_name="Mirror 2",
            is_encrypted=True,
        )
        # Encrypted artefact → ``.tar.wbenc.wbcommit``
        assert backend.upload_file.call_args.args[1] == "Bk_FULL.tar.wbenc.wbcommit"

    def test_remote_upload_failure_propagates(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("S3 unreachable")
        engine._get_backend = MagicMock(return_value=backend)
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        with pytest.raises(OSError, match="S3 unreachable"):
            engine._commit_mirror(ctx, self._mirror_cfg(remote=True), 0, "Mirror 1", False)

    def test_local_mirror_missing_artefact_raises(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        cfg = self._mirror_cfg(dest=str(tmp_path / "nowhere"))
        with pytest.raises(RuntimeError, match="artefact missing"):
            engine._commit_mirror(ctx, cfg, 0, "Mirror 1", False)

    def test_local_mirror_writes_marker_for_directory(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        artefact = tmp_path / "Bk_FULL"
        artefact.mkdir()
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        cfg = self._mirror_cfg(dest=str(tmp_path))
        with patch("src.core.phases.commit_marker.write_commit_marker") as mock_write:
            engine._commit_mirror(ctx, cfg, 0, "Mirror 1", False)
        mock_write.assert_called_once()
        # Mirror label must be ``mirror_<idx+1>`` so multiple mirrors
        # don't collide on a shared restore database.
        assert mock_write.call_args.kwargs["destination_label"] == "mirror_1"

    def test_local_mirror_writes_marker_for_encrypted_archive(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        artefact = tmp_path / "Bk_FULL.tar.wbenc"
        artefact.write_bytes(b"WBEC\x01")
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        cfg = self._mirror_cfg(dest=str(tmp_path))
        with patch("src.core.phases.commit_marker.write_commit_marker") as mock_write:
            engine._commit_mirror(ctx, cfg, 2, "Mirror 3", True)
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs["destination_label"] == "mirror_3"
        # The archive path (not the directory) is what gets the marker.
        assert mock_write.call_args.kwargs["backup_path"] == artefact

    def test_local_write_failure_propagates(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        artefact = tmp_path / "Bk_FULL"
        artefact.mkdir()
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        cfg = self._mirror_cfg(dest=str(tmp_path))
        with patch(
            "src.core.phases.commit_marker.write_commit_marker",
            side_effect=OSError("ENOSPC"),
        ):
            with pytest.raises(OSError, match="ENOSPC"):
                engine._commit_mirror(ctx, cfg, 0, "Mirror 1", False)


# ---------------------------------------------------------------------------
# Backup-type rollback (sentinel + apply)
# ---------------------------------------------------------------------------


class TestRollbackBackupTypeOnFailure:
    """The forced-FULL → original-type restore on pipeline failure."""

    def test_no_forced_full_does_nothing(self) -> None:
        engine = _bare_engine()
        ctx = MagicMock()
        ctx.forced_full = False
        # No mutation, no save_profile call.
        engine._rollback_backup_type_on_failure(ctx, BackupType.DIFFERENTIAL)
        ctx.config_manager.save_profile.assert_not_called()

    def test_already_at_original_type_does_nothing(self) -> None:
        engine = _bare_engine()
        ctx = MagicMock()
        ctx.forced_full = True
        ctx.profile.backup_type = BackupType.DIFFERENTIAL
        engine._rollback_backup_type_on_failure(ctx, BackupType.DIFFERENTIAL)
        ctx.config_manager.save_profile.assert_not_called()

    def test_restores_type_and_saves_profile(self) -> None:
        engine = _bare_engine()
        ctx = MagicMock()
        ctx.forced_full = True
        ctx.profile.backup_type = BackupType.FULL
        engine._rollback_backup_type_on_failure(ctx, BackupType.DIFFERENTIAL)
        assert ctx.profile.backup_type == BackupType.DIFFERENTIAL
        ctx.config_manager.save_profile.assert_called_once_with(ctx.profile)

    def test_save_failure_writes_sentinel(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "x.json")
        ctx = MagicMock()
        ctx.forced_full = True
        ctx.profile.id = "prof_xyz"
        ctx.profile.backup_type = BackupType.FULL
        ctx.config_manager.save_profile.side_effect = OSError("disk full")
        engine._rollback_backup_type_on_failure(ctx, BackupType.DIFFERENTIAL)
        sentinel = tmp_path / "prof_xyz.rollback"
        assert sentinel.exists()
        assert sentinel.read_text(encoding="utf-8") == BackupType.DIFFERENTIAL.value

    def test_double_failure_swallowed(self, tmp_path: Path) -> None:
        # Both ``save_profile`` AND the sentinel write fail. The
        # function must NOT propagate either — the run is already
        # failing for another reason.
        engine = _bare_engine()
        # Point manifest path at a parent that doesn't exist AND make
        # ``mkdir`` blow up to defeat the mkdir+write fallback. We
        # achieve that by patching ``Path.write_text`` only.
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "x.json")

        ctx = MagicMock()
        ctx.forced_full = True
        ctx.profile.id = "prof_zzz"
        ctx.profile.backup_type = BackupType.FULL
        ctx.config_manager.save_profile.side_effect = OSError("disk")

        with patch.object(Path, "write_text", side_effect=OSError("still no disk")):
            # No raise.
            engine._rollback_backup_type_on_failure(ctx, BackupType.DIFFERENTIAL)


class TestApplyPendingRollback:
    """Sentinel-driven rollback applied at the start of ``run_backup``."""

    def test_no_sentinel_does_nothing(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "manifest.json")
        profile = MagicMock()
        profile.id = "prof"
        profile.backup_type = BackupType.DIFFERENTIAL
        engine._apply_pending_rollback(profile)
        engine._config.save_profile.assert_not_called()

    def test_sentinel_applied_and_removed(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "x.json")
        sentinel = tmp_path / "prof.rollback"
        sentinel.write_text(BackupType.DIFFERENTIAL.value, encoding="utf-8")

        profile = MagicMock()
        profile.id = "prof"
        profile.backup_type = BackupType.FULL
        profile.name = "Profile X"
        engine._apply_pending_rollback(profile)
        assert profile.backup_type == BackupType.DIFFERENTIAL
        engine._config.save_profile.assert_called_once_with(profile)
        # Sentinel cleaned up so the rollback runs at most once.
        assert not sentinel.exists()

    def test_sentinel_already_at_target_just_removes_file(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "x.json")
        sentinel = tmp_path / "prof.rollback"
        sentinel.write_text(BackupType.DIFFERENTIAL.value, encoding="utf-8")
        profile = MagicMock()
        profile.id = "prof"
        profile.backup_type = BackupType.DIFFERENTIAL
        profile.name = "Profile X"
        engine._apply_pending_rollback(profile)
        # No save needed — already at target.
        engine._config.save_profile.assert_not_called()
        assert not sentinel.exists()

    def test_corrupted_sentinel_swallowed(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._config.get_manifest_path = MagicMock(return_value=tmp_path / "x.json")
        sentinel = tmp_path / "prof.rollback"
        sentinel.write_text("THIS_IS_NOT_A_VALID_BACKUP_TYPE", encoding="utf-8")
        profile = MagicMock()
        profile.id = "prof"
        profile.backup_type = BackupType.FULL
        profile.name = "Profile X"
        # Must not propagate — the sentinel is best-effort.
        engine._apply_pending_rollback(profile)
        # Profile untouched, sentinel left in place (we couldn't parse it
        # so we don't risk silently masking the issue with a delete).
        assert profile.backup_type == BackupType.FULL
