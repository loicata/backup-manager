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
    # Real profile name so sanitize_profile_name() works; chosen to match
    # the "Bk_..." backup_name below (orphan scan now filters by prefix).
    ctx.profile.name = "Bk"
    ctx.profile.storage.storage_type = StorageType.LOCAL
    ctx.profile.storage.s3_object_lock = False
    ctx.backup_name = "Bk_FULL_2026-05-08_120000"
    ctx.backup_path = None
    ctx.backup_remote_name = ""
    ctx.backend = None
    # Matches PipelineContext's real default. Without this the MagicMock
    # would yield a truthy child attribute, making _best_effort_cleanup
    # wrongly treat every backup as committed.
    ctx.primary_committed = False
    # Same rationale, mirror-side: match the real default (empty set) so
    # membership tests behave like the genuine PipelineContext field.
    ctx.mirrors_committed = set()
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

    def test_committed_primary_is_never_deleted(self) -> None:
        # Regression for the 15/05/2026 zero-backup-day: a post-commit
        # failure (mirror upload error, rotation error, user cancel) must
        # NOT destroy the already-committed, verified primary backup.
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = MagicMock()
        ctx = _make_ctx()
        ctx.backend = backend
        ctx.primary_committed = True

        engine._best_effort_cleanup(ctx)

        # The committed primary survives — not one delete attempt on it.
        backend.delete_backup.assert_not_called()

    def test_committed_primary_still_cleans_uncommitted_mirrors(self) -> None:
        # Keeping the committed primary must not leak the failed mirror's
        # uncommitted artefacts — those are still cleaned.
        engine = _bare_engine()
        engine._log = MagicMock()
        primary_backend = MagicMock()
        mirror_backend = MagicMock()
        engine._get_backend = MagicMock(return_value=mirror_backend)

        ctx = _make_ctx()
        ctx.backend = primary_backend
        ctx.primary_committed = True
        ctx.profile.mirror_destinations = [MagicMock()]

        engine._best_effort_cleanup(ctx)

        primary_backend.delete_backup.assert_not_called()
        # Mirror artefacts (directory + archive) are still reclaimed.
        assert mirror_backend.delete_backup.call_count == 2

    def test_uncommitted_primary_is_deleted(self) -> None:
        # The default (no commit yet) path is unchanged: an uncommitted
        # partial backup is reclaimed exactly as before.
        engine = _bare_engine()
        backend = MagicMock()
        ctx = _make_ctx()
        ctx.backend = backend
        ctx.primary_committed = False
        engine._best_effort_cleanup(ctx)
        assert backend.delete_backup.call_count == 2

    def test_committed_mirror_is_never_deleted(self) -> None:
        # Regression for the 2026-06-11 review finding: a failure or user
        # Cancel in a phase AFTER _phase_verify_mirrors (rotation) must
        # NOT destroy a mirror artefact whose .wbcommit was already
        # written — the committed-primary rule, applied per mirror.
        engine = _bare_engine()
        engine._log = MagicMock()
        engine._get_backend = MagicMock()

        ctx = _make_ctx()
        ctx.backend = MagicMock()
        ctx.primary_committed = True
        ctx.profile.mirror_destinations = [MagicMock()]
        ctx.mirrors_committed = {0}

        engine._best_effort_cleanup(ctx)

        # The committed mirror is skipped before its backend is even
        # built — zero delete attempts can reach it.
        engine._get_backend.assert_not_called()
        # And the skip is surfaced in the run log, like the primary's.
        logged = " ".join(str(c.args[0]) for c in engine._log.call_args_list)
        assert "mirror 1" in logged

    def test_committed_mirror_kept_while_uncommitted_mirror_cleaned(self) -> None:
        # Mixed outcome: mirror 1 committed (kept), mirror 2 not yet
        # committed when the run died (cleaned). Each mirror's fate is
        # decided independently by its own index.
        engine = _bare_engine()
        engine._log = MagicMock()
        uncommitted_backend = MagicMock()
        engine._get_backend = MagicMock(return_value=uncommitted_backend)

        ctx = _make_ctx()
        ctx.backend = MagicMock()
        ctx.primary_committed = True
        ctx.profile.mirror_destinations = [MagicMock(), MagicMock()]
        ctx.mirrors_committed = {0}

        engine._best_effort_cleanup(ctx)

        # Only mirror 2's backend was built and cleaned (dir + archive).
        assert engine._get_backend.call_count == 1
        assert uncommitted_backend.delete_backup.call_count == 2


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
            [{"name": "Bk_FULL_2026-01-01_000000", "size": 1234}]
        )
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        backend.delete_backup.assert_called_once_with("Bk_FULL_2026-01-01_000000")
        # Logger is called via ``self._log`` for visibility.
        engine._log.assert_called_once()

    def test_multiple_orphans_deleted(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        orphans = [{"name": f"Bk_orph_{i}", "size": 0} for i in range(5)]
        backend = self._make_backend_with_orphans(orphans)
        ctx = _make_ctx()
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        assert backend.delete_backup.call_count == 5

    def test_other_profiles_orphans_are_never_deleted(self) -> None:
        # Regression for the 18/05/2026 cross-profile deletion: an orphan
        # belonging to a DIFFERENT profile (different name prefix) — which
        # may be that profile's in-flight backup — must never be touched.
        engine = _bare_engine()
        engine._log = MagicMock()
        backend = self._make_backend_with_orphans(
            [
                {"name": "Bk_FULL_2026-01-01_000000", "size": 1},  # ours
                {"name": "OtherProfile_FULL_2026-01-01_000000", "size": 999},  # theirs
                {"name": "TestLoic_FULL_2026-05-18_205449", "size": 2_355_185_577},
            ]
        )
        ctx = _make_ctx()  # profile name "Bk"
        ctx.backend = backend
        engine._phase_orphan_scan(ctx)
        deleted = [c.args[0] for c in backend.delete_backup.call_args_list]
        assert deleted == ["Bk_FULL_2026-01-01_000000"]

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
        mirror_backend = self._make_backend_with_orphans([{"name": "Bk_mirror_orphan", "size": 0}])
        engine._get_backend = MagicMock(return_value=mirror_backend)

        mirror_cfg = MagicMock()
        mirror_cfg.storage_type = StorageType.LOCAL
        mirror_cfg.s3_object_lock = False
        ctx = _make_ctx()
        ctx.backend = primary
        ctx.profile.mirror_destinations = [mirror_cfg]
        engine._phase_orphan_scan(ctx)
        primary.delete_backup.assert_not_called()
        mirror_backend.delete_backup.assert_called_once_with("Bk_mirror_orphan")

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
        backend = self._make_backend_with_orphans([{"name": "Bk_racy", "size": 0}])
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
            [{"name": "Bk_a", "size": 0}, {"name": "Bk_b", "size": 0}, {"name": "Bk_c", "size": 0}]
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

    def test_local_success_sets_primary_committed(self, tmp_path: Path) -> None:
        # The flag that protects the primary from _best_effort_cleanup
        # must be set only AFTER the marker is written.
        engine = _bare_engine()
        engine._phase = MagicMock()
        backup = tmp_path / "BLoic_FULL_zzz"
        backup.mkdir()
        ctx = _make_ctx()
        ctx.backup_path = backup
        assert ctx.primary_committed is False
        with patch("src.core.phases.commit_marker.write_commit_marker"):
            engine._phase_commit_primary(ctx)
        assert ctx.primary_committed is True

    def test_remote_success_sets_primary_committed(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        ctx = _make_ctx()
        ctx.backup_path = None
        ctx.backup_remote_name = "RemoteBk"
        ctx.backend = MagicMock()
        engine._phase_commit_primary(ctx)
        assert ctx.primary_committed is True

    def test_local_failure_leaves_primary_uncommitted(self, tmp_path: Path) -> None:
        # If the marker write fails, the flag must stay False so cleanup
        # is still allowed to reclaim the (now orphaned) partial backup.
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backup = tmp_path / "BLoic_FULL_fail"
        backup.mkdir()
        ctx = _make_ctx()
        ctx.backup_path = backup
        with patch(
            "src.core.phases.commit_marker.write_commit_marker",
            side_effect=OSError("disk full"),
        ):
            with pytest.raises(OSError):
                engine._phase_commit_primary(ctx)
        assert ctx.primary_committed is False

    def test_remote_failure_leaves_primary_uncommitted(self) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("connection reset")
        ctx = _make_ctx()
        ctx.backup_path = None
        ctx.backup_remote_name = "RemoteBk"
        ctx.backend = backend
        with pytest.raises(OSError):
            engine._phase_commit_primary(ctx)
        assert ctx.primary_committed is False


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

    def test_remote_success_records_mirror_committed(self) -> None:
        # The index that protects this mirror from _best_effort_cleanup
        # must be recorded only AFTER the marker upload succeeded.
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._get_backend = MagicMock(return_value=MagicMock())
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        assert ctx.mirrors_committed == set()
        engine._commit_mirror(ctx, self._mirror_cfg(remote=True), 1, "Mirror 2", False)
        assert ctx.mirrors_committed == {1}

    def test_local_success_records_mirror_committed(self, tmp_path: Path) -> None:
        engine = _bare_engine()
        engine._phase = MagicMock()
        artefact = tmp_path / "Bk_FULL"
        artefact.mkdir()
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        cfg = self._mirror_cfg(dest=str(tmp_path))
        with patch("src.core.phases.commit_marker.write_commit_marker"):
            engine._commit_mirror(ctx, cfg, 0, "Mirror 1", False)
        assert ctx.mirrors_committed == {0}

    def test_remote_failure_does_not_record_committed(self) -> None:
        # A failed marker upload leaves the mirror unprotected so the
        # cleanup may reclaim the (orphaned) artefact.
        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._log = MagicMock()
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("S3 unreachable")
        engine._get_backend = MagicMock(return_value=backend)
        ctx = _make_ctx()
        ctx.backup_name = "Bk_FULL"
        with pytest.raises(OSError):
            engine._commit_mirror(ctx, self._mirror_cfg(remote=True), 0, "Mirror 1", False)
        assert ctx.mirrors_committed == set()

    def test_local_write_failure_does_not_record_committed(self, tmp_path: Path) -> None:
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
            with pytest.raises(OSError):
                engine._commit_mirror(ctx, cfg, 0, "Mirror 1", False)
        assert ctx.mirrors_committed == set()


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


# ---------------------------------------------------------------------------
# _verify_remote — stage-5 hole (empty remote listing committed as success)
# ---------------------------------------------------------------------------


def _remote_ctx(*, encrypted: bool, files: int = 1) -> MagicMock:
    """``_make_ctx`` tuned for the remote-verify path.

    ``encrypted`` toggles ``primary_is_encrypted(ctx.profile)`` by setting
    the three flags the predicate reads. ``files`` controls how many
    source files the run thought it was backing up (a real list, so
    ``len`` / truthiness behave).
    """
    ctx = _make_ctx()
    ctx.backup_remote_name = "RemoteBk"
    ctx.backend = MagicMock()
    ctx.files = [MagicMock() for _ in range(files)]
    ctx.profile.encrypt_primary = encrypted
    ctx.profile.encryption.enabled = encrypted
    ctx.profile.encryption.stored_password = "pw" if encrypted else ""
    return ctx


class TestVerifyRemote:
    """Phase 6 remote verification — the stage-5 silent-success guard."""

    def test_encrypted_archive_present_passes(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _remote_ctx(encrypted=True)
        ctx.backend.get_file_size.return_value = 4096

        engine._verify_remote(ctx)  # must not raise

        ctx.backend.get_file_size.assert_called_once_with("RemoteBk.tar.wbenc")
        # The plain file-listing path must never run for an encrypted primary.
        ctx.backend.list_backup_files.assert_not_called()

    def test_encrypted_archive_missing_raises(self) -> None:
        # get_file_size returns None for a missing object — the empty
        # upload that stage-5 used to commit as success.
        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _remote_ctx(encrypted=True)
        ctx.backend.get_file_size.return_value = None
        with pytest.raises(RuntimeError, match="missing or empty"):
            engine._verify_remote(ctx)

    def test_encrypted_archive_zero_bytes_raises(self) -> None:
        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _remote_ctx(encrypted=True)
        ctx.backend.get_file_size.return_value = 0
        with pytest.raises(RuntimeError, match="missing or empty"):
            engine._verify_remote(ctx)

    def test_empty_listing_with_files_raises(self) -> None:
        # The core stage-5 fix: a non-encrypted backup that uploaded
        # nothing (empty listing) while it had files to back up must FAIL,
        # not be silently skipped and committed as success.
        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _remote_ctx(encrypted=False, files=20)
        ctx.backend.verify_backup_files.return_value = []
        ctx.backend.list_backup_files.return_value = []
        with pytest.raises(RuntimeError, match="produced nothing"):
            engine._verify_remote(ctx)

    def test_empty_listing_no_files_skips(self) -> None:
        # No source files → an empty listing is genuinely fine.
        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _remote_ctx(encrypted=False, files=0)
        ctx.backend.verify_backup_files.return_value = []
        ctx.backend.list_backup_files.return_value = []
        engine._verify_remote(ctx)  # must not raise

    def test_checksums_dispatch_to_checksum_verifier(self) -> None:
        engine = _bare_engine()
        engine._verify_remote_checksums = MagicMock()
        ctx = _remote_ctx(encrypted=False, files=2)
        ctx.backend.verify_backup_files.return_value = [("a.txt", 10, "deadbeef")]
        engine._verify_remote(ctx)
        engine._verify_remote_checksums.assert_called_once()

    def test_nonempty_listing_dispatches_to_size_verifier(self) -> None:
        engine = _bare_engine()
        engine._verify_remote_sizes = MagicMock()
        ctx = _remote_ctx(encrypted=False, files=2)
        ctx.backend.verify_backup_files.return_value = []  # no checksums
        ctx.backend.list_backup_files.return_value = [("a.txt", 10), ("b.txt", 20)]
        engine._verify_remote(ctx)
        engine._verify_remote_sizes.assert_called_once()


# ---------------------------------------------------------------------------
# _phase_verify_mirrors — commit decoupled from verify
# ---------------------------------------------------------------------------


class TestVerifyMirrorsCommitDecoupled:
    """A mirror must get its .wbcommit after a successful upload regardless
    of auto_verify — otherwise the next run's orphan scan deletes it."""

    def _ctx_one_mirror(self, *, auto_verify: bool, upload_ok: bool = True):
        ctx = _make_ctx()
        cfg = MagicMock()
        cfg.is_remote = MagicMock(return_value=False)
        ctx.profile.mirror_destinations = [cfg]
        ctx.profile.verification.auto_verify = auto_verify
        ctx.profile.encryption.enabled = False
        ctx.profile.encrypt_mirror1 = False
        ctx.profile.encrypt_mirror2 = False
        ctx.result.mirror_results = [("Mirror 1", upload_ok, "OK", "desc")]
        return ctx

    def _engine(self):
        engine = _bare_engine()
        engine._log = MagicMock()
        engine._phase = MagicMock()
        engine._check_cancel = MagicMock()
        engine._get_backend = MagicMock(return_value=MagicMock())
        engine._commit_mirror = MagicMock()
        engine._verify_encrypted_archive = MagicMock()
        engine._verify_mirror_checksums = MagicMock()
        return engine

    def test_commit_happens_when_auto_verify_false(self) -> None:
        engine = self._engine()
        ctx = self._ctx_one_mirror(auto_verify=False)
        engine._phase_verify_mirrors(ctx)
        # The crown-jewel regression: marker written even with verify off.
        engine._commit_mirror.assert_called_once()
        # And no verification work ran.
        engine._verify_encrypted_archive.assert_not_called()
        engine._verify_mirror_checksums.assert_not_called()

    def test_failed_upload_not_committed(self) -> None:
        engine = self._engine()
        ctx = self._ctx_one_mirror(auto_verify=False, upload_ok=False)
        engine._phase_verify_mirrors(ctx)
        engine._commit_mirror.assert_not_called()

    def test_commit_happens_when_auto_verify_true_remote(self) -> None:
        engine = self._engine()
        ctx = self._ctx_one_mirror(auto_verify=True)
        ctx.profile.mirror_destinations[0].is_remote = MagicMock(return_value=True)
        backend = engine._get_backend.return_value
        backend.verify_backup_files.return_value = [("a.txt", 10, "deadbeef")]
        engine._phase_verify_mirrors(ctx)
        engine._verify_mirror_checksums.assert_called_once()
        engine._commit_mirror.assert_called_once()

    def test_no_mirrors_is_noop(self) -> None:
        engine = self._engine()
        ctx = _make_ctx()
        ctx.profile.mirror_destinations = []
        engine._phase_verify_mirrors(ctx)
        engine._commit_mirror.assert_not_called()


# ---------------------------------------------------------------------------
# _record_skipped_files — vanished files surface as warnings, not silence
# ---------------------------------------------------------------------------


class TestRecordSkippedFiles:
    """Files that vanished during a run must surface as WARNINGS (run still
    succeeds) and files_processed must reflect what was actually written."""

    def test_surfaces_warnings_and_corrects_count(self) -> None:
        from src.core.backup_result import BackupResult

        engine = _bare_engine()
        engine._log = MagicMock()
        ctx = _make_ctx()
        ctx.result = BackupResult()
        ctx.result.files_processed = 5  # set pre-write to len(ctx.files)
        ctx.integrity_manifest = {
            "files": {"a": {}, "b": {}},
            "skipped_files": [{"path": "c"}, {"path": "d"}, {"path": "e"}],
        }
        engine._record_skipped_files(ctx)
        assert ctx.result.warnings == 3
        # Corrected to the count actually written (manifest files).
        assert ctx.result.files_processed == 2
        # Warnings do NOT fail the run.
        assert ctx.result.success is True

    def test_noop_when_nothing_skipped(self) -> None:
        from src.core.backup_result import BackupResult

        engine = _bare_engine()
        ctx = _make_ctx()
        ctx.result = BackupResult()
        ctx.result.files_processed = 7
        ctx.integrity_manifest = {"files": {"a": {}}}
        engine._record_skipped_files(ctx)
        assert ctx.result.warnings == 0
        assert ctx.result.files_processed == 7  # untouched


# ---------------------------------------------------------------------------
# _phase_update_delta — vanished files excluded from the delta manifest (M01)
# ---------------------------------------------------------------------------


class TestDeltaManifestExcludesVanished:
    """A file that vanished mid-write must NOT be recorded as backed up in
    the differential reference manifest — otherwise an identical re-creation
    is skipped by every future differential and silently never backed up."""

    def test_skipped_files_excluded_from_delta_manifest(self, tmp_path) -> None:
        import json

        from src.core.config import BackupType
        from src.core.phases.collector import FileInfo

        src = tmp_path / "src"
        src.mkdir()
        (src / "keep.txt").write_text("keep", encoding="utf-8")
        (src / "vanished.txt").write_text("was here", encoding="utf-8")

        def fi(name: str) -> FileInfo:
            p = src / name
            return FileInfo(
                source_path=p,
                relative_path=name,
                size=p.stat().st_size,
                mtime=0.0,
                source_root=str(src),
            )

        engine = _bare_engine()
        engine._phase = MagicMock()
        engine._check_cancel = MagicMock()

        manifest_path = tmp_path / "manifest.json"
        ctx = _make_ctx()
        ctx.config_manager.get_manifest_path = MagicMock(return_value=manifest_path)
        ctx.profile.backup_type = BackupType.FULL
        ctx.forced_full = False
        ctx.all_files = [fi("keep.txt"), fi("vanished.txt")]
        # file_hashes still carries the vanished file (it was hashed in the
        # integrity phase, then vanished before the copy).
        ctx.file_hashes = {"keep.txt": "h_keep", "vanished.txt": "h_gone"}
        ctx.integrity_manifest = {
            "files": {"keep.txt": {"hash": "h_keep", "size": 4}},
            "total_checksum": "x",
            "skipped_files": [{"path": "vanished.txt", "reason": "vanished_during_write"}],
        }

        engine._phase_update_delta(ctx)

        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = saved.get("files", saved)  # tolerate either shape
        assert "keep.txt" in files
        assert "vanished.txt" not in files
