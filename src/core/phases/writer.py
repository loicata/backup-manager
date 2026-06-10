"""Unified backup writer — dispatches to local or remote.

Encapsulates the local/remote decision so that BackupEngine does not
need to know the storage details for writing.

Pipeline contract (since v3.3.19): ``_phase_integrity`` ALWAYS runs
BEFORE this phase, populating ``ctx.integrity_manifest`` via parallel
source hashing in ``manifest.py``. The writer therefore only needs to
move bytes — for local plain mode that means a pure ``shutil.copy2``
loop (kernel-fast), for encrypted mode a streaming tar through
``EncryptingWriter``, for remote mode the existing protocol-specific
upload. None of those branches has to compute hashes itself any more,
which restores v3.3.14-class throughput on USB SSDs.
"""

import logging
from collections.abc import Callable
from pathlib import Path

from src.core.phases.base import PipelineContext
from src.core.phases.local_writer import (
    write_encrypted_tar_with_hashes,
    write_flat,
)
from src.core.phases.remote_writer import write_remote
from src.security.secure_memory import SecurePassword

logger = logging.getLogger(__name__)


def write_backup(
    ctx: PipelineContext,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Write backup to the configured destination.

    Dispatches to local flat copy, encrypted tar, or remote streaming
    based on storage configuration.

    Updates ``ctx.backup_path`` (local) or ``ctx.backup_remote_name``
    (remote). The integrity manifest is read from ``ctx.integrity_manifest``
    (built by ``_phase_integrity`` BEFORE this phase) — encrypted tar
    embeds it inside the archive, remote uploads it alongside, plain
    local writes it as a sidecar in ``_phase_save_manifest``.

    The encryption password is wrapped in a ``SecurePassword`` and
    zeroed after the write phase completes.

    Args:
        ctx: Pipeline context with profile, files, backend, and
            integrity_manifest populated.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    secure_pw = _get_encrypt_password(ctx)
    try:
        encrypt_pw = secure_pw.get() if secure_pw else ""

        if ctx.profile.storage.is_remote():
            ctx.backup_remote_name = write_remote(
                ctx.files,
                ctx.backend,
                ctx.backup_name,
                encrypt_password=encrypt_pw,
                events=ctx.events,
                cancel_check=cancel_check,
                integrity_manifest=ctx.integrity_manifest if encrypt_pw else None,
            )
            return

        dest = Path(ctx.profile.storage.destination_path)

        if encrypt_pw:
            # Encrypted tar streams source files through a hashing
            # wrapper for the embedded ``.wbverify``. The returned
            # ``file_hashes`` is the set of files ACTUALLY written: the
            # encrypted writer skips a source that vanished mid-write
            # (logs "File vanished, skipping"). Compare it to ``ctx.files``
            # and prune the manifest for any missing entry, otherwise a
            # partial archive would carry a manifest claiming files it
            # does not contain and still report "Verification OK".
            archive_path, file_hashes = write_encrypted_tar_with_hashes(
                ctx.files,
                dest,
                ctx.backup_name,
                encrypt_pw,
                ctx.events,
                cancel_check=cancel_check,
            )
            ctx.backup_path = archive_path
            written = set(file_hashes)
            skipped = {f.relative_path for f in ctx.files if f.relative_path not in written}
            if skipped:
                from src.core.phases.manifest import prune_manifest_entries

                prune_manifest_entries(ctx.integrity_manifest, skipped)
            return

        # Plain local mode: pure kernel ``shutil.copy2`` per file.
        # The manifest was already built by ``_phase_integrity``;
        # the writer just moves bytes. A vanished source is skipped (not
        # fatal) and recorded in ``skipped`` so the manifest is pruned to
        # match what actually landed on disk.
        skipped_flat: set[str] = set()
        ctx.backup_path = write_flat(
            ctx.files,
            dest,
            ctx.backup_name,
            ctx.events,
            cancel_check=cancel_check,
            skipped_out=skipped_flat,
        )
        if skipped_flat:
            from src.core.phases.manifest import prune_manifest_entries

            prune_manifest_entries(ctx.integrity_manifest, skipped_flat)
    finally:
        if secure_pw:
            secure_pw.clear()


def primary_is_encrypted(profile) -> bool:
    """True when the primary destination is written as an encrypted .tar.wbenc.

    Single source of truth for the primary-encryption decision, shared by
    the writer (which encrypts) and ``BackupEngine._verify_remote`` (which
    must size-check the single ``{name}.tar.wbenc`` object instead of
    listing files under ``{name}/``). Keep these two callers in sync: if
    the verify side ever disagrees with the write side, an encrypted
    remote primary gets verified as a plain file listing — which is always
    empty for an archive — and verification is silently skipped (the
    stage-5 "empty remote upload committed as success" data-loss bug).

    Args:
        profile: The BackupProfile being executed.

    Returns:
        True if the primary will be / was written as an encrypted archive.
    """
    return bool(
        profile.encrypt_primary
        and profile.encryption.enabled
        and profile.encryption.stored_password
    )


def _get_encrypt_password(ctx: PipelineContext) -> SecurePassword | None:
    """Extract encryption password from context if applicable.

    Returns a SecurePassword wrapper that the caller must clear
    after use, or None if encryption is not enabled.

    Args:
        ctx: Pipeline context.

    Returns:
        SecurePassword wrapping the password, or None.
    """
    if primary_is_encrypted(ctx.profile):
        return SecurePassword(ctx.profile.encryption.stored_password)
    return None
