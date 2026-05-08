"""Unified backup writer — dispatches to local or remote.

Encapsulates the local/remote decision so that BackupEngine
does not need to know the storage details for writing.

For local destinations (plain or encrypted) the writer hashes each
source file in the same pass that copies/streams it, so the integrity
manifest is built from the bytes actually written and the
manifest→write TOCTOU is closed. ``ctx.file_hashes`` is filled with
``{relative_path: sha256_hex}`` for the pipeline's manifest phase to
consume directly (cache-only build, no second source read).

For remote destinations the legacy two-pass flow is preserved for now:
``ctx.integrity_manifest`` is expected to be pre-built by
``_phase_integrity`` BEFORE ``write_backup`` is called. The remote
TOCTOU window will be closed in a follow-up by hashing during upload.
"""

import logging
from collections.abc import Callable
from pathlib import Path

from src.core.phases.base import PipelineContext
from src.core.phases.local_writer import (
    write_encrypted_tar_with_hashes,
    write_flat_with_hashes,
)
from src.core.phases.remote_writer import write_remote
from src.security.secure_memory import SecurePassword

logger = logging.getLogger(__name__)


def write_backup(
    ctx: PipelineContext,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Write backup to the configured destination.

    Dispatches to local flat copy or remote streaming based on storage
    configuration.

    Updates ``ctx.backup_path`` (local) or ``ctx.backup_remote_name``
    (remote). For local destinations also fills ``ctx.file_hashes`` so
    the manifest phase can build ``ctx.integrity_manifest`` without
    re-reading the source.

    The encryption password is wrapped in a ``SecurePassword`` and
    zeroed after the write phase completes.

    Args:
        ctx: Pipeline context with profile, files, and backend populated.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    secure_pw = _get_encrypt_password(ctx)
    try:
        encrypt_pw = secure_pw.get() if secure_pw else ""

        if ctx.profile.storage.is_remote():
            # Remote upload still uses the legacy pre-built manifest
            # path. ``_run_pipeline`` runs ``_phase_integrity`` BEFORE
            # ``_phase_write`` for remote destinations, so
            # ``ctx.integrity_manifest`` is populated here.
            # TODO(phase-A.1): hash-during-upload for remote modes.
            ctx.backup_remote_name = write_remote(
                ctx.files,
                ctx.backend,
                ctx.backup_name,
                encrypt_password=encrypt_pw,
                events=ctx.events,
                cancel_check=cancel_check,
                integrity_manifest=ctx.integrity_manifest if encrypt_pw else None,
            )
        else:
            dest = Path(ctx.profile.storage.destination_path)

            if encrypt_pw:
                archive_path, file_hashes = write_encrypted_tar_with_hashes(
                    ctx.files,
                    dest,
                    ctx.backup_name,
                    encrypt_pw,
                    ctx.events,
                    cancel_check=cancel_check,
                )
                ctx.backup_path = archive_path
            else:
                backup_dir, file_hashes = write_flat_with_hashes(
                    ctx.files,
                    dest,
                    ctx.backup_name,
                    ctx.events,
                    cancel_check=cancel_check,
                )
                ctx.backup_path = backup_dir
            # Hand the hashes to the pipeline so ``_phase_integrity``
            # can build the manifest with zero source re-reads. This
            # is what makes the manifest hash-bound to the bytes that
            # actually landed on the destination.
            ctx.file_hashes = file_hashes
    finally:
        if secure_pw:
            secure_pw.clear()


def _get_encrypt_password(ctx: PipelineContext) -> SecurePassword | None:
    """Extract encryption password from context if applicable.

    Returns a SecurePassword wrapper that the caller must clear
    after use, or None if encryption is not enabled.

    Args:
        ctx: Pipeline context.

    Returns:
        SecurePassword wrapping the password, or None.
    """
    if (
        ctx.profile.encrypt_primary
        and ctx.profile.encryption.enabled
        and ctx.profile.encryption.stored_password
    ):
        return SecurePassword(ctx.profile.encryption.stored_password)
    return None
