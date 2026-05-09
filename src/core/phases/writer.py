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
            # Encrypted tar still streams source files through a
            # hashing wrapper for the embedded ``.wbverify`` inside
            # the archive. The hashes it computes happen to match
            # ``ctx.integrity_manifest`` (both come from the same
            # source bytes); kept for backward compatibility with
            # the in-archive layout.
            archive_path, _ = write_encrypted_tar_with_hashes(
                ctx.files,
                dest,
                ctx.backup_name,
                encrypt_pw,
                ctx.events,
                cancel_check=cancel_check,
            )
            ctx.backup_path = archive_path
            return

        # Plain local mode: pure kernel ``shutil.copy2`` per file.
        # The manifest was already built by ``_phase_integrity``;
        # the writer just moves bytes.
        ctx.backup_path = write_flat(
            ctx.files,
            dest,
            ctx.backup_name,
            ctx.events,
            cancel_check=cancel_check,
        )
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
