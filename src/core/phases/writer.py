"""Unified backup writer — dispatches to local or remote.

Encapsulates the local/remote decision so that BackupEngine
does not need to know the storage details for writing.
"""

import logging
from collections.abc import Callable
from pathlib import Path

from src.core.phases.base import PipelineContext
from src.core.phases.local_writer import write_encrypted_tar, write_flat
from src.core.phases.remote_writer import write_remote
from src.security.secure_memory import SecurePassword

logger = logging.getLogger(__name__)


def write_backup(
    ctx: PipelineContext,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Write backup to the configured destination.

    Dispatches to local flat copy or remote streaming
    based on storage configuration.

    Updates ctx.backup_path (local) or ctx.backup_remote_name (remote).

    The encryption password is wrapped in a SecurePassword and
    zeroed after the write phase completes.

    Args:
        ctx: Pipeline context with profile, files, and backend populated.
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
        else:
            dest = Path(ctx.profile.storage.destination_path)

            if encrypt_pw:
                ctx.backup_path = write_encrypted_tar(
                    ctx.files,
                    dest,
                    ctx.backup_name,
                    encrypt_pw,
                    ctx.events,
                    integrity_manifest=ctx.integrity_manifest,
                )
            else:
                ctx.backup_path = write_flat(
                    ctx.files,
                    dest,
                    ctx.backup_name,
                    ctx.events,
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
