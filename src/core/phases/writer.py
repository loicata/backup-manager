"""Unified backup writer — dispatches to local or remote.

Encapsulates the local/remote decision so that BackupEngine
does not need to know the storage details for writing.
"""

import logging
from collections.abc import Callable
from pathlib import Path

from src.core.phases.base import PipelineContext
from src.core.phases.local_writer import write_flat
from src.core.phases.remote_writer import write_remote

logger = logging.getLogger(__name__)


def write_backup(
    ctx: PipelineContext,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Write backup to the configured destination.

    Dispatches to local flat copy or remote streaming
    based on storage configuration.

    Updates ctx.backup_path (local) or ctx.backup_remote_name (remote).

    Args:
        ctx: Pipeline context with profile, files, and backend populated.
        cancel_check: Callable that raises CancelledError if cancelled.
    """
    if ctx.profile.storage.is_remote():
        encrypt_pw = _get_encrypt_password(ctx)

        ctx.backup_remote_name = write_remote(
            ctx.files,
            ctx.backend,
            ctx.backup_name,
            encrypt_password=encrypt_pw,
            events=ctx.events,
            cancel_check=cancel_check,
        )
    else:
        dest = Path(ctx.profile.storage.destination_path)
        ctx.backup_path = write_flat(
            ctx.files,
            dest,
            ctx.backup_name,
            ctx.events,
        )


def _get_encrypt_password(ctx: PipelineContext) -> str:
    """Extract encryption password from context if applicable.

    Args:
        ctx: Pipeline context.

    Returns:
        Encryption password or empty string.
    """
    if (
        ctx.profile.encrypt_primary
        and ctx.profile.encryption.enabled
        and ctx.profile.encryption.stored_password
    ):
        return ctx.profile.encryption.stored_password
    return ""
