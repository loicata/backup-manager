"""Centralized exception definitions for Backup Manager."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.storage._fs_utils import Residual


class CancelledError(Exception):
    """Raised when a backup is cancelled by the user."""

    pass


class WriteError(Exception):
    """Raised when a file write or upload fails during backup.

    Args:
        file_path: The file that failed to write/upload.
        original: The underlying exception that caused the failure.
    """

    def __init__(self, file_path: str, original: Exception):
        self.file_path = file_path
        self.original = original
        super().__init__(f"Failed to write {file_path}: {original}")


class StorageDeleteError(Exception):
    """Raised when a storage backend cannot fully delete a backup.

    Carries the list of paths that survived the delete attempt so the
    caller (rotator, cleanup script) can surface actionable diagnostic
    instead of treating a partial delete as success.

    Args:
        target: Backup name (e.g. ``Profile_FULL_2026-04-20_100017``)
            whose deletion left residuals.
        residuals: List of ``Residual`` entries describing each path
            that could not be removed.
    """

    def __init__(self, target: str, residuals: list[Residual]):
        self.target = target
        self.residuals = residuals
        sample = ", ".join(r.path for r in residuals[:3])
        suffix = f" (+{len(residuals) - 3} more)" if len(residuals) > 3 else ""
        super().__init__(
            f"Could not fully delete {target}: {len(residuals)} residual(s). "
            f"First: {sample}{suffix}"
        )
