"""Centralized exception definitions for Backup Manager."""


class CancelledError(Exception):
    """Raised when a backup is cancelled by the user."""

    pass
